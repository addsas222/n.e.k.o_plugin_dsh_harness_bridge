"""JSON-lines client for the Node sidecar (``sidecar/host.mjs``).

Kept free of SDK imports so it can be unit-tested with a real ``node``.

The sidecar owns a private event loop in a background thread. N.E.K.O runs
lifecycle hooks and entry triggers in *separate* ``asyncio.run()`` calls
(``plugin/core/host.py:1391`` vs ``:1804``), so a subprocess created during
``startup`` loses its reader task the moment that loop closes: the pid stays
alive and the pipes stay open, but nothing reads the replies any more. Every
later call then waits for an answer that cannot arrive. Owning the loop keeps
the connection independent of whichever loop happens to invoke a tool.
"""
from __future__ import annotations

import asyncio
import itertools
import json
import os
import shutil
import threading
from pathlib import Path
from typing import Any, Callable

SIDECAR_SCRIPT = Path(__file__).resolve().parent / "sidecar" / "host.mjs"
# Bundled with the DeepSeek Harness desktop runtime; used when no node is on PATH.
_DSH_BUNDLED_NODE = ("dsh-runtimes", "dsh-primary-runtime", "dependencies", "node")
# A single frame (tool result or load report) never needs more than this.
_STREAM_LIMIT = 8 * 1024 * 1024
# A reply too large for one frame arrives as ordered chunks; cap the reassembly so a buggy
# peer cannot make the plugin hold an unbounded string. Mirrors host.mjs's ceiling.
_MAX_CHUNKED_CHARS = 64_000_000
# Cross-loop hops get their own budget so the inner timeout reports the real reason.
_HOP_MARGIN_S = 5.0
_START_TIMEOUT_S = 60.0
_STOP_TIMEOUT_S = 10.0
_WORKER_THREAD_NAME = "dsh-sidecar"


class SidecarError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def resolve_dsh_home(configured: str = "") -> Path:
    value = (configured or os.environ.get("DSH_HOME") or "").strip()
    return Path(value).expanduser() if value else Path.home() / ".dsh"


def resolve_node(configured: str, dsh_home: Path) -> str | None:
    value = configured.strip()
    if value:
        # A configured path that does not exist would only surface later as a bare
        # "sidecar failed to start"; report "no node" now so the reason is nameable.
        return value if Path(value).is_file() else None
    found = shutil.which("node")
    if found:
        return found
    bundled = dsh_home.joinpath(*_DSH_BUNDLED_NODE)
    for name in ("node.exe", "node", "bin/node"):
        candidate = bundled / name
        if candidate.is_file():
            return str(candidate)
    return None


class _Connection:
    """One sidecar process plus its reader tasks. Every method runs on the owner loop."""

    def __init__(self, node: str, *, dsh_home: Path, on_stderr: Callable[[str], None] | None = None) -> None:
        self._node = node
        self._dsh_home = dsh_home
        self._on_stderr = on_stderr
        self._process: asyncio.subprocess.Process | None = None
        self._ids = itertools.count(1)
        self._pending: dict[int, asyncio.Future[Any]] = {}
        # Replies split across frames, keyed by request id then chunk index.
        self._chunks: dict[int, dict[int, str]] = {}
        self._tasks: list[asyncio.Task[None]] = []
        # The process can outlive its reader (an oversized frame used to kill the read loop),
        # leaving a live pid that answers nothing. Never report that as running.
        self._reader_dead = False

    @property
    def running(self) -> bool:
        return (
            self._process is not None
            and self._process.returncode is None
            and not self._reader_dead
        )

    async def start(self) -> None:
        self._reader_dead = False
        env = dict(os.environ)
        # Engines resolve their config mirrors from DSH_HOME.
        env["DSH_HOME"] = str(self._dsh_home)
        self._process = await asyncio.create_subprocess_exec(
            self._node,
            str(SIDECAR_SCRIPT),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            limit=_STREAM_LIMIT,
        )
        self._tasks = [
            asyncio.create_task(self._read_stdout()),
            asyncio.create_task(self._read_stderr()),
        ]

    async def request(self, op: str, timeout: float, **fields: Any) -> Any:
        if not self.running or self._process is None or self._process.stdin is None:
            raise SidecarError("SIDECAR_DOWN", "dsh_harness_bridge sidecar is not running")
        request_id = next(self._ids)
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        line = json.dumps({"id": request_id, "op": op, **fields}, ensure_ascii=False) + "\n"
        try:
            self._process.stdin.write(line.encode("utf-8"))
            await self._process.stdin.drain()
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError as exc:
            raise SidecarError("TIMEOUT", f"sidecar did not answer {op} within {timeout:.0f}s") from exc
        except (BrokenPipeError, ConnectionResetError) as exc:
            raise SidecarError("SIDECAR_DOWN", "dsh_harness_bridge sidecar exited") from exc
        finally:
            self._pending.pop(request_id, None)

    async def stop(self) -> None:
        process, self._process = self._process, None
        if process is not None and process.returncode is None:
            if process.stdin is not None:
                process.stdin.close()
            try:
                await asyncio.wait_for(process.wait(), 3.0)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        for task in self._tasks:
            task.cancel()
        self._tasks = []
        self._fail_pending("dsh_harness_bridge sidecar stopped")

    def _fail_pending(self, message: str) -> None:
        self._chunks.clear()
        for future in self._pending.values():
            if not future.done():
                future.set_exception(SidecarError("SIDECAR_DOWN", message))

    async def _read_stdout(self) -> None:
        assert self._process is not None and self._process.stdout is not None
        stream = self._process.stdout
        try:
            while True:
                try:
                    line = await stream.readline()
                except ValueError:
                    # One line exceeded the 8 MiB stream limit. StreamReader already dropped
                    # the buffer, so just carry on: letting this escape kills the reader and
                    # wedges every later request until a manual reload.
                    if self._on_stderr is not None:
                        self._on_stderr("dropped oversized sidecar frame (>8 MiB)")
                    continue
                if not line:
                    return
                try:
                    frame = json.loads(line)
                except ValueError:
                    continue
                future = self._pending.get(frame.get("id")) if isinstance(frame, dict) else None
                if future is None or future.done():
                    continue
                if isinstance(frame.get("chunk"), dict):
                    self._accept_chunk(future, frame.get("id"), frame["chunk"])
                    continue
                if frame.get("ok"):
                    future.set_result(frame.get("result"))
                else:
                    error = frame.get("error") or {}
                    future.set_exception(SidecarError(str(error.get("code", "TOOL_FAILED")), str(error.get("message", ""))))
        finally:
            self._reader_dead = True
            self._fail_pending("dsh_harness_bridge sidecar exited")

    def _accept_chunk(self, future: asyncio.Future[Any], frame_id: Any, chunk: dict[str, Any]) -> None:
        """Collect one frame of a split reply; the last index resolves the request.

        host.mjs slices the serialized JSON, so the pieces are joined in index order before
        parsing. Nothing resolves until every index has arrived, which is why the request
        budget has to cover the whole transfer rather than one frame.
        """
        index, total, data = chunk.get("i"), chunk.get("n"), chunk.get("data")
        if (
            not isinstance(index, int) or not isinstance(total, int)
            or not isinstance(data, str) or total < 1 or not 0 <= index < total
        ):
            self._pending.pop(frame_id, None)
            future.set_exception(SidecarError("BAD_FRAME", "malformed chunk frame"))
            return
        parts = self._chunks.setdefault(frame_id, {})
        parts[index] = data
        if len(parts) < total:
            if sum(len(part) for part in parts.values()) > _MAX_CHUNKED_CHARS:
                self._chunks.pop(frame_id, None)
                self._pending.pop(frame_id, None)
                future.set_exception(SidecarError("BAD_FRAME", "chunked reply exceeded the reassembly limit"))
            return
        self._chunks.pop(frame_id, None)
        try:
            payload = json.loads("".join(parts[position] for position in range(total)))
        except ValueError as exc:
            future.set_exception(SidecarError("BAD_FRAME", f"chunked reply is not valid JSON: {exc}"))
            return
        future.set_result(payload)

    async def _read_stderr(self) -> None:
        assert self._process is not None and self._process.stderr is not None
        stream = self._process.stderr
        while line := await stream.readline():
            if self._on_stderr is not None:
                self._on_stderr(line.decode("utf-8", "replace").rstrip())


class SidecarClient:
    """Loop-independent façade: callable from any event loop, safe to call repeatedly."""

    def __init__(self, node: str, *, dsh_home: Path, on_stderr: Callable[[str], None] | None = None) -> None:
        self._node = node
        self._dsh_home = dsh_home
        self._on_stderr = on_stderr
        self._connection: _Connection | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._guard = threading.Lock()

    @property
    def running(self) -> bool:
        connection = self._connection
        return connection is not None and connection.running

    async def start(self) -> None:
        if self.running:
            return
        try:
            await self._submit(self._connect(), timeout=_START_TIMEOUT_S)
        except BaseException:
            # A failure here must not leave a worker thread behind for the plugin's lifetime.
            self._shutdown_worker()
            raise

    async def stop(self) -> None:
        connection, self._connection = self._connection, None
        if connection is not None:
            try:
                await self._submit(connection.stop(), timeout=_STOP_TIMEOUT_S)
            except (SidecarError, asyncio.TimeoutError, OSError):
                pass  # Best effort: the worker loop is dropped either way.
        self._shutdown_worker()

    async def request(self, op: str, timeout: float, **fields: Any) -> Any:
        connection = self._connection
        if connection is None or not connection.running:
            raise SidecarError("SIDECAR_DOWN", "dsh_harness_bridge sidecar is not running")
        try:
            return await self._submit(connection.request(op, timeout, **fields), timeout=timeout + _HOP_MARGIN_S)
        except asyncio.TimeoutError as exc:
            raise SidecarError("TIMEOUT", f"sidecar did not answer {op} within {timeout:.0f}s") from exc

    async def _connect(self) -> None:
        connection = _Connection(self._node, dsh_home=self._dsh_home, on_stderr=self._on_stderr)
        await connection.start()
        self._connection = connection

    async def _submit(self, coroutine: Any, *, timeout: float) -> Any:
        loop = self._ensure_worker()
        if not loop.is_running():
            raise SidecarError("SIDECAR_DOWN", "dsh_harness_bridge sidecar worker loop is not running")
        future = asyncio.run_coroutine_threadsafe(coroutine, loop)
        try:
            return await asyncio.wait_for(asyncio.wrap_future(future), timeout)
        except BaseException:
            future.cancel()
            raise

    def _ensure_worker(self) -> asyncio.AbstractEventLoop:
        with self._guard:
            if self._loop is not None and self._thread is not None and self._thread.is_alive():
                return self._loop
            loop = asyncio.new_event_loop()
            thread = threading.Thread(
                target=self._run_worker, args=(loop,), name=_WORKER_THREAD_NAME, daemon=True,
            )
            self._loop, self._thread = loop, thread
            thread.start()
            return loop

    @staticmethod
    def _run_worker(loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        try:
            loop.run_forever()
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass

    def _shutdown_worker(self) -> None:
        with self._guard:
            loop, thread = self._loop, self._thread
            self._loop, self._thread = None, None
        if loop is None:
            return
        try:
            loop.call_soon_threadsafe(loop.stop)
        except RuntimeError:
            pass  # Already closed.
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=_STOP_TIMEOUT_S)
        if thread is None or not thread.is_alive():
            try:
                loop.close()
            except Exception:
                pass

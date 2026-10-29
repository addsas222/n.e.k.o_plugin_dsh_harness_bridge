"""
DSH Bridge plugin

Runs the agent half (``./engine``) of DeepSeek Harness (DSH) plugin packages
inside a Node sidecar and exposes the tools they register as NEKO LLM tools.

Architecture:
  NEKO chat ──(LLM tool call)──▶ this plugin ──(JSON lines / stdio)──▶ sidecar/host.mjs ──▶ DSH engine.apply(ctx)

The sidecar gives each engine a shim ctx with ``tools`` and ``systemPrompt``
only. Packages that need other DSH services (sessions, projections, commands,
the web server) are reported as incompatible instead of being half-mounted;
the ``status`` entry returns the per-package compatibility report.
"""
from __future__ import annotations

import asyncio
import json
import re
from typing import Any

from plugin.sdk.plugin import Err, NekoPluginBase, Ok, SdkError, lifecycle, neko_plugin, plugin_entry

from .sidecar_client import SidecarClient, SidecarError, resolve_dsh_home, resolve_node

# The real default lives in config.schema.json (and plugin.toml). This is only the fallback
# for "config could not be read", so it must not double as a recommendation.
_DEFAULT_PACKAGES = ""
_DEFAULT_PREFIX = "dsh_"
_LOAD_TIMEOUT_S = 30.0
_SECTIONS_TIMEOUT_S = 10.0
# Chat tool budget = engine budget + IPC margin, bounded by main_server's 300s cap.
_TOOL_TIMEOUT_SLACK_S = 5.0
_TOOL_TIMEOUT_CAP_S = 300.0
_DEFAULT_TOOL_TIMEOUT_S = 30.0
# register_llm_tool only queues the registration to the host; failures are logged by the host
# alone. Confirm against main_server, mirroring mcp_adapter's batching window.
_REMOTE_CONFIRM_TIMEOUT_S = 5.0
_REMOTE_CONFIRM_INTERVAL_S = 0.2
_REMOTE_TIMEOUT_S = 2.0


def parse_packages(value: Any) -> list[str]:
    items = value if isinstance(value, list) else str(value or "").split(",")
    seen: list[str] = []
    for item in items:
        text = str(item).strip()
        if text and text not in seen:
            seen.append(text)
    return seen


def chat_tool_name(prefix: str, tool_name: str, taken: set[str]) -> str | None:
    """Map a DSH tool name to a provider-portable, unique LLM tool name."""
    base = re.sub(r"[^A-Za-z0-9_\-]+", "_", f"{prefix}{tool_name}").strip("_-")[:60] or "dsh_tool"
    candidate, suffix = base, 2
    while candidate in taken:
        if suffix > 99:
            return None
        candidate = f"{base}_{suffix}"
        suffix += 1
    return candidate


def chat_timeout(timeout_ms: Any) -> float:
    """Engine ``timeoutMs`` → chat budget in seconds.

    DSH requires a positive finite number and treats omission as "no deadline"
    (packages/core/tools/src/contracts.ts); anything else is treated as omitted here too,
    so a bad value degrades to the default budget instead of a zero-length deadline.
    """
    try:
        seconds = float(timeout_ms) / 1000.0
    except (TypeError, ValueError):
        seconds = _DEFAULT_TOOL_TIMEOUT_S
    if not (seconds > 0):  # also catches NaN
        seconds = _DEFAULT_TOOL_TIMEOUT_S
    # The default gets the margin too: the sidecar waits 30s, so chat must wait longer.
    return min(seconds + _TOOL_TIMEOUT_SLACK_S, _TOOL_TIMEOUT_CAP_S)


def tool_reply(result: Any) -> dict[str, Any]:
    """Sidecar ``call`` result → main_server tool response (always wrapped in ``output``)."""
    if isinstance(result, dict):
        if result.get("truncated"):
            # The sidecar capped the frame; say so, or the model reasons over half a result.
            return {"output": f"[结果已截断 / result truncated]\n{result.get('text') or ''}"}
        text = result.get("text")
        if isinstance(text, str) and text.strip():
            return {"output": text}
        return {"output": result.get("value")}
    return {"output": result}


def render_instructions(sections: list[dict[str, Any]], packages: list[dict[str, Any]]) -> str:
    """Engine sections plus gated packages → one text block a model can actually read.

    N.E.K.O.'s SDK has no system-prompt hook (``plugin/sdk/`` has no such symbol), so an
    engine's guidance can only reach the model if the model asks for it. Keep the output
    self-describing: say where it came from, and say which packages are unusable so the
    model stops guessing at capabilities that were never loaded.
    """
    lines = ["[DSH engine instructions — these are NOT in your system prompt; they came from a tool call]"]
    grouped: dict[str, list[dict[str, Any]]] = {}
    for section in sections:
        if isinstance(section, dict):
            grouped.setdefault(str(section.get("package") or "unknown"), []).append(section)
    for package in sorted(grouped):
        lines.append(f"\n## {package}")
        for section in sorted(grouped[package], key=lambda item: item.get("order") or 0):
            text = str(section.get("text") or "").strip()
            if text:
                lines.append(f"### {section.get('name')}\n{text}")
    blocked = [item for item in packages if item.get("status") in {"incompatible", "not-found", "load-failed"}]
    if blocked:
        lines.append("\n## Packages that did not load")
        for item in blocked:
            reason = ", ".join(str(part) for part in (item.get("missing") or []))
            if not reason:
                reason = str((item.get("errors") or [item.get("status") or "unknown"])[0])
            lines.append(f"- {item.get('id')}: {reason}")
    return "\n".join(lines).strip()


@neko_plugin
class DshBridgePlugin(NekoPluginBase):
    def __init__(self, ctx: Any) -> None:
        super().__init__(ctx)
        self._client: SidecarClient | None = None
        self._report: list[dict[str, Any]] = []
        self._chat_tools: dict[str, str] = {}  # llm name -> dsh tool name
        self._meta_tools: set[str] = set()  # llm names this plugin added beyond engine tools
        self._unconfirmed: list[str] = []
        self._problem = ""
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # main_server introspection
    # ------------------------------------------------------------------
    async def _remote_tool_names(self) -> set[str]:
        """Tool names main_server actually holds, with the source that owns each.

        ``list_llm_tools`` only knows this plugin's local view, so it cannot see other
        plugins' names or registrations still queued.
        """
        import httpx
        from config import MAIN_SERVER_PORT

        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(_REMOTE_TIMEOUT_S, connect=1.0),
                proxy=None,
                trust_env=False,
            ) as client:
                resp = await client.get(f"http://127.0.0.1:{int(MAIN_SERVER_PORT)}/api/tools")
            if resp.status_code != 200:
                return set()
            body = resp.json()
        except Exception as exc:  # unreachable host must not block startup
            self.logger.warning("dsh_bridge: cannot query /api/tools: {}", exc)
            return set()
        names: set[str] = set()
        by_role = body.get("tools_by_role") if isinstance(body, dict) else None
        if isinstance(by_role, dict):
            for tools in by_role.values():
                if not isinstance(tools, list):
                    continue
                for tool in tools:
                    if isinstance(tool, dict) and isinstance(tool.get("name"), str):
                        names.add(str(tool["name"]))
        return names

    async def _confirm_chat_tools(self) -> None:
        """Drop local registrations main_server never accepted.

        ``register_llm_tool`` queues; the host logs failures privately. Anything absent from
        ``/api/tools`` within the window is rolled back and reported as unconfirmed, so
        ``status`` stops advertising tools the model cannot call.
        """
        if not self._published:
            self._unconfirmed = []
            return
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _REMOTE_CONFIRM_TIMEOUT_S
        pending = set(self._published)
        confirmed: set[str] = set()
        while pending and loop.time() < deadline:
            names = await self._remote_tool_names()
            confirmed |= {name for name in pending if name in names}
            pending = set(self._published) - confirmed
            if pending and loop.time() < deadline:
                await asyncio.sleep(min(_REMOTE_CONFIRM_INTERVAL_S, max(deadline - loop.time(), 0.0)))
        for name in sorted(set(self._published) - confirmed):
            try:
                self.unregister_llm_tool(name)
            except Exception as exc:
                self.logger.warning("Failed to roll back {}: {}", name, exc)
            self._chat_tools.pop(name, None)
            self._meta_tools.discard(name)
            self._unconfirmed.append(name)
        if self._unconfirmed:
            self.logger.warning(
                "DSH tools not confirmed by main_server ({}): {}",
                len(self._unconfirmed), self._unconfirmed,
            )

    @property
    def _published(self) -> set[str]:
        """Every name this plugin put into chat: engine tools plus the instructions reader."""
        return set(self._chat_tools) | set(self._meta_tools)

    @lifecycle(id="startup")
    async def startup(self, **_):
        async with self._lock:
            await self._start()
        return Ok(self._summary())

    @lifecycle(id="shutdown")
    async def shutdown(self, **_):
        async with self._lock:
            await self._stop()
        return Ok({"status": "stopped"})

    @plugin_entry(
        id="status",
        name="Compatibility report",
        description="Per-package DSH compatibility report: loaded tools, missing ctx services, degraded features.",
        llm_result_fields=["running", "packages"],
    )
    async def status(self, **_):
        return Ok(self._summary())

    @plugin_entry(id="reload", name="Reload", description="Restart the sidecar and reload the configured DSH packages.")
    async def reload(self, **_):
        async with self._lock:
            await self._stop()
            await self._start()
        return Ok(self._summary())

    @plugin_entry(
        id="call_tool",
        name="Call DSH tool",
        description="Call a loaded DSH tool by its DSH name with a JSON object of arguments (for testing).",
    )
    async def call_tool(self, tool: str, arguments: str = "{}", **_):
        try:
            args = json.loads(arguments or "{}")
        except ValueError:
            return Err(SdkError("arguments must be a JSON object"))
        if not isinstance(args, dict):
            return Err(SdkError("arguments must be a JSON object"))
        try:
            result = await self._call(tool, args)
        except SidecarError as exc:
            return Err(SdkError(f"{exc.code}: {exc}"))
        return Ok(result)

    @plugin_entry(
        id="prompt_sections",
        name="Prompt sections",
        description="Render the system-prompt sections the loaded DSH engines contribute (NEKO does not inject them).",
        llm_result_fields=["sections"],
    )
    async def prompt_sections(self, **_):
        client = self._client
        if client is None or not client.running:
            return Err(SdkError("dsh_bridge sidecar is not running"))
        try:
            result = await client.request("sections", _SECTIONS_TIMEOUT_S)
        except SidecarError as exc:
            return Err(SdkError(f"{exc.code}: {exc}"))
        return Ok({"sections": result.get("sections", []) if isinstance(result, dict) else []})

    async def _settings(self) -> dict[str, Any]:
        try:
            cfg = await self.config.dump(timeout=5.0)
        except Exception as exc:
            self.logger.warning("Failed to load config, using defaults: {}", exc)
            cfg = {}
        section = cfg.get("dsh_bridge") if isinstance(cfg, dict) else None
        return section if isinstance(section, dict) else {}

    async def _start(self) -> None:
        settings = await self._settings()
        dsh_home = resolve_dsh_home(str(settings.get("dsh_home") or ""))
        packages = parse_packages(settings.get("packages", _DEFAULT_PACKAGES))
        prefix = str(settings.get("tool_prefix", _DEFAULT_PREFIX) or "")
        inject = settings.get("inject_to_chat", True) is not False
        self._report, self._problem, self._unconfirmed = [], "", []
        self._meta_tools = set()
        if not packages:
            self.logger.warning("dsh_bridge: no DSH packages configured; add them under [dsh_bridge] in the plugin config")

        node = resolve_node(str(settings.get("node_path") or ""), dsh_home)
        if node is None:
            self._problem = (
                "node not found: install Node.js, or set dsh_bridge.node_path to an existing "
                "node executable (a configured path is used only if the file exists)"
            )
            self.logger.warning(self._problem)
            return
        client = SidecarClient(node, dsh_home=dsh_home, on_stderr=lambda line: self.logger.debug("[sidecar] {}", line))
        try:
            await client.start()
            loaded = await client.request("load", _LOAD_TIMEOUT_S, dshHome=str(dsh_home), packages=packages)
        except (OSError, SidecarError) as exc:
            await client.stop()
            self._problem = f"sidecar failed to start: {exc}"
            self.logger.warning(self._problem)
            return
        self._client = client
        self._report = loaded.get("packages", []) if isinstance(loaded, dict) else []
        if inject:
            # Names held by other plugins are only visible through main_server.
            foreign = await self._remote_tool_names()
        else:
            foreign = set()
        for package in self._report:
            self.logger.info(
                "DSH package {}: {} (tools={}, missing={})",
                package.get("id"), package.get("status"),
                [t.get("name") for t in package.get("tools", [])], package.get("missing", []),
            )
            if inject:
                for tool in package.get("tools", []):
                    self._inject_tool(prefix, tool, foreign)
        if inject:
            self._inject_instructions(prefix, foreign)
            await self._confirm_chat_tools()

    def _inject_tool(self, prefix: str, tool: dict[str, Any], foreign: set[str] | None = None) -> None:
        dsh_name = str(tool.get("name") or "")
        taken = set(self._chat_tools) | {item["name"] for item in self.list_llm_tools()} | (foreign or set())
        llm_name = chat_tool_name(prefix, dsh_name, taken)
        if not dsh_name or llm_name is None:
            return
        parameters = self._portable_parameters(dsh_name, tool.get("parameters"))
        try:
            registered = self.register_llm_tool(
                name=llm_name,
                description=str(tool.get("description") or f"DeepSeek Harness tool '{dsh_name}'."),
                parameters=parameters,
                handler=self._chat_handler(dsh_name),
                timeout=chat_timeout(tool.get("timeoutMs")),
            )
        except Exception as exc:
            self.logger.warning("Failed to inject DSH tool {} as {}: {}", dsh_name, llm_name, exc)
            return
        if registered is False:
            self.logger.warning("Host refused DSH tool {} as {}", dsh_name, llm_name)
            return
        self._chat_tools[llm_name] = dsh_name

    def _inject_instructions(self, prefix: str, foreign: set[str] | None = None) -> None:
        """Publish the engine sections as a tool.

        A plain ``@plugin_entry`` never reaches ``/api/tools`` (only ``__llm_tool__``
        registrations do), and the SDK offers no system-prompt hook, so a tool the model can
        call is the only path from an engine's written guidance to the conversation.
        """
        if not self._report:
            return
        taken = set(self._published) | {item["name"] for item in self.list_llm_tools()} | (foreign or set())
        llm_name = chat_tool_name(prefix, "bridge_instructions", taken)
        if llm_name is None:
            return
        try:
            registered = self.register_llm_tool(
                name=llm_name,
                description=(
                    "Read the behaviour rules the loaded DSH engines contribute (their system-prompt "
                    "sections), plus which packages failed to load. These rules are not in your system "
                    "prompt; call this before relying on DSH conventions."
                ),
                parameters={"type": "object", "properties": {}, "additionalProperties": False},
                handler=self._instructions_handler(),
                timeout=chat_timeout(None),
            )
        except Exception as exc:
            self.logger.warning("Failed to publish DSH instructions as {}: {}", llm_name, exc)
            return
        if registered is False:
            self.logger.warning("Host refused DSH instructions as {}", llm_name)
            return
        self._meta_tools.add(llm_name)

    def _instructions_handler(self) -> Any:
        async def handler(**_: Any) -> dict[str, Any]:
            try:
                return {"output": await self._instructions()}
            except SidecarError as exc:
                return {"output": {"error": str(exc)}, "is_error": True, "error": exc.code}

        return handler

    async def _instructions(self) -> str:
        client = self._client
        if client is None or not client.running:
            raise SidecarError("SIDECAR_DOWN", "dsh_bridge sidecar is not running")
        result = await client.request("sections", _SECTIONS_TIMEOUT_S)
        sections = result.get("sections", []) if isinstance(result, dict) else []
        return render_instructions(sections, self._report)

    def _portable_parameters(self, dsh_name: str, parameters: Any) -> dict[str, Any]:
        """Engine schema → something every chat provider can represent.

        DSH compiles its own author-facing DSL to JSON Schema at ``defineTool`` time, so a
        package's raw ``parameters`` is not guaranteed to be portable. Reuse mcp_adapter's
        converter; degrade to an empty object schema rather than dropping the tool.
        """
        empty = {"type": "object", "properties": {}}
        if not isinstance(parameters, dict):
            return empty
        try:
            from plugin.plugins.mcp_adapter.chat_schema import portable_chat_schema

            return portable_chat_schema(parameters)
        except Exception as exc:
            self.logger.warning("DSH tool {}: unportable parameter schema ({}); registered without parameters", dsh_name, exc)
            return empty

    def _chat_handler(self, dsh_name: str):
        async def handler(**kwargs: Any) -> dict[str, Any]:
            args = {key: value for key, value in kwargs.items() if not key.startswith("_")}
            try:
                return tool_reply(await self._call(dsh_name, args))
            except SidecarError as exc:
                # Engine errors are already short and key-free (fairy diagnostics redact).
                return {"output": {"tool": dsh_name, "error": str(exc)}, "is_error": True, "error": exc.code}

        return handler

    async def _call(self, dsh_name: str, args: dict[str, Any]) -> Any:
        client = self._client
        if client is None or not client.running:
            raise SidecarError("SIDECAR_DOWN", "dsh_bridge sidecar is not running; use the reload entry")
        budget = chat_timeout(self._tool_timeout_ms(dsh_name))
        return await client.request("call", budget, tool=dsh_name, args=args)

    def _tool_timeout_ms(self, dsh_name: str) -> Any:
        for package in self._report:
            for tool in package.get("tools", []):
                if tool.get("name") == dsh_name:
                    return tool.get("timeoutMs")
        return None

    async def _stop(self) -> None:
        for llm_name in sorted(self._published):
            try:
                self.unregister_llm_tool(llm_name)
            except Exception as exc:
                self.logger.warning("Failed to unregister {}: {}", llm_name, exc)
            # The SDK only guarantees local cleanup; drop it from main_server too, or a
            # reload leaves a dead callback behind and the next start renames to _2.
            await self._remote_unregister(llm_name)
        self._chat_tools.clear()
        self._meta_tools.clear()
        self._unconfirmed = []
        client, self._client = self._client, None
        if client is not None:
            await client.stop()

    async def _remote_unregister(self, llm_name: str) -> None:
        """Remove one tool from main_server, scoped to this plugin's source.

        ``expected_source`` is mandatory: without it a same-named tool owned by another
        plugin could be deleted. Failures are logged, never raised — stopping must finish.
        """
        import httpx
        from config import MAIN_SERVER_PORT

        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(_REMOTE_TIMEOUT_S, connect=1.0),
                proxy=None,
                trust_env=False,
            ) as client:
                await client.post(
                    f"http://127.0.0.1:{int(MAIN_SERVER_PORT)}/api/tools/unregister",
                    json={"name": llm_name, "role": None, "expected_source": f"plugin:{self.plugin_id}"},
                )
        except Exception as exc:
            self.logger.warning("dsh_bridge: remote unregister {} failed: {}", llm_name, exc)

    def _summary(self) -> dict[str, Any]:
        return {
            "running": self._client is not None and self._client.running,
            "problem": self._problem,
            "chat_tools": dict(self._chat_tools),
            "meta_tools": sorted(self._meta_tools),
            "unconfirmed": list(self._unconfirmed),
            "packages": self._report,
        }

#!/usr/bin/env python3
import asyncio
import json
import os
import re
import sys
import random
from collections import Counter
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple, Union

import httpx
import typer
from dotenv import load_dotenv
from httpx_sse import EventSource
from prompt_toolkit import PromptSession, print_formatted_text
from prompt_toolkit.completion import Completer, WordCompleter, FuzzyCompleter
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.styles import Style
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from rich.console import Console
from rich.live import Live
from rich.markdown import Markdown
from rich.markup import escape
from rich.panel import Panel
from rich.rule import Rule
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from rich.theme import Theme

# Load environment explicitly so /reload can refresh it
load_dotenv()

# Define a professional, eye-soothing Next-Gen AI theme (Catppuccin/One Dark inspired)
custom_theme = Theme({
    "markdown.strong": "bold #FFFFFF",
    "markdown.emph": "italic #FFFFFF",
    "markdown.code": "#F9E2AF",        # Peach for inline variables
    # "markdown.code_block": "#B4BEFE",  # Lavender tint for code blocks (handled by Syntax theme)
    "panel.border": "#6C7086",         # Dim Gray borders
})


class ForgeAIConfig(BaseSettings):
    base_url: str = Field(
        default="http://localhost:8001",
        validation_alias=AliasChoices("forgeai_base_url", "base_url")
    )
    bearer_token: str = ""
    github_token: str = ""
    jira_token: str = ""
    yeedu_token: str = ""
    forgeai_token: str = ""

    mode: str = "project"
    project_id: str = "1"
    stage: str = "stage_1_pipeline_specification"
    workstream_id: str = "1"
    conversation_id: str = ""
    model_name: str = ""
    timeout: int = 180

    jira_ticket: str = ""
    jira_username: str = ""
    jira_url: str = ""
    github_username: str = ""
    github_repo: str = ""
    github_branch: str = "master"

    databricks_auth_type: str = ""
    databricks_workspace_url: str = ""
    databricks_client_id: str = ""
    databricks_client_secret: str = ""
    databricks_pat_token: str = ""
    databricks_cluster_id: str = ""
    databricks_compute_type: str = "all_purpose_compute"

    user_id: str = "sudhnashu.sakhala@modak.com"

    compact: bool = False

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def headers(self) -> Dict[str, str]:
        # Critical for streaming: Force uncompressed, unbuffered connection
        h = {
            "Accept": "text/event-stream",
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Accept-Encoding": "identity"
        }

        def _is_real(val: str, prefix: str = "your_") -> bool:
            return bool(val) and not val.startswith(prefix)

        if _is_real(self.bearer_token):
            h["Authorization"] = f"Bearer {self.bearer_token}"
        if _is_real(self.github_token, "ghp_your"):
            h["X-GitHub-Token"] = self.github_token
        if _is_real(self.jira_token):
            h["X-Jira-Token"] = self.jira_token
        if _is_real(self.yeedu_token):
            h["X-Yeedu-Token"] = self.yeedu_token
        if _is_real(self.forgeai_token):
            h["X-ForgeAI-Token"] = self.forgeai_token

        if self.databricks_auth_type:
            h["X-Databricks-Auth-Type"] = self.databricks_auth_type
        if self.databricks_workspace_url:
            h["X-Databricks-Workspace-URL"] = self.databricks_workspace_url
        if self.databricks_auth_type == "spn":
            if self.databricks_client_id:
                h["X-Databricks-Client-ID"] = self.databricks_client_id
            if self.databricks_client_secret:
                h["X-Databricks-Client-Secret"] = self.databricks_client_secret
        elif self.databricks_auth_type == "pat":
            if self.databricks_pat_token:
                h["X-Databricks-PAT-Token"] = self.databricks_pat_token
        if self.user_id:
            h["X-User-Id"] = self.user_id
        return h

    @property
    def chat_url(self) -> str:
        if self.mode == "project":
            return f"{self.base_url}/project/{self.project_id}/conversation/chat?stage={self.stage}"
        if self.mode == "explore":
            return f"{self.base_url}/data-explorer/conversation/{self.conversation_id}/chat"
        return f"{self.base_url}/workstream/{self.workstream_id}/conversation/{self.conversation_id}/chat"

    def context_label(self) -> str:
        if self.mode == "project":
            return f"project: {self.project_id} / {self.stage}"
        conv = self.conversation_id[:8] + "…" if len(
            self.conversation_id) > 8 else (self.conversation_id or "new")
        if self.mode == "explore":
            return f"explore: {conv}"
        return f"ws: {self.workstream_id} / {conv}"

    def build_payload(self, message: str) -> Dict[str, Any]:
        ctx = {}
        if self.github_username and self.github_repo:
            ctx["github"] = {
                "username": self.github_username,
                "repositories": [{"owner": self.github_username, "repo": self.github_repo, "branch": self.github_branch}]
            }
        if self.jira_url and self.jira_username:
            ctx["jira"] = {"jira_ticket": self.jira_ticket,
                           "jira_username": self.jira_username, "jira_url": self.jira_url}
        if self.databricks_workspace_url:
            ctx["databricks"] = {"cluster_id": self.databricks_cluster_id,
                                 "compute_type": self.databricks_compute_type}

        return {"message": message, "model_name": self.model_name or None, "context": ctx if ctx else None}


class AsyncForgeAIClient:
    def __init__(self, config: ForgeAIConfig):
        self.config = config
        self.client = httpx.AsyncClient(timeout=float(config.timeout))

    async def close(self):
        await self.client.aclose()

    def refresh_client(self):
        self.client = httpx.AsyncClient(timeout=float(self.config.timeout))

    async def chat_stream(self, message: str) -> AsyncGenerator[Dict[str, Any], None]:
        payload = self.config.build_payload(message)
        async with self.client.stream("POST", self.config.chat_url, json=payload, headers=self.config.headers) as response:
            ctype = response.headers.get("content-type", "")
            if "text/event-stream" not in ctype.lower():
                # Server returned a non-streaming response (typically a JSON
                # error like a missing required header). Read the body and
                # surface it via HTTPStatusError so the existing handler can
                # render the actual server message instead of a cryptic SSE
                # content-type mismatch.
                await response.aread()
                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError:
                    raise
                # 2xx but not SSE: still an unexpected contract; expose body.
                raise httpx.HTTPStatusError(
                    f"Expected SSE stream, got '{ctype or 'unknown'}': {response.text}",
                    request=response.request,
                    response=response,
                )
            event_source = EventSource(response)
            async for event in event_source.aiter_sse():
                if event.data:
                    try:
                        yield json.loads(event.data)
                    except json.JSONDecodeError:
                        yield {"raw": event.data}

    async def request(self, method: str, path: str, **kwargs) -> Tuple[bool, Any]:
        try:
            url = path if path.startswith(
                "http") else f"{self.config.base_url}{path}"
            resp = await self.client.request(method, url, headers=self.config.headers, **kwargs)
            resp.raise_for_status()
            return True, resp.json() if resp.content else {}
        except httpx.HTTPStatusError as e:
            try:
                return False, e.response.json().get("message", e.response.text)
            except Exception:
                return False, f"HTTP {e.response.status_code}"
        except httpx.ConnectError:
            return False, f"Connection failed - ForgeAI Server unreachable at {self.config.base_url}"
        except Exception as e:
            return False, str(e)

    async def health(self, timeout: float = None) -> Tuple[bool, Dict]:
        kwargs = {"timeout": timeout} if timeout else {}
        ok, data = await self.request("GET", "/health", **kwargs)
        return ok, (data if ok else {"error": data})

    async def models(self) -> List[str]:
        ok, data = await self.request("GET", "/models")
        return data.get("models", []) if ok else []

    async def create_conversation(self) -> Tuple[bool, str]:
        if self.config.mode == "explore":
            ok, data = await self.request("POST", "/data-explorer/conversation/start", json={})
        else:
            ok, data = await self.request("POST", f"/workstream/{self.config.workstream_id}/conversation/start")
        return (True, data.get("conversation_id", "")) if ok else (False, str(data))

    async def list_conversations(self) -> Tuple[bool, Union[List[Dict], str]]:
        if self.config.mode == "explore":
            ok, data = await self.request("GET", "/data-explorer/conversations")
        else:
            ok, data = await self.request("GET", f"/workstream/{self.config.workstream_id}/conversations")
        if ok:
            return True, data.get("conversations", [])
        return False, str(data)

    async def get_history(self, limit: int = 50) -> Tuple[bool, Union[List[Dict], str], Optional[Dict]]:
        if self.config.mode == "project":
            url = f"/project/{self.config.project_id}/conversation/history?stage={self.config.stage}&limit={limit}"
        elif self.config.mode == "explore":
            url = f"/data-explorer/conversation/{self.config.conversation_id}/history?limit={limit}"
        else:
            url = f"/workstream/{self.config.workstream_id}/conversation/{self.config.conversation_id}/history?limit={limit}"
        ok, data = await self.request("GET", url)
        if ok:
            return True, data.get("messages", []), data.get("context_estimate")
        return False, str(data), None

    async def delete_conversation(self, cid: str = None) -> Tuple[bool, str]:
        if self.config.mode == "project":
            url = f"/project/{self.config.project_id}/conversation?stage={self.config.stage}"
        elif self.config.mode == "explore":
            target = cid or self.config.conversation_id
            url = f"/data-explorer/conversation/{target}"
        else:
            target = cid or self.config.conversation_id
            url = f"/workstream/{self.config.workstream_id}/conversation/{target}"
        ok, data = await self.request("DELETE", url)
        return ok, data.get("status", str(data)) if ok else str(data)

    async def rename_conversation(self, title: str) -> Tuple[bool, str]:
        if self.config.mode == "explore":
            url = f"/data-explorer/conversation/{self.config.conversation_id}/rename"
        else:
            url = f"/workstream/{self.config.workstream_id}/conversation/{self.config.conversation_id}/rename"
        ok, data = await self.request("POST", url, json={"title": title})
        return ok, data.get("title", str(data)) if ok else str(data)

    async def branch_conversation(self, message_id: str) -> Tuple[bool, Union[Dict, str]]:
        if self.config.mode == "explore":
            url = f"/data-explorer/conversation/{self.config.conversation_id}/branch"
        else:
            url = f"/workstream/{self.config.workstream_id}/conversation/{self.config.conversation_id}/branch"
        ok, data = await self.request("POST", url, json={"message_id": message_id})
        if ok:
            return True, data
        return False, str(data)

    async def explore_execute_sql(
        self,
        source_id: int,
        sql: str,
        row_limit: int = 1000,
        query_timeout_seconds: int = 30,
    ) -> Tuple[bool, Any]:
        payload = {
            "source_id": source_id,
            "sql": sql,
            "row_limit": row_limit,
            "query_timeout_seconds": query_timeout_seconds,
        }
        return await self.request("POST", "/data-explorer/execute_sql", json=payload)


class SlashCommandCompleter(Completer):
    """Custom completer that provides fuzzy matching ONLY for the first word starting with '/'"""

    def __init__(self, commands: List[str]):
        self.fuzzy = FuzzyCompleter(WordCompleter(commands, ignore_case=True))

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        if " " in text:
            return
        if not text.startswith("/"):
            return
        yield from self.fuzzy.get_completions(document, complete_event)


class TerminalUI:
    COMMANDS = [
        "/switch", "/workstream", "/explore", "/stage", "/list", "/open", "/new", "/history", "/delete", "/rename",
        "/branch", "/models", "/model", "/export", "/load", "/compact", "/toolview", "/last", "/sql",
        "/status", "/databricks", "/health", "/clear", "/reload", "/help", "/exit"
    ]

    STAGE_PRESETS = [
        "stage_1_pipeline_specification",
        "stage_2_pipeline_creation_and_execution",
        "stage_3_test_cases_and_validation"
    ]

    EXIT_COMMANDS = {"/exit", "/quit", "exit", "quit", "q", ":q", "bye"}

    def __init__(self, config: ForgeAIConfig):
        self.config = config
        self.api = AsyncForgeAIClient(config)
        self.console = Console(theme=custom_theme)

        self._conv_cache: List[Dict] = []
        self._history_cache: List[Dict] = []
        self._model_cache: List[str] = []
        self._last_response: str = ""
        self._conv_title: str = ""
        self._context_estimate: Dict[str, Any] = {}
        # Last SQL captured from a forgeai_execute_query tool call,
        # used by the /sql command for edit-and-rerun without the LLM.
        self._last_sql: Optional[Dict[str, Any]] = None

        # CRITICAL FIX: "noreverse bg:default" utterly annihilates the ugly white bar
        # that normally stretches across the screen padding on the bottom toolbar.
        pt_style = Style.from_dict({
            'bottom-toolbar': 'noreverse bg:default fg:default',
        })

        self.session = PromptSession(
            history=InMemoryHistory(),
            bottom_toolbar=self._get_bottom_toolbar,
            style=pt_style
        )
        self.completer = SlashCommandCompleter(self.COMMANDS)

        self.server_online: bool = False
        self._ping_task: Optional[asyncio.Task] = None

    def _get_bottom_toolbar(self):
        """Dynamic bottom toolbar showing live context window health."""
        if not self._context_estimate:
            return ""

        pct = self._context_estimate.get("percent_used", 0)
        tokens = self._context_estimate.get("total_input_tokens", 0)
        limit = self._context_estimate.get("model_context_limit", 0)
        imminent = self._context_estimate.get("compaction_imminent", False)

        # Transparent background (native terminal color) with floating colored text
        if imminent:
            # Danger: Coral Red text
            return HTML(f"<style fg='#F38BA8'>  ⚠ Context: {pct}% ({tokens:,} / {limit:,} tokens) — Compaction Imminent!  </style>")
        elif pct > 75:
            # Warning: Warm Amber text
            return HTML(f"<style fg='#FAB387'>  ⚡ Context: {pct}% ({tokens:,} / {limit:,} tokens)  </style>")
        else:
            # Safe: Lavender text
            return HTML(f"<style fg='#B4BEFE'>  ❖ Context: {pct}% ({tokens:,} / {limit:,} tokens)  </style>")

    async def _sync_history(self):
        self.session.history = InMemoryHistory()

        if self.config.mode in ("workstream", "explore") and not self.config.conversation_id:
            return

        ok, msgs, context = await self.api.get_history(limit=50)
        if ok:
            # Unconditionally set state to prevent token leakage from older chats
            self._context_estimate = context or {}

            if msgs:
                for m in msgs:
                    if m.get("role", "").lower() == "user":
                        content = m.get("content_for_llm") or m.get(
                            "content", "")
                        if content:
                            self.session.history.append_string(content)

    async def _ping_loop(self):
        while True:
            try:
                ok, _ = await self.api.health(timeout=3.0)
                if ok and not self.server_online:
                    self.server_online = True
                    print_formatted_text(
                        HTML("\n<style fg='#A6E3A1'><b>● ForgeAI Server is now Online</b></style>"))
                elif not ok and self.server_online:
                    self.server_online = False
                    print_formatted_text(
                        HTML("\n<style fg='#F38BA8'><b>○ ForgeAI Server went Offline</b></style>"))
            except Exception:
                pass
            await asyncio.sleep(10)

    async def _auto_conv(self) -> bool:
        if self.config.mode in ("workstream", "explore") and not self.config.conversation_id:
            ok, cid = await self.api.create_conversation()
            if ok:
                self.config.conversation_id = cid
                self.server_online = True
                self._conv_title = ""
                await self._sync_history()
                return True
            self.console.print(
                f"[bold #F38BA8]Failed to create conversation: {cid}[/]")
            return False
        return True

    def render_banner(self):
        os.system('cls' if os.name == 'nt' else 'clear')

        title_part = f' "{self._conv_title}"' if self._conv_title else ""
        status_indicator = "[bold #A6E3A1]● Online[/]" if self.server_online else "[bold #F38BA8]○ Offline[/]"

        banner_content = (
            f"[bold]ForgeAI Assistant[/]\n"
            f"{status_indicator}  "
            f"[dim]Ctx:[/] {self.config.context_label()}{title_part}  [dim]•[/]  "
            f"[dim]Server:[/] {self.config.base_url}  [dim]•[/]  "
            f"[dim]Model:[/] {self.config.model_name or 'default'}"
        )
        self.console.print(
            Panel.fit(banner_content, border_style="#B4BEFE", padding=(0, 2)))
        self.console.print()

    async def _get_multiline_input(self) -> str:
        lines = []
        self.console.print(
            "[dim italic]... (multiline mode active, type \"\"\" on a new line to end)[/]")
        while True:
            try:
                with patch_stdout():
                    line = await self.session.prompt_async(HTML("<style fg='#B4BEFE'>... </style>"))
                if line.strip() == '"""':
                    break
                lines.append(line)
            except (EOFError, KeyboardInterrupt):
                break
        return "\n".join(lines)

    async def run(self):
        ok, _ = await self.api.health()
        self.server_online = ok

        self._ping_task = asyncio.create_task(self._ping_loop())
        self.render_banner()
        if not await self._auto_conv():
            return

        await self._sync_history()

        while True:
            try:
                p_color = "#A6E3A1" if self.server_online else "#F38BA8"
                p_icon = "●" if self.server_online else "○"

                prompt_html = HTML(
                    f"<style fg='{p_color}'>╭─ {p_icon} {self.config.context_label()}</style>\n"
                    f"<style fg='{p_color}'>╰─❯</style> "
                )

                with patch_stdout():
                    user_input = await self.session.prompt_async(prompt_html, completer=self.completer)

                user_input = user_input.strip()

                if not user_input:
                    continue

                if user_input.lower() in self.EXIT_COMMANDS:
                    break

                if user_input == '"""' or user_input.startswith('"""'):
                    if user_input == '"""':
                        user_input = await self._get_multiline_input()
                    else:
                        rest = user_input[3:]
                        if rest.endswith('"""'):
                            user_input = rest[:-3]
                        else:
                            user_input = rest + "\n" + await self._get_multiline_input()
                    if not user_input.strip():
                        continue

                if '\n' in user_input:
                    self.session.history.append_string(user_input)

                if user_input.startswith("/"):
                    await self.handle_command(user_input)
                    self.console.print()
                    continue

                if not await self._auto_conv():
                    continue

                await self.stream_response(user_input)
                self.console.print()

            except (KeyboardInterrupt, EOFError):
                break
            except Exception as e:
                self.console.print(
                    f"\n[bold #F38BA8]Unexpected Error:[/] {escape(str(e))}")

        if self._ping_task:
            self._ping_task.cancel()
        await self.api.close()
        self.console.print("[dim]Goodbye.[/]")

    def _relative_time(self, iso: str) -> str:
        if not iso:
            return "unknown"
        try:
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
            now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now()
            delta = now - dt
            secs = int(delta.total_seconds())
            if secs < 0:
                return "just now"
            if secs < 60:
                return "just now"
            if secs < 3600:
                return f"{secs // 60}m ago"
            if secs < 86400:
                return f"{secs // 3600}h ago"
            return f"{secs // 86400}d ago"
        except Exception:
            return iso.split("T")[0] if "T" in iso else iso[:10]

    def safe_markdown(self, text: str):
        try:
            # 'one-dark' or 'dracula' provide gorgeous, standard IDE highlighting
            return Markdown(text, code_theme="one-dark")
        except Exception:
            return Text(text)

    def _format_tools_summary(self, tools: List) -> str:
        """Deduplicate tool list and show counts: 'tool_a ×3, tool_b ×1'."""
        # tools_used can be List[List[str]] (iteration-grouped) or
        # legacy List[str]; flatten before counting.
        flat = []
        for item in tools:
            if isinstance(item, list):
                flat.extend(item)
            else:
                flat.append(item)
        counts = Counter(flat)
        parts = []
        for name, count in counts.items():
            if count > 1:
                parts.append(f"{name} ×{count}")
            else:
                parts.append(name)
        return ", ".join(parts)

    def _render_content_with_tools(self, content: str):
        """Parse content string, render text as markdown and embedded tool_execution JSON blocks as panels."""
        # Regex to find ```json ... ``` fenced code blocks
        pattern = re.compile(r"```json\s*\n(.*?)\n```", re.DOTALL)

        last_end = 0
        for match in pattern.finditer(content):
            # Render any text before this code block
            text_before = content[last_end:match.start()].strip()
            if text_before:
                self.console.print(self.safe_markdown(text_before))
                self.console.print()

            json_str = match.group(1)
            last_end = match.end()

            # Try to parse as tool_execution block
            try:
                block = json.loads(json_str)
            except json.JSONDecodeError:
                # Not valid JSON — render as highlighted code block
                self.console.print(
                    Syntax(json_str, "json", theme="monokai", word_wrap=True))
                self.console.print()
                continue

            if isinstance(block, dict) and block.get("type") == "tool_execution":
                tools_list = block.get("tools", [])
                for tool_entry in tools_list:
                    tool_name = tool_entry.get("tool", "Unknown")
                    success = tool_entry.get("success", True)
                    params = tool_entry.get("params", {})
                    duration_ms = tool_entry.get("duration_ms", 0)
                    tool_result = tool_entry.get("tool_result", "")
                    tool_desc = tool_entry.get("tool_description", "")

                    color = "#A6E3A1" if success else "#F38BA8"
                    icon = "✔" if success else "✖"

                    # Params panel
                    if params:
                        params_str = json.dumps(
                            params, indent=2, ensure_ascii=False)
                        params_renderable = Syntax(
                            params_str, "json", theme="monokai", word_wrap=True)
                        self.console.print(Panel(
                            params_renderable,
                            title=f"⚙ Params: [bold #FAB387]{tool_name}[/]",
                            subtitle=f"[dim]{tool_desc}[/]" if tool_desc else None,
                            border_style="#6C7086",
                            padding=(0, 1)
                        ))

                    # Result panel
                    if tool_result is not None:
                        if isinstance(tool_result, (dict, list)):
                            result_str = json.dumps(
                                tool_result, indent=2, ensure_ascii=False)
                        else:
                            result_str = str(tool_result)

                        if len(result_str) > 2000:
                            result_str = result_str[:2000] + \
                                "\n... (truncated for display)"

                        # Detect if result looks like JSON
                        result_stripped = result_str.strip()
                        if result_stripped.startswith(("{", "[")):
                            try:
                                json.loads(result_stripped)
                                content_renderable = Syntax(
                                    result_str, "json", theme="monokai", word_wrap=True)
                            except json.JSONDecodeError:
                                content_renderable = Text(
                                    result_str, style="dim")
                        else:
                            content_renderable = Text(result_str, style="dim")

                        self.console.print(Panel(
                            content_renderable,
                            title=f"[{color}]{icon}[/] [bold #FAB387]{tool_name}[/] [bold #94E2D5]{duration_ms}ms[/]",
                            border_style="#6C7086",
                            padding=(0, 1)
                        ))
                    else:
                        self.console.print(
                            f"[{color}]{icon}[/] {tool_name} [bold #94E2D5]{duration_ms}ms[/]")

                    self.console.print()
            else:
                # Valid JSON but not a tool_execution — render as syntax
                formatted = json.dumps(block, indent=2, ensure_ascii=False)
                self.console.print(
                    Syntax(formatted, "json", theme="monokai", word_wrap=True))
                self.console.print()

        # Render any trailing text after the last code block
        trailing = content[last_end:].strip()
        if trailing:
            self.console.print(self.safe_markdown(trailing))

    def _print_token_summary(self, event: Dict) -> None:
        rt = event.get("request_token_usage", {})
        ct = event.get("chat_cumulative_token_usage", {})
        calls = event.get("model_call_count", 1)

        if not rt:
            return

        inp = rt.get("input_tokens", 0)
        out = rt.get("output_tokens", 0)
        cache_r = rt.get("cache_read_tokens", 0)

        parts = []
        if inp or out:
            parts.append(f"tokens: {inp:,} in / {out:,} out")
        if cache_r:
            parts.append(f"{cache_r:,} cached")
        if calls:
            parts.append(
                f"{calls} model call{'s' if calls != 1 else ''} this turn")

        if ct:
            c_total = ct.get("total_tokens", 0)
            c_reqs = ct.get("request_count", 0)
            if c_total:
                parts.append(
                    f"conversation: {c_total:,} tokens / {c_reqs} turn{'s' if c_reqs != 1 else ''}")

        if parts:
            summary = "  ·  ".join(parts)
            self.console.print(f"  [dim]{summary}[/]")

    def _finalize_live_text(self, live_text, current_md: str):
        """End the raw-text streaming phase.

        When live_text is True (streaming was active), print a trailing
        newline to close the raw-streamed block, then render the full
        response as styled Markdown below a thin separator.  This gives
        the user both instant raw output AND a clean formatted copy.
        """
        if live_text:
            # Close the raw-streamed block with a newline
            self.console.file.write("\n")
            self.console.file.flush()

    async def stream_response(self, message: str):
        self._last_response = ""
        current_md = ""
        active_tool = None
        tool_args_buffer = ""

        live_text = None
        live_tool = None
        status_spinner = None

        def safe_stop_spinner():
            nonlocal status_spinner
            if status_spinner:
                try:
                    status_spinner.stop()
                except Exception:
                    pass
                status_spinner = None

        def stop_all_live():
            """Ensure no Rich Live display is active before starting a new one."""
            nonlocal live_text, live_tool, current_md
            if live_text:
                self._finalize_live_text(live_text, current_md)
                live_text = None
                current_md = ""
            if live_tool:
                try:
                    live_tool.stop()
                except Exception:
                    pass
                live_tool = None
            safe_stop_spinner()

        # Print the Rich Divider for the incoming Assistant output instantly
        self.console.print(
            Rule("❖ ASSISTANT", style="#B4BEFE", align="center"))

        # Start the clean, random spinner
        spinner_text = random.choice(["❖ Waiting for response...", "❖ ..."])
        status_spinner = self.console.status(
            f"[bold #B4BEFE]{spinner_text}[/]", spinner="dots")
        status_spinner.start()

        try:
            async for event in self.api.chat_stream(message):
                etype = event.get("type")

                # Track pre-flight context window status
                if etype == "context_window_status":
                    if est := event.get("estimate"):
                        self._context_estimate = est
                    continue

                # Store message_id from guaranteed first event
                if etype == "message_start":
                    continue

                # Heartbeat keepalive during blocking operations (summarization, handoff)
                if etype == "heartbeat":
                    msg = event.get("message", "Processing")
                    elapsed = event.get("elapsed_seconds", 0)
                    label = f"{msg} ({elapsed}s)" if elapsed else msg
                    if status_spinner:
                        status_spinner.update(
                            f"[dim #B4BEFE]✧ {label}...[/]")
                    elif not live_text and not live_tool:
                        status_spinner = self.console.status(
                            f"[dim #B4BEFE]✧ {label}...[/]", spinner="dots")
                        status_spinner.start()
                    continue

                # Interim token tracking (silent — authoritative totals come in complete)
                if etype == "token_usage":
                    continue

                # content_start signals text generation beginning — no UI action needed
                if etype == "content_start":
                    continue

                if status_spinner and etype in ["text_delta", "content", "tool_start", "error", "complete"]:
                    safe_stop_spinner()

                if etype in ["text_delta", "content"]:
                    if live_tool:
                        try:
                            live_tool.stop()
                        except Exception:
                            pass
                        live_tool = None

                    # Raw-text streaming: write each token directly to
                    # stdout.  This avoids Rich's Live widget which
                    # re-renders the ENTIRE accumulated markdown on every
                    # token — causing scroll-back duplication when the
                    # content exceeds the terminal height.
                    if not live_text:
                        live_text = True  # sentinel: streaming is active

                    text = event.get("text", "")
                    if etype == "content":
                        text += event.get("content", "")
                    current_md += text
                    self._last_response = current_md
                    # Write the delta directly — one token, one write, no
                    # re-rendering, no scrollback issues.
                    self.console.file.write(text)
                    self.console.file.flush()

                elif etype == "tool_start":
                    if live_text:
                        # Finalize streamed text before switching to tool display
                        self._finalize_live_text(live_text, current_md)
                        live_text = None
                        current_md = ""

                    # Safely handle null or empty tool names
                    active_tool = event.get("tool_name") or "Unknown"
                    tool_args_buffer = ""

                    # Stop any existing live_tool before starting a fresh one
                    if live_tool:
                        try:
                            live_tool.stop()
                        except Exception:
                            pass
                        live_tool = None

                    live_tool = Live(console=self.console,
                                     auto_refresh=False,
                                     transient=True)
                    live_tool.start()

                elif etype == "tool_input_delta":
                    tool_args_buffer += event.get("partial_json", "")

                    display_tool = active_tool or event.get(
                        "tool_name") or "Unknown"

                    if not self.config.compact:
                        in_flight_args = Syntax(
                            tool_args_buffer, "json", theme="monokai", word_wrap=True)
                        tool_panel = Panel(
                            in_flight_args,
                            title=f"⚙ Params: [bold #FAB387]{display_tool}[/]",
                            border_style="#6C7086",
                            padding=(0, 1)
                        )
                        if live_tool:
                            live_tool.update(tool_panel, refresh=True)

                elif etype == "tool_executing":
                    # Stop ALL live displays before printing and starting spinner
                    if live_tool:
                        try:
                            live_tool.stop()
                        except Exception:
                            pass
                        live_tool = None
                    if live_text:
                        self._finalize_live_text(live_text, current_md)
                        live_text = None
                        current_md = ""
                    safe_stop_spinner()

                    active_tool = event.get("tool_name", active_tool or "tool")
                    args = event.get("tool_args", {})

                    # Capture SQL queries so the user can rerun them via /sql.
                    if (
                        active_tool == "forgeai_execute_query"
                        and isinstance(args, dict)
                        and args.get("source_id")
                        and args.get("sql")
                    ):
                        self._last_sql = {
                            "source_id": int(args["source_id"]),
                            "sql": str(args["sql"]),
                            "row_limit": int(args.get("row_limit") or 1000),
                            "query_timeout_seconds": int(args.get("query_timeout_seconds") or 30),
                        }

                    if not self.config.compact:
                        args_str = json.dumps(
                            args, indent=2) if args else "(no parameters)"
                        exec_syntax = Syntax(
                            args_str, "json", theme="monokai", word_wrap=True)

                        # This cleanly takes the place of the vanished streaming block
                        tool_panel = Panel(
                            exec_syntax,
                            title=f"⚙ Params: [bold #FAB387]{active_tool}[/]",
                            border_style="#6C7086",
                            padding=(0, 1)
                        )
                        self.console.print(tool_panel)

                    status_spinner = self.console.status(
                        f"[dim #B4BEFE]✧ Running {active_tool}...[/]", spinner="dots")
                    status_spinner.start()

                elif etype == "tool_result":
                    safe_stop_spinner()

                    success = event.get("success", True)
                    content = event.get("content")
                    dur = event.get("duration_ms", 0)

                    color = "#A6E3A1" if success else "#F38BA8"
                    icon = "✔" if success else "✖"

                    if self.config.compact:
                        self.console.print(
                            f"[{color}]{icon}[/] {active_tool} [bold #94E2D5]{dur}ms[/]")
                    else:
                        if content is None:
                            content_renderable = Text(
                                "(no content returned)", style="dim")
                        elif isinstance(content, (dict, list)):
                            content_str = json.dumps(
                                content, indent=2, ensure_ascii=False)
                            if len(content_str) > 2000:
                                content_str = content_str[:2000] + \
                                    "\n... (truncated for display)"
                            content_renderable = Syntax(
                                content_str, "json", theme="monokai", word_wrap=True)
                        else:
                            content_str = str(content)
                            if len(content_str) > 2000:
                                content_str = content_str[:2000] + \
                                    "\n... (truncated for display)"
                            content_renderable = Text(content_str, style="dim")

                        self.console.print(Panel(
                            content_renderable,
                            title=f"[{color}]{icon}[/] [bold #FAB387]{active_tool}[/] [bold #94E2D5]{dur}ms[/]",
                            border_style="#6C7086",
                            padding=(0, 1)
                        ))

                    if not self.config.compact:
                        self.console.print()

                elif etype == "continuing_after_tool":
                    stop_all_live()
                    status_spinner = self.console.status(
                        "[dim #B4BEFE]✧ Synthesizing...[/]", spinner="dots")
                    status_spinner.start()

                elif etype == "status":
                    if status_spinner:
                        status_spinner.update(
                            f"[dim #B4BEFE]✧ {event.get('message', 'Processing...')}...[/]")
                    elif not live_text and not live_tool:
                        status_spinner = self.console.status(
                            f"[dim #B4BEFE]✧ {event.get('message', 'Processing...')}...[/]", spinner="dots")
                        status_spinner.start()

                elif etype == "max_iterations_exceeded":
                    self.console.print(
                        f"\n[bold #FAB387]⚠ Warning:[/] {event.get('message')}")

                elif etype == "conversation_title":
                    self._conv_title = event.get('title', '')
                    self.console.print(
                        f"[dim italic]Conversation titled: {self._conv_title}[/]")

                elif etype == "tool_error":
                    stop_all_live()
                    tool_name = event.get(
                        "tool_name", active_tool or "Unknown")
                    self.console.print(
                        f"\n[bold #F38BA8]✖ Tool Error ({tool_name}):[/] {event.get('error', 'Unknown')}")

                elif etype == "error":
                    stop_all_live()
                    self.console.print(
                        f"\n[bold #F38BA8]Stream Error:[/] {event.get('error')}")

                elif etype in ["complete", "done"]:
                    stop_all_live()

                    if reason := event.get("stopped_reason"):
                        self.console.print(
                            f"[bold #FAB387]Stopped: {reason}[/]")

                    # Save the authoritative post-flight context estimate
                    if est := event.get("context_estimate"):
                        self._context_estimate = est

                    self._print_token_summary(event)

                    # Print persistent warning if compaction is imminent
                    if self._context_estimate.get("compaction_imminent"):
                        self.console.print(
                            f"  [bold #FAB387]⚠ Approaching Context Limit: Older messages will be summarized soon.[/]")

                    self.console.print()  # Spacer at end instead of a full line

        except httpx.HTTPStatusError as e:
            try:
                msg = json.dumps(e.response.json(), indent=2)
            except Exception:
                msg = e.response.text
            self.console.print(
                f"\n[bold #F38BA8]HTTP {e.response.status_code}:[/]\n{msg}")
        except httpx.ConnectError:
            self.server_online = False
            self.console.print(
                f"\n[bold #F38BA8]Connection failed:[/] Is the ForgeAI Server running at {self.config.base_url}?")
        except (asyncio.CancelledError, KeyboardInterrupt):
            if live_text:
                self._finalize_live_text(live_text, current_md)
                live_text = None
                current_md = ""
            if live_tool:
                live_tool.stop()
            safe_stop_spinner()
            self.console.print(
                "\n[bold #FAB387]⏹ Stream cancelled by user.[/]")
            return
        except httpx.ReadTimeout:
            self.console.print("\n[bold #F38BA8]Request timed out.[/]")
        except Exception as e:
            self.console.print(f"\n[bold #F38BA8]Stream failed:[/] {e}")
        finally:
            if live_text:
                self._finalize_live_text(live_text, current_md)
            if live_tool and live_tool.is_started:
                live_tool.stop()
            safe_stop_spinner()

    async def _handle_sql_command(self, args: List[str]):
        """Show / edit / rerun the last forgeai_execute_query SQL.

        Usage:
          /sql               Show last captured SQL + source_id.
          /sql edit          Open editor pre-filled with last SQL, then run it.
          /sql run           Re-run the last SQL unchanged.
        """
        sub = args[0].lower() if args else "show"

        if sub == "show":
            if not self._last_sql:
                self.console.print(
                    "[dim]No SQL captured yet. Ask a data question first "
                    "(in /explore mode) so the agent calls forgeai_execute_query.[/]")
                return
            self._print_sql_panel(
                self._last_sql["source_id"], self._last_sql["sql"], title="Last captured SQL")
            self.console.print(
                "[dim]Use [bold]/sql edit[/bold] to modify and run, or "
                "[bold]/sql run[/bold] to re-run unchanged.[/]")
            return

        if not self._last_sql:
            self.console.print(
                "[bold #F38BA8]No SQL captured. Run a question in /explore mode first.[/]")
            return

        source_id = self._last_sql["source_id"]
        row_limit = self._last_sql["row_limit"]
        timeout = self._last_sql["query_timeout_seconds"]

        if sub == "edit":
            self.console.print(
                f"[dim]Editing SQL for source_id=[/][bold #B4BEFE]{source_id}[/]"
                "[dim]. Press Enter to submit, Ctrl-C to cancel.[/]")
            try:
                new_sql = await self.session.prompt_async(
                    HTML("<ansiyellow>sql&gt; </ansiyellow>"),
                    default=self._last_sql["sql"],
                    multiline=True,
                )
            except (KeyboardInterrupt, EOFError):
                self.console.print("[dim]Cancelled.[/]")
                return
            new_sql = (new_sql or "").strip()
            if not new_sql:
                self.console.print(
                    "[bold #F38BA8]Empty SQL — nothing to run.[/]")
                return
            self._last_sql["sql"] = new_sql
        elif sub == "run":
            new_sql = self._last_sql["sql"]
        else:
            self.console.print(
                "[bold #F38BA8]Usage:[/] /sql | /sql edit | /sql run")
            return

        await self._run_explore_sql(source_id, new_sql, row_limit, timeout)

    def _print_sql_panel(self, source_id: int, sql: str, title: str):
        self.console.print(Panel(
            Syntax(sql, "sql", theme="monokai", word_wrap=True),
            title=f"{title} [dim](source_id={source_id})[/]",
            border_style="#6C7086",
            padding=(0, 1),
        ))

    async def _run_explore_sql(
        self, source_id: int, sql: str, row_limit: int, timeout: int
    ):
        self._print_sql_panel(source_id, sql, title="Executing SQL")
        with self.console.status("[dim #B4BEFE]✧ Running query...[/]", spinner="dots"):
            ok, data = await self.api.explore_execute_sql(
                source_id, sql, row_limit=row_limit, query_timeout_seconds=timeout)

        if not ok:
            self.console.print(f"[bold #F38BA8]✗ Query failed:[/] {data}")
            return

        cols = data.get("columns") or []
        rows = data.get("rows") or []
        executed = data.get("sql_executed") or sql
        duration_ms = data.get("duration_ms", 0)
        row_count = data.get("row_count", len(rows))
        truncated = data.get("truncated", False)

        # Update the cached SQL so subsequent /sql run uses the
        # server-canonicalised form (e.g. with LIMIT injected).
        if self._last_sql is not None:
            self._last_sql["sql"] = executed
            self._last_sql["source_id"] = source_id

        table = Table(
            show_header=True, header_style="bold #FAB387",
            border_style="#6C7086", expand=False,
        )
        if cols:
            for c in cols:
                table.add_column(str(c.get("name") or "?"))
        else:
            table.add_column("(no columns)")
        for r in rows[:200]:
            table.add_row(*[("" if v is None else str(v)) for v in r])

        self.console.print(table)
        footer = f"[#A6E3A1]✔[/] {row_count} row(s) · [bold #94E2D5]{duration_ms}ms[/]"
        if truncated:
            footer += "  [#F9E2AF](truncated)[/]"
        self.console.print(footer)

    async def handle_command(self, cmd_line: str):
        parts = cmd_line.split()
        cmd = parts[0].lower()
        args = parts[1:]

        if cmd == "/clear":
            self.render_banner()

        elif cmd == "/switch":
            target = args[0].lower() if args else (
                "workstream" if self.config.mode == "project" else "project")

            if target in ["ws", "work", "workstream"]:
                target = "workstream"
            elif target in ["p", "proj", "project"]:
                target = "project"
            elif target in ["e", "exp", "explore"]:
                target = "explore"

            if target in ["project", "workstream", "explore"]:
                self.config.mode = target
                if target == "explore":
                    self.config.conversation_id = ""
                    self.api.config = self.config
                    ok, cid = await self.api.create_conversation()
                    if ok:
                        self.config.conversation_id = cid
                        self._conv_title = ""
                        self.console.print(
                            f"[bold #A6E3A1]✓ Switched to explore mode, conversation: {cid[:8]}[/]")
                    else:
                        self.console.print(
                            f"[bold #F38BA8]✗ Switched to explore mode but failed to create conversation: {cid}[/]")
                elif target == "workstream" and len(args) >= 2:
                    ws_id = args[1]
                    self.config.workstream_id = ws_id
                    self.api.config = self.config
                    ok, cid = await self.api.create_conversation()
                    if ok:
                        self.config.conversation_id = cid
                        self._conv_title = ""
                        self.console.print(
                            f"[bold #A6E3A1]✓ Switched to workstream {ws_id}, conversation: {cid[:8]}[/]")
                    else:
                        self.console.print(
                            f"[bold #F38BA8]✗ Switched to workstream {ws_id} but failed to create conversation: {cid}[/]")
                else:
                    if target == "workstream" and not self.config.conversation_id:
                        await self._auto_conv()
                    self.console.print(
                        f"[bold #A6E3A1]✓ Switched to {target} mode.[/]")
                await self._sync_history()
            else:
                self.console.print(
                    "[bold #F38BA8]Usage: /switch [p|ws|explore] [id (workstream only)][/]")

        elif cmd in ["/workstream", "/ws"]:
            if not args:
                self.console.print(
                    f"Current workstream: [#B4BEFE]{self.config.workstream_id}[/]")
                return
            ws_id = args[0]
            self.config.mode = "workstream"
            self.config.workstream_id = ws_id
            self.api.config = self.config
            ok, cid = await self.api.create_conversation()
            if ok:
                self.config.conversation_id = cid
                self._conv_title = ""
                self.console.print(
                    f"[bold #A6E3A1]✓ Workstream → {ws_id}, conversation: {cid[:8]}[/]")
            else:
                self.console.print(
                    f"[bold #F38BA8]✗ Workstream → {ws_id} but failed to create conversation: {cid}[/]")
            await self._sync_history()

        elif cmd in ["/explore", "/exp"]:
            self.config.mode = "explore"
            self.config.conversation_id = ""
            self.api.config = self.config
            ok, cid = await self.api.create_conversation()
            if ok:
                self.config.conversation_id = cid
                self._conv_title = ""
                self.console.print(
                    f"[bold #A6E3A1]✓ Explore mode, conversation: {cid[:8]}[/]")
                self.console.print(
                    "[dim italic]Ask plain-English data questions. The LLM picks the source and runs the SQL.[/]")
            else:
                self.console.print(
                    f"[bold #F38BA8]✗ Explore mode but failed to create conversation: {cid}[/]")
            await self._sync_history()

        elif cmd == "/stage":
            if self.config.mode != "project":
                self.console.print(
                    "[bold #F38BA8]✗ Stages are only applicable in project mode. Use `/switch project` first.[/]")
                return

            if not args:
                lines = [
                    f"Current Stage: [#B4BEFE]{self.config.stage}[/]"]
                for i, s in enumerate(self.STAGE_PRESETS, 1):
                    act = " *" if s == self.config.stage else ""
                    lines.append(f"  [{i}] {s}{act}")
                lines.append("\nUse /stage <n> or /stage <name>")
                self.console.print("\n".join(lines))
                return

            arg = args[0]
            new_stage = None

            if arg.isdigit():
                n = int(arg)
                if 1 <= n <= len(self.STAGE_PRESETS):
                    new_stage = self.STAGE_PRESETS[n-1]
                else:
                    self.console.print(
                        f"[bold #F38BA8]✗ Invalid number. Choose between 1 and {len(self.STAGE_PRESETS)}.[/]")
                    return
            else:
                match = [s for s in self.STAGE_PRESETS if s ==
                         arg or s.startswith(arg)]
                if len(match) == 1:
                    new_stage = match[0]
                else:
                    self.console.print(
                        f"[bold #F38BA8]✗ Invalid stage: '{arg}'. Must be one of:[/]")
                    for i, s in enumerate(self.STAGE_PRESETS, 1):
                        self.console.print(f"  [{i}] {s}")
                    return

            self.config.stage = new_stage
            self.console.print(
                f"[bold #A6E3A1]✓ Stage set to:[/] {self.config.stage}")
            await self._sync_history()

        elif cmd == "/new":
            if self.config.mode not in ("workstream", "explore"):
                self.console.print(
                    "[bold #F38BA8]Only available in workstream/explore mode.[/]")
                return
            ok, cid = await self.api.create_conversation()
            if ok:
                self.config.conversation_id = cid
                self._conv_title = ""
                self.console.print(
                    f"[bold #A6E3A1]✓ Created new conversation:[/] {cid[:8]}")
                await self._sync_history()
            else:
                self.console.print(f"[bold #F38BA8]✗ Failed:[/] {cid}")

        elif cmd == "/list":
            if self.config.mode not in ("workstream", "explore"):
                self.console.print(
                    "[bold #F38BA8]Only available in workstream/explore mode.[/]")
                return
            ok, convs = await self.api.list_conversations()
            if not ok:
                self.console.print(
                    f"[bold #F38BA8]✗ Failed to fetch list:[/] {convs}")
                return

            self._conv_cache = convs
            if not convs:
                self.console.print("[dim]No conversations found.[/]")
                return

            table = Table(title="Conversations",
                          border_style="#6C7086", padding=(0, 2))
            table.add_column("ID", justify="right", style="bold")
            table.add_column("Title", style="default")
            table.add_column("Msgs", justify="right", style="dim")
            table.add_column("Last Active", style="dim #B4BEFE")
            table.add_column("Status", justify="center")

            for i, c in enumerate(convs, 1):
                active = "[bold #A6E3A1]●[/]" if c.get(
                    "conversation_id") == self.config.conversation_id else "[dim]○[/]"

                title = c.get("title") or "Untitled"
                if c.get("parent_conversation_id"):
                    title = f"🌱 {title}"

                last_time = c.get("last_message_at") or c.get("created_at", "")
                time_label = self._relative_time(last_time)

                table.add_row(
                    str(i),
                    title,
                    str(c.get('message_count', 0)),
                    time_label,
                    active
                )
            self.console.print(table)

        elif cmd == "/open":
            if not args or not args[0].isdigit():
                self.console.print("[bold #F38BA8]Usage: /open <number>[/]")
                return
            idx = int(args[0]) - 1
            if 0 <= idx < len(self._conv_cache):
                self.config.conversation_id = self._conv_cache[idx]["conversation_id"]
                self._conv_title = self._conv_cache[idx].get("title", "")
                self.console.print(
                    f"[bold #A6E3A1]✓ Opened conversation:[/] {self.config.conversation_id[:8]}")
                await self._sync_history()
            else:
                self.console.print(
                    "[bold #F38BA8]Invalid index. Run /list first.[/]")

        elif cmd == "/history":
            mode = "table"
            limit = 20
            show_tools = True
            detailed = False
            show_id = None

            for arg in args:
                al = arg.lower()
                if al in ["notools", "no-tools"]:
                    show_tools = False
                elif al == "detailed":
                    detailed = True
                elif al == "full":
                    mode = "full"
                    limit = 100
                elif al == "show":
                    mode = "show"
                elif arg.isdigit():
                    if mode == "show":
                        show_id = int(arg)
                    else:
                        limit = int(arg)

            if mode == "show" and show_id is None:
                self.console.print(
                    "[bold #F38BA8]Usage: /history show <id>[/]")
                return

            ok, msgs, context = await self.api.get_history(limit)
            if not ok:
                self.console.print(
                    f"[bold #F38BA8]✗ Failed to fetch history:[/] {msgs}")
                return

            self._history_cache = msgs
            # Overwrite state unconditionally to prevent token leakage
            self._context_estimate = context or {}

            if not msgs:
                self.console.print(
                    "[dim]No messages in this conversation yet.[/]")
                return

            if mode == "table":
                table = Table(title="Conversation History",
                              border_style="#6C7086", show_lines=True, padding=(0, 1))
                table.add_column("ID", justify="right", style="dim")
                table.add_column("Role", style="bold")
                table.add_column("Time", style="dim #B4BEFE")
                table.add_column("Content (Preview)")

                if show_tools:
                    table.add_column("Tools Executed", style="dim #FAB387")

                for i, m in enumerate(msgs, 1):
                    role = m.get("role", "?").upper()
                    color = "#A6E3A1" if role == "USER" else "#B4BEFE"
                    content = m.get("content_for_llm") or m.get("content", "")
                    preview = content.replace(
                        "\n", " ")[:75] + ("..." if len(content) > 75 else "")

                    ts = m.get("timestamp", "")
                    time_label = self._relative_time(ts)

                    row_data = [
                        str(i), f"[{color}]{role}[/]", time_label, preview]

                    if show_tools:
                        tools = m.get("tools_used") or []
                        row_data.append(", ".join(tools) if tools else "-")

                    table.add_row(*row_data)

                self.console.print(table)
                self.console.print(
                    "[dim italic]Tip: Use `/history show <id>` to read one, or `/history full [notools|detailed]` to read all.[/]\n")

            elif mode == "show":
                if 1 <= show_id <= len(msgs):
                    m = msgs[show_id - 1]
                    role = m.get("role", "?").upper()
                    color = "#A6E3A1" if role == "USER" else "#B4BEFE"
                    role_icon = "●" if role == "USER" else "❖"
                    tools = m.get("tools_used") or []
                    ts = m.get("timestamp", "")
                    stop_reason = m.get("stop_reason")
                    token_usage = m.get("token_usage")
                    user_id_val = m.get("user_id")

                    self.console.print()
                    self.console.print(
                        Rule(f"{role_icon} {role} (ID: {show_id})", style=color))

                    # Metadata line
                    meta_parts = []
                    if ts:
                        meta_parts.append(f"[dim]{self._relative_time(ts)}[/]")
                    if user_id_val:
                        meta_parts.append(f"[dim]by {user_id_val}[/]")
                    if stop_reason and stop_reason != "end_turn":
                        meta_parts.append(
                            f"[#FAB387]stopped: {stop_reason}[/]")
                    if token_usage:
                        inp = token_usage.get("input_tokens", 0)
                        out = token_usage.get("output_tokens", 0)
                        calls = token_usage.get("model_call_count", 0)
                        tok_parts = []
                        if inp or out:
                            tok_parts.append(f"{inp:,} in / {out:,} out")
                        if calls:
                            tok_parts.append(
                                f"{calls} call{'s' if calls != 1 else ''}")
                        if tok_parts:
                            meta_parts.append(
                                f"[dim]{' · '.join(tok_parts)}[/]")
                    if meta_parts:
                        self.console.print("  ".join(meta_parts))

                    if detailed:
                        # Render full content with embedded tool panels
                        content = m.get("content") or m.get(
                            "content_for_llm", "")
                        if tools:
                            self.console.print(
                                f"[{color}]🛠  Tools:[/] [#FAB387]{self._format_tools_summary(tools)}[/]")
                        self.console.print()
                        if content.strip():
                            self._render_content_with_tools(content)
                        else:
                            self.console.print(
                                "[dim italic](No text content)[/]")
                    else:
                        # Simple mode: show tool summary + content_for_llm as markdown
                        content = m.get("content_for_llm") or m.get(
                            "content", "")
                        if tools:
                            self.console.print(
                                f"[{color}]🛠  Tools:[/] [#FAB387]{self._format_tools_summary(tools)}[/]")
                        self.console.print()
                        if content.strip():
                            self.console.print(self.safe_markdown(content))
                        else:
                            self.console.print(
                                "[dim italic](No text content)[/]")

                    self.console.print()
                else:
                    self.console.print(
                        f"[bold #F38BA8]Invalid ID {show_id}. Available range: 1-{len(msgs)}.[/]")

            elif mode == "full":
                self.console.print(
                    f"\n[bold]Full Conversation ({len(msgs)} messages)[/]\n")

                for i, m in enumerate(msgs, 1):
                    role = m.get("role", "?").upper()
                    color = "#A6E3A1" if role == "USER" else "#B4BEFE"
                    role_icon = "●" if role == "USER" else "❖"
                    tools = m.get("tools_used") or []
                    ts = m.get("timestamp", "")
                    stop_reason = m.get("stop_reason")
                    token_usage = m.get("token_usage")
                    user_id_val = m.get("user_id")

                    # The Rich Divider
                    self.console.print(
                        Rule(f"{role_icon} {role} (ID: {i})", style=color))

                    # Metadata line
                    meta_parts = []
                    if ts:
                        meta_parts.append(f"[dim]{self._relative_time(ts)}[/]")
                    if user_id_val:
                        meta_parts.append(f"[dim]by {user_id_val}[/]")
                    if stop_reason and stop_reason != "end_turn":
                        meta_parts.append(
                            f"[#FAB387]stopped: {stop_reason}[/]")
                    if token_usage:
                        inp = token_usage.get("input_tokens", 0)
                        out = token_usage.get("output_tokens", 0)
                        calls = token_usage.get("model_call_count", 0)
                        tok_parts = []
                        if inp or out:
                            tok_parts.append(f"{inp:,} in / {out:,} out")
                        if calls:
                            tok_parts.append(
                                f"{calls} call{'s' if calls != 1 else ''}")
                        if tok_parts:
                            meta_parts.append(
                                f"[dim]{' · '.join(tok_parts)}[/]")
                    if meta_parts:
                        self.console.print("  ".join(meta_parts))

                    if detailed:
                        content = m.get("content") or m.get(
                            "content_for_llm", "")
                        if show_tools and tools:
                            self.console.print(
                                f"[{color}]🛠  Tools:[/] [#FAB387]{self._format_tools_summary(tools)}[/]")
                        self.console.print()
                        if content.strip():
                            self._render_content_with_tools(content)
                        else:
                            self.console.print(
                                "[dim italic](No text content)[/]")
                    else:
                        content = m.get("content_for_llm") or m.get(
                            "content", "")
                        if show_tools and tools:
                            self.console.print(
                                f"[{color}]🛠  Tools:[/] [#FAB387]{self._format_tools_summary(tools)}[/]")
                        self.console.print()
                        if content.strip():
                            self.console.print(self.safe_markdown(content))
                        else:
                            self.console.print(
                                "[dim italic](No text content)[/]")

                    self.console.print()  # Spacer

        elif cmd == "/delete":
            if args and args[0].isdigit() and self.config.mode in ("workstream", "explore"):
                idx = int(args[0]) - 1
                if 0 <= idx < len(self._conv_cache):
                    cid = self._conv_cache[idx]["conversation_id"]
                    ok, msg = await self.api.delete_conversation(cid)
                    if ok and cid == self.config.conversation_id:
                        await self._auto_conv()
                    self.console.print(f"[bold #A6E3A1]✓ {msg}[/]")
                else:
                    self.console.print(
                        "[bold #F38BA8]Invalid index. Run /list first.[/]")
            else:
                ok, msg = await self.api.delete_conversation()
                self.console.print(
                    f"[bold {'#A6E3A1' if ok else '#F38BA8'}]{'✓' if ok else '✗'} {msg}[/]")
                if ok and self.config.mode in ("workstream", "explore"):
                    self.config.conversation_id = ""
                    await self._auto_conv()

        elif cmd == "/rename":
            if not args or self.config.mode not in ("workstream", "explore"):
                self.console.print(
                    "[bold #F38BA8]Usage: /rename <title> (workstream/explore mode only)[/]")
                return
            title = " ".join(args)
            ok, msg = await self.api.rename_conversation(title)
            if ok:
                self._conv_title = title
            self.console.print(
                f"[bold {'#A6E3A1' if ok else '#F38BA8'}]{'✓' if ok else '✗'} {msg}[/]")

        elif cmd == "/branch":
            if not args or not args[0].isdigit() or self.config.mode not in ("workstream", "explore"):
                self.console.print(
                    "[bold #F38BA8]Usage: /branch <n> (get number from /history)[/]")
                return
            idx = int(args[0]) - 1
            if 0 <= idx < len(self._history_cache):
                msg = self._history_cache[idx]
                if msg.get("role") != "assistant":
                    self.console.print(
                        "[bold #F38BA8]Branch must be from an assistant message.[/]")
                    return
                ok, data = await self.api.branch_conversation(msg["message_id"])
                if ok:
                    self.config.conversation_id = data.get(
                        "conversation_id", "")
                    self._conv_title = data.get("title", "")
                    self.console.print(
                        f"[bold #A6E3A1]✓ Branched to new conversation:[/] {self.config.conversation_id[:8]}")
                    await self._sync_history()
                else:
                    self.console.print(f"[bold #F38BA8]✗ Failed:[/] {data}")
            else:
                self.console.print(
                    "[bold #F38BA8]Invalid index. Run /history first.[/]")

        elif cmd == "/models":
            models = await self.api.models()
            self._model_cache = models
            for i, m in enumerate(models, 1):
                active = " *" if m == self.config.model_name else ""
                self.console.print(
                    f"[dim][{i}][/] {m}[bold #A6E3A1]{active}[/]")

        elif cmd == "/model":
            if not args:
                self.console.print(
                    f"Current: {self.config.model_name or 'default'}")
                return
            # Numeric index from /models, otherwise join remaining tokens so
            # multi-word names like "Claude Opus 4.6" are preserved.
            if len(args) == 1 and args[0].isdigit():
                idx = int(args[0]) - 1
                if not self._model_cache:
                    self._model_cache = await self.api.models()
                if 0 <= idx < len(self._model_cache):
                    self.config.model_name = self._model_cache[idx]
                else:
                    self.console.print(
                        f"[bold #F38BA8]Invalid model index:[/] {args[0]}")
                    return
            else:
                name = " ".join(args)
                if not self._model_cache:
                    self._model_cache = await self.api.models()
                matches = [
                    m for m in self._model_cache
                    if m.lower() == name.lower()
                ]
                if matches:
                    self.config.model_name = matches[0]
                else:
                    # Allow exact typed name even if /models cache is stale
                    self.config.model_name = name
                    if self._model_cache:
                        self.console.print(
                            f"[bold #FAB387]⚠ Unknown model[/] (not in /models list): {name}")
            self.console.print(
                f"[bold #A6E3A1]✓ Model set to:[/] {self.config.model_name}")

        elif cmd == "/toolview":
            self.config.compact = not self.config.compact
            label = "minimal" if self.config.compact else "detailed"
            self.console.print(
                f"[bold]Tool view:[/] {label}")

        elif cmd == "/last":
            if self._last_response:
                self.console.print(self.safe_markdown(self._last_response))
            else:
                self.console.print("[dim]No previous response.[/]")

        elif cmd == "/sql":
            await self._handle_sql_command(args)

        elif cmd == "/export":
            fmt = args[0].lower() if args else "md"
            if fmt not in ["md", "json", "docx"]:
                self.console.print(
                    "[bold #F38BA8]Usage: /export [md|json|docx][/]")
                return
            ok, msgs, _ = await self.api.get_history(200)
            if not ok or not msgs:
                self.console.print("[bold #F38BA8]Export failed or empty.[/]")
                return
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = f"forgeai_export_{ts}.{fmt}"

            if fmt == "md":
                with open(fname, "w") as f:
                    for m in msgs:
                        role = "User" if m.get(
                            "role") == "user" else "Assistant"
                        f.write(
                            f"## {role}\n{m.get('content_for_llm') or m.get('content', '')}\n\n---\n\n")
            elif fmt == "json":
                with open(fname, "w") as f:
                    json.dump(msgs, f, indent=2)
            elif fmt == "docx":
                try:
                    from docx import Document
                    doc = Document()
                    for m in msgs:
                        role = "User" if m.get(
                            "role") == "user" else "Assistant"
                        doc.add_heading(role, level=2)
                        doc.add_paragraph(
                            m.get('content_for_llm') or m.get('content', ''))
                    doc.save(fname)
                except ImportError:
                    self.console.print(
                        "[bold #F38BA8]python-docx not installed. run `pip install python-docx`[/]")
                    return
            self.console.print(f"[bold #A6E3A1]✓ Exported to {fname}[/]")

        elif cmd == "/load":
            if not args:
                self.console.print(
                    "[bold #F38BA8]Usage: /load <file_path> [detailed][/]")
                return

            # Parse out the 'detailed' flag from args
            detailed_load = False
            path_parts = []
            for arg in args:
                if arg.lower() == "detailed":
                    detailed_load = True
                else:
                    path_parts.append(arg)

            if not path_parts:
                self.console.print(
                    "[bold #F38BA8]Usage: /load <file_path> [detailed][/]")
                return

            file_path = " ".join(path_parts)
            file_path = os.path.expanduser(file_path)

            if not os.path.isabs(file_path):
                file_path = os.path.abspath(file_path)

            if not os.path.exists(file_path):
                self.console.print(
                    f"[bold #F38BA8]✗ File not found:[/] {file_path}")
                return

            if not os.path.isfile(file_path):
                self.console.print(
                    f"[bold #F38BA8]✗ Path is not a file:[/] {file_path}")
                return

            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    raw = f.read()
            except Exception as e:
                self.console.print(
                    f"[bold #F38BA8]✗ Could not read file:[/] {escape(str(e))}")
                return

            if not raw.strip():
                self.console.print(
                    "[bold #F38BA8]✗ File is empty.[/]")
                return

            try:
                data = json.loads(raw)
            except json.JSONDecodeError as e:
                self.console.print(
                    f"[bold #F38BA8]✗ File does not contain valid JSON:[/] {escape(str(e))}")
                return

            # Accept both a raw list of messages and a dict with a "messages" key
            if isinstance(data, dict):
                msgs = data.get("messages")
                if msgs is None:
                    self.console.print(
                        "[bold #F38BA8]✗ JSON object has no 'messages' key. Expected a list of messages or {\"messages\": [...]}.[/]")
                    return
            elif isinstance(data, list):
                msgs = data
            else:
                self.console.print(
                    "[bold #F38BA8]✗ Unexpected JSON type. Expected a list of messages or {\"messages\": [...]}.[/]")
                return

            if not isinstance(msgs, list) or not msgs:
                self.console.print(
                    "[bold #F38BA8]✗ No messages found in file.[/]")
                return

            # Validate that entries look like conversation messages
            valid_msgs = []
            for i, m in enumerate(msgs):
                if not isinstance(m, dict):
                    self.console.print(
                        f"[bold #F38BA8]✗ Entry {i+1} is not a valid message object. Aborting.[/]")
                    return
                if "role" not in m:
                    self.console.print(
                        f"[bold #F38BA8]✗ Entry {i+1} is missing 'role' field. Aborting.[/]")
                    return
                valid_msgs.append(m)

            # Render header
            mode_label = "detailed" if detailed_load else "summary"
            self.console.print(
                f"\n[bold]Loaded Conversation from file ({len(valid_msgs)} messages, {mode_label})[/]")
            self.console.print(f"[dim]{file_path}[/]\n")

            for i, m in enumerate(valid_msgs, 1):
                role = m.get("role", "?").upper()
                color = "#A6E3A1" if role == "USER" else "#B4BEFE"
                role_icon = "●" if role == "USER" else "❖"
                tools = m.get("tools_used") or []
                ts = m.get("timestamp", "")
                stop_reason = m.get("stop_reason")
                token_usage = m.get("token_usage")
                user_id_val = m.get("user_id")

                self.console.print(
                    Rule(f"{role_icon} {role} (ID: {i})", style=color))

                meta_parts = []
                if ts:
                    meta_parts.append(f"[dim]{self._relative_time(ts)}[/]")
                if user_id_val:
                    meta_parts.append(f"[dim]by {user_id_val}[/]")
                if stop_reason and stop_reason != "end_turn":
                    meta_parts.append(
                        f"[#FAB387]stopped: {stop_reason}[/]")
                if token_usage:
                    inp = token_usage.get("input_tokens", 0)
                    out = token_usage.get("output_tokens", 0)
                    calls = token_usage.get("model_call_count", 0)
                    tok_parts = []
                    if inp or out:
                        tok_parts.append(f"{inp:,} in / {out:,} out")
                    if calls:
                        tok_parts.append(
                            f"{calls} call{'s' if calls != 1 else ''}")
                    if tok_parts:
                        meta_parts.append(
                            f"[dim]{' · '.join(tok_parts)}[/]")
                if meta_parts:
                    self.console.print("  ".join(meta_parts))

                if detailed_load:
                    # Detailed: render content with embedded tool panels
                    content = m.get("content") or m.get("content_for_llm", "")
                    if tools:
                        self.console.print(
                            f"[{color}]🛠  Tools:[/] [#FAB387]{self._format_tools_summary(tools)}[/]")
                    self.console.print()
                    if content.strip():
                        self._render_content_with_tools(content)
                    else:
                        self.console.print("[dim italic](No text content)[/]")
                else:
                    # Summary: tool list at top + content_for_llm as markdown
                    content = m.get("content_for_llm") or m.get("content", "")
                    if tools:
                        self.console.print(
                            f"[{color}]🛠  Tools:[/] [#FAB387]{self._format_tools_summary(tools)}[/]")
                    self.console.print()
                    if content.strip():
                        self.console.print(self.safe_markdown(content))
                    else:
                        self.console.print("[dim italic](No text content)[/]")

                self.console.print()

            self.console.print(
                f"[dim italic]Loaded {len(valid_msgs)} messages from file (read-only). "
                f"Your next message will continue in the active conversation.[/]\n")

        elif cmd == "/status":
            lines = [
                f"  [bold #B4BEFE]Mode:[/]    {self.config.mode}",
                f"  [bold #B4BEFE]Server:[/]  {self.config.base_url}",
                f"  [bold #B4BEFE]Context:[/] {self.config.context_label()}",
                f"  [bold #B4BEFE]Model:[/]   {self.config.model_name or 'default'}",
                f"  [bold #B4BEFE]Tool View:[/] {'minimal' if self.config.compact else 'detailed'}",
            ]
            if self.config.mode == "project":
                lines.append(
                    f"  [bold #B4BEFE]Project:[/] {self.config.project_id}")
                lines.append(
                    f"  [bold #B4BEFE]Stage:[/]   {self.config.stage}")
            elif self.config.mode == "explore":
                lines.append(
                    f"  [bold #B4BEFE]User:[/]    {self.config.user_id}")
                lines.append(
                    f"  [bold #B4BEFE]Conv ID:[/] {self.config.conversation_id or 'none'}")
                if self._conv_title:
                    lines.append(
                        f"  [bold #B4BEFE]Title:[/]   {self._conv_title}")
            else:
                lines.append(
                    f"  [bold #B4BEFE]WS ID:[/]   {self.config.workstream_id}")
                lines.append(
                    f"  [bold #B4BEFE]Conv ID:[/] {self.config.conversation_id or 'none'}")
                if self._conv_title:
                    lines.append(
                        f"  [bold #B4BEFE]Title:[/]   {self._conv_title}")

            tokens = []
            if self.config.bearer_token:
                tokens.append("Bearer")
            if self.config.github_token:
                tokens.append("GitHub")
            if self.config.jira_token:
                tokens.append("Jira")
            if self.config.yeedu_token:
                tokens.append("Yeedu")
            if self.config.forgeai_token:
                tokens.append("ForgeAI")
            if self.config.databricks_auth_type:
                tokens.append(
                    f"Databricks({self.config.databricks_auth_type})")

            lines.append(
                f"  [bold #B4BEFE]Tokens:[/]  {', '.join(tokens) if tokens else 'none'}")

            self.console.print(
                Panel("\n".join(lines), title="Config Status", border_style="#6C7086", expand=False))

        elif cmd == "/databricks":
            if not self.config.databricks_workspace_url:
                self.console.print(
                    "[dim]Databricks is not configured. Set DATABRICKS_WORKSPACE_URL.[/]")
                return

            lines = [
                f"  [bold #B4BEFE]Auth Type:[/]     {self.config.databricks_auth_type or 'not set'}",
                f"  [bold #B4BEFE]Workspace URL:[/] {self.config.databricks_workspace_url}",
                f"  [bold #B4BEFE]Cluster ID:[/]    {self.config.databricks_cluster_id or 'not set'}",
                f"  [bold #B4BEFE]Compute:[/]       {self.config.databricks_compute_type}",
            ]
            self.console.print(Panel(
                "\n".join(lines), title="Databricks Config", border_style="#6C7086", expand=False))

        elif cmd == "/reload":
            old_dump = self.config.model_dump()
            load_dotenv(override=True)

            new_config = ForgeAIConfig()
            new_config.mode = self.config.mode
            new_config.project_id = self.config.project_id
            new_config.workstream_id = self.config.workstream_id
            new_config.conversation_id = self.config.conversation_id

            self.config = new_config
            self.api.config = self.config
            self.api.refresh_client()

            new_dump = self.config.model_dump()
            changes = []
            for k, v_new in new_dump.items():
                v_old = old_dump.get(k)
                if v_old != v_new:
                    if any(x in k for x in ["token", "secret", "password"]):
                        changes.append(
                            f"  [dim]{k}:[/] [bold #FAB387]<updated>[/]")
                    else:
                        changes.append(
                            f"  [dim]{k}:[/] [#F38BA8]{v_old}[/] → [#A6E3A1]{v_new}[/]")

            self.console.print("[bold #A6E3A1]✓ Environment reloaded.[/]")
            if changes:
                self.console.print(
                    Panel("\n".join(changes), title="Changes Detected", border_style="#6C7086"))
            else:
                self.console.print(
                    "[dim]No configuration changes detected.[/]")

        elif cmd == "/health":
            ok, data = await self.api.health()
            self.server_online = ok
            if ok:
                syntax = Syntax(json.dumps(data, indent=2), "json",
                                theme="monokai", background_color="default")
                self.console.print(
                    Panel(syntax, title="Health Check", border_style="#6C7086", padding=(0, 1)))
            else:
                self.console.print(
                    f"[bold #F38BA8]✗ Health Check Failed:[/] {data}")

        elif cmd == "/help":
            self.console.print("""
[bold #B4BEFE]ForgeAI God-Level Terminal[/]
  [dim]Commands[/]
  /switch p|ws|explore [id]    Switch context mode (project/workstream/explore; id for ws only)
  /workstream <id>              Switch to workstream by ID (alias: /ws)
  /explore                      Switch to data-exploration mode (alias: /exp)
                                Scoped by your user identity (X-User-Id header).
                                Ask plain-English data questions; the LLM picks the ForgeAI
                                source and runs SQL automatically.
  /stage [n|name]               Set or pick project stage
  /list                         List conversations
  /open <n>                     Open a conversation by ID
  /new                          Create new conversation
  /history [n] [notools]        Show recent messages as a table
  /history full [notools|detailed]  Read the entire conversation chronologically
  /history show <id> [detailed] Read a specific message in full detail
  /branch <n>                   Fork from history ID
  /rename <title>               Rename current conversation
  /delete [n]                   Delete conversation
  /models                       List models
  /model <name>                 Set target model
  /export md|json|docx          Export history
  /load <file> [detailed]       Load and display a conversation from a JSON file
  /compact                      Trigger server-side context summarization
  /toolview                     Toggle tool execution display (detailed/minimal)
  /sql [edit|run]               Show / edit / re-run the last forgeai_execute_query SQL
                                (bypasses the LLM, calls /explore/execute_sql directly)
  /reload                       Reload .env configuration
  /status                       Show active configuration context
  /databricks                   Show active Databricks configuration
  /health                       Check server health
  /clear                        Clear terminal screen
  /exit                         Quit client
  
[dim italic]Tip: To enter multiline mode, type `\"\"\"` and hit Enter.[/]
            """)
        elif cmd == "/compact":
            # Server-side command: pass through to chat endpoint for context summarization
            if not await self._auto_conv():
                return
            await self.stream_response(cmd_line)

        else:
            self.console.print(f"[bold #F38BA8]Unknown command:[/] {cmd}")


cli = typer.Typer(add_completion=False)


@cli.command()
def main(
    server: str = typer.Option(
        None, "--server", help="ForgeAI MCP Client Base URL"),
    mode: str = typer.Option(None, "--mode", help="project | workstream"),
    workstream_id: str = typer.Option(None, "--workstream-id"),
    conversation_id: str = typer.Option(None, "--conversation-id"),
    timeout: int = typer.Option(None, "--timeout", help="Request timeout"),
):
    config = ForgeAIConfig()
    if server:
        config.base_url = server
    if mode:
        config.mode = mode
    if workstream_id:
        config.workstream_id = workstream_id
    if conversation_id:
        config.conversation_id = conversation_id
    if timeout:
        config.timeout = timeout

    ui = TerminalUI(config)

    try:
        asyncio.run(ui.run())
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    cli()

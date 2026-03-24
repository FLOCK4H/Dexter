from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
from pathlib import Path
import threading
from typing import Any

import aiohttp

from dexter_config import ENV_PATH, read_env_values, update_env_file
from dexter_operator import maybe_desktop_notify

TELEGRAM_COMMANDS = (
    {
        "command": "id",
        "description": "Reply with this chat id for Dexter alerts.",
    },
)


@dataclass(frozen=True)
class AlertTargets:
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    discord_webhook_url: str = ""
    desktop_notifications: bool = False


class AlertDispatcher:
    def __init__(self, targets: AlertTargets, *, env_path: Path | None = None):
        self.targets = targets
        self.env_path = env_path or ENV_PATH

    async def _post_json(self, url: str, payload: dict[str, Any]) -> None:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload) as response:
                await response.read()

    def _resolved_telegram_chat_id(self) -> str:
        configured = (self.targets.telegram_chat_id or "").strip()
        if configured:
            return configured
        env_values = read_env_values(self.env_path)
        return (env_values.get("DEXTER_TELEGRAM_CHAT_ID") or "").strip()

    async def send_telegram(self, title: str, body: str) -> bool:
        chat_id = self._resolved_telegram_chat_id()
        if not self.targets.telegram_bot_token or not chat_id:
            return False
        url = f"https://api.telegram.org/bot{self.targets.telegram_bot_token}/sendMessage"
        payload = {"chat_id": chat_id, "text": f"{title}\n{body}"}
        try:
            await self._post_json(url, payload)
            return True
        except Exception:
            return False

    async def send_discord(self, title: str, body: str) -> bool:
        if not self.targets.discord_webhook_url:
            return False
        payload = {"content": f"**{title}**\n{body}"}
        try:
            await self._post_json(self.targets.discord_webhook_url, payload)
            return True
        except Exception:
            return False

    async def send_desktop(self, title: str, body: str) -> bool:
        if not self.targets.desktop_notifications:
            return False
        return await asyncio.to_thread(maybe_desktop_notify, title, body)

    async def broadcast(self, title: str, body: str) -> dict[str, bool]:
        telegram, discord, desktop = await asyncio.gather(
            self.send_telegram(title, body),
            self.send_discord(title, body),
            self.send_desktop(title, body),
        )
        return {"telegram": telegram, "discord": discord, "desktop": desktop}


class TelegramCommandService:
    def __init__(
        self,
        *,
        bot_token: str = "",
        env_path: Path | None = None,
        state_path: Path | None = None,
    ):
        self.bot_token = (bot_token or "").strip()
        self.env_path = env_path or ENV_PATH
        self.state_path = state_path or (self.env_path.parent / "dev" / "state" / "telegram-command-service.json")
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def refresh(self, bot_token: str) -> None:
        normalized = (bot_token or "").strip()
        should_restart = normalized != self.bot_token or (
            normalized and (self._thread is None or not self._thread.is_alive())
        )
        if not should_restart:
            return
        self.stop()
        self.bot_token = normalized
        if self.bot_token:
            self.start()

    def start(self) -> bool:
        if not self.bot_token:
            return False
        if self._thread is not None and self._thread.is_alive():
            return False
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_thread,
            name="dexter-telegram-command-service",
            daemon=True,
        )
        self._thread.start()
        return True

    def stop(self, timeout: float = 2.0) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        self._thread = None

    def _run_thread(self) -> None:
        try:
            asyncio.run(self._poll_updates())
        except Exception:
            return

    def _load_offset(self) -> int:
        if not self.state_path.exists():
            return 0
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            return 0
        try:
            return int(payload.get("offset", 0) or 0)
        except Exception:
            return 0

    def _save_offset(self, offset: int) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"offset": int(offset)}
        self.state_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    async def _post_json(self, session: aiohttp.ClientSession, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"https://api.telegram.org/bot{self.bot_token}/{method}"
        async with session.post(url, json=payload) as response:
            data = await response.json(content_type=None)
            return data if isinstance(data, dict) else {}

    async def _register_commands(self, session: aiohttp.ClientSession) -> None:
        await self._post_json(session, "setMyCommands", {"commands": list(TELEGRAM_COMMANDS)})

    def _persist_chat_id(self, chat_id: str) -> None:
        if not chat_id:
            return
        env_values = read_env_values(self.env_path)
        current = env_values.get("DEXTER_TELEGRAM_CHAT_ID", "").strip()
        if current == chat_id:
            return
        update_env_file({"DEXTER_TELEGRAM_CHAT_ID": chat_id}, path=self.env_path)
        desktop_notifications = (env_values.get("DEXTER_DESKTOP_NOTIFICATIONS") or "").strip().lower()
        if desktop_notifications in {"1", "true", "yes", "on"}:
            maybe_desktop_notify("Dexter Telegram ready", f"Captured chat id {chat_id}.")

    async def _handle_update(self, session: aiohttp.ClientSession, update: dict[str, Any]) -> None:
        message = update.get("message") or update.get("edited_message")
        if not isinstance(message, dict):
            return
        text = str(message.get("text") or "").strip()
        if not text:
            return
        if not (text == "/id" or text.startswith("/id@")):
            return
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id") or "").strip()
        if not chat_id:
            return
        self._persist_chat_id(chat_id)
        reply = (
            f"Dexter chat id: `{chat_id}`\n"
            "Saved to DEXTER_TELEGRAM_CHAT_ID while Dexter is running."
        )
        await self._post_json(
            session,
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": reply,
                "parse_mode": "Markdown",
                "reply_to_message_id": message.get("message_id"),
            },
        )

    async def _poll_updates(self) -> None:
        if not self.bot_token:
            return
        timeout = aiohttp.ClientTimeout(total=6)
        offset = self._load_offset()
        commands_registered = False
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while not self._stop_event.is_set():
                try:
                    if not commands_registered:
                        await self._register_commands(session)
                        commands_registered = True
                    payload = {
                        "offset": offset,
                        "timeout": 3,
                        "allowed_updates": ["message", "edited_message"],
                    }
                    data = await self._post_json(session, "getUpdates", payload)
                    updates = data.get("result") or []
                    if not isinstance(updates, list):
                        updates = []
                    for item in updates:
                        if not isinstance(item, dict):
                            continue
                        await self._handle_update(session, item)
                        update_id = int(item.get("update_id", offset - 1) or (offset - 1))
                        offset = max(offset, update_id + 1)
                    self._save_offset(offset)
                except Exception:
                    await asyncio.sleep(2)

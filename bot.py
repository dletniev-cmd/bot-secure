#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TG Account Manager Bot — v4.0 (slim)
aiogram 3.x + Telethon MTProto

Оставлены только:
  • Рассылка
  • Автоответчик
  • Черновики
  • Последний код от Telegram
  • Очистка чёрного списка
"""

import asyncio
import logging
import os
import random
import re
import time
from datetime import datetime, timezone
from typing import Optional, Dict, List
from collections import defaultdict, deque
from pathlib import Path
from io import BytesIO

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile
)
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest
from aiogram.dispatcher.middlewares.base import BaseMiddleware

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError, PhoneCodeInvalidError,
    FloodWaitError, PhoneNumberInvalidError,
    UserPrivacyRestrictedError, PeerFloodError,
    UserIsBlockedError, InputUserDeactivatedError
)
from telethon.tl.functions.contacts import BlockRequest, UnblockRequest, GetBlockedRequest
from telethon.tl.functions.account import UpdateStatusRequest
from telethon.tl.types import (
    MessageMediaPhoto,
    MessageMediaDocument,
    User, Chat, Channel,
)

# ══════════════════════════════════════
# КОНФИГ
# ══════════════════════════════════════
import os

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN. Установите переменную окружения: export BOT_TOKEN=ваш_токен")
API_ID    = 2040
API_HASH  = "b18441a1ff607e10a989891a5462e627"
DB_PATH   = "manager_bot.db"
ALLOWED_USERS: set = set()

# ── Лимиты рассылки (anti-ban) ──
BROADCAST_DELAY_MIN   = 35        # минимум секунд между сообщениями
BROADCAST_DELAY_MAX   = 90        # максимум секунд между сообщениями
BROADCAST_BATCH_SIZE  = 5         # после скольких — длинная пауза
BROADCAST_BATCH_PAUSE_MIN = 90    # пауза после батча, сек
BROADCAST_BATCH_PAUSE_MAX = 180
BROADCAST_DAY_LIMIT   = 40        # максимум сообщений в сутки с одного акка
BROADCAST_SESSION_MAX = 30        # максимум за одну сессию (предупреждение)

# ── Лимиты автоответчика ──
AUTOREPLY_COOLDOWN_SEC = 120      # пауза между авто-ответами одному чату

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("TGManager")

# ══════════════════════════════════════
# УТИЛИТЫ
# ══════════════════════════════════════
def _serialize_entities(msg) -> list:
    result = []
    ents = msg.entities or msg.caption_entities or []
    for e in ents:
        d = {'type': e.type, 'offset': e.offset, 'length': e.length}
        if e.type == 'custom_emoji' and e.custom_emoji_id:
            d['custom_emoji_id'] = e.custom_emoji_id
        elif e.type == 'text_link' and e.url:
            d['url'] = e.url
        elif e.type == 'pre' and getattr(e, 'language', None):
            d['language'] = e.language
        result.append(d)
    return result


def _entities_to_html(text: str, entities_json: list) -> str:
    import html as _html_mod
    if not entities_json or not text:
        return _html_mod.escape(text)
    events_list = []
    for e in entities_json:
        o = e.get('offset', 0)
        l = e.get('length', 0)
        t = e.get('type', '')
        if t == 'bold':
            events_list += [(o, 0, '<b>'), (o+l, 1, '</b>')]
        elif t == 'italic':
            events_list += [(o, 0, '<i>'), (o+l, 1, '</i>')]
        elif t == 'underline':
            events_list += [(o, 0, '<u>'), (o+l, 1, '</u>')]
        elif t == 'strikethrough':
            events_list += [(o, 0, '<s>'), (o+l, 1, '</s>')]
        elif t == 'code':
            events_list += [(o, 0, '<code>'), (o+l, 1, '</code>')]
        elif t == 'pre':
            lang = e.get('language', '')
            open_t = f'<pre><code class="language-{lang}">' if lang else '<pre>'
            close_t = '</code></pre>' if lang else '</pre>'
            events_list += [(o, 0, open_t), (o+l, 1, close_t)]
        elif t == 'spoiler':
            events_list += [(o, 0, '<tg-spoiler>'), (o+l, 1, '</tg-spoiler>')]
        elif t == 'text_link':
            url = _html_mod.escape(e.get('url', ''))
            events_list += [(o, 0, f'<a href="{url}">'), (o+l, 1, '</a>')]
        elif t == 'custom_emoji':
            eid = e.get('custom_emoji_id', '')
            events_list += [(o, 0, f'<tg-emoji emoji-id="{eid}">'), (o+l, 1, '</tg-emoji>')]
    events_list.sort(key=lambda x: (x[0], x[1]))
    parts = []
    prev  = 0
    for pos, kind, tag in events_list:
        if pos > prev:
            parts.append(_html_mod.escape(text[prev:pos]))
        parts.append(tag)
        prev = pos
    if prev < len(text):
        parts.append(_html_mod.escape(text[prev:]))
    return ''.join(parts)


def _content_item_html(item: dict) -> str:
    t = item.get('type', '?')
    if t == 'text':
        if item.get('html'):
            return item['html']
        raw  = item.get('text', '')
        ents = item.get('entities', [])
        return _entities_to_html(raw, ents)
    _MEDIA_LABELS = {
        'photo': '🖼 [фото]', 'video': '📹 [видео]',
        'voice': '🎙 [голосовое]', 'video_note': '🎥 [кружок]',
        'sticker': '😊 [стикер]', 'audio': '🎵 [аудио]',
        'document': '📎 [файл]', 'animation': '🎞 [гиф]',
        'album': '🗂 [альбом]',
    }
    if t == 'album':
        n   = len(item.get('items', []))
        cap = item.get('caption', '')
        ents= item.get('entities', [])
        cap_html = _entities_to_html(cap, ents) if cap else ''
        base = f"🗂 [альбом · {n}]"
        return base + (f" {cap_html}" if cap_html else '')
    label = _MEDIA_LABELS.get(t, f'[{t}]')
    cap   = item.get('caption', '')
    ents  = item.get('entities', [])
    cap_html = _entities_to_html(cap, ents) if cap else ''
    return label + (f" {cap_html}" if cap_html else '')


def _build_telethon_entities(entities_json: list):
    from telethon.tl.types import (
        MessageEntityBold, MessageEntityItalic, MessageEntityCode,
        MessageEntityPre, MessageEntityTextUrl, MessageEntityCustomEmoji,
        MessageEntityUnderline, MessageEntityStrike, MessageEntitySpoiler,
    )
    _map = {
        'bold': MessageEntityBold,
        'italic': MessageEntityItalic,
        'code': MessageEntityCode,
        'underline': MessageEntityUnderline,
        'strikethrough': MessageEntityStrike,
        'spoiler': MessageEntitySpoiler,
    }
    result = []
    for e in (entities_json or []):
        t = e.get('type', '')
        o, l = e.get('offset', 0), e.get('length', 0)
        if t in _map:
            result.append(_map[t](offset=o, length=l))
        elif t == 'custom_emoji':
            eid = e.get('custom_emoji_id')
            if eid:
                result.append(MessageEntityCustomEmoji(offset=o, length=l, document_id=int(eid)))
        elif t == 'text_link':
            result.append(MessageEntityTextUrl(offset=o, length=l, url=e.get('url', '')))
        elif t == 'pre':
            result.append(MessageEntityPre(offset=o, length=l, language=e.get('language', '')))
    return result


def make_progress_bar(current: int, total: int, width: int = 12) -> str:
    if total <= 0:
        return f"[{'░' * width}]"
    pct    = min(current / total, 1.0)
    filled = round(width * pct)
    bar    = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {round(pct * 100)}%"


async def animate_loading(bot: "Bot", chat_id: int, msg_id: int,
                           title: str, stop_event: asyncio.Event,
                           extra: str = "") -> None:
    dots_seq = [".", "..", "..."]
    i = 0
    while not stop_event.is_set():
        dots = dots_seq[i % 3]
        i   += 1
        try:
            await bot.edit_message_text(
                f"{title}{dots}{chr(10) + extra if extra else ''}",
                chat_id=chat_id, message_id=msg_id, parse_mode='HTML'
            )
        except Exception:
            pass
        try:
            await asyncio.wait_for(asyncio.shield(stop_event.wait()), timeout=0.7)
        except asyncio.TimeoutError:
            pass


# ══════════════════════════════════════
# FSM СОСТОЯНИЯ
# ══════════════════════════════════════
class Auth(StatesGroup):
    phone    = State()
    code     = State()
    password = State()

class ARState(StatesGroup):
    trigger = State()
    content = State()
    match   = State()

class ARSchedule(StatesGroup):
    value = State()

class BroadcastState(StatesGroup):
    content   = State()
    usernames = State()

class DraftAdd(StatesGroup):
    trigger = State()
    content = State()


# ══════════════════════════════════════
# БАЗА ДАННЫХ
# ══════════════════════════════════════
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS accounts(
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id        INTEGER NOT NULL,
            phone          TEXT NOT NULL,
            session_string TEXT NOT NULL DEFAULT '',
            name           TEXT DEFAULT '',
            username       TEXT DEFAULT '',
            active         INTEGER DEFAULT 1,
            autoreply_on   INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS blacklist_cache(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id  INTEGER NOT NULL,
            peer_id     INTEGER,
            peer_name   TEXT DEFAULT '',
            peer_user   TEXT DEFAULT '',
            FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS autoreply_rules(
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id    INTEGER NOT NULL,
            trig          TEXT NOT NULL DEFAULT '',
            trigger_text  TEXT NOT NULL DEFAULT '',
            response      TEXT NOT NULL DEFAULT '',
            response_text TEXT NOT NULL DEFAULT '',
            match_type    TEXT DEFAULT 'contains',
            active        INTEGER DEFAULT 1,
            format_mode   TEXT DEFAULT 'html',
            content_json  TEXT DEFAULT '',
            buttons_json  TEXT DEFAULT '',
            schedule_start TEXT DEFAULT '',
            schedule_end   TEXT DEFAULT '',
            FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS drafts(
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id   INTEGER NOT NULL,
            trigger_text TEXT NOT NULL DEFAULT '',
            content      TEXT NOT NULL DEFAULT '',
            active       INTEGER DEFAULT 1,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS broadcast_log(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id  INTEGER NOT NULL,
            message     TEXT NOT NULL,
            total       INTEGER DEFAULT 0,
            sent        INTEGER DEFAULT 0,
            failed      INTEGER DEFAULT 0,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS tg_codes(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id  INTEGER NOT NULL,
            code        TEXT NOT NULL,
            full_text   TEXT DEFAULT '',
            received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_acc_user ON accounts(user_id);
        """)
        await db.commit()


async def db_get(q, p=()):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, p) as c:
            r = await c.fetchone()
            return dict(r) if r else None

async def db_all(q, p=()):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, p) as c:
            return [dict(r) for r in await c.fetchall()]

async def db_run(q, p=()):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(q, p)
        await db.commit()
        return c.lastrowid


# ══════════════════════════════════════
# КЛИЕНТ-МЕНЕДЖЕР
# ══════════════════════════════════════
class CM:
    def __init__(self):
        self.clients     : Dict[int, TelegramClient] = {}
        self.bot         : Optional[Bot] = None
        self._broadcasts : dict = {}
        self._ar_cooldown: Dict[str, float] = {}   # "aid:chat_id" → timestamp

    def set_bot(self, b): self.bot = b

    async def _notify_owner(self, aid: int, text: str):
        """Отправляет уведомление владельцу аккаунта через бот."""
        try:
            if not self.bot: return
            acc = await db_get("SELECT user_id, phone FROM accounts WHERE id=?", (aid,))
            if acc:
                await self.bot.send_message(
                    acc['user_id'],
                    f"🔔 <b>Аккаунт {acc['phone']}</b>\n\n{text}",
                    parse_mode='HTML'
                )
        except Exception as e:
            log.debug(f"_notify_owner: {e}")

    async def load_all(self):
        for acc in await db_all("SELECT * FROM accounts WHERE active=1"):
            try: await self.start(acc)
            except Exception as e: log.error(f"load {acc['id']}: {e}")

    async def start(self, acc) -> Optional[TelegramClient]:
        aid = acc['id']
        if aid in self.clients:
            c = self.clients[aid]
            if c.is_connected(): return c
            try: await c.disconnect()
            except: pass
        c = TelegramClient(StringSession(acc['session_string']), API_ID, API_HASH,
            device_model=_pick_device(aid)[0],
            system_version=_pick_device(aid)[1],
            app_version=_pick_device(aid)[2],
            connection_retries=5,
            retry_delay=3,
            auto_reconnect=True,
            flood_sleep_threshold=60,
        )
        await c.connect()
        if not await c.is_user_authorized():
            await db_run("UPDATE accounts SET active=0 WHERE id=?", (aid,))
            log.warning(f"Account {aid} is no longer authorized — deactivated")
            # Уведомляем владельца
            await self._notify_owner(aid, "⚠️ Аккаунт деавторизован (сессия устарела)")
            return None
        try:
            me = await c.get_me()
            if me is None:
                await db_run("UPDATE accounts SET active=0 WHERE id=?", (aid,))
                log.warning(f"Account {aid} — get_me() returned None, deactivated")
                return None
        except Exception as e:
            log.warning(f"Account {aid} get_me() failed: {e}")
            return None
        self.clients[aid] = c
        self._attach(c, acc)
        log.info(f"Client started for account {aid} ({acc['phone']})")
        return c

    def get(self, aid) -> Optional[TelegramClient]:
        c = self.clients.get(aid)
        return c if c and c.is_connected() else None

    async def stop(self, aid):
        if aid in self.clients:
            try: await self.clients.pop(aid).disconnect()
            except: pass

    def _attach(self, c: TelegramClient, acc: dict):
        aid = acc['id']

        @c.on(events.NewMessage(incoming=True))
        async def _new(ev):
            try: await self._on_new(c, ev, acc)
            except Exception as e: log.debug(f"on_new {e}")

        @c.on(events.NewMessage(outgoing=True))
        async def _outgoing(ev):
            try: await self._on_outgoing(c, ev)
            except Exception as e: log.debug(f"on_outgoing {e}")

    async def _on_new(self, c, ev, acc):
        aid = acc['id']
        msg = ev.message

        # ── перехват кодов от telegram (777000) ──
        try:
            sender = await ev.get_sender()
            if ev.is_private and getattr(sender, 'id', 0) == 777000:
                text_raw = msg.message or ''
                m = re.search(r'\b(\d{5,6})\b', text_raw)
                if m:
                    code = m.group(1)
                    await db_run(
                        "INSERT INTO tg_codes(account_id, code, full_text) VALUES(?,?,?)",
                        (aid, code, text_raw[:500])
                    )
        except: pass

        acc_row = await db_get("SELECT * FROM accounts WHERE id=?", (aid,))
        if not acc_row: return

        # ── Автоответчик ──
        if acc_row.get('autoreply_on', 0) and ev.is_private:
            try:
                sender = await ev.get_sender()
                if sender and not getattr(sender, 'bot', False):
                    await self._autoreply(c, ev, aid)
            except: pass

    async def _on_outgoing(self, c, ev):
        # ── Черновики: заменяем триггер на сохранённый контент ──
        try:
            aid = next((k for k, v in self.clients.items() if v is c), None)
            if aid is not None:
                raw = (ev.message.message or '').strip()
                if raw:
                    draft = await db_get(
                        "SELECT * FROM drafts WHERE account_id=? AND active=1 "
                        "AND LOWER(trigger_text)=LOWER(?)",
                        (aid, raw)
                    )
                    if draft:
                        import json as _json
                        try:
                            raw_content = _json.loads(draft['content'])
                            items = raw_content if isinstance(raw_content, list) else [raw_content]
                            reply_to_id = ev.message.reply_to_msg_id

                            async def _send_item(item, reply_to=None):
                                if item.get('type') == 'text':
                                    raw_text  = item.get('text', '')
                                    ents_json = item.get('entities', [])
                                    kw = {'link_preview': False}
                                    if reply_to: kw['reply_to'] = reply_to
                                    if ents_json:
                                        tl_ents = _build_telethon_entities(ents_json)
                                        await c.send_message(ev.chat_id, raw_text,
                                            formatting_entities=tl_ents, **kw)
                                    else:
                                        await c.send_message(ev.chat_id, raw_text,
                                            parse_mode='html', **kw)
                                else:
                                    await self._send_content(c, ev.chat_id, item,
                                                              reply_to=reply_to)

                            try:
                                await ev.message.delete()
                            except Exception:
                                pass
                            await asyncio.sleep(0.05)

                            target_reply = reply_to_id
                            for i, item in enumerate(items):
                                await _send_item(item, reply_to=target_reply if i == 0 else None)
                                if i < len(items) - 1:
                                    await asyncio.sleep(0.3)
                        except Exception as de:
                            log.debug(f"draft send: {de}")
                        return
        except: pass

    async def _autoreply(self, c, ev, aid):
        import json as _json
        # ── Cooldown: не отвечаем одному чату чаще AUTOREPLY_COOLDOWN_SEC ──
        ck = f"{aid}:{ev.chat_id}"
        now_ts = time.time()
        if now_ts - self._ar_cooldown.get(ck, 0) < AUTOREPLY_COOLDOWN_SEC:
            return
        self._ar_cooldown[ck] = now_ts

        rules = await db_all("SELECT * FROM autoreply_rules WHERE account_id=? AND active=1", (aid,))
        text  = (ev.message.message or '').lower()
        now_h = datetime.now().hour * 60 + datetime.now().minute
        for r in rules:
            sch_start = (r.get('schedule_start') or '').strip()
            sch_end   = (r.get('schedule_end')   or '').strip()
            if sch_start and sch_end:
                try:
                    sh, sm = map(int, sch_start.split(':'))
                    eh, em = map(int, sch_end.split(':'))
                    s_min  = sh * 60 + sm; e_min = eh * 60 + em
                    if s_min <= e_min:
                        if not (s_min <= now_h <= e_min): continue
                    else:
                        if not (now_h >= s_min or now_h <= e_min): continue
                except: pass

            raw_trig = r.get('trig') or r.get('trigger_text') or ''
            triggers = [t.strip().lower() for t in raw_trig.split('|') if t.strip()]
            m   = r.get('match_type', 'contains')
            hit = False
            for t in triggers:
                if (m == 'exact' and text == t) or \
                   (m == 'contains' and t in text) or \
                   (m == 'startswith' and text.startswith(t)):
                    hit = True
                    break
            if not hit: continue
            try:
                content_raw = r.get('content_json') or ''
                if content_raw:
                    content_data = _json.loads(content_raw)
                    items = content_data if isinstance(content_data, list) else [content_data]
                    for i, item in enumerate(items):
                        reply_to = ev.message.id if i == 0 else None
                        await self._send_content(c, ev.chat_id, item, reply_to=reply_to)
                        if i < len(items) - 1:
                            await asyncio.sleep(0.3)
                else:
                    resp = r.get('response') or r.get('response_text') or ''
                    if resp:
                        await c.send_message(ev.chat_id, resp, reply_to=ev.message.id, parse_mode='html')
            except Exception as e:
                log.debug(f"autoreply send error: {e}")
            break

    # ── Чёрный список ──
    async def get_blacklist(self, aid) -> List[dict]:
        c   = self.get(aid)
        out = []
        if c:
            try:
                result = await c(GetBlockedRequest(offset=0, limit=200))
                users  = getattr(result, 'users', []) or []
                for u in users:
                    out.append({
                        'id':       u.id,
                        'name':     ((getattr(u, 'first_name', '') or '') + ' ' + (getattr(u, 'last_name', '') or '')).strip(),
                        'username': getattr(u, 'username', '') or '',
                    })
                await db_run("DELETE FROM blacklist_cache WHERE account_id=?", (aid,))
                for r in out:
                    await db_run(
                        "INSERT INTO blacklist_cache(account_id,peer_id,peer_name,peer_user) VALUES(?,?,?,?)",
                        (aid, r['id'], r['name'], r['username'])
                    )
            except Exception as e:
                log.warning(f"get_blacklist live failed: {e}, using cache")
                rows = await db_all("SELECT * FROM blacklist_cache WHERE account_id=?", (aid,))
                out  = [{'id': r['peer_id'], 'name': r['peer_name'] or '—', 'username': r['peer_user'] or ''} for r in rows]
        else:
            rows = await db_all("SELECT * FROM blacklist_cache WHERE account_id=?", (aid,))
            out  = [{'id': r['peer_id'], 'name': r['peer_name'] or '—', 'username': r['peer_user'] or ''} for r in rows]
        return out

    async def unblock_user(self, aid, user_id) -> bool:
        c = self.get(aid)
        if not c: return False
        try:
            await c(UnblockRequest(id=user_id))
            await db_run("DELETE FROM blacklist_cache WHERE account_id=? AND peer_id=?", (aid, user_id))
            return True
        except: return False

    async def clear_bl(self, aid) -> int:
        c = self.get(aid)
        if not c: return 0
        n = 0
        try:
            result = await c(GetBlockedRequest(offset=0, limit=200))
            users  = getattr(result, 'users', []) or []
            for u in users:
                try:
                    await c(UnblockRequest(id=u.id))
                    n += 1
                    await asyncio.sleep(0.4)
                except: pass
            await db_run("DELETE FROM blacklist_cache WHERE account_id=?", (aid,))
        except Exception as e:
            log.error(f"clear_bl {e}")
        return n

    # ── Отправка контента ──
    async def _send_content(self, c, entity, content: dict, reply_to=None) -> None:
        ctype   = content.get('type', 'text')
        caption = content.get('caption', '') or ''
        extra   = {}
        if reply_to: extra['reply_to'] = reply_to
        if ctype == 'text':
            raw_text = content.get('text', '')
            ents_json = content.get('entities', [])
            if ents_json:
                tl_entities = _build_telethon_entities(ents_json)
                await c.send_message(entity, raw_text,
                                     formatting_entities=tl_entities,
                                     link_preview=False, **extra)
            else:
                await c.send_message(entity, raw_text, parse_mode='html',
                                     link_preview=False, **extra)
            return
        if ctype == 'album':
            items = content.get('items', [])
            if not items: return
            files = []
            cap   = content.get('caption', '') or ''
            for i, it in enumerate(items):
                bio = BytesIO()
                await self.bot.download(it['file_id'], destination=bio)
                bio.seek(0)
                bio.name = it.get('filename', 'file')
                files.append(bio)
            await c.send_file(entity, files,
                              caption=cap or None,
                              parse_mode='html', **extra)
            return
        bio = BytesIO()
        await self.bot.download(content['file_id'], destination=bio)
        bio.seek(0)
        bio.name = content.get('filename', 'file')
        if ctype == 'sticker':
            await c.send_file(entity, bio, **extra)
        elif ctype == 'video_note':
            await c.send_file(entity, bio, video_note=True, **extra)
        elif ctype == 'voice':
            await c.send_file(entity, bio, voice_note=True,
                              caption=caption, parse_mode='html', **extra)
        else:
            await c.send_file(entity, bio, caption=caption, parse_mode='html', **extra)

    async def broadcast(self, aid: int, user_id: int, usernames: List[str],
                        items: list, progress_cb=None) -> dict:
        c = self.get(aid)
        if not c:  return {'sent': 0, 'failed': 0, 'errors': ['клиент не активен']}
        if not self.bot: return {'sent': 0, 'failed': 0, 'errors': ['bot не задан']}

        # ── Дневной лимит ──
        today = datetime.now().strftime('%Y-%m-%d')
        day_row = await db_get(
            "SELECT COALESCE(SUM(sent),0) as s FROM broadcast_log "
            "WHERE account_id=? AND date(created_at)=?", (aid, today)
        )
        day_sent = (day_row or {}).get('s', 0) or 0
        if day_sent >= BROADCAST_DAY_LIMIT:
            return {
                'sent': 0, 'failed': 0, 'total': len(usernames),
                'errors': [f"Дневной лимит {BROADCAST_DAY_LIMIT} сообщений исчерпан. "
                           f"Уже отправлено сегодня: {day_sent}"]
            }
        allowed = BROADCAST_DAY_LIMIT - day_sent
        if len(usernames) > allowed:
            usernames = usernames[:allowed]
            log.info(f"Broadcast aid={aid}: обрезано до {allowed} (дневной лимит)")

        total  = len(usernames)
        sent   = 0
        failed = 0
        errors = []
        self._broadcasts[aid] = {'running': True}

        if isinstance(items, dict):
            items = [items]

        preview = items[0].get('text') or items[0].get('caption') or f"[{items[0].get('type','?')}]" if items else "?"
        log_id = await db_run(
            "INSERT INTO broadcast_log(account_id,message,total) VALUES(?,?,?)",
            (aid, preview[:500], total)
        )

        _ACTION_MAP = {
            'text': 'typing', 'photo': 'upload-photo',
            'video': 'upload-video', 'voice': 'record-audio',
            'video_note': 'record-round', 'audio': 'upload-audio',
            'document': 'upload-document', 'sticker': 'typing',
            'animation': 'upload-video', 'album': 'upload-photo',
        }

        for i, uname in enumerate(usernames):
            if not self._broadcasts.get(aid, {}).get('running', True):
                errors.append("рассылка остановлена")
                break
            uname = uname.strip().lstrip('@')
            if not uname: continue
            try:
                # Имитируем онлайн перед отправкой
                try:
                    await c(UpdateStatusRequest(offline=False))
                except Exception:
                    pass

                entity = await c.get_entity(uname)
                for item in items:
                    act = _ACTION_MAP.get(item.get('type', 'text'), 'typing')
                    # Реалистичная «печать» 2-4 секунды
                    async with c.action(entity, act):
                        await asyncio.sleep(random.uniform(2.0, 4.5))
                    await self._send_content(c, entity, item)
                    if len(items) > 1:
                        await asyncio.sleep(random.uniform(2.0, 6.0))
                sent += 1
                await db_run("UPDATE broadcast_log SET sent=? WHERE id=?", (sent, log_id))
                if progress_cb:
                    await progress_cb(i + 1, total, sent, failed)

                # ── Длинная пауза каждые BROADCAST_BATCH_SIZE сообщений ──
                if sent % BROADCAST_BATCH_SIZE == 0:
                    batch_pause = random.uniform(BROADCAST_BATCH_PAUSE_MIN, BROADCAST_BATCH_PAUSE_MAX)
                    log.info(f"Broadcast batch pause {batch_pause:.0f}s after {sent} sent")
                    if progress_cb:
                        await progress_cb(i + 1, total, sent, failed,
                                          status=f"Пауза {batch_pause:.0f}s (защита от бана)...")
                    await asyncio.sleep(batch_pause)
                else:
                    # Базовая задержка между сообщениями — человекоподобная
                    delay = random.uniform(BROADCAST_DELAY_MIN, BROADCAST_DELAY_MAX)
                    await asyncio.sleep(delay)

            except FloodWaitError as e:
                wait = e.seconds + random.randint(15, 40)
                log.warning(f"broadcast FloodWait {wait}s")
                if progress_cb:
                    await progress_cb(i + 1, total, sent, failed,
                                      status=f"FloodWait, жду {wait}s...")
                await asyncio.sleep(wait)
                try:
                    entity = await c.get_entity(uname)
                    for item in items:
                        act = _ACTION_MAP.get(item.get('type', 'text'), 'typing')
                        async with c.action(entity, act):
                            await asyncio.sleep(random.uniform(2.0, 4.0))
                        await self._send_content(c, entity, item)
                        if len(items) > 1:
                            await asyncio.sleep(random.uniform(2.0, 6.0))
                    sent += 1
                except Exception as e2:
                    failed += 1; errors.append(f"@{uname}: {e2}")
            except PeerFloodError:
                errors.append("PeerFlood — Telegram ограничил рассылку. Рассылка остановлена.")
                log.error(f"PeerFloodError on aid={aid}, stopping broadcast")
                await self._notify_owner(
                    aid,
                    "🚨 <b>Рассылка остановлена</b>\n\n"
                    "Telegram выдал PeerFloodError.\n"
                    "Дайте аккаунту отдохнуть минимум 24 часа перед следующей рассылкой."
                )
                break
            except (UserPrivacyRestrictedError, UserIsBlockedError):
                failed += 1
                errors.append(f"@{uname}: приватность/заблокирован")
            except InputUserDeactivatedError:
                failed += 1
                errors.append(f"@{uname}: аккаунт удалён")
            except Exception as e:
                failed += 1
                errors.append(f"@{uname}: {e}")

        await db_run(
            "UPDATE broadcast_log SET sent=?, failed=? WHERE id=?",
            (sent, failed, log_id)
        )
        self._broadcasts.pop(aid, None)
        return {'sent': sent, 'failed': failed, 'total': total, 'errors': errors}

    def stop_broadcast(self, aid: int):
        if aid in self._broadcasts:
            self._broadcasts[aid]['running'] = False


cm = CM()

# ── Пул fingerprint-ов устройств (anti-fingerprint) ──
_DEVICE_POOL = [
    ("Samsung Galaxy S24 Ultra",  "Android 14", "10.14.0"),
    ("Samsung Galaxy S23",        "Android 13", "10.9.1"),
    ("Google Pixel 8 Pro",        "Android 14", "10.14.0"),
    ("Xiaomi 14 Pro",             "Android 14", "10.13.1"),
    ("OnePlus 12",                "Android 14", "10.12.4"),
    ("Samsung Galaxy A54",        "Android 13", "10.8.1"),
    ("Google Pixel 7a",           "Android 13", "10.10.0"),
]

def _pick_device(account_id: int) -> tuple:
    """Детерминировано выбирает fingerprint по id аккаунта."""
    return _DEVICE_POOL[account_id % len(_DEVICE_POOL)]

# ══════════════════════════════════════
# UI HELPERS
# ══════════════════════════════════════
def kb(*rows) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=list(rows))

def b(text, data) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)

def paginate(items, page, per=6):
    total = len(items)
    pages = max(1, (total + per - 1) // per)
    page  = max(0, min(page, pages - 1))
    return items[page*per:(page+1)*per], page, pages

def nav(page, pages, pfx):
    row = []
    if page > 0:          row.append(b("◀️", f"{pfx}:{page-1}"))
    if page < pages - 1: row.append(b("▶️", f"{pfx}:{page+1}"))
    return row

async def edit(cb: CallbackQuery, text: str, markup: InlineKeyboardMarkup):
    try:
        await cb.message.edit_text(text, reply_markup=markup, parse_mode='HTML')
    except TelegramBadRequest:
        pass
    await cb.answer()

async def delete_user_msg(msg: Message):
    try: await msg.delete()
    except: pass


# ══════════════════════════════════════
# КОНТЕНТ-ХЕЛПЕРЫ (черновики / рассылка / автоответ)
# ══════════════════════════════════════
_DRAFT_ICONS = {
    'text': '✏️', 'photo': '🖼', 'video': '📹', 'voice': '🎙',
    'video_note': '🎥', 'sticker': '😊', 'audio': '🎵',
    'document': '📎', 'animation': '🎞', 'album': '🗂',
}
_DRAFT_LABELS = {
    'text': None, 'photo': '[фото]', 'video': '[видео]',
    'voice': '[голосовое]', 'video_note': '[кружок]',
    'sticker': '[стикер]', 'audio': '[аудио]',
    'document': '[файл]', 'animation': '[гиф]', 'album': '[альбом]',
}
_DRAFT_PAGE = 4


def _draft_summary(content_json_str: str) -> tuple:
    import json as _j
    try:
        raw = _j.loads(content_json_str)
        items = raw if isinstance(raw, list) else [raw]
    except Exception:
        return '📄', '', 1
    count = len(items)
    parts = []
    for it in items:
        line = _content_item_html(it)
        parts.append(f"• {line[:300]}" if line else "• (пусто)")
    icon = _DRAFT_ICONS.get(items[0].get('type', ''), '📄') if items else '📄'
    return icon, '\n'.join(parts), count


def _msg_to_content(msg: Message) -> dict:
    ents = _serialize_entities(msg)
    if msg.text:
        return {'type': 'text', 'text': msg.text, 'entities': ents,
                'html': msg.html_text or msg.text}
    if msg.photo:
        cap_html = msg.html_text if msg.caption else ''
        return {'type': 'photo', 'file_id': msg.photo[-1].file_id,
                'caption': msg.caption or '', 'entities': ents, 'filename': 'photo.jpg',
                'html': cap_html}
    if msg.video:
        cap_html = msg.html_text if msg.caption else ''
        return {'type': 'video', 'file_id': msg.video.file_id,
                'caption': msg.caption or '', 'entities': ents, 'filename': 'video.mp4',
                'html': cap_html}
    if msg.video_note:
        return {'type': 'video_note', 'file_id': msg.video_note.file_id, 'filename': 'vnote.mp4'}
    if msg.voice:
        return {'type': 'voice', 'file_id': msg.voice.file_id,
                'caption': msg.caption or '', 'filename': 'voice.ogg'}
    if msg.audio:
        return {'type': 'audio', 'file_id': msg.audio.file_id,
                'caption': msg.caption or '', 'entities': ents,
                'filename': msg.audio.file_name or 'audio.mp3'}
    if msg.sticker:
        return {'type': 'sticker', 'file_id': msg.sticker.file_id, 'filename': 'sticker.webp'}
    if msg.animation:
        return {'type': 'animation', 'file_id': msg.animation.file_id,
                'caption': msg.caption or '', 'entities': ents, 'filename': 'anim.gif'}
    if msg.document:
        return {'type': 'document', 'file_id': msg.document.file_id,
                'caption': msg.caption or '', 'entities': ents,
                'filename': msg.document.file_name or 'file'}
    return {}


# ══════════════════════════════════════
# РОУТЕР И БЕЗОПАСНОСТЬ
# ══════════════════════════════════════
router = Router()
_auth_clients: dict = {}

from typing import Callable, Any

_rate_counters: dict = defaultdict(lambda: deque())
_RATE_LIMIT_MSGS = 20
_RATE_LIMIT_WINDOW = 10

def _check_rate_limit(user_id: int) -> bool:
    now = time.time()
    q = _rate_counters[user_id]
    q.append(now)
    while q and now - q[0] > _RATE_LIMIT_WINDOW:
        q.popleft()
    return len(q) <= _RATE_LIMIT_MSGS

def _is_allowed(user_id: int) -> bool:
    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        return False
    return True

class AccessGuardMiddleware(BaseMiddleware):
    async def __call__(self, handler: Callable, event: Any, data: dict) -> Any:
        user = getattr(event, 'from_user', None)
        if not user: return
        uid = user.id
        if not _is_allowed(uid):
            log.warning(f"Unauthorized access from user_id={uid}")
            return
        if not _check_rate_limit(uid):
            if isinstance(event, CallbackQuery):
                try: await event.answer("Слишком много запросов, подождите", show_alert=True)
                except: pass
            elif isinstance(event, Message):
                try: await event.answer("Слишком много запросов, подождите немного")
                except: pass
            return
        return await handler(event, data)


# ══════════════════════════════════════
# ГЛАВНОЕ МЕНЮ
# ══════════════════════════════════════
MAIN_TEXT = "🤖 <b>мои аккаунты</b>\n\nуправляй аккаунтами в одном месте"

def main_kb():
    return kb([b("Мои аккаунты", "accs:0"), b("Добавить", "add_acc")])


@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    if msg.from_user.id in _auth_clients:
        try: await _auth_clients.pop(msg.from_user.id).disconnect()
        except: pass
    accs = await db_all("SELECT id FROM accounts WHERE user_id=? AND active=1", (msg.from_user.id,))
    txt  = MAIN_TEXT + f"\n\n<b>аккаунтов:</b> {len(accs)}"
    data = await state.get_data()
    if data.get('menu_id'):
        try: await msg.bot.delete_message(msg.chat.id, data['menu_id'])
        except: pass
    sent = await msg.answer(txt, reply_markup=main_kb(), parse_mode='HTML')
    await state.update_data(menu_id=sent.message_id)

@router.callback_query(F.data == "main")
async def cb_main(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    if cb.from_user.id in _auth_clients:
        try: await _auth_clients.pop(cb.from_user.id).disconnect()
        except: pass
    accs = await db_all("SELECT id FROM accounts WHERE user_id=? AND active=1", (cb.from_user.id,))
    txt  = MAIN_TEXT + f"\n\n<b>аккаунтов:</b> {len(accs)}"
    await edit(cb, txt, main_kb())

@router.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery): await cb.answer()


# ══════════════════════════════════════
# СПИСОК АККАУНТОВ
# ══════════════════════════════════════
@router.callback_query(F.data.startswith("accs:"))
async def cb_accs(cb: CallbackQuery):
    page = int(cb.data.split(":")[1])
    accs = await db_all("SELECT * FROM accounts WHERE user_id=? AND active=1", (cb.from_user.id,))
    if not accs:
        await edit(cb,
            "👥 <b>мои аккаунты</b>\n\nнет аккаунтов — добавьте первый",
            kb([b("Добавить", "add_acc")], [b("Главная", "main")])
        ); return
    chunk, page, pages = paginate(accs, page, 5)
    rows = []
    for a in chunk:
        online = "●" if cm.get(a['id']) else "○"
        label  = f"{online} {a['name'] or a['phone']}".strip()
        rows.append([b(label, f"acc:{a['id']}")])
    if pages > 1: rows.append(nav(page, pages, "accs"))
    rows.append([b("Добавить", "add_acc"), b("Главная", "main")])
    await edit(cb, f"<b>Мои аккаунты</b>  ·  {len(accs)} шт", kb(*rows))


# ══════════════════════════════════════
# МЕНЮ АККАУНТА
# ══════════════════════════════════════
@router.callback_query(F.data.startswith("acc:"))
async def cb_acc(cb: CallbackQuery):
    aid = int(cb.data.split(":")[1])
    acc = await db_get("SELECT * FROM accounts WHERE id=? AND user_id=?", (aid, cb.from_user.id))
    if not acc: await cb.answer(); return
    st = "Онлайн" if cm.get(aid) else "Офлайн"
    ar = "✅" if acc.get('autoreply_on') else "☐"
    await edit(cb,
        f"<b>{acc['name'] or acc['phone']}</b>\n\n"
        f"<code>{acc['phone']}</code>  ·  @{acc['username'] or '—'}\n"
        f"{st}",
        kb(
            [b(f"Автоответчик {ar}", f"ar:{aid}:menu"), b("Черновики", f"drafts:{aid}:list:0")],
            [b("Рассылка", f"broadcast:{aid}:menu"), b("Чёрный список", f"bl:{aid}:0")],
            [b("Последний код", f"tgcode:{aid}")],
            [b("Выйти", f"rm:{aid}"), b("Назад", "accs:0")]
        )
    )


# ══════════════════════════════════════
# ДОБАВЛЕНИЕ АККАУНТА
# ══════════════════════════════════════
@router.callback_query(F.data == "add_acc")
async def cb_add(cb: CallbackQuery, state: FSMContext):
    await state.set_state(Auth.phone)
    await state.update_data(msg_id=cb.message.message_id, chat_id=cb.message.chat.id)
    await edit(cb,
        "<b>Добавление аккаунта</b>\n\nВведите номер телефона:\n<code>+79991234567</code>",
        kb([b("Отмена", "main")])
    )

@router.message(Auth.phone)
async def auth_phone(msg: Message, state: FSMContext):
    await delete_user_msg(msg)
    data  = await state.get_data()
    mid   = data.get('msg_id')
    cid   = data.get('chat_id', msg.chat.id)
    raw   = msg.text.strip() if msg.text else ""
    phone = re.sub(r'[\s\-().]', '', raw)
    if not phone.startswith('+'): phone = '+' + phone
    if re.match(r'^\+8\d{10}$', phone):
        phone = '+7' + phone[2:]

    async def upd(text, markup=kb()):
        try: await msg.bot.edit_message_text(text, chat_id=cid, message_id=mid,
                                              reply_markup=markup, parse_mode='HTML')
        except: pass

    if not re.match(r'^\+\d{7,15}$', phone):
        await upd(
            "➕ <b>добавление аккаунта</b>\n\n"
            "Неверный формат\nпример: <code>+79991234567</code>",
            kb([b("Отмена", "main")])
        ); return

    await upd("<b>Добавление аккаунта</b>\n\n🔌 подключаюсь к Telegram...")
    await asyncio.sleep(0.5)

    client = TelegramClient(
        StringSession(), API_ID, API_HASH,
        device_model="Samsung Galaxy S24",
        system_version="Android 14",
        app_version="10.14.0",
        lang_code="ru",
        system_lang_code="ru-RU",
        connection_retries=5,
        retry_delay=3,
        flood_sleep_threshold=60,
    )
    try:
        await client.connect()
        await upd(f"<b>Добавление аккаунта</b>\n\n📱 <code>{phone}</code>\n\n📤 отправляю код...")
        await asyncio.sleep(0.4)
        result = await client.send_code_request(phone)
        _auth_clients[msg.from_user.id] = client
        await state.update_data(phone=phone, hash=result.phone_code_hash, entered="")
        await state.set_state(Auth.code)
        text, markup = _code_view(phone, "")
        await upd(text, markup)
    except FloodWaitError as e:
        try: await client.disconnect()
        except: pass

        from datetime import timedelta
        MSK = timezone(timedelta(hours=3))

        def _fmt_flood(seconds_left: int) -> str:
            ready_at = datetime.now(MSK) + timedelta(seconds=seconds_left)
            ready_str = ready_at.strftime("%H:%M:%S")
            if seconds_left < 60:
                human = f"{seconds_left} сек"
            elif seconds_left < 3600:
                m, s = divmod(seconds_left, 60)
                human = f"{m} мин {s} сек" if s else f"{m} мин"
            else:
                h, rem = divmod(seconds_left, 3600)
                m = rem // 60
                human = f"{h} ч {m} мин" if m else f"{h} ч"
            return (
                f"<b>Слишком много запросов</b>\n\n"
                f"осталось ждать: <b>{human}</b>\n"
                f"попробовать можно в: <b>{ready_str}</b>"
            )

        total = e.seconds
        flood_markup = kb([b("Назад", "main")])
        await upd(_fmt_flood(total), flood_markup)

        async def _flood_autoretry(seconds_total: int):
            import time as _time
            _start = _time.monotonic()
            while True:
                await asyncio.sleep(10)
                _elapsed = int(_time.monotonic() - _start)
                _left = seconds_total - _elapsed
                if _left <= 0: break
                try: await upd(_fmt_flood(_left), flood_markup)
                except: pass
            await asyncio.sleep(2)
            try:
                await upd(f"<b>Добавление аккаунта</b>\n\n🔄 повторяю отправку кода...")
                retry_client = TelegramClient(
                    StringSession(), API_ID, API_HASH,
                    device_model="Galaxy S25 Ultra",
                    system_version="Android 15",
                    app_version="12.4.3",
                    lang_code="ru",
                    system_lang_code="ru-RU",
                )
                await retry_client.connect()
                retry_result = await retry_client.send_code_request(phone)
                _auth_clients[cid] = retry_client
                await state.update_data(phone=phone, hash=retry_result.phone_code_hash, entered="")
                await state.set_state(Auth.code)
                text, markup = _code_view(phone, "")
                await upd(text, markup)
            except FloodWaitError as e2:
                await upd(_fmt_flood(e2.seconds), flood_markup)
                asyncio.create_task(_flood_autoretry(e2.seconds))
            except Exception as ex:
                await upd(f"Ошибка при повторной отправке: <code>{ex}</code>", kb([b("Назад", "main")]))

        asyncio.create_task(_flood_autoretry(total))
    except PhoneNumberInvalidError:
        try: await client.disconnect()
        except: pass
        await upd("Неверный номер телефона", kb([b("Назад", "main")]))
    except Exception as e:
        try: await client.disconnect()
        except: pass
        await upd(f"Ошибка подключения: <code>{e}</code>", kb([b("Назад", "main")]))


def _code_view(phone: str, code: str):
    slots  = [f"<b>{code[i]}</b>" if i < len(code) else "–" for i in range(5)]
    visual = "  ".join(slots)
    text   = (
        f"<b>Введите код из Telegram</b>\n\n"
        f"<code>{phone}</code>\n\n"
        f"┌ {visual} ┐\n"
        f"Код придёт в приложении Telegram"
    )
    markup = kb(
        [b("1","cd:1"), b("2","cd:2"), b("3","cd:3")],
        [b("4","cd:4"), b("5","cd:5"), b("6","cd:6")],
        [b("7","cd:7"), b("8","cd:8"), b("9","cd:9")],
        [b("⌫","cd:⌫"), b("0","cd:0"), b("✅","cd:✅")],
        [b("Отмена","main")]
    )
    return text, markup


@router.callback_query(F.data.startswith("cd:"), Auth.code)
async def auth_code_btn(cb: CallbackQuery, state: FSMContext):
    digit = cb.data.split(":")[1]
    data  = await state.get_data()
    code  = data.get('entered', '')
    phone = data.get('phone', '')
    if digit == '⌫':
        code = code[:-1]
    elif digit == '✅':
        if len(code) < 5:
            await cb.answer("Введите 5 цифр")
            return
        await cb.answer()
        uid = cb.from_user.id
        client = _auth_clients.get(uid)
        if not client:
            await edit(cb, "Сессия потеряна, начните заново", kb([b("Назад", "main")]))
            await state.clear()
            return
        mid = data.get('msg_id')
        cid = data.get('chat_id', cb.message.chat.id)

        async def upd(text, markup=kb()):
            try: await cb.bot.edit_message_text(text, chat_id=cid, message_id=mid,
                                                 reply_markup=markup, parse_mode='HTML')
            except: pass

        try:
            await upd("<b>Проверяю код...</b>")
            await client.sign_in(phone, code, phone_code_hash=data['hash'])
            me = await client.get_me()
            ss = client.session.save()
            _auth_clients.pop(uid, None)
            aid = await db_run(
                "INSERT INTO accounts(user_id,phone,session_string,name,username) VALUES(?,?,?,?,?)",
                (uid, phone, ss, me.first_name or '', me.username or '')
            )
            acc = await db_get("SELECT * FROM accounts WHERE id=?", (aid,))
            await cm.start(acc)
            await upd(
                f"<b>Аккаунт добавлен</b>\n\n"
                f"<b>{me.first_name or ''}</b>\n<code>+{me.phone}</code>",
                kb([b("Управление", f"acc:{aid}")], [b("Главная", "main")])
            )
        except SessionPasswordNeededError:
            await state.set_state(Auth.password)
            await upd(
                f"<b>Двухфакторная защита</b>\n\n📱 <code>{phone}</code>\n\n"
                f"Введите облачный пароль:",
                kb([b("Отмена", "main")])
            )
        except PhoneCodeInvalidError:
            await state.update_data(entered="")
            text, markup = _code_view(phone, "")
            await upd(f"Неверный код\n\n{text}", markup)
        except Exception as e:
            _auth_clients.pop(uid, None)
            try: await client.disconnect()
            except: pass
            await upd(f"❌ ошибка: <code>{e}</code>", kb([b("Назад", "main")]))
            await state.clear()
        return
    else:
        if len(code) < 5:
            code += digit
    await state.update_data(entered=code)
    text, markup = _code_view(phone, code)
    try:
        await cb.message.edit_text(text, reply_markup=markup, parse_mode='HTML')
    except TelegramBadRequest:
        pass
    await cb.answer()


@router.message(Auth.password)
async def auth_password(msg: Message, state: FSMContext):
    await delete_user_msg(msg)
    data   = await state.get_data()
    mid    = data.get('msg_id')
    cid    = data.get('chat_id', msg.chat.id)
    phone  = data.get('phone', '')
    uid    = msg.from_user.id
    client = _auth_clients.get(uid)
    if not client:
        try: await msg.bot.edit_message_text("Сессия потеряна",
                                              chat_id=cid, message_id=mid,
                                              reply_markup=kb([b("Назад", "main")]),
                                              parse_mode='HTML')
        except: pass
        await state.clear(); return

    async def upd(text, markup=kb()):
        try: await msg.bot.edit_message_text(text, chat_id=cid, message_id=mid,
                                              reply_markup=markup, parse_mode='HTML')
        except: pass

    pwd = (msg.text or '').strip()
    try:
        await upd("<b>Проверяю пароль...</b>")
        await client.sign_in(password=pwd)
        me = await client.get_me()
        ss = client.session.save()
        _auth_clients.pop(uid, None)
        await state.clear()
        aid = await db_run(
            "INSERT INTO accounts(user_id,phone,session_string,name,username) VALUES(?,?,?,?,?)",
            (uid, phone, ss, me.first_name or '', me.username or '')
        )
        acc = await db_get("SELECT * FROM accounts WHERE id=?", (aid,))
        await cm.start(acc)
        await upd(
            f"<b>Аккаунт добавлен</b>\n\n"
            f"<b>{me.first_name or ''}</b>\n<code>+{me.phone}</code>",
            kb([b("Управление", f"acc:{aid}")], [b("Главная", "main")])
        )
    except Exception as e:
        try: await client.disconnect()
        except: pass
        await upd(
            f"<b>Двухфакторная защита</b>\n\nНеверный пароль\n\nвведите снова:",
            kb([b("Отмена", "main")])
        )


# ══════════════════════════════════════
# ВЫХОД ИЗ АККАУНТА
# ══════════════════════════════════════
@router.callback_query(F.data.startswith("rm:"))
async def cb_rm(cb: CallbackQuery):
    parts = cb.data.split(":")
    if parts[1] == "ok":
        aid = int(parts[2])
        acc = await db_get("SELECT * FROM accounts WHERE id=? AND user_id=?", (aid, cb.from_user.id))
        if not acc: return
        await cm.stop(aid)
        await db_run("UPDATE accounts SET active=0 WHERE id=?", (aid,))
        await edit(cb,
            f"Выход выполнен из <code>{acc['phone']}</code>",
            kb([b("Аккаунты", "accs:0"), b("Главная", "main")])
        ); return
    aid = int(parts[1])
    acc = await db_get("SELECT * FROM accounts WHERE id=? AND user_id=?", (aid, cb.from_user.id))
    if not acc: await cb.answer(); return
    await edit(cb,
        f"<b>Выйти из аккаунта?</b>\n\n📱 <code>{acc['phone']}</code>\n\nДанные сохранятся",
        kb([b("Да, выйти", f"rm:ok:{aid}"), b("Отмена", f"acc:{aid}")])
    )


# ══════════════════════════════════════
# ЧЁРНЫЙ СПИСОК
# ══════════════════════════════════════
@router.callback_query(F.data.startswith("bl:"))
async def cb_bl(cb: CallbackQuery):
    await cb.answer()
    parts = cb.data.split(":")

    if parts[1] == "clr":
        aid = int(parts[2])
        acc = await db_get("SELECT * FROM accounts WHERE id=? AND user_id=?", (aid, cb.from_user.id))
        if not acc: return
        stop = asyncio.Event()
        asyncio.create_task(animate_loading(cb.bot, cb.message.chat.id, cb.message.message_id,
                                            "<b>Очищаю чёрный список</b>", stop))
        n = await cm.clear_bl(aid)
        stop.set()
        await edit(cb, f"Разблокировано <b>{n}</b> пользователей",
                   kb([b("Чёрный список", f"bl:{aid}:0"), b("Назад", f"acc:{aid}")])); return

    if parts[1] == "unblock":
        aid = int(parts[2]); uid = int(parts[3])
        idx = int(parts[4]) if len(parts) > 4 else 0
        acc = await db_get("SELECT * FROM accounts WHERE id=? AND user_id=?", (aid, cb.from_user.id))
        if not acc: return
        await cm.unblock_user(aid, uid)
        bl = await cm.get_blacklist(aid)
        if not bl:
            await edit(cb, "<b>Чёрный список</b>\n\nпуст — никто не заблокирован",
                       kb([b("Обновить", f"bl:{aid}:0"), b("Назад", f"acc:{aid}")])); return
        per   = 4
        pages = max(1, (len(bl) + per - 1) // per)
        page  = min(idx, pages - 1)
        await _show_bl_slide(cb, aid, bl, page); return

    aid  = int(parts[1])
    idx  = int(parts[2]) if len(parts) > 2 else 0
    acc  = await db_get("SELECT * FROM accounts WHERE id=? AND user_id=?", (aid, cb.from_user.id))
    if not acc: return
    stop = asyncio.Event()
    asyncio.create_task(animate_loading(cb.bot, cb.message.chat.id, cb.message.message_id,
                                        "<b>Загружаю чёрный список</b>", stop))
    bl = await cm.get_blacklist(aid)
    stop.set()
    if not bl:
        await edit(cb, "<b>Чёрный список</b>\n\nпуст — никто не заблокирован",
                   kb([b("Обновить", f"bl:{aid}:0"), b("Назад", f"acc:{aid}")])); return
    await _show_bl_slide(cb, aid, bl, idx)


async def _show_bl_slide(cb: CallbackQuery, aid: int, bl: list, page: int):
    per   = 4
    total = len(bl)
    pages = max(1, (total + per - 1) // per)
    page  = max(0, min(page, pages - 1))
    chunk = bl[page * per : (page + 1) * per]

    page_txt = f"  ·  {page+1}/{pages}" if pages > 1 else ""
    lines    = [f"<b>Чёрный список</b>  ·  {total} чел{page_txt}\n"]
    for u in chunk:
        tag  = f"@{u['username']}" if u['username'] else f"id:{u['id']}"
        name = (u['name'] or '—').strip()
        lines.append(f"<b>{name}</b>  <code>{tag}</code>")

    rows = []
    for u in chunk:
        name_s = (u['name'] or '—').strip()[:18]
        rows.append([b(f"Разблокировать {name_s}", f"bl:unblock:{aid}:{u['id']}:{page}")])

    nav_row = []
    if page > 0:       nav_row.append(b("◀️", f"bl:{aid}:{page - 1}"))
    if page < pages-1: nav_row.append(b("▶️", f"bl:{aid}:{page + 1}"))
    if nav_row: rows.append(nav_row)

    rows.append([b("Очистить всё", f"bl:clr:{aid}"), b("Назад", f"acc:{aid}")])
    await edit(cb, "\n".join(lines), kb(*rows))


# ══════════════════════════════════════
# АВТООТВЕТЧИК
# ══════════════════════════════════════
async def _ar_menu(cb, aid, acc):
    rules = await db_all("SELECT id FROM autoreply_rules WHERE account_id=?", (aid,))
    st    = "✅" if acc.get('autoreply_on') else "☐"
    await edit(cb,
        f"<b>Автоответчик</b>\n\nПравил: <b>{len(rules)}</b>",
        kb(
            [b(f"Вкл / выкл {st}", f"ar:{aid}:toggle")],
            [b("Правила", f"ar:{aid}:list:0"), b("Добавить", f"ar:{aid}:add")],
            [b("Назад", f"acc:{aid}")]
        )
    )


@router.callback_query(F.data.startswith("ar:"))
async def cb_ar(cb: CallbackQuery, state: FSMContext):
    parts  = cb.data.split(":")
    aid    = int(parts[1])
    action = parts[2] if len(parts) > 2 else "menu"
    extra  = parts[3] if len(parts) > 3 else "0"
    acc    = await db_get("SELECT * FROM accounts WHERE id=? AND user_id=?", (aid, cb.from_user.id))
    if not acc: await cb.answer(); return

    if action == "menu":
        await _ar_menu(cb, aid, acc)

    elif action == "toggle":
        new = 0 if acc.get('autoreply_on', 0) else 1
        await db_run("UPDATE accounts SET autoreply_on=? WHERE id=?", (new, aid))
        acc['autoreply_on'] = new
        await cb.answer()
        await _ar_menu(cb, aid, acc)

    elif action == "list":
        page  = int(extra)
        rules = await db_all("SELECT * FROM autoreply_rules WHERE account_id=?", (aid,))
        if not rules:
            await edit(cb, "Правил пока нет",
                       kb([b("Добавить", f"ar:{aid}:add")],
                          [b("Назад", f"ar:{aid}:menu")])); return
        chunk, page, pages = paginate(rules, page, 5)
        lines = [f"<b>Правила</b>  ·  {len(rules)} шт\n"]
        rows  = []
        for r in chunk:
            st2  = "✅" if r['active'] else "❌"
            raw_trig = (r.get('trig') or r.get('trigger_text') or '?')
            trigs = [t.strip() for t in raw_trig.split('|') if t.strip()]
            trig_display = ' | '.join(trigs)[:25]
            import json as _jl
            cj = r.get('content_json') or ''
            count_label = ''
            if cj:
                try:
                    items_l = _jl.loads(cj)
                    if isinstance(items_l, list) and len(items_l) > 1:
                        count_label = f" · {len(items_l)} сообщ"
                except: pass
            sch  = ""
            if r.get('schedule_start') and r.get('schedule_end'):
                sch = f" {r['schedule_start']}-{r['schedule_end']}"
            lines.append(f"{st2} <code>{trig_display}</code>{count_label}{sch}")
            rows.append([
                b(f"Удалить {trig_display[:15]}", f"ar:{aid}:del:{r['id']}"),
                b(f"Расписание", f"ar:{aid}:sched:{r['id']}")
            ])
        if pages > 1: rows.append(nav(page, pages, f"ar:{aid}:list"))
        rows.append([b("Добавить", f"ar:{aid}:add"), b("Назад", f"ar:{aid}:menu")])
        await edit(cb, "\n".join(lines), kb(*rows))

    elif action == "del":
        rid = int(extra)
        await db_run("DELETE FROM autoreply_rules WHERE id=? AND account_id=?", (rid, aid))
        await cb.answer()
        cb.data = f"ar:{aid}:list:0"
        await cb_ar(cb, state)

    elif action == "sched":
        rid  = int(extra)
        rule = await db_get("SELECT * FROM autoreply_rules WHERE id=? AND account_id=?", (rid, aid))
        if not rule: await cb.answer(); return
        ss = rule.get('schedule_start') or ''
        se = rule.get('schedule_end')   or ''
        sch_txt = f"{ss}–{se}" if ss and se else "не задано"
        await state.set_state(ARSchedule.value)
        await state.update_data(aid=aid, rid=rid, msg_id=cb.message.message_id, chat_id=cb.message.chat.id)
        await edit(cb,
            f"<b>Расписание автоответа</b>\n\n"
            f"текущее: <b>{sch_txt}</b>\n\n"
            f"формат: <code>09:00-23:00</code>\n"
            f"отправь <code>-</code> чтобы убрать расписание",
            kb([b("Отмена", f"ar:{aid}:list:0")])
        )

    elif action == "add":
        await state.set_state(ARState.trigger)
        await state.update_data(aid=aid, msg_id=cb.message.message_id, chat_id=cb.message.chat.id)
        await edit(cb,
            "<b>Новое правило</b>  ·  шаг 1/3\n\nВведите триггер или несколько триггеров — каждый с новой строки:",
            kb([b("Отмена", f"ar:{aid}:menu")])
        )


@router.message(ARState.trigger)
async def ar_trigger(msg: Message, state: FSMContext):
    await delete_user_msg(msg)
    data = await state.get_data()
    raw = (msg.text or '').strip()
    if not raw:
        return
    triggers = [t.strip() for t in raw.splitlines() if t.strip()]
    if not triggers:
        return
    trig_str = ' | '.join(triggers)
    trig_db  = '|'.join(triggers)
    aid = data['aid']
    await state.update_data(ar_trig=trig_db, ar_trig_display=trig_str, ar_items=[], ar_album_buf={})
    await state.set_state(ARState.content)
    try:
        await msg.bot.edit_message_text(
            f"<b>Новое правило</b>  ·  шаг 2/3\n\n"
            f"триггер(ы): <code>{trig_str[:200]}</code>\n\n"
            f"Отправьте одно или несколько сообщений — они будут отправляться по триггеру\n"
            f"Когда закончите — нажмите <b>готово</b>",
            chat_id=data['chat_id'], message_id=data['msg_id'],
            reply_markup=kb([
                b("Готово", f"ar_done:{aid}"),
                b("Отмена",   f"ar:{aid}:menu"),
            ]),
            parse_mode='HTML'
        )
    except: pass


@router.message(ARState.content)
async def ar_content_input(msg: Message, state: FSMContext):
    await delete_user_msg(msg)
    data  = await state.get_data()
    aid   = data['aid']; mid = data['msg_id']; cid = data['chat_id']
    trig_str = data.get('ar_trig_display', '')
    items = data.get('ar_items', [])
    album_buf = data.get('ar_album_buf', {})

    # ── Альбом ──
    group_id = msg.media_group_id
    if group_id:
        grp_key = str(group_id)
        item = _msg_to_content(msg)
        if not item: return
        if grp_key not in album_buf:
            album_buf[grp_key] = []
        album_buf[grp_key].append(item)
        await state.update_data(ar_album_buf=album_buf)

        pending = data.get('ar_album_tasks', {})
        if grp_key in pending:
            try: pending[grp_key].cancel()
            except: pass

        async def _flush_ar_album(gk=grp_key):
            await asyncio.sleep(1.0)
            d2 = await state.get_data()
            buf2 = d2.get('ar_album_buf', {})
            group_items = buf2.pop(gk, [])
            if not group_items: return
            itms2 = d2.get('ar_items', [])
            cap = next((i.get('caption', '') for i in group_items if i.get('caption')), '')
            album_item = {'type': 'album', 'items': group_items, 'caption': cap}
            itms2.append(album_item)
            await state.update_data(ar_items=itms2, ar_album_buf=buf2)
            import json as _j2
            _, summ, _ = _draft_summary(_j2.dumps(itms2))
            try:
                await msg.bot.edit_message_text(
                    f"<b>Новое правило</b>  ·  шаг 2/3\n\n"
                    f"триггер(ы): <code>{trig_str[:200]}</code>\n"
                    f"добавлено: <b>{len(itms2)}</b>  ·  последнее: 🗂 альбом ({len(group_items)} шт)\n\n"
                    f"<blockquote expandable>{summ[:400]}</blockquote>\n\n"
                    f"Можете отправить ещё или нажмите <b>готово</b>",
                    chat_id=cid, message_id=mid,
                    reply_markup=kb([
                        b(f"Готово ({len(itms2)})", f"ar_done:{aid}"),
                        b("Отмена", f"ar:{aid}:menu"),
                    ]),
                    parse_mode='HTML'
                )
            except: pass

        task = asyncio.create_task(_flush_ar_album())
        pending[grp_key] = task
        await state.update_data(ar_album_tasks=pending)
        return

    # ── Одиночное сообщение ──
    item = _msg_to_content(msg)
    if not item: return

    items.append(item)
    await state.update_data(ar_items=items)

    import json as _j
    count = len(items)
    _, summary, _ = _draft_summary(_j.dumps(items))
    t  = item.get('type', '?')
    ic = _DRAFT_ICONS.get(t, '📄')
    lbl = _DRAFT_LABELS.get(t) or t
    try:
        await msg.bot.edit_message_text(
            f"<b>Новое правило</b>  ·  шаг 2/3\n\n"
            f"триггер(ы): <code>{trig_str[:200]}</code>\n"
            f"добавлено: <b>{count}</b>  ·  последнее: {ic} {lbl}\n\n"
            f"<blockquote expandable>{summary[:400]}</blockquote>\n\n"
            f"Можете отправить ещё или нажмите <b>готово</b>",
            chat_id=cid, message_id=mid,
            reply_markup=kb([
                b(f"Готово ({count})", f"ar_done:{aid}"),
                b("Отмена", f"ar:{aid}:menu"),
            ]),
            parse_mode='HTML'
        )
    except: pass


@router.callback_query(F.data.startswith("ar_done:"))
async def ar_done(cb: CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    if current_state != ARState.content.state:
        await cb.answer()
        return
    aid  = int(cb.data.split(":")[1])
    data = await state.get_data()
    items = data.get('ar_items', [])
    if not items:
        await cb.answer("Добавьте хотя бы одно сообщение", show_alert=True)
        return
    await cb.answer()
    await state.set_state(ARState.match)
    cid = data['chat_id']; mid = data['msg_id']
    trig_str = data.get('ar_trig_display', '')
    import json as _j
    _, summary, count = _draft_summary(_j.dumps(items))
    try:
        await cb.bot.edit_message_text(
            f"<b>Новое правило</b>  ·  шаг 3/3\n\n"
            f"триггер(ы): <code>{trig_str[:200]}</code>\n"
            f"сообщений: <b>{count}</b>\n\n"
            f"<blockquote expandable>{summary[:300]}</blockquote>\n\n"
            f"тип совпадения триггера:",
            chat_id=cid, message_id=mid,
            reply_markup=kb(
                [b("Содержит", "ar_m:contains"), b("Точное", "ar_m:exact")],
                [b("Начинается с", "ar_m:startswith")],
                [b("Отмена", f"ar:{aid}:menu")]
            ), parse_mode='HTML'
        )
    except: pass


@router.callback_query(F.data.startswith("ar_m:"))
async def ar_match(cb: CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    if current_state != ARState.match.state:
        await cb.answer()
        return
    import json as _j
    mt   = cb.data.split(":")[1]
    data = await state.get_data()
    aid  = data['aid']
    trig_db  = data.get('ar_trig', '')
    items    = data.get('ar_items', [])
    content_json = _j.dumps(items, ensure_ascii=False) if items else ''
    await db_run(
        "INSERT INTO autoreply_rules(account_id,trig,trigger_text,response,response_text,match_type,format_mode,content_json,buttons_json)"
        " VALUES(?,?,?,?,?,?,?,?,?)",
        (aid, trig_db, trig_db, '', '', mt, 'html', content_json, '')
    )
    await state.clear()
    await cb.answer()
    mt_labels = {'contains': 'содержит', 'exact': 'точное', 'startswith': 'начинается с'}
    trig_display = data.get('ar_trig_display', trig_db)
    _, summary, count = _draft_summary(content_json) if content_json else ('', '?', 0)
    try:
        await cb.message.edit_text(
            f"<b>Правило добавлено</b>\n\n"
            f"триггер(ы): <code>{trig_display[:200]}</code>  ·  <b>{mt_labels.get(mt, mt)}</b>\n"
            f"сообщений: <b>{count}</b>",
            reply_markup=kb([b("Правила", f"ar:{aid}:list:0")], [b("Назад", f"ar:{aid}:menu")]),
            parse_mode='HTML'
        )
    except: pass


@router.message(ARSchedule.value)
async def ar_schedule_input(msg: Message, state: FSMContext):
    await delete_user_msg(msg)
    data = await state.get_data()
    aid  = data['aid']; mid = data['msg_id']; cid = data['chat_id']
    rid  = data.get('rid')
    text = (msg.text or '').strip()
    await state.clear()
    if text == '-':
        await db_run("UPDATE autoreply_rules SET schedule_start='', schedule_end='' WHERE id=? AND account_id=?",
                     (rid, aid))
        ss, se = '', ''
    else:
        try:
            parts = text.replace(' ', '').split('-')
            ss, se = parts[0].strip(), parts[1].strip()
            for t in (ss, se):
                h, m = map(int, t.split(':'))
                assert 0 <= h <= 23 and 0 <= m <= 59
            await db_run(
                "UPDATE autoreply_rules SET schedule_start=?, schedule_end=? WHERE id=? AND account_id=?",
                (ss, se, rid, aid)
            )
        except:
            try:
                await msg.bot.edit_message_text(
                    "Неверный формат. пример: <code>09:00-23:00</code>",
                    chat_id=cid, message_id=mid,
                    reply_markup=kb([b("Отмена", f"ar:{aid}:list:0")]), parse_mode='HTML'
                )
            except: pass
            return
    sch_txt = f"{ss}–{se}" if ss else "убрано"
    try:
        await msg.bot.edit_message_text(
            f"✅ расписание: <b>{sch_txt}</b>",
            chat_id=cid, message_id=mid,
            reply_markup=kb([b("К правилам", f"ar:{aid}:list:0")]), parse_mode='HTML'
        )
    except: pass


# ══════════════════════════════════════
# 📢 РАССЫЛКА
# ══════════════════════════════════════
@router.callback_query(F.data.startswith("broadcast:"))
async def cb_broadcast(cb: CallbackQuery, state: FSMContext):
    parts  = cb.data.split(":")
    aid    = int(parts[1])
    action = parts[2] if len(parts) > 2 else "menu"
    acc    = await db_get("SELECT * FROM accounts WHERE id=? AND user_id=?", (aid, cb.from_user.id))
    if not acc: await cb.answer(); return

    if action == "menu":
        logs = await db_all(
            "SELECT * FROM broadcast_log WHERE account_id=? ORDER BY created_at DESC LIMIT 3", (aid,)
        )
        today = datetime.now().strftime('%Y-%m-%d')
        day_row = await db_get(
            "SELECT COALESCE(SUM(sent),0) as s FROM broadcast_log "
            "WHERE account_id=? AND date(created_at)=?", (aid, today)
        )
        day_sent = (day_row or {}).get('s', 0) or 0
        day_left = max(0, BROADCAST_DAY_LIMIT - day_sent)

        lines = [
            "<b>Рассылка</b>\n\n"
            f"Задержка {BROADCAST_DELAY_MIN}–{BROADCAST_DELAY_MAX}с между отправками\n"
            f"Лимит в сутки: <b>{BROADCAST_DAY_LIMIT}</b>  ·  "
            f"осталось сегодня: <b>{day_left}</b>\n"
        ]
        if logs:
            lines.append("\n<b>Последние:</b>")
            for l in logs:
                lines.append(f"  📤 {l['sent']}/{l['total']} · {(l['created_at'] or '')[:16]}")
        is_running = cm._broadcasts.get(aid, {}).get('running', False)
        rows = []
        if is_running:
            rows.append([b("Остановить", f"broadcast:{aid}:stop")])
        elif day_left == 0:
            rows.append([b("Лимит исчерпан (завтра)", "noop")])
        else:
            rows.append([b("Новая рассылка", f"broadcast:{aid}:start")])
        rows.append([b("Назад", f"acc:{aid}")])
        await edit(cb, "\n".join(lines), kb(*rows))

    elif action == "start":
        await state.set_state(BroadcastState.content)
        await state.update_data(aid=aid, msg_id=cb.message.message_id,
                                chat_id=cb.message.chat.id, bcast_items=[])
        await edit(cb,
            "<b>Новая рассылка</b>  ·  шаг 1/2\n\n"
            "Отправьте сообщения для рассылки\n\n"
            "Когда добавите всё — нажмите <b>готово</b>",
            kb([b("Готово", f"broadcast:{aid}:done")],
               [b("Отмена", f"broadcast:{aid}:menu")])
        )

    elif action == "done":
        data_d = await state.get_data()
        items  = data_d.get('bcast_items', [])
        if not items:
            await cb.answer("Добавьте хотя бы одно сообщение", show_alert=True)
            return
        mid_d = data_d.get('msg_id'); cid_d = data_d.get('chat_id')
        await state.set_state(BroadcastState.usernames)
        await state.update_data(bcast_usernames_collected=[])
        cnt_label = f"{len(items)} сообщ" if len(items) > 1 else "1 сообщение"
        await cb.answer()
        try:
            await cb.bot.edit_message_text(
                f"<b>Новая рассылка</b>  ·  шаг 2/2\n\n"
                f"контент: <b>{cnt_label}</b>\n\n"
                f"📋 введите список получателей:\n"
                f"• по одному @username в строку, или через запятую\n"
                f"• максимум 100 получателей\n\n"
                f"пример:\n<code>@user1\n@user2</code>",
                chat_id=cid_d, message_id=mid_d,
                reply_markup=kb([b("Отмена", f"broadcast:{aid}:menu")]),
                parse_mode='HTML'
            )
        except Exception: pass

    elif action == "stop":
        cm.stop_broadcast(aid)
        await cb.answer()
        cb.data = f"broadcast:{aid}:menu"
        await cb_broadcast(cb, state)


@router.message(BroadcastState.content)
async def broadcast_content_input(msg: Message, state: FSMContext):
    await delete_user_msg(msg)
    data  = await state.get_data()
    aid   = data['aid']; mid = data['msg_id']; cid = data['chat_id']
    items = data.get('bcast_items', [])

    item = _msg_to_content(msg)
    if not item:
        try:
            await msg.bot.edit_message_text(
                "Тип контента не поддерживается\n\nОтправьте текст, фото, видео, голосовое, кружок, стикер, GIF или файл",
                chat_id=cid, message_id=mid,
                reply_markup=kb([b("Готово", f"broadcast:{aid}:done")],
                                [b("Отмена", f"broadcast:{aid}:menu")]),
                parse_mode='HTML'
            )
        except: pass
        return

    items.append(item)
    await state.update_data(bcast_items=items)
    import json as _jb
    count = len(items)
    t   = item.get('type', '?')
    ic  = _DRAFT_ICONS.get(t, '📄')
    lbl = _DRAFT_LABELS.get(t) or t
    _, summary, _ = _draft_summary(_jb.dumps(items, ensure_ascii=False))
    try:
        await msg.bot.edit_message_text(
            f"<b>Новая рассылка</b>  ·  шаг 1/2\n\n"
            f"добавлено: <b>{count}</b>  ·  последнее: {ic} {lbl}\n\n"
            f"<blockquote expandable>{summary[:400]}</blockquote>\n\n"
            f"Можете добавить ещё или нажмите <b>готово</b>",
            chat_id=cid, message_id=mid,
            reply_markup=kb([b(f"Готово ({count})", f"broadcast:{aid}:done")],
                            [b("Отмена", f"broadcast:{aid}:menu")]),
            parse_mode='HTML'
        )
    except: pass


@router.message(BroadcastState.usernames)
async def broadcast_usernames_input(msg: Message, state: FSMContext):
    await delete_user_msg(msg)
    data = await state.get_data()
    aid  = data['aid']; mid = data['msg_id']; cid = data['chat_id']
    text = (msg.text or '').strip()

    async def upd(t, markup=kb()):
        try: await msg.bot.edit_message_text(t, chat_id=cid, message_id=mid,
                                              reply_markup=markup, parse_mode='HTML')
        except: pass

    # Extract @usernames from any text
    found = re.findall(r'@([a-zA-Z0-9_]{3,32})', text)
    # Also try plain usernames (lines/comma separated without @)
    if not found:
        raw = re.split(r'[,\n\s]+', text)
        found = [u.strip().lstrip('@') for u in raw if u.strip()]

    collected = data.get('bcast_usernames_collected', [])
    new_users = [u for u in found if u and u not in collected]
    collected.extend(new_users)
    collected = list(dict.fromkeys(collected))[:100]  # unique, max 100
    await state.update_data(bcast_usernames_collected=collected)

    if not collected:
        await upd(
            "<b>Получатели</b>\n\n"
            "Не найдено ни одного username.\n"
            "Отправьте текст с @username или список получателей.",
            kb([b("Отмена", f"broadcast:{aid}:menu")])
        )
        return

    await upd(
        f"<b>Новая рассылка</b>  ·  шаг 2/2\n\n"
        f"Получателей: <b>{len(collected)}</b>\n\n"
        f"Можете отправить ещё сообщение с юзернеймами\n"
        f"или нажмите <b>Начать</b> для запуска рассылки",
        kb([b(f"Начать ({len(collected)})", f"broadcast:{aid}:go")],
           [b("Отмена", f"broadcast:{aid}:menu")])
    )


@router.callback_query(F.data.startswith("broadcast:") & F.data.endswith(":go"))
async def broadcast_go(cb: CallbackQuery, state: FSMContext):
    parts = cb.data.split(":")
    aid   = int(parts[1])
    data  = await state.get_data()
    usernames = data.get('bcast_usernames_collected', [])
    items     = data.get('bcast_items', [])
    if not usernames or not items:
        await cb.answer("Нет получателей или сообщений", show_alert=True)
        return
    mid = data.get('msg_id'); cid = data.get('chat_id')
    await state.clear()
    await cb.answer()

    cnt_lbl = f"{len(items)} сообщ." if len(items) > 1 else "1 сообщение"
    warn = ""
    if len(usernames) > BROADCAST_SESSION_MAX:
        warn = (f"\n\n⚠️ Много получателей ({len(usernames)}) — "
                f"будут применены длинные паузы ({BROADCAST_BATCH_PAUSE_MIN}–{BROADCAST_BATCH_PAUSE_MAX}с "
                f"каждые {BROADCAST_BATCH_SIZE} сообщений)")
    try:
        await cb.bot.edit_message_text(
            f"<b>Рассылка запущена</b>\n\n"
            f"Получателей: <b>{len(usernames)}</b>\n"
            f"Контент: <b>{cnt_lbl}</b>"
            f"{warn}\n\n"
            f"Подождите...",
            chat_id=cid, message_id=mid,
            reply_markup=kb([b("Остановить", f"broadcast:{aid}:stop")]),
            parse_mode='HTML'
        )
    except: pass

    last_edit = [0]

    async def progress(i, total, sent, failed, status=None):
        now = time.time()
        if now - last_edit[0] < 3 and i < total: return
        last_edit[0] = now
        bar = make_progress_bar(i, total)
        st  = f"\n{status}" if status else ""
        try:
            await cb.bot.edit_message_text(
                f"<b>Рассылка</b>\n\n"
                f"{bar}\n"
                f"Отправлено: {sent}  |  Ошибок: {failed}  |  Всего: {total}{st}",
                chat_id=cid, message_id=mid,
                reply_markup=kb([b("Остановить", f"broadcast:{aid}:stop")]),
                parse_mode='HTML'
            )
        except: pass

    uid = cb.from_user.id
    result = await cm.broadcast(aid, uid, usernames, items, progress)
    errs = result['errors'][:5]
    errs_txt = "\n".join(f"  · {e}" for e in errs) if errs else ""
    try:
        await cb.bot.edit_message_text(
            f"<b>Рассылка завершена</b>\n\n"
            f"Отправлено: <b>{result['sent']}</b>\n"
            f"Ошибок: <b>{result['failed']}</b>\n"
            f"Всего: <b>{result['total']}</b>"
            + (f"\n\n<blockquote expandable>{errs_txt}</blockquote>" if errs_txt else ""),
            chat_id=cid, message_id=mid,
            reply_markup=kb([b("Ещё рассылку", f"broadcast:{aid}:start")],
                            [b("Назад", f"acc:{aid}")]),
            parse_mode='HTML'
        )
    except: pass


# ══════════════════════════════════════
# ЧЕРНОВИКИ
# ══════════════════════════════════════
# ══════════════════════════════════════
async def _drafts_list(bot, chat_id: int, msg_id: int,
                       aid: int, page: int, state: FSMContext):
    import json as _j
    items = await db_all(
        "SELECT * FROM drafts WHERE account_id=? ORDER BY id DESC", (aid,)
    )
    total = len(items)

    async def _edit(text, markup):
        try:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id,
                                         reply_markup=markup, parse_mode='HTML')
        except Exception:
            pass

    if not items:
        await _edit(
            "<b>Черновики</b>\n\nпусто\n\nзадайте триггер — при его отправке "
            "сообщение автоматически заменится на черновик",
            kb([b("Добавить", f"drafts:{aid}:add"), b("Назад", f"acc:{aid}")])
        )
        return

    pages = max(1, (total + _DRAFT_PAGE - 1) // _DRAFT_PAGE)
    page  = max(0, min(page, pages - 1))
    chunk = items[page * _DRAFT_PAGE: (page + 1) * _DRAFT_PAGE]

    lines = [f"<b>Черновики</b>  ·  {total} шт  ·  стр {page+1}/{pages}\n"]
    btn_rows = []

    for d in chunk:
        icon, summary, count = _draft_summary(d['content'])
        trig = d['trigger_text'][:25]
        cnt_label = f"  ·  {count} сообщ" if count > 1 else ""
        is_on = d.get('active', 1)
        status = "✅" if is_on else "❌"
        lines.append(f"{status} {icon} <code>{trig}</code>{cnt_label}")
        btn_rows.append([b(f"{icon} {trig}{cnt_label}", f"drafts:{aid}:view:{d['id']}:{page}")])

    if pages > 1:
        nav_btns = []
        if page > 0:
            nav_btns.append(b("◀", f"drafts:{aid}:list:{page-1}"))
        nav_btns.append(b(f"  {page+1} / {pages}  ", f"drafts:{aid}:noop"))
        if page < pages - 1:
            nav_btns.append(b("▶", f"drafts:{aid}:list:{page+1}"))
        btn_rows.append(nav_btns)

    btn_rows.append([b("Добавить", f"drafts:{aid}:add"), b("Назад", f"acc:{aid}")])
    await _edit("\n".join(lines), kb(*btn_rows))


@router.callback_query(F.data.startswith("drafts:"))
async def cb_drafts(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    parts  = cb.data.split(":")
    aid    = int(parts[1])
    action = parts[2] if len(parts) > 2 else "list"

    acc = await db_get("SELECT * FROM accounts WHERE id=? AND user_id=?", (aid, cb.from_user.id))
    if not acc: return

    cid = cb.message.chat.id
    mid = cb.message.message_id

    if action == "list":
        page = int(parts[3]) if len(parts) > 3 else 0
        await _drafts_list(cb.bot, cid, mid, aid, page, state)

    elif action == "noop":
        pass

    elif action == "add":
        await state.set_state(DraftAdd.trigger)
        await state.update_data(aid=aid, msg_id=mid, chat_id=cid)
        try:
            await cb.bot.edit_message_text(
                "<b>Новый черновик</b>  ·  шаг 1/2\n\n"
                "Введите триггер — слово, по которому будет отправляться черновик:",
                chat_id=cid, message_id=mid,
                reply_markup=kb([b("Отмена", f"drafts:{aid}:cancel_add")]),
                parse_mode='HTML'
            )
        except: pass

    elif action == "cancel_add":
        await state.clear()
        await _drafts_list(cb.bot, cid, mid, aid, 0, state)

    elif action == "done":
        data = await state.get_data()
        items = data.get('draft_items', [])
        if not items:
            return
        trig = data.get('draft_trigger', '')
        import json as _j
        content_json = _j.dumps(items, ensure_ascii=False)
        await db_run(
            "INSERT INTO drafts(account_id,trigger_text,content) VALUES(?,?,?)",
            (aid, trig, content_json)
        )
        await state.clear()
        _, summary, count = _draft_summary(content_json)
        try:
            await cb.bot.edit_message_text(
                f"<b>Черновик добавлен</b>\n\n"
                f"триггер: <code>{trig}</code>\n"
                f"сообщений: <b>{count}</b>",
                chat_id=cid, message_id=mid,
                reply_markup=kb([b("Черновики", f"drafts:{aid}:list:0")],
                                [b("Назад", f"acc:{aid}")]),
                parse_mode='HTML'
            )
        except: pass

    elif action == "view":
        did  = int(parts[3])
        page = int(parts[4]) if len(parts) > 4 else 0
        import json as _j
        draft = await db_get("SELECT * FROM drafts WHERE id=? AND account_id=?", (did, aid))
        if not draft:
            await _drafts_list(cb.bot, cid, mid, aid, page, state)
            return
        icon, summary, count = _draft_summary(draft['content'])
        trig = draft['trigger_text']
        is_on = draft.get('active', 1)
        status = "Включён" if is_on else "Выключен"
        cnt_lbl = f" · {count} сообщений" if count > 1 else ""
        try:
            await cb.bot.edit_message_text(
                f"<b>Черновик</b>\n\n"
                f"триггер: <code>{trig}</code>{cnt_lbl}\n"
                f"статус: {status}\n\n"
                f"<blockquote expandable>{summary[:800]}</blockquote>",
                chat_id=cid, message_id=mid,
                reply_markup=kb(
                    [b("Вкл/выкл", f"drafts:{aid}:toggle:{did}:{page}"),
                     b("Удалить", f"drafts:{aid}:del:{did}:{page}")],
                    [b("К списку", f"drafts:{aid}:list:{page}")]
                ), parse_mode='HTML'
            )
        except: pass

    elif action == "toggle":
        did  = int(parts[3])
        page = int(parts[4]) if len(parts) > 4 else 0
        draft = await db_get("SELECT * FROM drafts WHERE id=? AND account_id=?", (did, aid))
        if draft:
            new = 0 if draft.get('active', 1) else 1
            await db_run("UPDATE drafts SET active=? WHERE id=?", (new, did))
        cb.data = f"drafts:{aid}:view:{did}:{page}"
        await cb_drafts(cb, state)

    elif action == "del":
        did  = int(parts[3])
        page = int(parts[4]) if len(parts) > 4 else 0
        await db_run("DELETE FROM drafts WHERE id=? AND account_id=?", (did, aid))
        await _drafts_list(cb.bot, cid, mid, aid, page, state)


@router.message(DraftAdd.trigger)
async def draft_trigger_input(msg: Message, state: FSMContext):
    await delete_user_msg(msg)
    data = await state.get_data()
    aid  = data['aid']; mid = data['msg_id']; cid = data['chat_id']
    trig = (msg.text or '').strip()
    if not trig:
        return
    await state.update_data(draft_trigger=trig, draft_items=[], draft_album_buf={})
    await state.set_state(DraftAdd.content)
    try:
        await msg.bot.edit_message_text(
            f"<b>Новый черновик</b>  ·  шаг 2/2\n\n"
            f"триггер: <code>{trig}</code>\n\n"
            f"Отправьте одно или несколько сообщений\n"
            f"Когда всё готово — нажмите <b>готово</b>",
            chat_id=cid, message_id=mid,
            reply_markup=kb([
                b("Готово", f"drafts:{aid}:done"),
                b("Отмена",   f"drafts:{aid}:cancel_add"),
            ]),
            parse_mode='HTML'
        )
    except Exception: pass


@router.message(DraftAdd.content)
async def draft_content_input(msg: Message, state: FSMContext):
    await delete_user_msg(msg)
    data  = await state.get_data()
    aid   = data['aid']; mid = data['msg_id']; cid = data['chat_id']
    trig  = data.get('draft_trigger', '')
    items = data.get('draft_items', [])
    album_buf = data.get('draft_album_buf', {})

    # ── Альбом ──
    group_id = msg.media_group_id
    if group_id:
        grp_key = str(group_id)
        item = _msg_to_content(msg)
        if not item: return
        if grp_key not in album_buf:
            album_buf[grp_key] = []
        album_buf[grp_key].append(item)
        await state.update_data(draft_album_buf=album_buf)

        pending = data.get('draft_album_tasks', {})
        if grp_key in pending:
            try: pending[grp_key].cancel()
            except: pass

        async def _flush_album(gk=grp_key):
            await asyncio.sleep(1.0)
            d2 = await state.get_data()
            buf2 = d2.get('draft_album_buf', {})
            group_items = buf2.pop(gk, [])
            if not group_items: return
            itms2 = d2.get('draft_items', [])
            cap = next((i.get('caption', '') for i in group_items if i.get('caption')), '')
            album_item = {'type': 'album', 'items': group_items, 'caption': cap}
            itms2.append(album_item)
            await state.update_data(draft_items=itms2, draft_album_buf=buf2)
            import json as _j2
            _, summ, _ = _draft_summary(_j2.dumps(itms2))
            try:
                await msg.bot.edit_message_text(
                    f"<b>Новый черновик</b>  ·  шаг 2/2\n\n"
                    f"триггер: <code>{trig}</code>\n"
                    f"добавлено: <b>{len(itms2)}</b>  ·  последнее: 🗂 альбом ({len(group_items)} шт)\n\n"
                    f"<blockquote expandable>{summ[:400]}</blockquote>\n\n"
                    f"Можете отправить ещё или нажмите <b>готово</b>",
                    chat_id=cid, message_id=mid,
                    reply_markup=kb([
                        b(f"Готово ({len(itms2)})", f"drafts:{aid}:done"),
                        b("Отмена", f"drafts:{aid}:cancel_add"),
                    ]),
                    parse_mode='HTML'
                )
            except Exception: pass

        task = asyncio.create_task(_flush_album())
        pending[grp_key] = task
        await state.update_data(draft_album_tasks=pending)
        return

    # ── Одиночное сообщение ──
    item = _msg_to_content(msg)
    if not item: return

    items.append(item)
    await state.update_data(draft_items=items)

    import json as _j
    count = len(items)
    _, summary, _ = _draft_summary(_j.dumps(items))
    t   = item.get('type', '?')
    ic  = _DRAFT_ICONS.get(t, '📄')
    lbl = _DRAFT_LABELS.get(t) or t
    try:
        await msg.bot.edit_message_text(
            f"<b>Новый черновик</b>  ·  шаг 2/2\n\n"
            f"триггер: <code>{trig}</code>\n"
            f"добавлено: <b>{count}</b>  ·  последнее: {ic} {lbl}\n\n"
            f"<blockquote expandable>{summary[:400]}</blockquote>\n\n"
            f"Можете отправить ещё или нажмите <b>готово</b>",
            chat_id=cid, message_id=mid,
            reply_markup=kb([
                b(f"Готово ({count})", f"drafts:{aid}:done"),
                b("Отмена", f"drafts:{aid}:cancel_add"),
            ]),
            parse_mode='HTML'
        )
    except Exception: pass


# ══════════════════════════════════════
# ПОСЛЕДНИЙ КОД ОТ TELEGRAM
# ══════════════════════════════════════
@router.callback_query(F.data.startswith("tgcode:"))
async def cb_tgcode(cb: CallbackQuery):
    await cb.answer()
    aid = int(cb.data.split(":")[1])
    acc = await db_get("SELECT * FROM accounts WHERE id=? AND user_id=?", (aid, cb.from_user.id))
    if not acc: return

    row = await db_get(
        "SELECT code, full_text, received_at FROM tg_codes "
        "WHERE account_id=? ORDER BY received_at DESC LIMIT 1",
        (aid,)
    )

    if not row:
        try:
            await cb.message.edit_text(
                "<b>Последний код</b>\n\nкодов пока нет — бот перехватит следующий автоматически",
                reply_markup=kb([b("Назад", f"acc:{aid}")]),
                parse_mode='HTML'
            )
        except: pass
        return

    code     = row['code']
    received = row['received_at']
    try:
        dt = datetime.fromisoformat(str(received))
        time_str = dt.strftime("%d.%m.%Y  %H:%M:%S")
    except:
        time_str = str(received)

    try:
        await cb.message.edit_text(
            f"<b>Последний код от Telegram</b>\n\n"
            f"<code>{code}</code>\n\n"
            f"🕐 {time_str}",
            reply_markup=kb([b("Назад", f"acc:{aid}")]),
            parse_mode='HTML'
        )
    except: pass


# ══════════════════════════════════════
# ЗАПУСК
# ══════════════════════════════════════
async def main():
    await init_db()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
    dp  = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    dp.message.middleware(AccessGuardMiddleware())
    dp.callback_query.middleware(AccessGuardMiddleware())

    cm.set_bot(bot)
    await cm.load_all()

    log.info("Bot v4.0 started")

    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])

if __name__ == "__main__":
    asyncio.run(main())

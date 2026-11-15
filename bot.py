import asyncio
import logging
import os
import secrets
import string
from datetime import datetime, timezone

import aiohttp
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])
TURSO_URL = os.environ["TURSO_URL"].replace("libsql://", "https://").rstrip("/")
TURSO_TOKEN = os.environ["TURSO_TOKEN"]

TRUSTED_MONTHLY_LIMIT = 5
USER_TOTAL_LIMIT = 1


# ───────────────────────── Turso (HTTP API) ─────────────────────────
class Turso:
    def __init__(self, url: str, token: str):
        self.url = url
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        self.session: aiohttp.ClientSession | None = None

    @staticmethod
    def _arg(v):
        if v is None:
            return {"type": "null"}
        if isinstance(v, bool):
            return {"type": "integer", "value": str(int(v))}
        if isinstance(v, int):
            return {"type": "integer", "value": str(v)}
        return {"type": "text", "value": str(v)}

    async def execute(self, sql: str, args: list | None = None):
        """Возвращает (rows, affected_row_count). rows — список списков значений."""
        if self.session is None:
            self.session = aiohttp.ClientSession()
        body = {
            "requests": [
                {"type": "execute", "stmt": {"sql": sql, "args": [self._arg(a) for a in (args or [])]}},
                {"type": "close"},
            ]
        }
        async with self.session.post(f"{self.url}/v2/pipeline", json=body, headers=self.headers) as r:
            data = await r.json()
        res = data["results"][0]
        if res["type"] == "error":
            raise RuntimeError(res["error"]["message"])
        result = res["response"]["result"]
        rows = []
        for row in result["rows"]:
            rows.append([None if c["type"] == "null" else c.get("value") for c in row])
        return rows, result.get("affected_row_count", 0)

    async def init(self):
        await self.execute(
            """CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                lang TEXT,
                role TEXT NOT NULL DEFAULT 'user',
                banned INTEGER NOT NULL DEFAULT 0,
                activated INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )"""
        )
        await self.execute(
            """CREATE TABLE IF NOT EXISTS invites (
                code TEXT PRIMARY KEY,
                creator_id INTEGER NOT NULL,
                used_by INTEGER,
                used_at TEXT,
                created_at TEXT NOT NULL
            )"""
        )
        await self.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")


db = Turso(TURSO_URL, TURSO_TOKEN)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def month_start_iso() -> str:
    n = datetime.now(timezone.utc)
    return n.strftime("%Y-%m-01T00:00:00")


# ───────────────────────── Работа с данными ─────────────────────────
async def get_user(user_id: int):
    rows, _ = await db.execute(
        "SELECT user_id, lang, role, banned, activated FROM users WHERE user_id=?", [user_id]
    )
    if not rows:
        return None
    r = rows[0]
    return {
        "id": int(r[0]),
        "lang": r[1],
        "role": r[2],
        "banned": bool(int(r[3])),
        "activated": bool(int(r[4])),
    }


async def ensure_user(user_id: int, username: str | None):
    await db.execute(
        "INSERT OR IGNORE INTO users (user_id, username, created_at) VALUES (?,?,?)",
        [user_id, username, now_iso()],
    )
    if username:
        await db.execute("UPDATE users SET username=? WHERE user_id=?", [username, user_id])


async def set_setting(key: str, value: str):
    await db.execute(
        "INSERT INTO settings (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        [key, value],
    )


async def get_setting(key: str):
    rows, _ = await db.execute("SELECT value FROM settings WHERE key=?", [key])
    return rows[0][0] if rows else None


def gen_code() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "VDLC-" + "".join(secrets.choice(alphabet) for _ in range(10))


async def create_invite(creator_id: int) -> str:
    code = gen_code()
    await db.execute(
        "INSERT INTO invites (code, creator_id, created_at) VALUES (?,?,?)",
        [code, creator_id, now_iso()],
    )
    return code


async def invite_quota(user_id: int):
    """Возвращает (можно_ли, сколько_осталось | None если безлимит)."""
    if user_id == OWNER_ID:
        return True, None
    u = await get_user(user_id)
    if not u or not u["activated"]:
        return False, 0
    if u["role"] == "trusted":
        rows, _ = await db.execute(
            "SELECT COUNT(*) FROM invites WHERE creator_id=? AND created_at>=?",
            [user_id, month_start_iso()],
        )
        left = TRUSTED_MONTHLY_LIMIT - int(rows[0][0])
        return left > 0, max(left, 0)
    rows, _ = await db.execute("SELECT COUNT(*) FROM invites WHERE creator_id=?", [user_id])
    left = USER_TOTAL_LIMIT - int(rows[0][0])
    return left > 0, max(left, 0)


async def redeem_invite(code: str, user_id: int) -> bool:
    _, affected = await db.execute(
        "UPDATE invites SET used_by=?, used_at=? WHERE code=? AND used_by IS NULL",
        [user_id, now_iso(), code],
    )
    if affected:
        await db.execute("UPDATE users SET activated=1 WHERE user_id=?", [user_id])
        return True
    return False


# ───────────────────────── Тексты ─────────────────────────
T = {
    "ru": {
        "welcome": "👋 Привет! Это бот доступа к VenorDLC.\n\nВыбери вариант:",
        "have": "У меня есть инвайт!",
        "dont": "У меня нет инвайта :(",
        "enter_code": "🔑 Введите код инвайта:",
        "bad_code": "❌ Неверный или уже использованный код. Попробуйте ещё раз или /start.",
        "ok": "✅ Код принят! Вот ваш файл:",
        "nofile": "⚠️ Файл пока не загружен администратором. Напишите позже.",
        "free": "🛠 Бесплатная версия в разработке.",
        "banned": "🚫 Вы заблокированы.",
        "menu_invite": "🎟 Создать инвайт",
        "menu_file": "📁 Получить файл",
        "menu": "Главное меню:",
        "inv_created": "🎟 Ваш инвайт-код:\n<code>{code}</code>\n\nОсталось: {left}",
        "inv_denied": "❌ Лимит инвайтов исчерпан или у вас нет доступа.",
        "left_unlim": "∞",
    },
    "en": {
        "welcome": "👋 Hi! This is the VenorDLC access bot.\n\nChoose an option:",
        "have": "I have invite!",
        "dont": "I dont have invite :(",
        "enter_code": "🔑 Enter your invite code:",
        "bad_code": "❌ Invalid or already used code. Try again or /start.",
        "ok": "✅ Code accepted! Here is your file:",
        "nofile": "⚠️ The file hasn't been uploaded by the admin yet. Try again later.",
        "free": "🛠 The free version is in development.",
        "banned": "🚫 You are banned.",
        "menu_invite": "🎟 Create invite",
        "menu_file": "📁 Get file",
        "menu": "Main menu:",
        "inv_created": "🎟 Your invite code:\n<code>{code}</code>\n\nLeft: {left}",
        "inv_denied": "❌ Invite limit reached or you have no access.",
        "left_unlim": "∞",
    },
}


def tr(lang: str | None, key: str) -> str:
    return T.get(lang or "ru", T["ru"])[key]


# ───────────────────────── Клавиатуры ─────────────────────────
lang_kb = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang:ru"),
            InlineKeyboardButton(text="🇬🇧 English", callback_data="lang:en"),
        ]
    ]
)


def main_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=tr(lang, "have"), callback_data="have")],
            [InlineKeyboardButton(text=tr(lang, "dont"), callback_data="dont")],
        ]
    )


def menu_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=tr(lang, "menu_file"), callback_data="getfile")],
            [InlineKeyboardButton(text=tr(lang, "menu_invite"), callback_data="mkinvite")],
        ]
    )


admin_kb = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать инвайт", callback_data="adm:gen")],
        [
            InlineKeyboardButton(text="⭐ Выдать trusted", callback_data="adm:trust"),
            InlineKeyboardButton(text="✖ Снять trusted", callback_data="adm:untrust"),
        ],
        [
            InlineKeyboardButton(text="🔨 Бан", callback_data="adm:ban"),
            InlineKeyboardButton(text="♻ Разбан", callback_data="adm:unban"),
        ],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
    ]
)


# ───────────────────────── FSM ─────────────────────────
class Form(StatesGroup):
    code = State()


class Adm(StatesGroup):
    trust = State()
    untrust = State()
    ban = State()
    unban = State()


# ───────────────────────── Middleware бана ─────────────────────────
class BanMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if user and user.id != OWNER_ID:
            u = await get_user(user.id)
            if u and u["banned"]:
                text = tr(u["lang"], "banned")
                if isinstance(event, CallbackQuery):
                    await event.answer(text, show_alert=True)
                elif isinstance(event, Message):
                    await event.answer(text)
                return
        return await handler(event, data)


router = Router()
router.message.middleware(BanMiddleware())
router.callback_query.middleware(BanMiddleware())


async def send_file(target: Message, lang: str):
    file_id = await get_setting("file_id")
    if not file_id:
        await target.answer(tr(lang, "nofile"))
        return
    await target.answer_document(file_id, caption=tr(lang, "ok"), protect_content=True)
    await target.answer(tr(lang, "menu"), reply_markup=menu_kb(lang))


# ───────────────────────── Пользовательские хендлеры ─────────────────────────
@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    await ensure_user(m.from_user.id, m.from_user.username)
    await m.answer("🌐 Выберите язык / Choose language:", reply_markup=lang_kb)


@router.callback_query(F.data.startswith("lang:"))
async def set_lang(c: CallbackQuery):
    lang = c.data.split(":")[1]
    await ensure_user(c.from_user.id, c.from_user.username)
    await db.execute("UPDATE users SET lang=? WHERE user_id=?", [lang, c.from_user.id])
    await c.message.edit_text(tr(lang, "welcome"), reply_markup=main_kb(lang))
    await c.answer()


@router.callback_query(F.data == "dont")
async def dont(c: CallbackQuery):
    u = await get_user(c.from_user.id)
    await c.message.answer(tr(u["lang"] if u else "ru", "free"))
    await c.answer()


@router.callback_query(F.data == "have")
async def have(c: CallbackQuery, state: FSMContext):
    u = await get_user(c.from_user.id)
    lang = u["lang"] if u else "ru"
    await c.answer()
    # уже активирован (или владелец) — сразу отдаём файл
    if c.from_user.id == OWNER_ID or (u and u["activated"]):
        await send_file(c.message, lang)
        return
    await state.set_state(Form.code)
    await c.message.answer(tr(lang, "enter_code"))


@router.message(Form.code)
async def enter_code(m: Message, state: FSMContext):
    u = await get_user(m.from_user.id)
    lang = u["lang"] if u else "ru"
    code = (m.text or "").strip().upper()
    if await redeem_invite(code, m.from_user.id):
        await state.clear()
        await send_file(m, lang)
    else:
        await m.answer(tr(lang, "bad_code"))


@router.callback_query(F.data == "getfile")
async def getfile(c: CallbackQuery):
    u = await get_user(c.from_user.id)
    await c.answer()
    if c.from_user.id == OWNER_ID or (u and u["activated"]):
        await send_file(c.message, u["lang"] if u else "ru")


@router.callback_query(F.data == "mkinvite")
async def mkinvite(c: CallbackQuery):
    u = await get_user(c.from_user.id)
    lang = u["lang"] if u else "ru"
    ok, left = await invite_quota(c.from_user.id)
    if not ok:
        await c.answer(tr(lang, "inv_denied"), show_alert=True)
        return
    code = await create_invite(c.from_user.id)
    _, left_after = await invite_quota(c.from_user.id)
    left_txt = tr(lang, "left_unlim") if left_after is None else str(left_after)
    await c.message.answer(tr(lang, "inv_created").format(code=code, left=left_txt), parse_mode="HTML")
    await c.answer()


@router.message(Command("invite"))
async def cmd_invite(m: Message):
    u = await get_user(m.from_user.id)
    lang = u["lang"] if u else "ru"
    ok, _ = await invite_quota(m.from_user.id)
    if not ok:
        await m.answer(tr(lang, "inv_denied"))
        return
    code = await create_invite(m.from_user.id)
    _, left_after = await invite_quota(m.from_user.id)
    left_txt = tr(lang, "left_unlim") if left_after is None else str(left_after)
    await m.answer(tr(lang, "inv_created").format(code=code, left=left_txt), parse_mode="HTML")


# ───────────────────────── Админка ─────────────────────────
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID


@router.message(Command("admin"), F.from_user.id == OWNER_ID)
async def admin_panel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("🛠 Админ-панель", reply_markup=admin_kb)


@router.message(F.from_user.id == OWNER_ID, F.document, F.caption.startswith("/setfile"))
async def set_file(m: Message):
    await set_setting("file_id", m.document.file_id)
    await m.answer(f"✅ Файл сохранён: {m.document.file_name}\nТеперь его получают пользователи с инвайтом.")


@router.callback_query(F.data.startswith("adm:"), F.from_user.id == OWNER_ID)
async def adm_actions(c: CallbackQuery, state: FSMContext):
    action = c.data.split(":")[1]
    await c.answer()
    if action == "gen":
        code = await create_invite(OWNER_ID)
        await c.message.answer(f"🎟 Инвайт:\n<code>{code}</code>", parse_mode="HTML")
    elif action in ("trust", "untrust", "ban", "unban"):
        await state.set_state(getattr(Adm, action))
        await c.message.answer("Отправьте Telegram ID пользователя (числом):")
    elif action == "stats":
        users, _ = await db.execute("SELECT COUNT(*) FROM users")
        act, _ = await db.execute("SELECT COUNT(*) FROM users WHERE activated=1")
        tru, _ = await db.execute("SELECT COUNT(*) FROM users WHERE role='trusted'")
        ban, _ = await db.execute("SELECT COUNT(*) FROM users WHERE banned=1")
        inv, _ = await db.execute("SELECT COUNT(*), SUM(used_by IS NOT NULL) FROM invites")
        await c.message.answer(
            f"📊 Пользователей: {users[0][0]}\n"
            f"✅ Активированных: {act[0][0]}\n"
            f"⭐ Trusted: {tru[0][0]}\n"
            f"🔨 Забанено: {ban[0][0]}\n"
            f"🎟 Инвайтов создано: {inv[0][0]}, использовано: {inv[0][1] or 0}"
        )


async def _admin_set(m: Message, state: FSMContext, sql: str, done: str):
    try:
        uid = int((m.text or "").strip())
    except ValueError:
        await m.answer("Нужен числовой ID. Попробуйте ещё раз:")
        return
    _, affected = await db.execute(sql, [uid])
    await state.clear()
    await m.answer(done.format(uid=uid) if affected else "Пользователь не найден (он должен хотя бы раз нажать /start).")


@router.message(Adm.trust, F.from_user.id == OWNER_ID)
async def a_trust(m: Message, state: FSMContext):
    await _admin_set(m, state, "UPDATE users SET role='trusted' WHERE user_id=?", "⭐ {uid} теперь trusted (5 инвайтов/мес)")


@router.message(Adm.untrust, F.from_user.id == OWNER_ID)
async def a_untrust(m: Message, state: FSMContext):
    await _admin_set(m, state, "UPDATE users SET role='user' WHERE user_id=?", "✖ {uid} больше не trusted")


@router.message(Adm.ban, F.from_user.id == OWNER_ID)
async def a_ban(m: Message, state: FSMContext):
    await _admin_set(m, state, "UPDATE users SET banned=1 WHERE user_id=?", "🔨 {uid} забанен")


@router.message(Adm.unban, F.from_user.id == OWNER_ID)
async def a_unban(m: Message, state: FSMContext):
    await _admin_set(m, state, "UPDATE users SET banned=0 WHERE user_id=?", "♻ {uid} разбанен")


# ───────────────────────── Запуск ─────────────────────────
async def main():
    await db.init()
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    try:
        await dp.start_polling(bot)
    finally:
        if db.session:
            await db.session.close()


if __name__ == "__main__":
    asyncio.run(main())

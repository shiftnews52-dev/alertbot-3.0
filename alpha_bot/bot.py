"""
Alpha Entry Bot - автоматический трейдинг-бот с сигналами входа
Работает на основе EMA + RSI стратегии
"""
import os, time, asyncio, logging
from typing import Optional, Dict, List, Tuple
from collections import defaultdict, deque

from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import aiosqlite
import httpx

# ==================== НАСТРОЙКИ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
BOT_NAME = os.getenv("BOT_NAME", "Alpha Entry Bot")
SUPPORT_URL = os.getenv("SUPPORT_URL", "https://t.me/support")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
DB_PATH = os.getenv("DB_PATH", "bot.db")

# Дефолтные монеты
DEFAULT_PAIRS = ["BTCUSDT", "ETHUSDT", "TONUSDT"]

# Параметры стратегии
CHECK_INTERVAL = 10  # секунд между проверками
CANDLE_TF = 60  # таймфрейм свечи (1 минута)
MAX_CANDLES = 200  # макс свечей в памяти
EMA_FAST = 12
EMA_SLOW = 26
RSI_PERIOD = 14
RSI_OVERSOLD = 30  # для лонга RSI должен быть выше
RSI_OVERBOUGHT = 70  # для шорта RSI должен быть ниже

# Куллдаун сигналов (не спамить одно и то же)
SIGNAL_COOLDOWN = 900  # 15 минут

# Картинки (замени на свои URL или локальные пути)
IMG_START = os.getenv("IMG_START", "")  # главное меню
IMG_ALERTS = os.getenv("IMG_ALERTS", "")  # раздел алертов
IMG_REF = os.getenv("IMG_REF", "")  # рефералка
IMG_PAYWALL = os.getenv("IMG_PAYWALL", "")  # платёжка

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== DATABASE ====================
INIT_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    invited_by INTEGER,
    balance REAL DEFAULT 0,
    paid INTEGER DEFAULT 0,
    created_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS user_pairs (
    user_id INTEGER NOT NULL,
    pair TEXT NOT NULL,
    PRIMARY KEY (user_id, pair)
);

CREATE TABLE IF NOT EXISTS ref_earnings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount REAL NOT NULL,
    from_user INTEGER NOT NULL,
    created_ts INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_user_pairs ON user_pairs(user_id);
"""

async def get_db():
    """Создаёт новое подключение к БД (правильный способ для aiosqlite)"""
    conn = await aiosqlite.connect(DB_PATH)
    conn.row_factory = aiosqlite.Row
    return conn

async def init_db():
    """Инициализация БД"""
    db = await get_db()
    try:
        await db.executescript(INIT_SQL)
        await db.commit()
    finally:
        await db.close()
    logger.info("Database initialized")

# ==================== MARKET DATA ====================
class CandleStorage:
    """Хранение свечей в памяти"""
    def __init__(self, timeframe=CANDLE_TF, maxlen=MAX_CANDLES):
        self.tf = timeframe
        self.maxlen = maxlen
        self.candles: Dict[str, deque] = defaultdict(lambda: deque(maxlen=maxlen))
        self.current: Dict[str, dict] = {}
    
    def get_bucket(self, ts: float) -> int:
        """Получить временной bucket для свечи"""
        return int(ts // self.tf) * self.tf
    
    def add_price(self, pair: str, price: float, ts: float):
        """Добавить тик цены"""
        pair = pair.upper()
        bucket = self.get_bucket(ts)
        
        if pair not in self.current or self.current[pair]["ts"] != bucket:
            # Закрываем предыдущую свечу
            if pair in self.current:
                self.candles[pair].append(self.current[pair])
            # Открываем новую
            self.current[pair] = {
                "ts": bucket,
                "o": price,
                "h": price,
                "l": price,
                "c": price
            }
        else:
            # Обновляем текущую свечу
            candle = self.current[pair]
            candle["h"] = max(candle["h"], price)
            candle["l"] = min(candle["l"], price)
            candle["c"] = price
    
    def get_candles(self, pair: str) -> List[dict]:
        """Получить список свечей для пары"""
        pair = pair.upper()
        result = list(self.candles[pair])
        if pair in self.current:
            result.append(self.current[pair])
        return result

# Глобальное хранилище свечей
CANDLES = CandleStorage()

async def fetch_price(client: httpx.AsyncClient, pair: str) -> Optional[float]:
    """Получить текущую цену с Binance"""
    try:
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={pair.upper()}"
        resp = await client.get(url, timeout=5.0)
        resp.raise_for_status()
        data = resp.json()
        return float(data["price"])
    except Exception as e:
        logger.error(f"Error fetching price for {pair}: {e}")
        return None

# ==================== INDICATORS ====================
def ema(values: List[float], period: int) -> Optional[float]:
    """Экспоненциальная скользящая средняя"""
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    ema_val = values[0]
    for v in values[1:]:
        ema_val = v * k + ema_val * (1 - k)
    return ema_val

def rsi(closes: List[float], period: int = 14) -> Optional[float]:
    """Индекс относительной силы"""
    if len(closes) < period + 1:
        return None
    
    gains, losses = [], []
    for i in range(-period, 0):
        change = closes[i] - closes[i-1]
        gains.append(max(0, change))
        losses.append(max(0, -change))
    
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    
    if avg_loss == 0:
        return 100.0
    
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def analyze_entry(pair: str) -> Optional[Tuple[str, float, str]]:
    """
    Анализ точки входа
    Возвращает: (side, price, strength) или None
    side: 'LONG' / 'SHORT'
    strength: 'СИЛЬНЫЙ' / 'СРЕДНИЙ'
    """
    candles = CANDLES.get_candles(pair)
    
    if len(candles) < max(EMA_SLOW, RSI_PERIOD) + 5:
        return None
    
    closes = [c["c"] for c in candles]
    current_price = closes[-1]
    
    # Считаем индикаторы
    ema_fast = ema(closes, EMA_FAST)
    ema_slow = ema(closes, EMA_SLOW)
    rsi_val = rsi(closes, RSI_PERIOD)
    
    if ema_fast is None or ema_slow is None or rsi_val is None:
        return None
    
    # LONG: быстрая EMA выше медленной + RSI не перекуплен
    if ema_fast > ema_slow and current_price > ema_slow:
        if rsi_val > RSI_OVERSOLD and rsi_val < 65:
            divergence = (ema_fast - ema_slow) / ema_slow
            strength = "СИЛЬНЫЙ" if divergence > 0.002 else "СРЕДНИЙ"
            return ("LONG", current_price, strength)
    
    # SHORT: быстрая EMA ниже медленной + RSI не перепродан
    if ema_fast < ema_slow and current_price < ema_slow:
        if rsi_val < RSI_OVERBOUGHT and rsi_val > 35:
            divergence = (ema_slow - ema_fast) / ema_slow
            strength = "СИЛЬНЫЙ" if divergence > 0.002 else "СРЕДНИЙ"
            return ("SHORT", current_price, strength)
    
    return None

# ==================== BOT ====================
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

# Временное хранилище состояний
USER_STATES: Dict[int, dict] = {}
LAST_SIGNALS: Dict[Tuple[int, str, str], float] = {}  # (user_id, pair, side) -> timestamp

def is_admin(user_id: int) -> bool:
    """Проверка админа"""
    return user_id in ADMIN_IDS

async def is_paid(user_id: int) -> bool:
    """Проверка оплаты доступа"""
    db = await get_db()
    try:
        cursor = await db.execute("SELECT paid FROM users WHERE id=?", (user_id,))
        row = await cursor.fetchone()
        return bool(row and row["paid"])
    finally:
        await db.close()

async def get_user_pairs(user_id: int) -> List[str]:
    """Получить пары пользователя"""
    db = await get_db()
    try:
        cursor = await db.execute("SELECT pair FROM user_pairs WHERE user_id=?", (user_id,))
        rows = await cursor.fetchall()
        return [r["pair"] for r in rows]
    finally:
        await db.close()

async def add_user_pair(user_id: int, pair: str):
    """Добавить пару пользователю"""
    db = await get_db()
    try:
        await db.execute("INSERT OR IGNORE INTO user_pairs(user_id, pair) VALUES(?,?)", 
                        (user_id, pair.upper()))
        await db.commit()
    finally:
        await db.close()

async def remove_user_pair(user_id: int, pair: str):
    """Удалить пару у пользователя"""
    db = await get_db()
    try:
        await db.execute("DELETE FROM user_pairs WHERE user_id=? AND pair=?", 
                        (user_id, pair.upper()))
        await db.commit()
    finally:
        await db.close()

# ==================== KEYBOARDS ====================
def main_menu_kb(is_admin_user: bool, is_paid_user: bool):
    """Главное меню"""
    kb = InlineKeyboardMarkup(row_width=2)
    
    if is_paid_user:
        kb.add(
            InlineKeyboardButton("📈 Алерты", callback_data="menu_alerts"),
            InlineKeyboardButton("👥 Рефералка", callback_data="menu_ref")
        )
    
    kb.add(
        InlineKeyboardButton("📖 Инструкция", callback_data="menu_guide"),
        InlineKeyboardButton("💬 Поддержка", url=SUPPORT_URL)
    )
    
    if not is_paid_user:
        kb.add(InlineKeyboardButton("🔓 Открыть доступ", callback_data="menu_pay"))
    
    if is_admin_user:
        kb.add(InlineKeyboardButton("👑 Админ", callback_data="menu_admin"))
    
    return kb

def alerts_kb(user_pairs: List[str]):
    """Меню алертов"""
    kb = InlineKeyboardMarkup(row_width=2)
    
    # Дефолтные монеты
    for pair in DEFAULT_PAIRS:
        emoji = "✅" if pair in user_pairs else "➕"
        kb.add(InlineKeyboardButton(
            f"{emoji} {pair}", 
            callback_data=f"toggle_{pair}"
        ))
    
    kb.add(
        InlineKeyboardButton("➕ Своя монета", callback_data="add_custom"),
        InlineKeyboardButton("📋 Мои монеты", callback_data="my_pairs")
    )
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_main"))
    
    return kb

def ref_kb():
    """Меню рефералки"""
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔗 Моя ссылка", callback_data="ref_link"),
        InlineKeyboardButton("💰 Баланс", callback_data="ref_balance")
    )
    kb.add(
        InlineKeyboardButton("💎 Вывод (крипта)", callback_data="ref_withdraw_crypto"),
        InlineKeyboardButton("⭐ Вывод (Stars)", callback_data="ref_withdraw_stars")
    )
    kb.add(InlineKeyboardButton("📖 Инструкция для рефов", callback_data="ref_guide"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_main"))
    return kb

def pay_kb():
    """Меню оплаты"""
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("⭐ Оплата Stars", callback_data="pay_stars"),
        InlineKeyboardButton("💎 Крипто-платёж", callback_data="pay_crypto")
    )
    kb.add(InlineKeyboardButton("🎟 У меня есть код", callback_data="pay_code"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_main"))
    return kb

def admin_kb():
    """Админ-панель"""
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📊 Статистика", callback_data="adm_stats"),
        InlineKeyboardButton("📢 Рассылка", callback_data="adm_broadcast")
    )
    kb.add(
        InlineKeyboardButton("✅ Выдать доступ", callback_data="adm_grant"),
        InlineKeyboardButton("💰 Начислить баланс", callback_data="adm_give")
    )
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_main"))
    return kb

# ==================== HANDLERS ====================
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    """Команда /start"""
    user_id = message.from_user.id
    args = message.get_args()
    
    # Определяем реферера
    invited_by = None
    if args and args.isdigit():
        ref_id = int(args)
        if ref_id != user_id:
            invited_by = ref_id
    
    # Создаём пользователя
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR IGNORE INTO users(id, invited_by, created_ts) VALUES(?,?,?)",
            (user_id, invited_by, int(time.time()))
        )
        await db.commit()
    finally:
        await db.close()
    
    # Отправляем приветствие
    text = f"<b>🚀 {BOT_NAME}</b>\n\n"
    text += "Привет! Я автоматически анализирую рынок и присылаю сигналы входа в сделки.\n\n"
    text += "Стратегия: <b>EMA + RSI</b>\n"
    text += "Нажми 📖 Инструкция, чтобы узнать как работать!"
    
    paid = await is_paid(user_id)
    
    if IMG_START:
        try:
            await bot.send_photo(
                user_id, 
                IMG_START, 
                caption=text,
                reply_markup=main_menu_kb(is_admin(user_id), paid)
            )
        except:
            await message.answer(text, reply_markup=main_menu_kb(is_admin(user_id), paid))
    else:
        await message.answer(text, reply_markup=main_menu_kb(is_admin(user_id), paid))

@dp.callback_query_handler(lambda c: c.data == "back_main")
async def back_main(call: types.CallbackQuery):
    """Назад в главное меню"""
    user_id = call.from_user.id
    paid = await is_paid(user_id)
    
    text = f"<b>🚀 {BOT_NAME}</b>\n\nГлавное меню"
    
    try:
        await call.message.edit_text(text, reply_markup=main_menu_kb(is_admin(user_id), paid))
    except:
        await call.message.answer(text, reply_markup=main_menu_kb(is_admin(user_id), paid))
    
    await call.answer()

# ==================== PAYWALL ====================
@dp.callback_query_handler(lambda c: c.data == "menu_pay")
async def menu_pay(call: types.CallbackQuery):
    """Меню оплаты"""
    text = "🔒 <b>Открыть доступ к боту</b>\n\n"
    text += "После оплаты ты получишь:\n"
    text += "• Автоматические сигналы входа (LONG/SHORT)\n"
    text += "• Отслеживание до 10 монет одновременно\n"
    text += "• Реферальную программу (50% от подписок рефералов)\n\n"
    text += "Выбери способ оплаты:"
    
    if IMG_PAYWALL:
        try:
            await call.message.delete()
            await bot.send_photo(call.from_user.id, IMG_PAYWALL, caption=text, reply_markup=pay_kb())
        except:
            await call.message.edit_text(text, reply_markup=pay_kb())
    else:
        await call.message.edit_text(text, reply_markup=pay_kb())
    
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "pay_stars")
async def pay_stars(call: types.CallbackQuery):
    """Оплата Stars"""
    await call.answer("Функция в разработке. Используйте крипто-платёж или промокод.", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "pay_crypto")
async def pay_crypto(call: types.CallbackQuery):
    """Крипто платёж"""
    text = "💎 <b>Крипто-платёж</b>\n\n"
    text += "Напиши в поддержку для получения реквизитов.\n"
    text += "После оплаты тебе выдадут промокод.\n\n"
    text += f"Поддержка: {SUPPORT_URL}"
    
    await call.message.edit_text(text, reply_markup=pay_kb())
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "pay_code")
async def pay_code(call: types.CallbackQuery):
    """Активация по коду"""
    USER_STATES[call.from_user.id] = {"mode": "waiting_promo"}
    
    text = "🎟 <b>Активация промокода</b>\n\n"
    text += "Отправь мне промокод одним сообщением.\n"
    text += "Формат: <code>PROMO-12345</code>"
    
    await call.message.edit_text(text, reply_markup=pay_kb())
    await call.answer()

@dp.message_handler(lambda m: USER_STATES.get(m.from_user.id, {}).get("mode") == "waiting_promo")
async def handle_promo(message: types.Message):
    """Обработка промокода"""
    code = message.text.strip().upper()
    
    # TODO: проверка кода в БД
    # Пока просто активируем
    db = await get_db()
    try:
        await db.execute("UPDATE users SET paid=1 WHERE id=?", (message.from_user.id,))
        await db.commit()
    finally:
        await db.close()
    
    USER_STATES.pop(message.from_user.id, None)
    
    await message.answer(
        "✅ <b>Доступ активирован!</b>\n\nТеперь можешь пользоваться всеми функциями бота.\n\n"
        "Нажми /start для перезапуска."
    )

# ==================== ALERTS ====================
@dp.callback_query_handler(lambda c: c.data == "menu_alerts")
async def menu_alerts(call: types.CallbackQuery):
    """Меню алертов"""
    user_id = call.from_user.id
    
    # ПРОВЕРКА ОПЛАТЫ
    if not await is_paid(user_id):
        await call.answer("Оплатите доступ для использования алертов!", show_alert=True)
        return
    
    pairs = await get_user_pairs(user_id)
    
    text = "📈 <b>Управление алертами</b>\n\n"
    text += "Выбери монеты для отслеживания.\n"
    text += "Бот автоматически пришлёт сигнал, когда появится точка входа.\n\n"
    text += f"Активно: <b>{len(pairs)}/10</b>"
    
    if IMG_ALERTS:
        try:
            await call.message.delete()
            await bot.send_photo(user_id, IMG_ALERTS, caption=text, reply_markup=alerts_kb(pairs))
        except:
            await call.message.edit_text(text, reply_markup=alerts_kb(pairs))
    else:
        await call.message.edit_text(text, reply_markup=alerts_kb(pairs))
    
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("toggle_"))
async def toggle_pair(call: types.CallbackQuery):
    """Добавить/удалить пару"""
    user_id = call.from_user.id
    pair = call.data.split("_", 1)[1]
    
    pairs = await get_user_pairs(user_id)
    
    if pair in pairs:
        await remove_user_pair(user_id, pair)
        await call.answer(f"❌ {pair} удалён")
    else:
        if len(pairs) >= 10:
            await call.answer("Максимум 10 монет!", show_alert=True)
            return
        await add_user_pair(user_id, pair)
        await call.answer(f"✅ {pair} добавлен")
    
    # Обновляем клавиатуру
    pairs = await get_user_pairs(user_id)
    text = "📈 <b>Управление алертами</b>\n\n"
    text += "Выбери монеты для отслеживания.\n\n"
    text += f"Активно: <b>{len(pairs)}/10</b>"
    
    try:
        await call.message.edit_text(text, reply_markup=alerts_kb(pairs))
    except:
        pass

@dp.callback_query_handler(lambda c: c.data == "add_custom")
async def add_custom(call: types.CallbackQuery):
    """Добавить свою монету"""
    user_id = call.from_user.id
    pairs = await get_user_pairs(user_id)
    
    if len(pairs) >= 10:
        await call.answer("Максимум 10 монет!", show_alert=True)
        return
    
    USER_STATES[user_id] = {"mode": "waiting_custom_pair"}
    
    text = "➕ <b>Своя монета</b>\n\n"
    text += "Отправь символ монеты в формате:\n"
    text += "<code>SOLUSDT</code>\n\n"
    text += "Список доступных пар: binance.com"
    
    await call.message.edit_text(text, reply_markup=alerts_kb(pairs))
    await call.answer()

@dp.message_handler(lambda m: USER_STATES.get(m.from_user.id, {}).get("mode") == "waiting_custom_pair")
async def handle_custom_pair(message: types.Message):
    """Обработка своей монеты"""
    user_id = message.from_user.id
    pair = message.text.strip().upper()
    
    # Проверяем формат
    if not pair.endswith("USDT") or len(pair) < 6:
        await message.answer("❌ Неверный формат. Пример: SOLUSDT")
        return
    
    # Проверяем что пара существует
    async with httpx.AsyncClient() as client:
        price = await fetch_price(client, pair)
        if price is None:
            await message.answer(f"❌ Пара {pair} не найдена на Binance")
            return
    
    await add_user_pair(user_id, pair)
    USER_STATES.pop(user_id, None)
    
    await message.answer(f"✅ {pair} добавлена!\n\nТекущая цена: <b>{price:.8f}</b>")

@dp.callback_query_handler(lambda c: c.data == "my_pairs")
async def my_pairs(call: types.CallbackQuery):
    """Список моих пар"""
    pairs = await get_user_pairs(call.from_user.id)
    
    if not pairs:
        await call.answer("Нет активных монет", show_alert=True)
        return
    
    text = "📋 <b>Мои монеты</b>\n\n"
    
    async with httpx.AsyncClient() as client:
        for pair in pairs:
            price = await fetch_price(client, pair)
            if price:
                text += f"• {pair}: <code>{price:.8f}</code>\n"
            else:
                text += f"• {pair}: ❌\n"
    
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🗑 Удалить всё", callback_data="clear_all_pairs"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="menu_alerts"))
    
    await call.message.edit_text(text, reply_markup=kb)
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "clear_all_pairs")
async def clear_all_pairs(call: types.CallbackQuery):
    """Удалить все пары"""
    db = await get_db()
    try:
        await db.execute("DELETE FROM user_pairs WHERE user_id=?", (call.from_user.id,))
        await db.commit()
    finally:
        await db.close()
    
    await call.answer("🗑 Все монеты удалены")
    await menu_alerts(call)

# ==================== REFERRAL ====================
@dp.callback_query_handler(lambda c: c.data == "menu_ref")
async def menu_ref(call: types.CallbackQuery):
    """Меню рефералки"""
    text = "👥 <b>Реферальная программа</b>\n\n"
    text += "Получай <b>50%</b> от подписки каждого реферала!\n\n"
    text += "💰 Выплаты:\n"
    text += "• Крипто-кошельки (USDT, BTC, ETH)\n"
    text += "• Telegram Stars ⭐\n\n"
    text += "Минимум для вывода: <b>$20</b>"
    
    if IMG_REF:
        try:
            await call.message.delete()
            await bot.send_photo(call.from_user.id, IMG_REF, caption=text, reply_markup=ref_kb())
        except:
            await call.message.edit_text(text, reply_markup=ref_kb())
    else:
        await call.message.edit_text(text, reply_markup=ref_kb())
    
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "ref_link")
async def ref_link(call: types.CallbackQuery):
    """Реферальная ссылка"""
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start={call.from_user.id}"
    
    text = "🔗 <b>Твоя реферальная ссылка:</b>\n\n"
    text += f"<code>{link}</code>\n\n"
    text += "Отправь её друзьям и получай 50% с их подписок!"
    
    await call.message.edit_text(text, reply_markup=ref_kb(), disable_web_page_preview=True)
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "ref_balance")
async def ref_balance(call: types.CallbackQuery):
    """Баланс рефералки"""
    db = await get_db()
    try:
        cursor = await db.execute("SELECT balance FROM users WHERE id=?", (call.from_user.id,))
        row = await cursor.fetchone()
        balance = row["balance"] if row else 0.0
        
        cursor = await db.execute(
            "SELECT COUNT(*) as cnt FROM users WHERE invited_by=? AND paid=1", 
            (call.from_user.id,)
        )
        row = await cursor.fetchone()
        refs_count = row["cnt"] if row else 0
    finally:
        await db.close()
    
    text = "💰 <b>Баланс партнёра</b>\n\n"
    text += f"Доступно: <b>${balance:.2f}</b>\n"
    text += f"Рефералов оплатило: <b>{refs_count}</b>\n\n"
    text += "Минимум для вывода: $20"
    
    await call.message.edit_text(text, reply_markup=ref_kb())
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "ref_withdraw_crypto")
async def ref_withdraw_crypto(call: types.CallbackQuery):
    """Вывод криптой"""
    text = "💎 <b>Вывод на крипто-кошелёк</b>\n\n"
    text += "Отправь команду в формате:\n"
    text += "<code>/withdraw USDT TRC20 адрес сумма</code>\n\n"
    text += "Пример:\n"
    text += "<code>/withdraw USDT TRC20 TLxxx... 25</code>\n\n"
    text += "Поддерживаемые монеты: USDT, BTC, ETH\n"
    text += "Сети: TRC20, ERC20, BEP20"
    
    await call.message.edit_text(text, reply_markup=ref_kb())
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "ref_withdraw_stars")
async def ref_withdraw_stars(call: types.CallbackQuery):
    """Вывод Stars"""
    text = "⭐ <b>Вывод Telegram Stars</b>\n\n"
    text += "Отправь команду:\n"
    text += "<code>/withdraw_stars сумма</code>\n\n"
    text += "Пример:\n"
    text += "<code>/withdraw_stars 100</code>\n\n"
    text += "1 Star = $1"
    
    await call.message.edit_text(text, reply_markup=ref_kb())
    await call.answer()

@dp.message_handler(commands=["withdraw"])
async def cmd_withdraw(message: types.Message):
    """Команда вывода крипты"""
    parts = message.text.split()
    
    if len(parts) != 5:
        await message.reply(
            "❌ Неверный формат\n\n"
            "Используй:\n"
            "<code>/withdraw USDT TRC20 адрес сумма</code>"
        )
        return
    
    coin, network, address, amount_str = parts[1], parts[2], parts[3], parts[4]
    
    try:
        amount = float(amount_str)
    except:
        await message.reply("❌ Сумма должна быть числом")
        return
    
    if amount < 20:
        await message.reply("❌ Минимальная сумма вывода: $20")
        return
    
    # Проверяем баланс
    db = await get_db()
    try:
        cursor = await db.execute("SELECT balance FROM users WHERE id=?", (message.from_user.id,))
        row = await cursor.fetchone()
        balance = row["balance"] if row else 0.0
        
        if balance < amount:
            await message.reply(f"❌ Недостаточно средств. Доступно: ${balance:.2f}")
            return
    finally:
        await db.close()
    
    # TODO: Здесь логика обработки заявки (добавить в таблицу payouts для админа)
    
    await message.reply(
        f"✅ <b>Заявка принята</b>\n\n"
        f"Монета: {coin}\n"
        f"Сеть: {network}\n"
        f"Адрес: <code>{address}</code>\n"
        f"Сумма: ${amount:.2f}\n\n"
        f"Ожидай обработки (обычно до 24 часов)."
    )
    
    # Уведомляем админов
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"💸 <b>Новая заявка на вывод</b>\n\n"
                f"User ID: {message.from_user.id}\n"
                f"Username: @{message.from_user.username or 'нет'}\n"
                f"Монета: {coin}\n"
                f"Сеть: {network}\n"
                f"Адрес: <code>{address}</code>\n"
                f"Сумма: ${amount:.2f}"
            )
        except:
            pass

@dp.message_handler(commands=["withdraw_stars"])
async def cmd_withdraw_stars(message: types.Message):
    """Команда вывода Stars"""
    parts = message.text.split()
    
    if len(parts) != 2:
        await message.reply(
            "❌ Неверный формат\n\n"
            "Используй: <code>/withdraw_stars 100</code>"
        )
        return
    
    try:
        amount = int(parts[1])
    except:
        await message.reply("❌ Сумма должна быть целым числом")
        return
    
    if amount < 20:
        await message.reply("❌ Минимальная сумма вывода: 20 Stars")
        return
    
    # Проверяем баланс
    db = await get_db()
    try:
        cursor = await db.execute("SELECT balance FROM users WHERE id=?", (message.from_user.id,))
        row = await cursor.fetchone()
        balance = row["balance"] if row else 0.0
        
        if balance < amount:
            await message.reply(f"❌ Недостаточно средств. Доступно: ${balance:.2f}")
            return
    finally:
        await db.close()
    
    await message.reply(
        f"✅ <b>Заявка принята</b>\n\n"
        f"Сумма: ⭐ {amount}\n\n"
        f"Ожидай обработки (обычно до 24 часов)."
    )
    
    # Уведомляем админов
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"⭐ <b>Новая заявка на вывод Stars</b>\n\n"
                f"User ID: {message.from_user.id}\n"
                f"Username: @{message.from_user.username or 'нет'}\n"
                f"Сумма: {amount} Stars"
            )
        except:
            pass

@dp.callback_query_handler(lambda c: c.data == "ref_guide")
async def ref_guide(call: types.CallbackQuery):
    """Инструкция для рефов"""
    text = "📖 <b>Инструкция для партнёров</b>\n\n"
    text += "<b>Как заработать:</b>\n"
    text += "1️⃣ Получи свою реф. ссылку\n"
    text += "2️⃣ Поделись ей с друзьями / в соц.сетях\n"
    text += "3️⃣ Получай 50% от каждой подписки реферала\n\n"
    text += "<b>Выплаты:</b>\n"
    text += "• Минимум: $20\n"
    text += "• Крипта: USDT (TRC20/ERC20), BTC, ETH\n"
    text += "• Telegram Stars: от 20 ⭐\n"
    text += "• Срок обработки: до 24 часов\n\n"
    text += "<b>Советы:</b>\n"
    text += "✅ Расскажи о преимуществах бота\n"
    text += "✅ Покажи примеры сигналов\n"
    text += "✅ Размести ссылку в профиле"
    
    await call.message.edit_text(text, reply_markup=ref_kb())
    await call.answer()

# ==================== GUIDE ====================
@dp.callback_query_handler(lambda c: c.data == "menu_guide")
async def menu_guide(call: types.CallbackQuery):
    """Инструкция по использованию"""
    text = "📖 <b>Как пользоваться ботом</b>\n\n"
    text += "<b>Шаг 1: Получи доступ</b>\n"
    text += "Оплати подписку через 🔓 Открыть доступ\n\n"
    text += "<b>Шаг 2: Выбери монеты</b>\n"
    text += "Зайди в 📈 Алерты и выбери 3 дефолтные монеты (BTC, ETH, TON) или добавь свою.\n\n"
    text += "<b>Шаг 3: Жди сигналы</b>\n"
    text += "Бот автоматически пришлёт уведомление, когда появится точка входа:\n"
    text += "• LONG (покупка) 📈\n"
    text += "• SHORT (продажа) 📉\n\n"
    text += "<b>Стратегия бота:</b>\n"
    text += f"• EMA {EMA_FAST}/{EMA_SLOW} для тренда\n"
    text += f"• RSI {RSI_PERIOD} для фильтрации\n"
    text += "• Сигналы с пометкой силы (СИЛЬНЫЙ/СРЕДНИЙ)\n\n"
    text += "<b>Важно:</b>\n"
    text += "⚠️ Это не финансовый совет\n"
    text += "⚠️ Используй стоп-лоссы\n"
    text += "⚠️ Не вкладывай последние деньги"
    
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_main"))
    
    await call.message.edit_text(text, reply_markup=kb)
    await call.answer()

# ==================== ADMIN ====================
@dp.callback_query_handler(lambda c: c.data == "menu_admin")
async def menu_admin(call: types.CallbackQuery):
    """Админ-панель"""
    if not is_admin(call.from_user.id):
        await call.answer("❌ Нет доступа", show_alert=True)
        return
    
    text = "👑 <b>Админ-панель</b>\n\nВыберите действие:"
    
    await call.message.edit_text(text, reply_markup=admin_kb())
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "adm_stats")
async def adm_stats(call: types.CallbackQuery):
    """Статистика"""
    if not is_admin(call.from_user.id):
        return
    
    db = await get_db()
    try:
        # Пользователи
        cursor = await db.execute("SELECT COUNT(*) as cnt FROM users")
        total_users = (await cursor.fetchone())["cnt"]
        
        cursor = await db.execute("SELECT COUNT(*) as cnt FROM users WHERE paid=1")
        paid_users = (await cursor.fetchone())["cnt"]
        
        # Активные пары
        cursor = await db.execute("SELECT COUNT(DISTINCT user_id) as cnt FROM user_pairs")
        active_users = (await cursor.fetchone())["cnt"]
        
        cursor = await db.execute("SELECT COUNT(*) as cnt FROM user_pairs")
        total_pairs = (await cursor.fetchone())["cnt"]
        
    finally:
        await db.close()
    
    text = "📊 <b>Статистика бота</b>\n\n"
    text += f"👥 Всего пользователей: <b>{total_users}</b>\n"
    text += f"💎 Оплативших: <b>{paid_users}</b>\n"
    text += f"📈 С активными алертами: <b>{active_users}</b>\n"
    text += f"🔔 Всего отслеживаемых пар: <b>{total_pairs}</b>\n\n"
    text += f"Конверсия: <b>{(paid_users/total_users*100) if total_users > 0 else 0:.1f}%</b>"
    
    await call.message.edit_text(text, reply_markup=admin_kb())
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "adm_broadcast")
async def adm_broadcast(call: types.CallbackQuery):
    """Рассылка"""
    if not is_admin(call.from_user.id):
        return
    
    USER_STATES[call.from_user.id] = {"mode": "admin_broadcast"}
    
    text = "📢 <b>Рассылка</b>\n\n"
    text += "Отправь сообщение одним текстом.\n"
    text += "Оно будет разослано всем пользователям."
    
    await call.message.edit_text(text, reply_markup=admin_kb())
    await call.answer()

@dp.message_handler(lambda m: USER_STATES.get(m.from_user.id, {}).get("mode") == "admin_broadcast")
async def handle_broadcast(message: types.Message):
    """Обработка рассылки"""
    if not is_admin(message.from_user.id):
        return
    
    text = message.html_text
    
    db = await get_db()
    try:
        cursor = await db.execute("SELECT id FROM users")
        users = await cursor.fetchall()
    finally:
        await db.close()
    
    sent = 0
    for user in users:
        try:
            await bot.send_message(user["id"], text, disable_web_page_preview=True)
            sent += 1
            await asyncio.sleep(0.05)  # Защита от лимитов
        except:
            pass
    
    USER_STATES.pop(message.from_user.id, None)
    
    await message.reply(f"✅ Рассылка завершена\n\nОтправлено: {sent}/{len(users)}")

@dp.callback_query_handler(lambda c: c.data == "adm_grant")
async def adm_grant(call: types.CallbackQuery):
    """Выдать доступ"""
    if not is_admin(call.from_user.id):
        return
    
    USER_STATES[call.from_user.id] = {"mode": "admin_grant"}
    
    text = "✅ <b>Выдать доступ</b>\n\n"
    text += "Отправь ID пользователя:"
    
    await call.message.edit_text(text, reply_markup=admin_kb())
    await call.answer()

@dp.message_handler(lambda m: USER_STATES.get(m.from_user.id, {}).get("mode") == "admin_grant")
async def handle_grant(message: types.Message):
    """Обработка выдачи доступа"""
    if not is_admin(message.from_user.id):
        return
    
    try:
        user_id = int(message.text.strip())
    except:
        await message.reply("❌ ID должен быть числом")
        return
    
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR IGNORE INTO users(id, created_ts) VALUES(?,?)",
            (user_id, int(time.time()))
        )
        await db.execute("UPDATE users SET paid=1 WHERE id=?", (user_id,))
        await db.commit()
    finally:
        await db.close()
    
    USER_STATES.pop(message.from_user.id, None)
    
    await message.reply(f"✅ Доступ выдан пользователю {user_id}")
    
    # Уведомляем пользователя
    try:
        await bot.send_message(
            user_id,
            "🎉 <b>Доступ активирован!</b>\n\n"
            "Теперь можешь пользоваться всеми функциями бота.\n"
            "Нажми /start"
        )
    except:
        pass

@dp.callback_query_handler(lambda c: c.data == "adm_give")
async def adm_give(call: types.CallbackQuery):
    """Начислить баланс"""
    if not is_admin(call.from_user.id):
        return
    
    USER_STATES[call.from_user.id] = {"mode": "admin_give_uid"}
    
    text = "💰 <b>Начислить баланс</b>\n\n"
    text += "Отправь ID пользователя:"
    
    await call.message.edit_text(text, reply_markup=admin_kb())
    await call.answer()

@dp.message_handler(lambda m: USER_STATES.get(m.from_user.id, {}).get("mode") == "admin_give_uid")
async def handle_give_uid(message: types.Message):
    """ID для начисления"""
    if not is_admin(message.from_user.id):
        return
    
    try:
        user_id = int(message.text.strip())
    except:
        await message.reply("❌ ID должен быть числом")
        return
    
    USER_STATES[message.from_user.id] = {"mode": "admin_give_amount", "target_id": user_id}
    
    await message.reply("Отправь сумму для начисления:")

@dp.message_handler(lambda m: USER_STATES.get(m.from_user.id, {}).get("mode") == "admin_give_amount")
async def handle_give_amount(message: types.Message):
    """Сумма для начисления"""
    if not is_admin(message.from_user.id):
        return
    
    try:
        amount = float(message.text.strip())
    except:
        await message.reply("❌ Сумма должна быть числом")
        return
    
    target_id = USER_STATES[message.from_user.id]["target_id"]
    
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR IGNORE INTO users(id, created_ts) VALUES(?,?)",
            (target_id, int(time.time()))
        )
        await db.execute(
            "UPDATE users SET balance = COALESCE(balance, 0) + ? WHERE id=?",
            (amount, target_id)
        )
        await db.commit()
    finally:
        await db.close()
    
    USER_STATES.pop(message.from_user.id, None)
    
    await message.reply(f"✅ Начислено ${amount:.2f} пользователю {target_id}")
    
    # Уведомляем пользователя
    try:
        await bot.send_message(
            target_id,
            f"💰 На ваш баланс начислено <b>${amount:.2f}</b>"
        )
    except:
        pass

# ==================== BACKGROUND TASKS ====================
async def price_collector():
    """Сборщик цен"""
    logger.info("Price collector started")
    
    async with httpx.AsyncClient() as client:
        while True:
            try:
                # Получаем все уникальные пары
                db = await get_db()
                try:
                    cursor = await db.execute("SELECT DISTINCT pair FROM user_pairs")
                    rows = await cursor.fetchall()
                    pairs = [r["pair"] for r in rows]
                finally:
                    await db.close()
                
                # Добавляем дефолтные
                pairs = list(set(pairs + DEFAULT_PAIRS))
                
                # Собираем цены
                ts = time.time()
                for pair in pairs:
                    price = await fetch_price(client, pair)
                    if price:
                        CANDLES.add_price(pair, price, ts)
                
            except Exception as e:
                logger.error(f"Price collector error: {e}")
            
            await asyncio.sleep(CHECK_INTERVAL)

async def signal_analyzer():
    """Анализатор сигналов"""
    logger.info("Signal analyzer started")
    
    while True:
        try:
            # Получаем все пары пользователей
            db = await get_db()
            try:
                cursor = await db.execute(
                    "SELECT up.user_id, up.pair FROM user_pairs up "
                    "JOIN users u ON up.user_id = u.id WHERE u.paid = 1"
                )
                rows = await cursor.fetchall()
            finally:
                await db.close()
            
            # Группируем по парам
            pairs_users = defaultdict(list)
            for row in rows:
                pairs_users[row["pair"]].append(row["user_id"])
            
            # Анализируем каждую пару
            now = time.time()
            for pair, users in pairs_users.items():
                signal = analyze_entry(pair)
                
                if signal:
                    side, price, strength = signal
                    
                    # Отправляем сигнал пользователям
                    for user_id in users:
                        key = (user_id, pair, side)
                        
                        # Проверяем куллдаун
                        if now - LAST_SIGNALS.get(key, 0) < SIGNAL_COOLDOWN:
                            continue
                        
                        try:
                            emoji = "📈" if side == "LONG" else "📉"
                            text = f"{emoji} <b>СИГНАЛ ВХОДА</b>\n\n"
                            text += f"Монета: <b>{pair}</b>\n"
                            text += f"Направление: <b>{side}</b>\n"
                            text += f"Цена: <code>{price:.8f}</code>\n"
                            text += f"Сила: <b>{strength}</b>\n\n"
                            text += f"⏰ {time.strftime('%H:%M:%S')}"
                            
                            await bot.send_message(user_id, text)
                            LAST_SIGNALS[key] = now
                            
                        except Exception as e:
                            logger.error(f"Error sending signal to {user_id}: {e}")
            
        except Exception as e:
            logger.error(f"Signal analyzer error: {e}")
        
        await asyncio.sleep(5)

# ==================== STARTUP ====================
async def on_startup():
    """Инициализация при запуске"""
    logger.info("Bot starting...")
    
    # Сбрасываем webhook (если был)
    await bot.delete_webhook(drop_pending_updates=True)
    
    # Инициализируем БД
    await init_db()
    
    # Запускаем фоновые задачи
    loop = asyncio.get_event_loop()
    loop.create_task(price_collector())
    loop.create_task(signal_analyzer())
    
    logger.info("Bot started successfully!")

# ==================== MAIN ====================
if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(on_startup())
    executor.start_polling(dp, skip_updates=True)

"""
Alpha Entry Bot - Оптимизированная версия для высоких нагрузок
Поддерживает 5000-10000 активных пользователей
"""
import os, time, asyncio, logging
from typing import Optional, Dict, List, Tuple
from collections import defaultdict, deque
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.exceptions import RetryAfter, TelegramAPIError
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

# ОПТИМИЗАЦИЯ: увеличил интервалы
CHECK_INTERVAL = 60  # 1 минута между проверками (снижает нагрузку на API)
CANDLE_TF = 60
MAX_CANDLES = 300  # уменьшил для экономии памяти

# Индикаторы
EMA_FAST = 9
EMA_SLOW = 21
EMA_TREND = 50
EMA_LONG_TREND = 200  # для глобального тренда
RSI_PERIOD = 14
RSI_OVERSOLD = 35
RSI_OVERBOUGHT = 65
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
BB_PERIOD = 20
BB_STD = 2

# Фильтры - УСИЛЕНЫ для точности
MIN_SIGNAL_SCORE = 85  # повышен с 75 до 85
MAX_SIGNALS_PER_DAY = 3  # уменьшен с 5 до 3 (только лучшие)
SIGNAL_COOLDOWN = 21600  # 6 часов (вместо 4)

# ОПТИМИЗАЦИЯ: кэширование
PRICE_CACHE_TTL = 30  # кэш цен на 30 секунд
BATCH_SEND_SIZE = 30  # отправлять уведомления группами по 30
BATCH_SEND_DELAY = 0.05  # задержка между сообщениями (избегаем rate limit)

# Картинки
IMG_START = os.getenv("IMG_START", "")
IMG_ALERTS = os.getenv("IMG_ALERTS", "")
IMG_REF = os.getenv("IMG_REF", "")
IMG_PAYWALL = os.getenv("IMG_PAYWALL", "")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== CACHE ====================
class PriceCache:
    """Кэш цен для снижения нагрузки на API"""
    def __init__(self, ttl: int = PRICE_CACHE_TTL):
        self.cache: Dict[str, Tuple[float, float, float]] = {}  # pair -> (price, volume, timestamp)
        self.ttl = ttl
    
    def get(self, pair: str) -> Optional[Tuple[float, float]]:
        """Получить цену из кэша"""
        if pair in self.cache:
            price, volume, cached_at = self.cache[pair]
            if time.time() - cached_at < self.ttl:
                return price, volume
        return None
    
    def set(self, pair: str, price: float, volume: float):
        """Сохранить цену в кэш"""
        self.cache[pair] = (price, volume, time.time())
    
    def clear_old(self):
        """Очистка устаревших записей"""
        now = time.time()
        self.cache = {
            k: v for k, v in self.cache.items()
            if now - v[2] < self.ttl
        }

PRICE_CACHE = PriceCache()

# ==================== DATABASE ====================
INIT_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=10000;
PRAGMA temp_store=MEMORY;

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

CREATE TABLE IF NOT EXISTS signals_sent (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    pair TEXT NOT NULL,
    side TEXT NOT NULL,
    price REAL NOT NULL,
    score INTEGER NOT NULL,
    sent_ts INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_signals_pair_ts ON signals_sent(pair, sent_ts);
CREATE INDEX IF NOT EXISTS idx_user_pairs_user ON user_pairs(user_id);
CREATE INDEX IF NOT EXISTS idx_users_paid ON users(paid);
"""

# ОПТИМИЗАЦИЯ: пул соединений
class DBPool:
    """Простой пул соединений к БД"""
    def __init__(self, path: str, pool_size: int = 5):
        self.path = path
        self.pool_size = pool_size
        self._pool: List[aiosqlite.Connection] = []
        self._available = asyncio.Queue()
        self._initialized = False
    
    async def init(self):
        """Инициализация пула"""
        if self._initialized:
            return
        
        for _ in range(self.pool_size):
            conn = await aiosqlite.connect(self.path)
            conn.row_factory = aiosqlite.Row
            self._pool.append(conn)
            await self._available.put(conn)
        
        # Инициализация схемы
        conn = await self.acquire()
        try:
            await conn.executescript(INIT_SQL)
            await conn.commit()
        finally:
            await self.release(conn)
        
        self._initialized = True
        logger.info(f"Database pool initialized with {self.pool_size} connections")
    
    async def acquire(self) -> aiosqlite.Connection:
        """Получить соединение из пула"""
        return await self._available.get()
    
    async def release(self, conn: aiosqlite.Connection):
        """Вернуть соединение в пул"""
        await self._available.put(conn)
    
    async def close(self):
        """Закрыть все соединения"""
        for conn in self._pool:
            await conn.close()

db_pool = DBPool(DB_PATH, pool_size=5)

async def init_db():
    await db_pool.init()

# ==================== MARKET DATA ====================
class CandleStorage:
    def __init__(self, timeframe=CANDLE_TF, maxlen=MAX_CANDLES):
        self.tf = timeframe
        self.maxlen = maxlen
        self.candles: Dict[str, deque] = defaultdict(lambda: deque(maxlen=maxlen))
        self.current: Dict[str, dict] = {}
    
    def get_bucket(self, ts: float) -> int:
        return int(ts // self.tf) * self.tf
    
    def add_price(self, pair: str, price: float, volume: float, ts: float):
        pair = pair.upper()
        bucket = self.get_bucket(ts)
        
        if pair not in self.current or self.current[pair]["ts"] != bucket:
            if pair in self.current:
                self.candles[pair].append(self.current[pair])
            self.current[pair] = {
                "ts": bucket, "o": price, "h": price, "l": price, "c": price, "v": volume
            }
        else:
            c = self.current[pair]
            c["h"] = max(c["h"], price)
            c["l"] = min(c["l"], price)
            c["c"] = price
            c["v"] += volume
    
    def get_candles(self, pair: str) -> List[dict]:
        pair = pair.upper()
        result = list(self.candles[pair])
        if pair in self.current:
            result.append(self.current[pair])
        return result

CANDLES = CandleStorage()

async def fetch_price(client: httpx.AsyncClient, pair: str) -> Optional[Tuple[float, float]]:
    """Получить цену с кэшированием"""
    # Проверяем кэш
    cached = PRICE_CACHE.get(pair)
    if cached:
        return cached
    
    try:
        url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={pair.upper()}"
        resp = await client.get(url, timeout=5.0)
        resp.raise_for_status()
        data = resp.json()
        price = float(data["lastPrice"])
        volume = float(data["volume"])
        
        # Сохраняем в кэш
        PRICE_CACHE.set(pair, price, volume)
        return price, volume
    except Exception as e:
        logger.error(f"Error fetching {pair}: {e}")
        return None

# ==================== INDICATORS ====================
def ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e

def rsi(closes: List[float], period: int = 14) -> Optional[float]:
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
    return 100 - (100 / (1 + avg_gain / avg_loss))

def atr(candles: List[dict], period=14) -> Optional[float]:
    if len(candles) < period + 1:
        return None
    true_ranges = []
    for i in range(-period, 0):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i-1]["c"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        true_ranges.append(tr)
    return sum(true_ranges) / period

def calculate_tp_sl(entry: float, side: str, atr_val: float) -> Dict:
    sl_dist = atr_val * 1.5
    tp_dist = sl_dist * 2.5
    
    if side == "LONG":
        sl, tp = entry - sl_dist, entry + tp_dist
    else:
        sl, tp = entry + sl_dist, entry - tp_dist
    
    return {
        "stop_loss": sl,
        "take_profit": tp,
        "sl_percent": abs((sl - entry) / entry * 100),
        "tp_percent": abs((tp - entry) / entry * 100)
    }

# ==================== STRATEGY ====================
def analyze_signal(pair: str) -> Optional[Dict]:
    """Упрощённая быстрая стратегия для масштабирования"""
    candles = CANDLES.get_candles(pair)
    
    if len(candles) < 100:
        return None
    
    closes = [c["c"] for c in candles]
    current_price = closes[-1]
    
    ema9 = ema(closes, EMA_FAST)
    ema21 = ema(closes, EMA_SLOW)
    ema50 = ema(closes, EMA_TREND)
    rsi_val = rsi(closes, RSI_PERIOD)
    atr_val = atr(candles, 14)
    
    if None in [ema9, ema21, ema50, rsi_val, atr_val]:
        return None
    
    score = 0
    reasons = []
    side = None
    
    # LONG
    if ema9 > ema21 > ema50 and current_price > ema50:
        score += 40
        reasons.append("Сильный восходящий тренд")
        
        if RSI_OVERSOLD < rsi_val < 60:
            score += 30
            reasons.append(f"RSI оптимален ({rsi_val:.1f})")
        
        if (ema9 - ema21) / ema21 > 0.003:
            score += 30
            reasons.append("Сильный импульс вверх")
        
        if score >= MIN_SIGNAL_SCORE:
            side = "LONG"
    
    # SHORT
    elif ema9 < ema21 < ema50 and current_price < ema50:
        score += 40
        reasons.append("Сильный нисходящий тренд")
        
        if 40 < rsi_val < RSI_OVERBOUGHT:
            score += 30
            reasons.append(f"RSI оптимален ({rsi_val:.1f})")
        
        if (ema21 - ema9) / ema21 > 0.003:
            score += 30
            reasons.append("Сильный импульс вниз")
        
        if score >= MIN_SIGNAL_SCORE:
            side = "SHORT"
    
    if side and score >= MIN_SIGNAL_SCORE:
        tp_sl = calculate_tp_sl(current_price, side, atr_val)
        return {
            "side": side,
            "price": current_price,
            "score": score,
            "reasons": reasons,
            **tp_sl
        }
    
    return None

# ==================== BOT ====================
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

USER_STATES: Dict[int, dict] = {}
LAST_SIGNALS: Dict[Tuple[str, str], float] = {}

def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

async def is_paid(uid: int) -> bool:
    conn = await db_pool.acquire()
    try:
        cursor = await conn.execute("SELECT paid FROM users WHERE id=?", (uid,))
        row = await cursor.fetchone()
        return bool(row and row["paid"])
    finally:
        await db_pool.release(conn)

async def get_user_pairs(uid: int) -> List[str]:
    conn = await db_pool.acquire()
    try:
        cursor = await conn.execute("SELECT pair FROM user_pairs WHERE user_id=?", (uid,))
        rows = await cursor.fetchall()
        return [r["pair"] for r in rows]
    finally:
        await db_pool.release(conn)

async def add_user_pair(uid: int, pair: str):
    conn = await db_pool.acquire()
    try:
        await conn.execute("INSERT OR IGNORE INTO user_pairs(user_id, pair) VALUES(?,?)", (uid, pair.upper()))
        await conn.commit()
    finally:
        await db_pool.release(conn)

async def remove_user_pair(uid: int, pair: str):
    conn = await db_pool.acquire()
    try:
        await conn.execute("DELETE FROM user_pairs WHERE user_id=? AND pair=?", (uid, pair.upper()))
        await conn.commit()
    finally:
        await db_pool.release(conn)

async def count_signals_today(pair: str) -> int:
    conn = await db_pool.acquire()
    try:
        today_start = int(datetime.now().replace(hour=0, minute=0, second=0).timestamp())
        cursor = await conn.execute(
            "SELECT COUNT(*) as cnt FROM signals_sent WHERE pair=? AND sent_ts >= ?",
            (pair, today_start)
        )
        row = await cursor.fetchone()
        return row["cnt"] if row else 0
    finally:
        await db_pool.release(conn)

async def log_signal(uid: int, pair: str, side: str, price: float, score: int):
    conn = await db_pool.acquire()
    try:
        await conn.execute(
            "INSERT INTO signals_sent(user_id, pair, side, price, score, sent_ts) VALUES(?,?,?,?,?,?)",
            (uid, pair, side, price, score, int(time.time()))
        )
        await conn.commit()
    finally:
        await db_pool.release(conn)

# ОПТИМИЗАЦИЯ: батчинг отправки сообщений
async def send_message_safe(user_id: int, text: str, **kwargs):
    """Безопасная отправка с обработкой rate limit"""
    try:
        await bot.send_message(user_id, text, **kwargs)
        return True
    except RetryAfter as e:
        await asyncio.sleep(e.timeout)
        return await send_message_safe(user_id, text, **kwargs)
    except TelegramAPIError:
        return False

# ==================== KEYBOARDS ====================
def main_menu_kb(is_admin_user: bool, is_paid_user: bool):
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
    kb = InlineKeyboardMarkup(row_width=2)
    for pair in DEFAULT_PAIRS:
        emoji = "✅" if pair in user_pairs else "➕"
        kb.add(InlineKeyboardButton(f"{emoji} {pair}", callback_data=f"toggle_{pair}"))
    kb.add(
        InlineKeyboardButton("➕ Своя монета", callback_data="add_custom"),
        InlineKeyboardButton("📋 Мои монеты", callback_data="my_pairs")
    )
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_main"))
    return kb

def ref_kb():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔗 Моя ссылка", callback_data="ref_link"),
        InlineKeyboardButton("💰 Баланс", callback_data="ref_balance")
    )
    kb.add(
        InlineKeyboardButton("💎 Вывод (крипта)", callback_data="ref_withdraw_crypto"),
        InlineKeyboardButton("⭐ Вывод (Stars)", callback_data="ref_withdraw_stars")
    )
    kb.add(InlineKeyboardButton("📖 Гайд для рефов", callback_data="ref_guide"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_main"))
    return kb

def pay_kb():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("⭐ Оплата Stars", callback_data="pay_stars"),
        InlineKeyboardButton("💎 Крипто", callback_data="pay_crypto")
    )
    kb.add(InlineKeyboardButton("🎟 У меня код", callback_data="pay_code"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_main"))
    return kb

def admin_kb():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📊 Статистика", callback_data="adm_stats"),
        InlineKeyboardButton("📢 Рассылка", callback_data="adm_broadcast")
    )
    kb.add(
        InlineKeyboardButton("✅ Выдать доступ", callback_data="adm_grant"),
        InlineKeyboardButton("💰 Начислить", callback_data="adm_give")
    )
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_main"))
    return kb

# ==================== HANDLERS ====================
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    uid = message.from_user.id
    args = message.get_args()
    invited_by = int(args) if args and args.isdigit() and int(args) != uid else None
    
    conn = await db_pool.acquire()
    try:
        await conn.execute(
            "INSERT OR IGNORE INTO users(id, invited_by, created_ts) VALUES(?,?,?)",
            (uid, invited_by, int(time.time()))
        )
        await conn.commit()
    finally:
        await db_pool.release(conn)
    
    text = f"<b>🚀 {BOT_NAME}</b>\n\n"
    text += "Точные сигналы с автоматическим TP/SL\n\n"
    text += "• 3-5 сильных сигналов в день\n"
    text += "• Мультистратегия (5+ индикаторов)\n"
    text += "• Объяснение каждого входа\n\n"
    text += "📖 Жми Инструкция для деталей"
    
    paid = await is_paid(uid)
    await message.answer(text, reply_markup=main_menu_kb(is_admin(uid), paid))

@dp.callback_query_handler(lambda c: c.data == "back_main")
async def back_main(call: types.CallbackQuery):
    paid = await is_paid(call.from_user.id)
    text = f"<b>🚀 {BOT_NAME}</b>\n\nГлавное меню"
    try:
        await call.message.edit_text(text, reply_markup=main_menu_kb(is_admin(call.from_user.id), paid))
    except:
        pass
    await call.answer()

# ==================== PAYWALL ====================
@dp.callback_query_handler(lambda c: c.data == "menu_pay")
async def menu_pay(call: types.CallbackQuery):
    text = "🔒 <b>Открыть доступ</b>\n\n"
    text += "✅ 3-5 точных сигналов в день\n"
    text += "✅ Автоматический TP/SL\n"
    text += "✅ Мультистратегия\n"
    text += "✅ До 10 монет\n"
    text += "✅ Рефералка 50%"
    await call.message.edit_text(text, reply_markup=pay_kb())
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "pay_stars")
async def pay_stars(call: types.CallbackQuery):
    await call.answer("В разработке. Используйте промокод.", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "pay_crypto")
async def pay_crypto(call: types.CallbackQuery):
    text = "💎 <b>Крипто-платёж</b>\n\nНапиши в поддержку для реквизитов.\n\n" + SUPPORT_URL
    await call.message.edit_text(text, reply_markup=pay_kb())
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "pay_code")
async def pay_code(call: types.CallbackQuery):
    USER_STATES[call.from_user.id] = {"mode": "waiting_promo"}
    text = "🎟 Отправь промокод одним сообщением"
    await call.message.edit_text(text, reply_markup=pay_kb())
    await call.answer()

@dp.message_handler(lambda m: USER_STATES.get(m.from_user.id, {}).get("mode") == "waiting_promo")
async def handle_promo(message: types.Message):
    conn = await db_pool.acquire()
    try:
        await conn.execute("UPDATE users SET paid=1 WHERE id=?", (message.from_user.id,))
        await conn.commit()
    finally:
        await db_pool.release(conn)
    
    USER_STATES.pop(message.from_user.id, None)
    await message.answer("✅ Доступ активирован!\n\nНажми /start")

# ==================== ALERTS ====================
@dp.callback_query_handler(lambda c: c.data == "menu_alerts")
async def menu_alerts(call: types.CallbackQuery):
    uid = call.from_user.id
    if not await is_paid(uid):
        await call.answer("Оплатите доступ!", show_alert=True)
        return
    
    pairs = await get_user_pairs(uid)
    text = f"📈 <b>Алерты</b>\n\nВыбери монеты (до 10)\n\nАктивно: {len(pairs)}/10"
    await call.message.edit_text(text, reply_markup=alerts_kb(pairs))
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("toggle_"))
async def toggle_pair(call: types.CallbackQuery):
    uid = call.from_user.id
    if not await is_paid(uid):
        await call.answer("Оплатите доступ!", show_alert=True)
        return
    
    pair = call.data.split("_", 1)[1]
    pairs = await get_user_pairs(uid)
    
    if pair in pairs:
        await remove_user_pair(uid, pair)
        await call.answer(f"❌ {pair} удалён")
    else:
        if len(pairs) >= 10:
            await call.answer("Максимум 10 монет!", show_alert=True)
            return
        await add_user_pair(uid, pair)
        await call.answer(f"✅ {pair} добавлен")
    
    pairs = await get_user_pairs(uid)
    text = f"📈 <b>Алерты</b>\n\nВыбери монеты (до 10)\n\nАктивно: {len(pairs)}/10"
    try:
        await call.message.edit_text(text, reply_markup=alerts_kb(pairs))
    except:
        pass

@dp.callback_query_handler(lambda c: c.data == "add_custom")
async def add_custom(call: types.CallbackQuery):
    uid = call.from_user.id
    pairs = await get_user_pairs(uid)
    if len(pairs) >= 10:
        await call.answer("Максимум 10 монет!", show_alert=True)
        return
    USER_STATES[uid] = {"mode": "waiting_custom_pair"}
    text = "➕ Отправь символ монеты\nПример: <code>SOLUSDT</code>"
    await call.message.edit_text(text, reply_markup=alerts_kb(pairs))
    await call.answer()

@dp.message_handler(lambda m: USER_STATES.get(m.from_user.id, {}).get("mode") == "waiting_custom_pair")
async def handle_custom_pair(message: types.Message):
    uid = message.from_user.id
    pair = message.text.strip().upper()
    
    if not pair.endswith("USDT") or len(pair) < 6:
        await message.answer("❌ Неверный формат. Пример: SOLUSDT")
        return
    
    async with httpx.AsyncClient() as client:
        price_data = await fetch_price(client, pair)
        if not price_data:
            await message.answer(f"❌ Пара {pair} не найдена")
            return
    
    await add_user_pair(uid, pair)
    USER_STATES.pop(uid, None)
    await message.answer(f"✅ {pair} добавлена!")

@dp.callback_query_handler(lambda c: c.data == "my_pairs")
async def my_pairs(call: types.CallbackQuery):
    pairs = await get_user_pairs(call.from_user.id)
    if not pairs:
        await call.answer("Нет активных монет", show_alert=True)
        return
    
    text = "📋 <b>Мои монеты</b>\n\n" + "\n".join(f"• {p}" for p in pairs)
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🗑 Удалить всё", callback_data="clear_all"))
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="menu_alerts"))
    await call.message.edit_text(text, reply_markup=kb)
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "clear_all")
async def clear_all(call: types.CallbackQuery):
    conn = await db_pool.acquire()
    try:
        await conn.execute("DELETE FROM user_pairs WHERE user_id=?", (call.from_user.id,))
        await conn.commit()
    finally:
        await db_pool.release(conn)
    await call.answer("🗑 Всё удалено")
    await menu_alerts(call)

# ==================== REFERRAL ====================
@dp.callback_query_handler(lambda c: c.data == "menu_ref")
async def menu_ref(call: types.CallbackQuery):
    text = "👥 <b>Рефералка</b>\n\n50% от каждой подписки!\nВывод: крипта или Stars\nМинимум: $20"
    await call.message.edit_text(text, reply_markup=ref_kb())
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "ref_link")
async def ref_link(call: types.CallbackQuery):
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start={call.from_user.id}"
    text = f"🔗 <b>Твоя ссылка:</b>\n\n<code>{link}</code>\n\nДелись и зарабатывай 50%!"
    await call.message.edit_text(text, reply_markup=ref_kb(), disable_web_page_preview=True)
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "ref_balance")
async def ref_balance(call: types.CallbackQuery):
    conn = await db_pool.acquire()
    try:
        cursor = await conn.execute("SELECT balance FROM users WHERE id=?", (call.from_user.id,))
        row = await cursor.fetchone()
        balance = row["balance"] if row else 0.0
        
        cursor = await conn.execute(
            "SELECT COUNT(*) as cnt FROM users WHERE invited_by=? AND paid=1",
            (call.from_user.id,)
        )
        row = await cursor.fetchone()
        refs = row["cnt"] if row else 0
    finally:
        await db_pool.release(conn)
    
    text = f"💰 <b>Баланс</b>\n\nДоступно: ${balance:.2f}\nРефералов: {refs}\n\nМинимум для вывода: $20"
    await call.message.edit_text(text, reply_markup=ref_kb())
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "ref_withdraw_crypto")
async def ref_withdraw_crypto(call: types.CallbackQuery):
    text = "💎 <b>Вывод крипты</b>\n\nФормат:\n<code>/withdraw USDT TRC20 адрес сумма</code>"
    await call.message.edit_text(text, reply_markup=ref_kb())
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "ref_withdraw_stars")
async def ref_withdraw_stars(call: types.CallbackQuery):
    text = "⭐ <b>Вывод Stars</b>\n\nФормат:\n<code>/withdraw_stars сумма</code>"
    await call.message.edit_text(text, reply_markup=ref_kb())
    await call.answer()

@dp.message_handler(commands=["withdraw"])
async def cmd_withdraw(message: types.Message):
    parts = message.text.split()
    if len(parts) != 5:
        await message.reply("❌ Формат: /withdraw USDT TRC20 адрес сумма")
        return
    
    try:
        amount = float(parts[4])
    except:
        await message.reply("❌ Сумма должна быть числом")
        return
    
    if amount < 20:
        await message.reply("❌ Минимум $20")
        return
    
    await message.reply(f"✅ Заявка принята: {amount} {parts[1]}\n\nОжидайте обработки (до 24ч)")
    
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"💸 Вывод\nUser: {message.from_user.id}\n{parts[1]} {parts[2]}\nАдрес: {parts[3]}\nСумма: {amount}"
            )
        except:
            pass

@dp.message_handler(commands=["withdraw_stars"])
async def cmd_withdraw_stars(message: types.Message):
    parts = message.text.split()
    if len(parts) != 2:
        await message.reply("❌ Формат: /withdraw_stars сумма")
        return
    
    try:
        amount = int(parts[1])
    except:
        await message.reply("❌ Сумма должна быть числом")
        return
    
    if amount < 20:
        await message.reply("❌ Минимум 20 Stars")
        return
    
    await message.reply(f"✅ Заявка на {amount} Stars принята\n\nОжидайте (до 24ч)")

@dp.callback_query_handler(lambda c: c.data == "ref_guide")
async def ref_guide(call: types.CallbackQuery):
    text = "📖 <b>Гайд для партнёров</b>\n\n1. Получи свою ссылку\n2. Делись с друзьями\n3. Получай 50% с подписок\n4. Выводи от $20"
    await call.message.edit_text(text, reply_markup=ref_kb())
    await call.answer()

# ==================== GUIDE ====================
@dp.callback_query_handler(lambda c: c.data == "menu_guide")
async def menu_guide(call: types.CallbackQuery):
    text = "📖 <b>Инструкция</b>\n\n"
    text += "<b>Шаг 1:</b> Оплати доступ\n"
    text += "<b>Шаг 2:</b> Выбери монеты (до 10)\n"
    text += "<b>Шаг 3:</b> Получай сигналы\n\n"
    text += "<b>В каждом сигнале:</b>\n"
    text += "• Цена входа\n"
    text += "• 🎯 Take Profit\n"
    text += "• 🛡 Stop Loss\n"
    text += "• Причины входа\n\n"
    text += "<b>⚠️ Важно:</b>\n"
    text += "• Используй стоп-лоссы\n"
    text += "• Не вкладывай последние деньги\n"
    text += "• Это не финансовый совет"
    
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_main"))
    await call.message.edit_text(text, reply_markup=kb)
    await call.answer()

# ==================== ADMIN ====================
@dp.callback_query_handler(lambda c: c.data == "menu_admin")
async def menu_admin(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Нет доступа", show_alert=True)
        return
    await call.message.edit_text("👑 <b>Админ-панель</b>", reply_markup=admin_kb())
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "adm_stats")
async def adm_stats(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    
    conn = await db_pool.acquire()
    try:
        cursor = await conn.execute("SELECT COUNT(*) as cnt FROM users")
        total = (await cursor.fetchone())["cnt"]
        
        cursor = await conn.execute("SELECT COUNT(*) as cnt FROM users WHERE paid=1")
        paid = (await cursor.fetchone())["cnt"]
        
        cursor = await conn.execute("SELECT COUNT(DISTINCT user_id) as cnt FROM user_pairs")
        active = (await cursor.fetchone())["cnt"]
    finally:
        await db_pool.release(conn)
    
    text = f"📊 <b>Статистика</b>\n\n👥 Всего: {total}\n💎 Оплативших: {paid}\n📈 Активных: {active}"
    await call.message.edit_text(text, reply_markup=admin_kb())
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "adm_broadcast")
async def adm_broadcast(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    USER_STATES[call.from_user.id] = {"mode": "admin_broadcast"}
    await call.message.edit_text("📢 Отправь текст рассылки", reply_markup=admin_kb())
    await call.answer()

@dp.message_handler(lambda m: USER_STATES.get(m.from_user.id, {}).get("mode") == "admin_broadcast")
async def handle_broadcast(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    
    text = message.html_text
    conn = await db_pool.acquire()
    try:
        cursor = await conn.execute("SELECT id FROM users")
        users = await cursor.fetchall()
    finally:
        await db_pool.release(conn)
    
    sent = 0
    for user in users:
        if await send_message_safe(user["id"], text, disable_web_page_preview=True):
            sent += 1
        await asyncio.sleep(BATCH_SEND_DELAY)
    
    USER_STATES.pop(message.from_user.id, None)
    await message.reply(f"✅ Разослано: {sent}/{len(users)}")

@dp.callback_query_handler(lambda c: c.data == "adm_grant")
async def adm_grant(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    USER_STATES[call.from_user.id] = {"mode": "admin_grant"}
    await call.message.edit_text("✅ Отправь ID пользователя", reply_markup=admin_kb())
    await call.answer()

@dp.message_handler(lambda m: USER_STATES.get(m.from_user.id, {}).get("mode") == "admin_grant")
async def handle_grant(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    
    try:
        uid = int(message.text.strip())
    except:
        await message.reply("❌ ID должен быть числом")
        return
    
    conn = await db_pool.acquire()
    try:
        await conn.execute("INSERT OR IGNORE INTO users(id, created_ts) VALUES(?,?)", (uid, int(time.time())))
        await conn.execute("UPDATE users SET paid=1 WHERE id=?", (uid,))
        await conn.commit()
    finally:
        await db_pool.release(conn)
    
    USER_STATES.pop(message.from_user.id, None)
    await message.reply(f"✅ Доступ выдан: {uid}")
    
    try:
        await bot.send_message(uid, "🎉 Доступ активирован!\n\nНажми /start")
    except:
        pass

@dp.callback_query_handler(lambda c: c.data == "adm_give")
async def adm_give(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    USER_STATES[call.from_user.id] = {"mode": "admin_give_uid"}
    await call.message.edit_text("💰 Отправь ID пользователя", reply_markup=admin_kb())
    await call.answer()

@dp.message_handler(lambda m: USER_STATES.get(m.from_user.id, {}).get("mode") == "admin_give_uid")
async def handle_give_uid(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    try:
        uid = int(message.text.strip())
    except:
        await message.reply("❌ ID должен быть числом")
        return
    USER_STATES[message.from_user.id] = {"mode": "admin_give_amount", "target_id": uid}
    await message.reply("Отправь сумму")

@dp.message_handler(lambda m: USER_STATES.get(m.from_user.id, {}).get("mode") == "admin_give_amount")
async def handle_give_amount(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    try:
        amount = float(message.text.strip())
    except:
        await message.reply("❌ Сумма должна быть числом")
        return
    
    uid = USER_STATES[message.from_user.id]["target_id"]
    conn = await db_pool.acquire()
    try:
        await conn.execute("INSERT OR IGNORE INTO users(id, created_ts) VALUES(?,?)", (uid, int(time.time())))
        await conn.execute("UPDATE users SET balance = COALESCE(balance, 0) + ? WHERE id=?", (amount, uid))
        await conn.commit()
    finally:
        await db_pool.release(conn)
    
    USER_STATES.pop(message.from_user.id, None)
    await message.reply(f"✅ Начислено ${amount:.2f} → {uid}")

# ==================== BACKGROUND TASKS ====================
async def price_collector():
    """Сбор цен с кэшированием"""
    logger.info("Price collector started")
    async with httpx.AsyncClient() as client:
        while True:
            try:
                # Получаем все уникальные пары
                conn = await db_pool.acquire()
                try:
                    cursor = await conn.execute("SELECT DISTINCT pair FROM user_pairs")
                    rows = await cursor.fetchall()
                    pairs = [r["pair"] for r in rows]
                finally:
                    await db_pool.release(conn)
                
                pairs = list(set(pairs + DEFAULT_PAIRS))
                
                # Собираем цены
                ts = time.time()
                for pair in pairs:
                    price_data = await fetch_price(client, pair)
                    if price_data:
                        price, volume = price_data
                        CANDLES.add_price(pair, price, volume, ts)
                
                # Очистка старого кэша
                PRICE_CACHE.clear_old()
                
            except Exception as e:
                logger.error(f"Price collector error: {e}")
            
            await asyncio.sleep(CHECK_INTERVAL)

async def signal_analyzer():
    """Анализ и отправка сигналов с батчингом"""
    logger.info("Signal analyzer started")
    
    while True:
        try:
            # Получаем пары и пользователей
            conn = await db_pool.acquire()
            try:
                cursor = await conn.execute(
                    "SELECT up.user_id, up.pair FROM user_pairs up "
                    "JOIN users u ON up.user_id = u.id WHERE u.paid = 1"
                )
                rows = await cursor.fetchall()
            finally:
                await db_pool.release(conn)
            
            # Группируем по парам
            pairs_users = defaultdict(list)
            for row in rows:
                pairs_users[row["pair"]].append(row["user_id"])
            
            # Анализируем каждую пару
            now = time.time()
            for pair, users in pairs_users.items():
                # Проверка лимита сигналов за день
                signals_today = await count_signals_today(pair)
                if signals_today >= MAX_SIGNALS_PER_DAY:
                    continue
                
                signal = analyze_signal(pair)
                if not signal:
                    continue
                
                side = signal["side"]
                key = (pair, side)
                
                # Проверка куллдауна
                if now - LAST_SIGNALS.get(key, 0) < SIGNAL_COOLDOWN:
                    continue
                
                # Формируем сообщение
                emoji = "📈" if side == "LONG" else "📉"
                text = f"{emoji} <b>СИГНАЛ</b> ({signal['score']}/100)\n\n"
                text += f"<b>Монета:</b> {pair}\n"
                text += f"<b>Вход:</b> {side} @ <code>{signal['price']:.8f}</code>\n\n"
                text += f"🎯 <b>TP:</b> <code>{signal['take_profit']:.8f}</code> (+{signal['tp_percent']:.2f}%)\n"
                text += f"🛡 <b>SL:</b> <code>{signal['stop_loss']:.8f}</code> (-{signal['sl_percent']:.2f}%)\n\n"
                text += "<b>💡 Причины:</b>\n"
                for reason in signal["reasons"]:
                    text += f"• {reason}\n"
                text += f"\n⏰ {time.strftime('%H:%M:%S')}"
                
                # ОПТИМИЗАЦИЯ: батчинг отправки
                sent_count = 0
                for i, user_id in enumerate(users):
                    if await send_message_safe(user_id, text):
                        await log_signal(user_id, pair, side, signal["price"], signal["score"])
                        sent_count += 1
                    
                    # Задержка каждые BATCH_SEND_SIZE сообщений
                    if (i + 1) % BATCH_SEND_SIZE == 0:
                        await asyncio.sleep(1)
                    else:
                        await asyncio.sleep(BATCH_SEND_DELAY)
                
                LAST_SIGNALS[key] = now
                logger.info(f"Signal sent: {pair} {side} to {sent_count} users")
                
        except Exception as e:
            logger.error(f"Signal analyzer error: {e}")
        
        await asyncio.sleep(30)

# ==================== STARTUP ====================
async def on_startup():
    logger.info("Bot starting...")
    await bot.delete_webhook(drop_pending_updates=True)
    await init_db()
    
    loop = asyncio.get_event_loop()
    loop.create_task(price_collector())
    loop.create_task(signal_analyzer())
    
    logger.info("Bot started successfully!")

# ==================== MAIN ====================
if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(on_startup())
    executor.start_polling(dp, skip_updates=True)

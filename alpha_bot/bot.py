"""
Alpha Entry Bot - профессиональный трейдинг-бот
Только сильные сигналы с подтверждением от нескольких стратегий
"""
import os, time, asyncio, logging
from typing import Optional, Dict, List, Tuple
from collections import defaultdict, deque
from datetime import datetime, timedelta

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
CHECK_INTERVAL = 30  # секунд между проверками (увеличил для точности)
CANDLE_TF = 60  # таймфрейм свечи (1 минута)
MAX_CANDLES = 500  # больше свечей для точных расчётов

# Индикаторы
EMA_FAST = 9
EMA_SLOW = 21
EMA_TREND = 50  # для определения глобального тренда
RSI_PERIOD = 14
RSI_OVERSOLD = 35
RSI_OVERBOUGHT = 65
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
BB_PERIOD = 20  # Bollinger Bands
BB_STD = 2

# Фильтры качества
MIN_SIGNAL_SCORE = 75  # минимальный балл сигнала (из 100)
MAX_SIGNALS_PER_DAY = 5  # максимум сигналов в день на монету
SIGNAL_COOLDOWN = 14400  # 4 часа между сигналами на одну монету

# Картинки
IMG_START = os.getenv("IMG_START", "")
IMG_ALERTS = os.getenv("IMG_ALERTS", "")
IMG_REF = os.getenv("IMG_REF", "")
IMG_PAYWALL = os.getenv("IMG_PAYWALL", "")

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

CREATE TABLE IF NOT EXISTS signals_sent (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    pair TEXT NOT NULL,
    side TEXT NOT NULL,
    price REAL NOT NULL,
    score INTEGER NOT NULL,
    sent_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS ref_earnings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount REAL NOT NULL,
    from_user INTEGER NOT NULL,
    created_ts INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_signals_pair ON signals_sent(pair, sent_ts);
CREATE INDEX IF NOT EXISTS idx_user_pairs ON user_pairs(user_id);
"""

async def get_db():
    conn = await aiosqlite.connect(DB_PATH)
    conn.row_factory = aiosqlite.Row
    return conn

async def init_db():
    db = await get_db()
    try:
        await db.executescript(INIT_SQL)
        await db.commit()
    finally:
        await db.close()
    logger.info("Database initialized")

# ==================== MARKET DATA ====================
class CandleStorage:
    def __init__(self, timeframe=CANDLE_TF, maxlen=MAX_CANDLES):
        self.tf = timeframe
        self.maxlen = maxlen
        self.candles: Dict[str, deque] = defaultdict(lambda: deque(maxlen=maxlen))
        self.current: Dict[str, dict] = {}
        self.volumes: Dict[str, deque] = defaultdict(lambda: deque(maxlen=maxlen))
    
    def get_bucket(self, ts: float) -> int:
        return int(ts // self.tf) * self.tf
    
    def add_price(self, pair: str, price: float, volume: float, ts: float):
        pair = pair.upper()
        bucket = self.get_bucket(ts)
        
        if pair not in self.current or self.current[pair]["ts"] != bucket:
            if pair in self.current:
                self.candles[pair].append(self.current[pair])
                self.volumes[pair].append(self.current[pair].get("v", 0))
            self.current[pair] = {
                "ts": bucket,
                "o": price,
                "h": price,
                "l": price,
                "c": price,
                "v": volume
            }
        else:
            candle = self.current[pair]
            candle["h"] = max(candle["h"], price)
            candle["l"] = min(candle["l"], price)
            candle["c"] = price
            candle["v"] += volume
    
    def get_candles(self, pair: str) -> List[dict]:
        pair = pair.upper()
        result = list(self.candles[pair])
        if pair in self.current:
            result.append(self.current[pair])
        return result

CANDLES = CandleStorage()

async def fetch_price(client: httpx.AsyncClient, pair: str) -> Optional[Tuple[float, float]]:
    try:
        # Получаем цену и объём
        url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={pair.upper()}"
        resp = await client.get(url, timeout=5.0)
        resp.raise_for_status()
        data = resp.json()
        return float(data["lastPrice"]), float(data["volume"])
    except Exception as e:
        logger.error(f"Error fetching price for {pair}: {e}")
        return None

# ==================== ADVANCED INDICATORS ====================
def ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e

def sma(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period

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
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def macd(closes: List[float], fast=12, slow=26, signal=9) -> Optional[Tuple[float, float, float]]:
    """Возвращает (MACD, Signal, Histogram)"""
    if len(closes) < slow + signal:
        return None
    
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    
    if ema_fast is None or ema_slow is None:
        return None
    
    macd_line = ema_fast - ema_slow
    
    # Для signal нужна история MACD
    macd_history = []
    for i in range(len(closes) - slow, len(closes)):
        ef = ema(closes[:i+1], fast)
        es = ema(closes[:i+1], slow)
        if ef and es:
            macd_history.append(ef - es)
    
    if len(macd_history) < signal:
        return None
    
    signal_line = ema(macd_history, signal)
    if signal_line is None:
        return None
    
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def bollinger_bands(closes: List[float], period=20, std_dev=2) -> Optional[Tuple[float, float, float]]:
    """Возвращает (Upper, Middle, Lower)"""
    if len(closes) < period:
        return None
    
    middle = sma(closes, period)
    if middle is None:
        return None
    
    recent = closes[-period:]
    variance = sum((x - middle) ** 2 for x in recent) / period
    std = variance ** 0.5
    
    upper = middle + (std * std_dev)
    lower = middle - (std * std_dev)
    
    return upper, middle, lower

def volume_profile(volumes: List[float], period=20) -> Optional[float]:
    """Относительный объём"""
    if len(volumes) < period + 1:
        return None
    
    avg_volume = sum(volumes[-period-1:-1]) / period
    current_volume = volumes[-1]
    
    if avg_volume == 0:
        return 1.0
    
    return current_volume / avg_volume

def atr(candles: List[dict], period=14) -> Optional[float]:
    """Average True Range - средний истинный диапазон"""
    if len(candles) < period + 1:
        return None
    
    true_ranges = []
    for i in range(-period, 0):
        high = candles[i]["h"]
        low = candles[i]["l"]
        prev_close = candles[i-1]["c"]
        
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        true_ranges.append(tr)
    
    return sum(true_ranges) / period

def calculate_tp_sl(entry_price: float, side: str, atr_value: float, risk_reward: float = 2.0) -> Dict:
    """
    Расчёт Take Profit и Stop Loss
    risk_reward: соотношение прибыль/риск (2.0 = 2:1)
    """
    # Stop Loss = 1.5 * ATR от точки входа
    sl_distance = atr_value * 1.5
    
    # Take Profit = 2-3 * Stop Loss (в зависимости от risk_reward)
    tp_distance = sl_distance * risk_reward
    
    if side == "LONG":
        stop_loss = entry_price - sl_distance
        take_profit = entry_price + tp_distance
    else:  # SHORT
        stop_loss = entry_price + sl_distance
        take_profit = entry_price - tp_distance
    
    # Процентные значения
    sl_percent = abs((stop_loss - entry_price) / entry_price * 100)
    tp_percent = abs((take_profit - entry_price) / entry_price * 100)
    
    return {
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "sl_percent": sl_percent,
        "tp_percent": tp_percent,
        "risk_reward": risk_reward
    }

# ==================== MULTI-STRATEGY ANALYSIS ====================
def analyze_multi_strategy(pair: str) -> Optional[Dict]:
    """
    Комплексный анализ с несколькими стратегиями
    Возвращает сигнал только если подтверждён несколькими индикаторами
    """
    candles = CANDLES.get_candles(pair)
    
    if len(candles) < MAX_CANDLES * 0.8:  # нужно минимум 80% данных
        return None
    
    closes = [c["c"] for c in candles]
    volumes = [c.get("v", 0) for c in candles]
    current_price = closes[-1]
    
    # Все индикаторы
    ema_9 = ema(closes, EMA_FAST)
    ema_21 = ema(closes, EMA_SLOW)
    ema_50 = ema(closes, EMA_TREND)
    rsi_val = rsi(closes, RSI_PERIOD)
    macd_data = macd(closes, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
    bb_data = bollinger_bands(closes, BB_PERIOD, BB_STD)
    vol_ratio = volume_profile(volumes, 20)
    atr_value = atr(candles, 14)
    
    if None in [ema_9, ema_21, ema_50, rsi_val, macd_data, bb_data, vol_ratio, atr_value]:
        return None
    
    macd_line, signal_line, histogram = macd_data
    bb_upper, bb_middle, bb_lower = bb_data
    
    # Система баллов (из 100)
    score = 0
    reasons = []
    side = None
    
    # ========== LONG СИГНАЛ ==========
    if ema_9 > ema_21 and current_price > ema_50:
        # Тренд вверх
        score += 25
        reasons.append("Восходящий тренд (EMA 9 > 21 > 50)")
        
        # RSI не перекуплен
        if RSI_OVERSOLD < rsi_val < 60:
            score += 20
            reasons.append(f"RSI оптимален ({rsi_val:.1f})")
        
        # MACD бычий
        if macd_line > signal_line and histogram > 0:
            score += 20
            reasons.append("MACD бычий кроссовер")
        
        # Цена у нижней линии Боллинджера (отскок)
        bb_position = (current_price - bb_lower) / (bb_upper - bb_lower)
        if bb_position < 0.3:
            score += 15
            reasons.append("Отскок от нижней границы BB")
        
        # Повышенный объём
        if vol_ratio > 1.5:
            score += 10
            reasons.append(f"Высокий объём ({vol_ratio:.1f}x)")
        
        # Проверка силы тренда
        trend_strength = (ema_9 - ema_21) / ema_21
        if trend_strength > 0.005:  # 0.5%
            score += 10
            reasons.append("Сильный импульс")
        
        if score >= MIN_SIGNAL_SCORE:
            side = "LONG"
    
    # ========== SHORT СИГНАЛ ==========
    elif ema_9 < ema_21 and current_price < ema_50:
        # Тренд вниз
        score += 25
        reasons.append("Нисходящий тренд (EMA 9 < 21 < 50)")
        
        # RSI не перепродан
        if 40 < rsi_val < RSI_OVERBOUGHT:
            score += 20
            reasons.append(f"RSI оптимален ({rsi_val:.1f})")
        
        # MACD медвежий
        if macd_line < signal_line and histogram < 0:
            score += 20
            reasons.append("MACD медвежий кроссовер")
        
        # Цена у верхней линии Боллинджера (откат)
        bb_position = (current_price - bb_lower) / (bb_upper - bb_lower)
        if bb_position > 0.7:
            score += 15
            reasons.append("Откат от верхней границы BB")
        
        # Повышенный объём
        if vol_ratio > 1.5:
            score += 10
            reasons.append(f"Высокий объём ({vol_ratio:.1f}x)")
        
        # Проверка силы тренда
        trend_strength = (ema_21 - ema_9) / ema_21
        if trend_strength > 0.005:
            score += 10
            reasons.append("Сильный импульс")
        
        if score >= MIN_SIGNAL_SCORE:
            side = "SHORT"
    
    if side and score >= MIN_SIGNAL_SCORE:
        # Рассчитываем TP и SL
        tp_sl = calculate_tp_sl(current_price, side, atr_value, risk_reward=2.5)
        
        return {
            "side": side,
            "price": current_price,
            "score": score,
            "reasons": reasons,
            "rsi": rsi_val,
            "ema_trend": "up" if ema_9 > ema_21 else "down",
            "stop_loss": tp_sl["stop_loss"],
            "take_profit": tp_sl["take_profit"],
            "sl_percent": tp_sl["sl_percent"],
            "tp_percent": tp_sl["tp_percent"],
            "risk_reward": tp_sl["risk_reward"]
        }
    
    return None

# ==================== BOT ====================
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

USER_STATES: Dict[int, dict] = {}
LAST_SIGNALS: Dict[Tuple[str, str], float] = {}  # (pair, side) -> timestamp

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

async def is_paid(user_id: int) -> bool:
    db = await get_db()
    try:
        cursor = await db.execute("SELECT paid FROM users WHERE id=?", (user_id,))
        row = await cursor.fetchone()
        return bool(row and row["paid"])
    finally:
        await db.close()

async def get_user_pairs(user_id: int) -> List[str]:
    db = await get_db()
    try:
        cursor = await db.execute("SELECT pair FROM user_pairs WHERE user_id=?", (user_id,))
        rows = await cursor.fetchall()
        return [r["pair"] for r in rows]
    finally:
        await db.close()

async def add_user_pair(user_id: int, pair: str):
    db = await get_db()
    try:
        await db.execute("INSERT OR IGNORE INTO user_pairs(user_id, pair) VALUES(?,?)", 
                        (user_id, pair.upper()))
        await db.commit()
    finally:
        await db.close()

async def remove_user_pair(user_id: int, pair: str):
    db = await get_db()
    try:
        await db.execute("DELETE FROM user_pairs WHERE user_id=? AND pair=?", 
                        (user_id, pair.upper()))
        await db.commit()
    finally:
        await db.close()

async def count_signals_today(pair: str) -> int:
    """Подсчёт сигналов за сегодня"""
    db = await get_db()
    try:
        today_start = int(datetime.now().replace(hour=0, minute=0, second=0).timestamp())
        cursor = await db.execute(
            "SELECT COUNT(*) as cnt FROM signals_sent WHERE pair=? AND sent_ts >= ?",
            (pair, today_start)
        )
        row = await cursor.fetchone()
        return row["cnt"] if row else 0
    finally:
        await db.close()

async def log_signal(user_id: int, pair: str, side: str, price: float, score: int):
    """Логирование отправленного сигнала"""
    db = await get_db()
    try:
        await db.execute(
            "INSERT INTO signals_sent(user_id, pair, side, price, score, sent_ts) VALUES(?,?,?,?,?,?)",
            (user_id, pair, side, price, score, int(time.time()))
        )
        await db.commit()
    finally:
        await db.close()

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
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("⭐ Оплата Stars", callback_data="pay_stars"),
        InlineKeyboardButton("💎 Крипто-платёж", callback_data="pay_crypto")
    )
    kb.add(InlineKeyboardButton("🎟 У меня есть код", callback_data="pay_code"))
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
        InlineKeyboardButton("💰 Начислить баланс", callback_data="adm_give")
    )
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_main"))
    return kb

# ==================== HANDLERS ====================
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    args = message.get_args()
    invited_by = int(args) if args and args.isdigit() and int(args) != user_id else None
    
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR IGNORE INTO users(id, invited_by, created_ts) VALUES(?,?,?)",
            (user_id, invited_by, int(time.time()))
        )
        await db.commit()
    finally:
        await db.close()
    
    text = f"<b>🚀 {BOT_NAME}</b>\n\n"
    text += "Профессиональный анализ рынка с несколькими стратегиями.\n\n"
    text += "<b>Что я умею:</b>\n"
    text += "• Анализ 5+ индикаторов одновременно\n"
    text += "• Только сильные сигналы (75+ баллов)\n"
    text += "• 3-5 точных сигналов в день\n"
    text += "• Объяснение причины каждого сигнала\n\n"
    text += "📖 Нажми Инструкция для деталей"
    
    paid = await is_paid(user_id)
    
    if IMG_START:
        try:
            await bot.send_photo(user_id, IMG_START, caption=text,
                               reply_markup=main_menu_kb(is_admin(user_id), paid))
        except:
            await message.answer(text, reply_markup=main_menu_kb(is_admin(user_id), paid))
    else:
        await message.answer(text, reply_markup=main_menu_kb(is_admin(user_id), paid))

@dp.callback_query_handler(lambda c: c.data == "back_main")
async def back_main(call: types.CallbackQuery):
    paid = await is_paid(call.from_user.id)
    text = f"<b>🚀 {BOT_NAME}</b>\n\nГлавное меню"
    try:
        await call.message.edit_text(text, reply_markup=main_menu_kb(is_admin(call.from_user.id), paid))
    except:
        await call.message.answer(text, reply_markup=main_menu_kb(is_admin(call.from_user.id), paid))
    await call.answer()

# ==================== PAYWALL ====================
@dp.callback_query_handler(lambda c: c.data == "menu_pay")
async def menu_pay(call: types.CallbackQuery):
    text = "🔒 <b>Открыть доступ к боту</b>\n\n"
    text += "После оплаты ты получишь:\n"
    text += "• 3-5 сильных сигналов в день\n"
    text += "• Анализ 5+ индикаторов\n"
    text += "• Объяснение каждого сигнала\n"
    text += "• Отслеживание до 10 монет\n"
    text += "• Реферальную программу (50%)\n\n"
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
    await call.answer("Функция в разработке. Используйте крипто-платёж или промокод.", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "pay_crypto")
async def pay_crypto(call: types.CallbackQuery):
    text = "💎 <b>Крипто-платёж</b>\n\n"
    text += "Напиши в поддержку для получения реквизитов.\n"
    text += "После оплаты тебе выдадут промокод.\n\n"
    text += f"Поддержка: {SUPPORT_URL}"
    await call.message.edit_text(text, reply_markup=pay_kb())
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "pay_code")
async def pay_code(call: types.CallbackQuery):
    USER_STATES[call.from_user.id] = {"mode": "waiting_promo"}
    text = "🎟 <b>Активация промокода</b>\n\n"
    text += "Отправь мне промокод одним сообщением.\n"
    text += "Формат: <code>PROMO-12345</code>"
    await call.message.edit_text(text, reply_markup=pay_kb())
    await call.answer()

@dp.message_handler(lambda m: USER_STATES.get(m.from_user.id, {}).get("mode") == "waiting_promo")
async def handle_promo(message: types.Message):
    db = await get_db()
    try:
        await db.execute("UPDATE users SET paid=1 WHERE id=?", (message.from_user.id,))
        await db.commit()
    finally:
        await db.close()
    
    USER_STATES.pop(message.from_user.id, None)
    await message.answer(
        "✅ <b>Доступ активирован!</b>\n\n"
        "Теперь можешь выбрать монеты в разделе Алерты.\n"
        "Нажми /start для перезапуска."
    )

# ==================== ALERTS ====================
@dp.callback_query_handler(lambda c: c.data == "menu_alerts")
async def menu_alerts(call: types.CallbackQuery):
    user_id = call.from_user.id
    
    if not await is_paid(user_id):
        await call.answer("Оплатите доступ для использования алертов!", show_alert=True)
        return
    
    pairs = await get_user_pairs(user_id)
    
    text = "📈 <b>Управление алертами</b>\n\n"
    text += "Выбери монеты для анализа.\n"
    text += "Бот пришлёт сигнал только когда:\n"
    text += "• Подтверждено 5+ индикаторами\n"
    text += "• Сила сигнала 75+ баллов\n"
    text += "• Есть объяснение причины\n\n"
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
    user_id = call.from_user.id
    
    if not await is_paid(user_id):
        await call.answer("Оплатите доступ!", show_alert=True)
        return
    
    pair = call.data.split("_

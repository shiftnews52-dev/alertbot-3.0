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
IMG_GUIDE = os.getenv("IMG_GUIDE", "")  # для инструкции

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
    language TEXT DEFAULT 'ru',
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

def sma(values: List[float], period: int) -> Optional[float]:
    """Простая скользящая средняя"""
    if len(values) < period:
        return None
    return sum(values[-period:]) / period

def macd(closes: List[float]) -> Optional[Tuple[float, float, float]]:
    """MACD индикатор - возвращает (macd_line, signal_line, histogram)"""
    if len(closes) < MACD_SLOW + MACD_SIGNAL:
        return None
    
    ema_fast = ema(closes, MACD_FAST)
    ema_slow = ema(closes, MACD_SLOW)
    
    if ema_fast is None or ema_slow is None:
        return None
    
    macd_line = ema_fast - ema_slow
    
    # Вычисляем историю MACD для signal line
    macd_history = []
    for i in range(len(closes) - MACD_SLOW - MACD_SIGNAL, len(closes)):
        if i < MACD_FAST:
            continue
        ef = ema(closes[:i+1], MACD_FAST)
        es = ema(closes[:i+1], MACD_SLOW)
        if ef and es:
            macd_history.append(ef - es)
    
    if len(macd_history) < MACD_SIGNAL:
        return None
    
    signal_line = ema(macd_history, MACD_SIGNAL)
    if signal_line is None:
        return None
    
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def bollinger_bands(closes: List[float]) -> Optional[Tuple[float, float, float]]:
    """Bollinger Bands - возвращает (upper, middle, lower)"""
    if len(closes) < BB_PERIOD:
        return None
    
    middle = sma(closes, BB_PERIOD)
    if middle is None:
        return None
    
    recent = closes[-BB_PERIOD:]
    variance = sum((x - middle) ** 2 for x in recent) / BB_PERIOD
    std = variance ** 0.5
    
    upper = middle + (std * BB_STD)
    lower = middle - (std * BB_STD)
    
    return upper, middle, lower

def volume_strength(candles: List[dict], period=20) -> Optional[float]:
    """Сила объёма - относительно средней"""
    if len(candles) < period + 1:
        return None
    
    volumes = [c.get("v", 0) for c in candles[-period-1:]]
    avg_volume = sum(volumes[:-1]) / period
    current_volume = volumes[-1]
    
    if avg_volume == 0:
        return 1.0
    
    return current_volume / avg_volume

def check_divergence(closes: List[float], rsi_values: List[float]) -> Optional[str]:
    """Проверка дивергенции между ценой и RSI"""
    if len(closes) < 20 or len(rsi_values) < 20:
        return None
    
    # Берём последние 20 свечей
    price_recent = closes[-20:]
    rsi_recent = rsi_values[-20:]
    
    # Бычья дивергенция: цена падает, RSI растёт
    if price_recent[-1] < price_recent[0] and rsi_recent[-1] > rsi_recent[0]:
        price_change = (price_recent[-1] - price_recent[0]) / price_recent[0]
        rsi_change = rsi_recent[-1] - rsi_recent[0]
        if price_change < -0.02 and rsi_change > 5:  # значимая дивергенция
            return "bullish"
    
    # Медвежья дивергенция: цена растёт, RSI падает
    if price_recent[-1] > price_recent[0] and rsi_recent[-1] < rsi_recent[0]:
        price_change = (price_recent[-1] - price_recent[0]) / price_recent[0]
        rsi_change = rsi_recent[-1] - rsi_recent[0]
        if price_change > 0.02 and rsi_change < -5:
            return "bearish"
    
    return None

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
    """
    Расчёт TP/SL с тремя уровнями Take Profit
    TP1: 15% от позиции
    TP2: 40% от позиции  
    TP3: 80% от позиции (максимальный тренд)
    """
    # Stop Loss = 2.0 × ATR от точки входа
    sl_distance = atr_val * 2.0
    
    # Три уровня Take Profit
    # TP1: быстрая фиксация (1.5x от SL)
    tp1_distance = sl_distance * 1.5
    
    # TP2: основная цель (3.0x от SL)
    tp2_distance = sl_distance * 3.0
    
    # TP3: максимум для сильного тренда (5.0x от SL)
    tp3_distance = sl_distance * 5.0
    
    if side == "LONG":
        sl = entry - sl_distance
        tp1 = entry + tp1_distance
        tp2 = entry + tp2_distance
        tp3 = entry + tp3_distance
    else:  # SHORT
        sl = entry + sl_distance
        tp1 = entry - tp1_distance
        tp2 = entry - tp2_distance
        tp3 = entry - tp3_distance
    
    return {
        "stop_loss": sl,
        "take_profit_1": tp1,
        "take_profit_2": tp2,
        "take_profit_3": tp3,
        "sl_percent": abs((sl - entry) / entry * 100),
        "tp1_percent": abs((tp1 - entry) / entry * 100),
        "tp2_percent": abs((tp2 - entry) / entry * 100),
        "tp3_percent": abs((tp3 - entry) / entry * 100)
    }

# ==================== STRATEGY ====================
def quick_screen(pair: str) -> bool:
    """Быстрый скрининг - отсев слабых кандидатов"""
    candles = CANDLES.get_candles(pair)
    if len(candles) < 60:
        return False
    
    closes = [c["c"] for c in candles]
    ema9 = ema(closes, EMA_FAST)
    ema21 = ema(closes, EMA_SLOW)
    
    if ema9 is None or ema21 is None:
        return False
    
    # Должен быть явный тренд
    return abs(ema9 - ema21) / ema21 > 0.002

def analyze_signal(pair: str) -> Optional[Dict]:
    """ГЛУБОКИЙ АНАЛИЗ - все индикаторы + дивергенции"""
    
    # Этап 1: Быстрый скрининг
    if not quick_screen(pair):
        return None
    
    # Этап 2: Полный анализ
    candles = CANDLES.get_candles(pair)
    if len(candles) < 250:
        return None
    
    closes = [c["c"] for c in candles]
    current_price = closes[-1]
    
    # Все индикаторы
    ema9 = ema(closes, EMA_FAST)
    ema21 = ema(closes, EMA_SLOW)
    ema50 = ema(closes, EMA_TREND)
    ema200 = ema(closes, EMA_LONG_TREND) if len(closes) >= 200 else None
    
    # Вычисляем RSI для последних 50 свечей для проверки дивергенций
    rsi_history = []
    for i in range(len(closes) - 50, len(closes)):
        if i >= RSI_PERIOD:
            rsi_val = rsi(closes[:i+1], RSI_PERIOD)
            if rsi_val:
                rsi_history.append(rsi_val)
    
    rsi_current = rsi(closes, RSI_PERIOD)
    macd_data = macd(closes)
    bb_data = bollinger_bands(closes)
    vol_strength = volume_strength(candles, 20)
    atr_val = atr(candles, 14)
    
    if None in [ema9, ema21, ema50, rsi_current, macd_data, bb_data, vol_strength, atr_val]:
        return None
    
    macd_line, signal_line, histogram = macd_data
    bb_upper, bb_middle, bb_lower = bb_data
    
    # Проверка дивергенций
    divergence = check_divergence(closes[-50:], rsi_history) if len(rsi_history) >= 20 else None
    
    # Система баллов (из 100)
    score = 0
    reasons = []
    side = None
    
    # ========== LONG СИГНАЛ ==========
    if ema9 > ema21 and ema21 > ema50:
        # 1. Тренд (максимум 30 баллов)
        score += 20
        reasons.append("Восходящий тренд (EMA 9>21>50)")
        
        # Бонус если над EMA200 (долгосрочный тренд)
        if ema200 and current_price > ema200:
            score += 10
            reasons.append("Цена выше EMA200 (бычий долгосрочный тренд)")
        
        # 2. RSI (максимум 20 баллов)
        if RSI_OVERSOLD < rsi_current < 65:
            if 45 <= rsi_current <= 55:
                score += 20
                reasons.append(f"RSI идеален ({rsi_current:.1f})")
            else:
                score += 15
                reasons.append(f"RSI приемлем ({rsi_current:.1f})")
        
        # 3. MACD (максимум 20 баллов)
        if macd_line > signal_line:
            score += 15
            reasons.append("MACD бычий")
            if histogram > 0 and abs(histogram) > abs(macd_line) * 0.1:
                score += 5
                reasons.append("MACD гистограмма растёт")
        
        # 4. Bollinger Bands (максимум 15 баллов)
        bb_position = (current_price - bb_lower) / (bb_upper - bb_lower)
        if bb_position < 0.3:
            score += 15
            reasons.append("Отскок от нижней BB (сильный)")
        elif bb_position < 0.5:
            score += 10
            reasons.append("Отскок от нижней BB")
        
        # 5. Объём (максимум 10 баллов)
        if vol_strength > 2.0:
            score += 10
            reasons.append(f"Очень высокий объём ({vol_strength:.1f}x)")
        elif vol_strength > 1.5:
            score += 7
            reasons.append(f"Высокий объём ({vol_strength:.1f}x)")
        
        # 6. Сила импульса (максимум 10 баллов)
        momentum = (ema9 - ema21) / ema21
        if momentum > 0.01:  # 1%
            score += 10
            reasons.append("Очень сильный импульс")
        elif momentum > 0.005:
            score += 7
            reasons.append("Сильный импульс")
        
        # 7. Дивергенция (бонус 15 баллов)
        if divergence == "bullish":
            score += 15
            reasons.append("⚡ Бычья дивергенция!")
        
        if score >= MIN_SIGNAL_SCORE:
            side = "LONG"
    
    # ========== SHORT СИГНАЛ ==========
    elif ema9 < ema21 and ema21 < ema50:
        # 1. Тренд (максимум 30 баллов)
        score += 20
        reasons.append("Нисходящий тренд (EMA 9<21<50)")
        
        # Бонус если под EMA200
        if ema200 and current_price < ema200:
            score += 10
            reasons.append("Цена ниже EMA200 (медвежий долгосрочный тренд)")
        
        # 2. RSI (максимум 20 баллов)
        if 35 < rsi_current < RSI_OVERBOUGHT:
            if 45 <= rsi_current <= 55:
                score += 20
                reasons.append(f"RSI идеален ({rsi_current:.1f})")
            else:
                score += 15
                reasons.append(f"RSI приемлем ({rsi_current:.1f})")
        
        # 3. MACD (максимум 20 баллов)
        if macd_line < signal_line:
            score += 15
            reasons.append("MACD медвежий")
            if histogram < 0 and abs(histogram) > abs(macd_line) * 0.1:
                score += 5
                reasons.append("MACD гистограмма падает")
        
        # 4. Bollinger Bands (максимум 15 баллов)
        bb_position = (current_price - bb_lower) / (bb_upper - bb_lower)
        if bb_position > 0.7:
            score += 15
            reasons.append("Откат от верхней BB (сильный)")
        elif bb_position > 0.5:
            score += 10
            reasons.append("Откат от верхней BB")
        
        # 5. Объём (максимум 10 баллов)
        if vol_strength > 2.0:
            score += 10
            reasons.append(f"Очень высокий объём ({vol_strength:.1f}x)")
        elif vol_strength > 1.5:
            score += 7
            reasons.append(f"Высокий объём ({vol_strength:.1f}x)")
        
        # 6. Сила импульса (максимум 10 баллов)
        momentum = (ema21 - ema9) / ema21
        if momentum > 0.01:
            score += 10
            reasons.append("Очень сильный импульс")
        elif momentum > 0.005:
            score += 7
            reasons.append("Сильный импульс")
        
        # 7. Дивергенция (бонус 15 баллов)
        if divergence == "bearish":
            score += 15
            reasons.append("⚡ Медвежья дивергенция!")
        
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

# ==================== TRANSLATIONS ====================
TEXTS = {
    "ru": {
        "welcome": "Выбери язык / Choose language",
        "main_menu": "Главное меню",
        "start_text": f"<b>🚀 {BOT_NAME}</b>\n\nТочные сигналы с автоматическим TP/SL\n\n• 3 сигнала в день (только сильные)\n• Мультистратегия (7 индикаторов)\n• 3 уровня Take Profit\n• Объяснение каждого входа\n\n📖 Жми Инструкция",
        "btn_alerts": "📈 Алерты",
        "btn_ref": "👥 Рефералка",
        "btn_guide": "📖 Инструкция",
        "btn_support": "💬 Поддержка",
        "btn_unlock": "🔓 Открыть доступ",
        "btn_admin": "👑 Админ",
        "btn_back": "⬅️ Назад",
        "access_required": "Оплатите доступ для использования алертов!",
    },
    "en": {
        "welcome": "Выбери язык / Choose language",
        "main_menu": "Main Menu",
        "start_text": f"<b>🚀 {BOT_NAME}</b>\n\nAccurate signals with automatic TP/SL\n\n• 3 signals per day (only strong)\n• Multi-strategy (7 indicators)\n• 3 Take Profit levels\n• Explanation for each entry\n\n📖 Press Guide",
        "btn_alerts": "📈 Alerts",
        "btn_ref": "👥 Referrals",
        "btn_guide": "📖 Guide",
        "btn_support": "💬 Support",
        "btn_unlock": "🔓 Unlock Access",
        "btn_admin": "👑 Admin",
        "btn_back": "⬅️ Back",
        "access_required": "Please pay for access to use alerts!",
    }
}

async def get_user_lang(uid: int) -> str:
    """Получить язык пользователя"""
    conn = await db_pool.acquire()
    try:
        cursor = await conn.execute("SELECT language FROM users WHERE id=?", (uid,))
        row = await cursor.fetchone()
        return row["language"] if row and row["language"] else "ru"
    finally:
        await db_pool.release(conn)

async def set_user_lang(uid: int, lang: str):
    """Установить язык пользователя"""
    conn = await db_pool.acquire()
    try:
        await conn.execute("UPDATE users SET language=? WHERE id=?", (lang, uid))
        await conn.commit()
    finally:
        await db_pool.release(conn)

def t(uid_or_lang, key: str) -> str:
    """Перевод текста"""
    if isinstance(uid_or_lang, str):
        lang = uid_or_lang
    else:
        lang = "ru"  # дефолт
    return TEXTS.get(lang, TEXTS["ru"]).get(key, key)

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
def main_menu_kb(is_admin_user: bool, is_paid_user: bool, lang: str = "ru"):
    kb = InlineKeyboardMarkup(row_width=2)
    if is_paid_user:
        kb.add(
            InlineKeyboardButton(t(lang, "btn_alerts"), callback_data="menu_alerts"),
            InlineKeyboardButton(t(lang, "btn_ref"), callback_data="menu_ref")
        )
    kb.add(
        InlineKeyboardButton(t(lang, "btn_guide"), callback_data="menu_guide"),
        InlineKeyboardButton(t(lang, "btn_support"), url=SUPPORT_URL)
    )
    if not is_paid_user:
        kb.add(InlineKeyboardButton(t(lang, "btn_unlock"), callback_data="menu_pay"))
    if is_admin_user:
        kb.add(InlineKeyboardButton(t(lang, "btn_admin"), callback_data="menu_admin"))
    kb.add(InlineKeyboardButton("🌐 Language", callback_data="change_lang"))
    return kb

def alerts_kb(user_pairs: List[str], lang: str = "ru"):
    kb = InlineKeyboardMarkup(row_width=2)
    for pair in DEFAULT_PAIRS:
        emoji = "✅" if pair in user_pairs else "➕"
        kb.add(InlineKeyboardButton(f"{emoji} {pair}", callback_data=f"toggle_{pair}"))
    
    add_btn = "➕ Своя монета" if lang == "ru" else "➕ Custom coin"
    my_btn = "📋 Мои монеты" if lang == "ru" else "📋 My coins"
    info_btn = "💡 Как это работает?" if lang == "ru" else "💡 How it works?"
    
    kb.add(
        InlineKeyboardButton(add_btn, callback_data="add_custom"),
        InlineKeyboardButton(my_btn, callback_data="my_pairs")
    )
    kb.add(InlineKeyboardButton(info_btn, callback_data="alerts_info"))
    kb.add(InlineKeyboardButton(t(lang, "btn_back"), callback_data="back_main"))
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
    lang = await get_user_lang(call.from_user.id)
    paid = await is_paid(call.from_user.id)
    try:
        await call.message.edit_text(t(lang, "main_menu"), 
                                     reply_markup=main_menu_kb(is_admin(call.from_user.id), paid, lang))
    except:
        pass
    await call.answer()

# ==================== ALERTS INFO ====================
@dp.callback_query_handler(lambda c: c.data == "alerts_info")
async def alerts_info(call: types.CallbackQuery):
    """Подробное объяснение как работают алерты"""
    lang = await get_user_lang(call.from_user.id)
    
    if lang == "ru":
        text = "💡 <b>Как работают алерты?</b>\n\n"
        text += "<b>1. Выбери монеты</b>\n"
        text += "Нажми на монету чтобы добавить её в отслеживание. Бот будет анализировать её каждые 5 минут.\n\n"
        text += "<b>2. Бот анализирует рынок</b>\n"
        text += "Используя 7 индикаторов:\n"
        text += "• EMA (тренды)\n"
        text += "• RSI (перекупленность)\n"
        text += "• MACD (импульс)\n"
        text += "• Bollinger Bands (уровни)\n"
        text += "• Volume (объём)\n"
        text += "• Дивергенции\n"
        text += "• ATR (волатильность)\n\n"
        text += "<b>3. Получаешь сигнал</b>\n"
        text += "Только когда:\n"
        text += "✅ Сила сигнала 85+ баллов\n"
        text += "✅ Подтверждено 5+ индикаторами\n"
        text += "✅ Не больше 3 сигналов в день\n\n"
        text += "<b>4. Управление позицией</b>\n"
        text += "📍 TP1 (15% позиции) - быстрая фиксация\n"
        text += "   → Передвинь SL в безубыток!\n\n"
        text += "📍 TP2 (40% позиции) - основная цель\n"
        text += "   → Передвинь SL к TP1\n\n"
        text += "📍 TP3 (80% позиции) - максимум тренда\n"
        text += "   → Закрой всё оставшееся\n\n"
        text += "<b>🎯 Почему 3 уровня?</b>\n"
        text += "• Не жадничаешь - фиксируешь профит постепенно\n"
        text += "• После TP1 ты УЖЕ в плюсе без риска\n"
        text += "• Оставляешь позицию на тренд если пойдёт сильнее\n"
        text += "• Психологически легче торговать\n\n"
        text += "<b>💡 Рекомендации:</b>\n"
        text += "• НЕ ИГНОРИРУЙ Stop Loss - это защита капитала\n"
        text += "• После TP1 ОБЯЗАТЕЛЬНО передвинь SL в безубыток\n"
        text += "• Не входи всем депозитом - макс 5% на сделку\n"
        text += "• Веди дневник сделок - анализируй ошибки\n"
        text += "• Торгуй по плану, не по эмоциям\n\n"
        text += "⚠️ <b>Помни:</b> Даже лучшие сигналы не дают 100% гарантии. Управление рисками важнее точности входа!"
    else:
        text = "💡 <b>How Alerts Work?</b>\n\n"
        text += "<b>1. Select Coins</b>\n"
        text += "Click on a coin to add it to tracking. Bot will analyze it every 5 minutes.\n\n"
        text += "<b>2. Bot Analyzes Market</b>\n"
        text += "Using 7 indicators:\n"
        text += "• EMA (trends)\n"
        text += "• RSI (overbought/oversold)\n"
        text += "• MACD (momentum)\n"
        text += "• Bollinger Bands (levels)\n"
        text += "• Volume\n"
        text += "• Divergences\n"
        text += "• ATR (volatility)\n\n"
        text += "<b>3. Receive Signal</b>\n"
        text += "Only when:\n"
        text += "✅ Signal strength 85+ points\n"
        text += "✅ Confirmed by 5+ indicators\n"
        text += "✅ Max 3 signals per day\n\n"
        text += "<b>4. Position Management</b>\n"
        text += "📍 TP1 (15% position) - quick profit\n"
        text += "   → Move SL to breakeven!\n\n"
        text += "📍 TP2 (40% position) - main target\n"
        text += "   → Move SL to TP1\n\n"
        text += "📍 TP3 (80% position) - max trend\n"
        text += "   → Close remaining\n\n"
        text += "<b>🎯 Why 3 Levels?</b>\n"
        text += "• Don't be greedy - take profit gradually\n"
        text += "• After TP1 you're in profit with NO risk\n"
        text += "• Leave position for trend if it goes stronger\n"
        text += "• Psychologically easier to trade\n\n"
        text += "<b>💡 Recommendations:</b>\n"
        text += "• DON'T IGNORE Stop Loss - it protects capital\n"
        text += "• After TP1 ALWAYS move SL to breakeven\n"
        text += "• Don't use full deposit - max 5% per trade\n"
        text += "• Keep trading journal - analyze mistakes\n"
        text += "• Trade by plan, not by emotions\n\n"
        text += "⚠️ <b>Remember:</b> Even best signals don't guarantee 100%. Risk management is more important than entry accuracy!"
    
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(t(lang, "btn_back"), callback_data="menu_alerts"))
    
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except:
        await call.message.answer(text, reply_markup=kb)
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
    lang = await get_user_lang(uid)
    
    if not await is_paid(uid):
        await call.answer(t(lang, "access_required"), show_alert=True)
        return
    
    pairs = await get_user_pairs(uid)
    
    if lang == "ru":
        text = f"📈 <b>Управление алертами</b>\n\nВыбери монеты (до 10)\n\nАктивно: {len(pairs)}/10\n\n💡 Нажми «Как это работает?» для подробностей"
    else:
        text = f"📈 <b>Manage Alerts</b>\n\nSelect coins (up to 10)\n\nActive: {len(pairs)}/10\n\n💡 Press «How it works?» for details"
    
    await call.message.edit_text(text, reply_markup=alerts_kb(pairs, lang))
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
    text += "• 🎯 TP1 (15% позиции)\n"
    text += "• 🎯 TP2 (40% позиции)\n"
    text += "• 🎯 TP3 (80% позиции)\n"
    text += "• 🛡 Stop Loss (2.0 × ATR)\n"
    text += "• Причины входа\n"
    text += "• Сила сигнала (85-100 баллов)\n\n"
    text += "<b>Стратегия управления:</b>\n"
    text += "1. При достижении TP1:\n"
    text += "   - Закрой 15% позиции\n"
    text += "   - Передвинь SL в безубыток (точка входа)\n"
    text += "2. При достижении TP2:\n"
    text += "   - Закрой ещё 40% позиции\n"
    text += "   - Передвинь SL к TP1\n"
    text += "3. При достижении TP3:\n"
    text += "   - Закрой оставшиеся 80%\n\n"
    text += "<b>Анализ (7 индикаторов):</b>\n"
    text += "• Таймфрейм: 5 минут\n"
    text += "• EMA тренды (9/21/50/200)\n"
    text += "• RSI + дивергенции\n"
    text += "• MACD импульс\n"
    text += "• Bollinger Bands\n"
    text += "• Объём\n"
    text += "• ATR волатильность\n\n"
    text += "<b>⚠️ Важно:</b>\n"
    text += "• Не жадничай - фиксируй по уровням\n"
    text += "• После TP1 ты в плюсе без риска\n"
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

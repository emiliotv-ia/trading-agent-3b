import os
import json
import math
import random
import requests
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
import threading
import time
import pytz

app = Flask(__name__)
CORS(app)

DATABASE_URL      = os.environ.get("DATABASE_URL")
ALPACA_API_KEY    = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")
ALPACA_BASE_URL   = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
FINNHUB_API_KEY   = os.environ.get("FINNHUB_API_KEY")
NEWS_API_KEY      = os.environ.get("NEWS_API_KEY")

# --- AUTH ---
API_TOKEN = os.environ.get("API_TOKEN", "")

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if API_TOKEN:
            token = request.headers.get("Authorization", "").replace("Bearer ", "").strip()
            if token != API_TOKEN:
                return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated

trading_client    = None
stock_data_client = None

def init_alpaca():
    global trading_client, stock_data_client
    try:
        if ALPACA_API_KEY and ALPACA_SECRET_KEY:
            paper = "paper" in ALPACA_BASE_URL
            trading_client    = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=paper)
            stock_data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
            print("✅ Alpaca conectado")
        else:
            print("⚠️  Sin variables Alpaca — modo simulacion")
    except Exception as e:
        print(f"❌ Error Alpaca: {e}")

STOCK_SYMBOLS = ["NVDA","MSFT","META","GOOGL","AMZN","PLTR","ARKK","QQQ","MU","TSM","CEG","GEV"]

SECTOR = {
    "NVDA":"Chips IA","MSFT":"Cloud/IA","META":"IA Consumo","GOOGL":"IA/Search",
    "AMZN":"Cloud","PLTR":"Data IA","ARKK":"ETF Tech","QQQ":"ETF NASDAQ",
    "MU":"Chips/Memoria","TSM":"Chips IA","CEG":"Energia","GEV":"Energia"
}

BASE_PRICES = {
    "NVDA":870.0,"MSFT":415.0,"META":530.0,"GOOGL":175.0,
    "AMZN":195.0,"PLTR":22.0,"ARKK":48.0,"QQQ":450.0,
    "MU":130.0,"TSM":150.0,"CEG":165.0,"GEV":175.0
}

# -------------------------------------------------------
# SENTIMENT
# -------------------------------------------------------
sentiment_cache = {}
sentiment_lock  = threading.Lock()
SENTIMENT_UPDATE_INTERVAL = 900

def _label(score):
    if score >= 0.15:  return "bullish"
    if score <= -0.15: return "bearish"
    return "neutral"

def _finnhub_sentiment(sym):
    try:
        r = requests.get(
            f"https://finnhub.io/api/v1/news-sentiment?symbol={sym}&token={FINNHUB_API_KEY}",
            timeout=8)
        data = r.json()
        raw  = data.get("companyNewsScore")
        score = round((raw - 0.5) * 2, 3) if raw is not None else None
        today = datetime.utcnow().strftime("%Y-%m-%d")
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        rn = requests.get(
            f"https://finnhub.io/api/v1/company-news?symbol={sym}&from={yesterday}&to={today}&token={FINNHUB_API_KEY}",
            timeout=8)
        news_raw = rn.json() if rn.status_code == 200 else []
        news = [{"title": n.get("headline",""), "source": "Finnhub",
                 "url": n.get("url",""),
                 "ts": datetime.utcfromtimestamp(n.get("datetime",0)).strftime("%d/%m %H:%M") if n.get("datetime") else ""}
                for n in news_raw[:5]]
        return score, news
    except Exception as e:
        print(f"Finnhub error {sym}: {e}")
        return None, []

def _newsapi_sentiment(sym):
    POSITIVE = ["surge","soar","beat","bullish","gains","record","strong","growth",
                "upgrade","buy","profit","rally","outperform","rises","higher","positive","boom","jumps"]
    NEGATIVE = ["drop","fall","miss","bearish","losses","decline","weak","downgrade",
                "sell","loss","crash","underperform","lower","negative","warn","cut","risk","plunge"]
    COMPANY  = {"NVDA":"nvidia","MSFT":"microsoft","META":"meta","GOOGL":"google",
                "AMZN":"amazon","PLTR":"palantir","MU":"micron","TSM":"tsmc",
                "ARKK":"ark invest","QQQ":"nasdaq etf","CEG":"constellation energy","GEV":"ge vernova"}
    try:
        q = COMPANY.get(sym, sym)
        r = requests.get("https://newsapi.org/v2/everything",
            params={"q":q,"language":"en","sortBy":"publishedAt","pageSize":10,
                    "apiKey":NEWS_API_KEY,"from":(datetime.utcnow()-timedelta(days=1)).strftime("%Y-%m-%d")},
            timeout=8)
        articles = r.json().get("articles",[])
        if not articles:
            return None, []
        pos = neg = 0
        news = []
        for a in articles[:10]:
            text = (a.get("title","") + " " + a.get("description","")).lower()
            p = sum(1 for w in POSITIVE if w in text)
            n = sum(1 for w in NEGATIVE if w in text)
            pos += p; neg += n
            news.append({"title":a.get("title",""),"source":a.get("source",{}).get("name",""),
                         "url":a.get("url",""),"ts":a.get("publishedAt","")[:10]})
        total = pos + neg
        score = round((pos - neg) / total, 3) if total > 0 else 0
        return score, news[:5]
    except Exception as e:
        print(f"NewsAPI error {sym}: {e}")
        return None, []

def _alpaca_news_sentiment(sym):
    try:
        if not stock_data_client:
            return None, []
        from alpaca.data.requests import NewsRequest
        from alpaca.data.historical import NewsClient
        return None, []
    except:
        return None, []

def update_sentiment_cache():
    for sym in STOCK_SYMBOLS:
        try:
            fh_score, fh_news = _finnhub_sentiment(sym)
            na_score, na_news = _newsapi_sentiment(sym)
            sources = {}
            weights = []
            if fh_score is not None:
                sources["finnhub"] = fh_score
                weights.append((fh_score, 0.6))
            if na_score is not None:
                sources["newsapi"] = na_score
                weights.append((na_score, 0.4))
            if not weights:
                continue
            total_w = sum(w for _, w in weights)
            composite = sum(s * w for s, w in weights) / total_w
            composite = round(composite, 3)
            news = fh_news + [n for n in na_news if n not in fh_news]
            with sentiment_lock:
                sentiment_cache[sym] = {
                    "score": composite, "label": _label(composite),
                    "sources": sources, "news": news[:8],
                    "updated": datetime.utcnow().strftime("%d/%m %H:%M")
                }
            time.sleep(0.5)
        except Exception as e:
            print(f"Sentiment update error {sym}: {e}")

def sentiment_loop():
    time.sleep(10)
    while True:
        update_sentiment_cache()
        time.sleep(SENTIMENT_UPDATE_INTERVAL)

# -------------------------------------------------------
# DATABASE
# -------------------------------------------------------
def get_db():
    if not DATABASE_URL:
        return None
    try:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    except Exception as e:
        print(f"DB error: {e}")
        return None

def init_db():
    conn = get_db()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS agent3b_state (
                    id SERIAL PRIMARY KEY,
                    data JSONB NOT NULL,
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS agent3b_reports (
                    id SERIAL PRIMARY KEY,
                    texto TEXT,
                    resumen JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()
    finally:
        conn.close()

def save_state(s):
    conn = get_db()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM agent3b_state LIMIT 1")
            row = cur.fetchone()
            data = json.dumps(s)
            if row:
                cur.execute("UPDATE agent3b_state SET data=%s, updated_at=NOW() WHERE id=%s", (data, row["id"]))
            else:
                cur.execute("INSERT INTO agent3b_state (data) VALUES (%s)", (data,))
            conn.commit()
    except Exception as e:
        print(f"Save state error: {e}")
    finally:
        conn.close()

def load_state():
    conn = get_db()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM agent3b_state ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            return row["data"] if row else None
    except Exception as e:
        print(f"Load state error: {e}")
        return None
    finally:
        conn.close()

# -------------------------------------------------------
# ML — ONLINE LOGISTIC REGRESSION (SGD)
# -------------------------------------------------------
N_FEATURES  = 5
ML_LR       = 0.05
ML_MIN_TRADES = 8

def _sigmoid(x):
    return 1.0 / (1.0 + math.exp(-max(-500, min(500, x))))

def ml_predict(s, sym):
    weights = s.get("ml_weights", {}).get(sym, [0.0] * N_FEATURES)
    bias    = s.get("ml_bias",    {}).get(sym, 0.0)
    feats   = _extract_features(s, sym)
    z = bias + sum(w * f for w, f in zip(weights, feats))
    return _sigmoid(z)

def ml_update(s, sym, features, won):
    if "ml_weights" not in s: s["ml_weights"] = {}
    if "ml_bias"    not in s: s["ml_bias"]    = {}
    w = s["ml_weights"].get(sym, [0.0] * N_FEATURES)
    b = s["ml_bias"].get(sym, 0.0)
    y = 1.0 if won else 0.0
    z = b + sum(wi * fi for wi, fi in zip(w, features))
    p = _sigmoid(z)
    err = p - y
    # L2 regularization
    w = [wi * (1 - ML_LR * 0.001) - ML_LR * err * fi for wi, fi in zip(w, features)]
    b = b - ML_LR * err
    s["ml_weights"][sym] = w
    s["ml_bias"][sym]    = b

def ml_confidence_multiplier(s, sym):
    trades = [m for m in s.get("memory", []) if m["sym"] == sym]
    if len(trades) < ML_MIN_TRADES:
        return 1.0
    prob = ml_predict(s, sym)
    if prob >= 0.65: return 1.3
    if prob <= 0.35: return 0.6
    return 1.0

# -------------------------------------------------------
# KELLY CRITERION
# -------------------------------------------------------
KELLY_MIN_TRADES = 8
KELLY_MIN_PCT    = 0.05
KELLY_MAX_PCT    = 0.25

def kelly_position_size(s, sym, total_capital):
    trades = [m for m in s.get("memory", []) if m["sym"] == sym]
    if len(trades) < KELLY_MIN_TRADES:
        sz = s["config"].get("sz", 20) / 100
        return max(KELLY_MIN_PCT, min(KELLY_MAX_PCT, sz))
    wins   = [m for m in trades if m["won"]]
    losses = [m for m in trades if not m["won"]]
    win_rate = len(wins) / len(trades)
    avg_win  = sum(m.get("ret", 0) for m in wins)  / max(len(wins), 1)
    avg_loss = abs(sum(m.get("ret", 0) for m in losses)) / max(len(losses), 1)
    if avg_loss == 0:
        return KELLY_MAX_PCT
    b = avg_win / avg_loss
    kelly = (b * win_rate - (1 - win_rate)) / b
    half_kelly = kelly * 0.5
    return max(KELLY_MIN_PCT, min(KELLY_MAX_PCT, half_kelly))

# -------------------------------------------------------
# DRAWDOWN PROTECTION
# -------------------------------------------------------
MAX_DRAWDOWN_PCT = 0.15

def check_drawdown(s):
    """
    Calcula drawdown y ajusta el multiplicador de tamaño de posición.
    En vez de detener el agente, reduce el tamaño progresivamente:
      - DD < 10%:  escala normal  (1.0)
      - DD 10-13%: mitad del tamaño (0.5)
      - DD > 13%:  cuarto del tamaño (0.25)
    Nunca detiene el agente automáticamente.
    """
    total = s["cash"] + sum(p["qty"] * gp(s, sym) for sym, p in s["positions"].items() if p.get("qty", 0) > 0)
    if "peak_capital" not in s or total > s["peak_capital"]:
        s["peak_capital"] = round(total, 2)
    dd = (s["peak_capital"] - total) / s["peak_capital"] if s["peak_capital"] > 0 else 0
    s["current_drawdown"] = round(dd * 100, 2)
    s["drawdown_stopped"]  = False  # nunca se detiene solo

    if dd >= 0.13:
        s["drawdown_scale"] = 0.25
        log(s, f"⚠️ Drawdown {s['current_drawdown']}% — operando al 25% del tamaño normal", "warn")
    elif dd >= 0.10:
        s["drawdown_scale"] = 0.5
        log(s, f"⚠️ Drawdown {s['current_drawdown']}% — operando al 50% del tamaño normal", "warn")
    else:
        s["drawdown_scale"] = 1.0

# -------------------------------------------------------
# HELPERS
# -------------------------------------------------------
def gp(s, sym):
    return s["prices"].get(sym, BASE_PRICES.get(sym, 100.0))

def log(s, msg, typ="think"):
    s["log"].insert(0, {"t": datetime.now().strftime("%H:%M:%S"), "msg": msg, "type": typ})
    if len(s["log"]) > 200:
        s["log"] = s["log"][:200]

def is_market_open():
    try:
        if trading_client:
            clock = trading_client.get_clock()
            return clock.is_open
    except:
        pass
    ny = pytz.timezone("America/New_York")
    now = datetime.now(ny)
    if now.weekday() >= 5:
        return False
    return now.hour * 60 + now.minute >= 570 and now.hour * 60 + now.minute < 960

def update_brain(s, sym, won, ret, features=None):
    s["wins" if won else "losses"] += 1
    entry = {"sym": sym, "won": won, "ret": round(ret, 4),
             "t": datetime.now().strftime("%Y-%m-%d %H:%M"),
             "features": features}
    s["memory"].insert(0, entry)
    if len(s["memory"]) > 500:
        s["memory"] = s["memory"][:500]
    if features:
        ml_update(s, sym, features, won)
    # Update scores
    if sym not in s["scores"]:
        s["scores"][sym] = {"score": 50, "trades": 0, "wins": 0}
    sc = s["scores"][sym]
    sc["trades"] += 1
    if won:
        sc["wins"] += 1
        sc["score"] = min(100, sc["score"] + 4)
    else:
        sc["score"] = max(0,   sc["score"] - 3)

# -------------------------------------------------------
# BAR HISTORY (OHLCV)
# -------------------------------------------------------
MAX_BARS      = 200
MIN_BARS_CALC = 30   # minimum bars to compute all 7 factors

def _append_bar(s, sym, o, h, l, c, v):
    if "bar_history" not in s:
        s["bar_history"] = {}
    if sym not in s["bar_history"]:
        s["bar_history"][sym] = []
    s["bar_history"][sym].append((float(o), float(h), float(l), float(c), float(v)))
    if len(s["bar_history"][sym]) > MAX_BARS:
        s["bar_history"][sym] = s["bar_history"][sym][-MAX_BARS:]

def _closes(bars): return [b[3] for b in bars]
def _highs(bars):  return [b[1] for b in bars]
def _lows(bars):   return [b[2] for b in bars]
def _vols(bars):   return [b[4] for b in bars]

def fetch_bars_ohlcv(s, sym, limit=10):
    """Fetch latest OHLCV bars from Alpaca and append to history."""
    try:
        if stock_data_client:
            end   = datetime.utcnow()
            start = end - timedelta(hours=6)
            req   = StockBarsRequest(symbol_or_symbols=sym,
                                     timeframe=TimeFrame.Minute,
                                     start=start, end=end, limit=limit,
                                     feed="iex")
            bars  = stock_data_client.get_stock_bars(req)
            bl    = bars[sym] if sym in bars else []
            if bl:
                for bar in bl[-limit:]:
                    _append_bar(s, sym, bar.open, bar.high, bar.low, bar.close, bar.volume)
                s["prices"][sym] = float(bl[-1].close)
                return True
    except Exception as e:
        print(f"Bars fetch error {sym}: {e}")
    # Simulation fallback
    base  = gp(s, sym)
    close = round(base * (1 + random.gauss(0, 0.004)), 2)
    high  = round(close * (1 + abs(random.gauss(0, 0.002))), 2)
    low   = round(close * (1 - abs(random.gauss(0, 0.002))), 2)
    vol   = random.randint(500_000, 2_000_000)
    _append_bar(s, sym, close, high, low, close, vol)
    s["prices"][sym] = close
    return False

# -------------------------------------------------------
# FACTOR 1 — MOMENTUM (multi-timeframe)
# -------------------------------------------------------
def factor_momentum(bars):
    """
    Computes price momentum at 3 horizons.
    Returns +1 if majority bullish, -1 if majority bearish, 0 neutral.
    """
    closes = _closes(bars)
    if len(closes) < 6:
        return 0, None
    signals = []
    thresholds = [(5, 0.003), (20, 0.010), (60, 0.020)]
    details = {}
    for lookback, thresh in thresholds:
        if len(closes) >= lookback + 1:
            ret = (closes[-1] - closes[-(lookback+1)]) / closes[-(lookback+1)]
            sig = 1 if ret > thresh else -1 if ret < -thresh else 0
            signals.append(sig)
            details[f"ret{lookback}"] = round(ret * 100, 2)
    if not signals:
        return 0, None
    score = sum(signals)
    return (1 if score > 0 else -1 if score < 0 else 0), details

# -------------------------------------------------------
# FACTOR 2 — RSI
# -------------------------------------------------------
def _calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    recent = deltas[-period:]
    gains  = [d for d in recent if d > 0]
    losses = [-d for d in recent if d < 0]
    avg_g  = sum(gains)  / period
    avg_l  = sum(losses) / period
    if avg_l == 0:
        return 100.0
    return round(100 - 100 / (1 + avg_g / avg_l), 1)

def factor_rsi(bars):
    closes = _closes(bars)
    rsi = _calc_rsi(closes)
    if rsi is None:
        return 0, None
    if rsi < 35:
        return 1, {"rsi": rsi, "signal": "oversold"}
    if rsi > 65:
        return -1, {"rsi": rsi, "signal": "overbought"}
    return 0, {"rsi": rsi, "signal": "neutral"}

# -------------------------------------------------------
# FACTOR 3 — VOLUME SURGE
# -------------------------------------------------------
def factor_volume(bars):
    if len(bars) < 22:
        return 0, None
    closes = _closes(bars)
    vols   = _vols(bars)
    avg_vol    = sum(vols[-21:-1]) / 20
    if avg_vol == 0:
        return 0, None
    vol_ratio  = vols[-1] / avg_vol
    price_up   = closes[-1] > closes[-2] if len(closes) >= 2 else False
    detail     = {"vol_ratio": round(vol_ratio, 2), "price_up": price_up}
    if vol_ratio > 1.5:
        return (1 if price_up else -1), detail
    if vol_ratio < 0.5:
        return 0, {**detail, "note": "low volume"}
    return 0, detail

# -------------------------------------------------------
# FACTOR 4 — TREND (EMA crossover + slope)
# -------------------------------------------------------
def _ema(data, period):
    if len(data) < period:
        return None
    k   = 2.0 / (period + 1)
    ema = sum(data[:period]) / period
    for v in data[period:]:
        ema = v * k + ema * (1 - k)
    return ema

def factor_trend(bars):
    closes = _closes(bars)
    if len(closes) < 22:
        return 0, None
    ema9  = _ema(closes, 9)
    ema21 = _ema(closes, 21)
    if not ema9 or not ema21:
        return 0, None
    diff_pct = (ema9 - ema21) / ema21 * 100
    # Also check EMA9 slope
    if len(closes) >= 12:
        ema9_prev = _ema(closes[:-3], 9)
        slope = (ema9 - ema9_prev) / ema9_prev * 100 if ema9_prev else 0
    else:
        slope = 0
    detail = {"ema9": round(ema9, 2), "ema21": round(ema21, 2),
              "diff_pct": round(diff_pct, 3), "slope": round(slope, 3)}
    if diff_pct > 0.15 and slope > 0:
        return 1, {**detail, "signal": "uptrend"}
    if diff_pct < -0.15 and slope < 0:
        return -1, {**detail, "signal": "downtrend"}
    return 0, {**detail, "signal": "ranging"}

# -------------------------------------------------------
# FACTOR 5 — VOLATILITY (ATR-based)
# -------------------------------------------------------
def _calc_atr(bars, period=14):
    if len(bars) < period + 1:
        return None
    highs  = _highs(bars)
    lows   = _lows(bars)
    closes = _closes(bars)
    trs = [max(highs[i] - lows[i],
               abs(highs[i] - closes[i-1]),
               abs(lows[i]  - closes[i-1]))
           for i in range(1, len(bars))]
    return sum(trs[-period:]) / period

def factor_volatility(bars):
    atr = _calc_atr(bars)
    if atr is None:
        return 0, None
    closes  = _closes(bars)
    atr_pct = atr / closes[-1] * 100
    detail  = {"atr": round(atr, 3), "atr_pct": round(atr_pct, 3)}
    if atr_pct < 0.5:
        return 1, {**detail, "signal": "low_vol"}   # stable — good for entry
    if atr_pct > 1.8:
        return -1, {**detail, "signal": "high_vol"}  # too risky
    return 0, detail

# -------------------------------------------------------
# FACTOR 6 — SENTIMENT
# -------------------------------------------------------
def factor_sentiment(sym):
    with sentiment_lock:
        data = sentiment_cache.get(sym)
    if not data:
        return 0, None
    score = data.get("score", 0)
    label = data.get("label", "neutral")
    if score >= 0.15:
        return 1, {"score": score, "label": label}
    if score <= -0.15:
        return -1, {"score": score, "label": label}
    return 0, {"score": score, "label": label}

# -------------------------------------------------------
# FACTOR 7 — ML (online logistic regression)
# -------------------------------------------------------
def factor_ml(s, sym):
    trades = [m for m in s.get("memory", []) if m["sym"] == sym]
    if len(trades) < ML_MIN_TRADES:
        return 0, {"status": "accumulating", "trades": len(trades)}
    prob = ml_predict(s, sym)
    if prob >= 0.62:
        return 1, {"prob": round(prob, 3), "signal": "high_prob_win"}
    if prob <= 0.38:
        return -1, {"prob": round(prob, 3), "signal": "low_prob_win"}
    return 0, {"prob": round(prob, 3), "signal": "uncertain"}

# -------------------------------------------------------
# CONVERGENCE CALCULATOR
# -------------------------------------------------------
CONVERGENCE_BUY_THRESHOLD  = 3   # ≥3/7 to BUY (agente 3B — más activo)
CONVERGENCE_EXIT_THRESHOLD = -2  # ≤-2/7 to EXIT

FACTOR_NAMES = ["momentum", "rsi", "volume", "trend", "volatility", "sentiment", "ml"]

def _extract_features(s, sym):
    """5 normalized features for ML training."""
    bars   = s.get("bar_history", {}).get(sym, [])
    closes = _closes(bars)
    vols   = _vols(bars)

    rsi_val  = _calc_rsi(closes) if len(closes) >= 15 else 50.0
    rsi_norm = ((rsi_val or 50) - 50) / 50

    vol_factor = 1.0
    if len(vols) >= 21 and sum(vols[-21:-1]) > 0:
        vol_factor = vols[-1] / (sum(vols[-21:-1]) / 20)
    vol_norm = min(vol_factor - 1, 2) / 2

    with sentiment_lock:
        sent_data = sentiment_cache.get(sym)
    sent_norm = max(-1, min(1, sent_data.get("score", 0) if sent_data else 0))

    # Convergence score as feature
    score, _ = calc_convergence(s, sym)
    conv_norm = score / 7.0

    sym_trades = [m for m in s.get("memory", []) if m["sym"] == sym]
    sym_wins   = [m for m in sym_trades if m["won"]]
    sym_wr = (len(sym_wins) / len(sym_trades) - 0.5) * 2 if sym_trades else 0.0

    return [rsi_norm, vol_norm, sent_norm, conv_norm, sym_wr]

def calc_convergence(s, sym):
    """
    Computes all 7 factors and returns (score, factor_breakdown).
    score: integer from -7 to +7
    """
    bars = s.get("bar_history", {}).get(sym, [])
    f_mom,  d_mom  = factor_momentum(bars)
    f_rsi,  d_rsi  = factor_rsi(bars)
    f_vol,  d_vol  = factor_volume(bars)
    f_trd,  d_trd  = factor_trend(bars)
    f_atr,  d_atr  = factor_volatility(bars)
    f_sent, d_sent = factor_sentiment(sym)
    f_ml,   d_ml   = factor_ml(s, sym)

    factors = {
        "momentum":   {"signal": f_mom,  "detail": d_mom},
        "rsi":        {"signal": f_rsi,  "detail": d_rsi},
        "volume":     {"signal": f_vol,  "detail": d_vol},
        "trend":      {"signal": f_trd,  "detail": d_trd},
        "volatility": {"signal": f_atr,  "detail": d_atr},
        "sentiment":  {"signal": f_sent, "detail": d_sent},
        "ml":         {"signal": f_ml,   "detail": d_ml},
    }
    score = sum(v["signal"] for v in factors.values())
    return score, factors

# -------------------------------------------------------
# BACKTEST
# -------------------------------------------------------
def run_backtest(s):
    history = s.get("history", [])
    if len(history) < 5:
        s["backtest_done"] = False
        return
    rets = [h.get("ret", 0) for h in history]
    wins = [r for r in rets if r > 0]
    lss  = [r for r in rets if r <= 0]
    wr   = len(wins) / len(rets) * 100

    # Sharpe ratio (annualized, assume 390 min/day, 252 days)
    avg  = sum(rets) / len(rets)
    var  = sum((r - avg)**2 for r in rets) / len(rets)
    std  = math.sqrt(var) if var > 0 else 1e-9
    sharpe = round((avg / std) * math.sqrt(252 * 390 / 60), 2)

    # Profit factor
    gross_profit = sum(wins)
    gross_loss   = abs(sum(lss))
    pf = round(gross_profit / gross_loss, 2) if gross_loss > 0 else float("inf")

    # Max drawdown (equity curve simulation)
    equity = 1000.0
    peak   = equity
    max_dd = 0.0
    for r in rets:
        equity *= (1 + r)
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak * 100
        if dd > max_dd:
            max_dd = dd

    # Average convergence score at entry
    avg_conv = 0
    conv_data = [h.get("convergence_score") for h in history if h.get("convergence_score") is not None]
    if conv_data:
        avg_conv = round(sum(conv_data) / len(conv_data), 1)

    s["backtest_done"] = True
    s["backtest_summary"] = {
        "ops": len(rets),
        "win_rate": round(wr, 1),
        "sharpe": sharpe,
        "profit_factor": pf,
        "max_drawdown": round(max_dd, 1),
        "avg_ret_pct": round(avg * 100, 3),
        "avg_convergence_score": avg_conv,
    }
    log(s, f"Backtest: Sharpe {sharpe} · PF {pf} · MaxDD {max_dd:.1f}%", "think")

# -------------------------------------------------------
# PRICE & ORDER EXECUTION
# -------------------------------------------------------
def execute_buy(s, sym, qty, price, conv_score, factor_breakdown, features):
    cost = round(qty * price, 2)
    if cost > s["cash"]:
        return False
    if trading_client and s.get("mode") == "beta":
        try:
            order = MarketOrderRequest(symbol=sym, qty=qty,
                                       side=OrderSide.BUY, time_in_force=TimeInForce.DAY)
            trading_client.submit_order(order)
        except Exception as e:
            err_str = str(e).lower()
            if "unauthorized" in err_str or "401" in err_str:
                log(s, f"⚠️ Auth error BUY {sym} — reiniciando cliente Alpaca...", "warn")
                init_alpaca()  # auto-recover
                try:
                    if trading_client:
                        trading_client.submit_order(order)
                    else:
                        return False
                except Exception as e2:
                    log(s, f"Order error BUY {sym} (retry): {e2}", "warn")
                    return False
            else:
                log(s, f"Order error BUY {sym}: {e}", "warn")
                return False
    s["cash"] -= cost
    s["positions"][sym] = {"qty": qty, "avg_cost": price,
                           "entry_features": features,
                           "convergence_score": conv_score}
    s["decisions"].insert(0, {
        "sym": sym, "action": "COMPRA", "price": price,
        "detail": f"conv:{conv_score}/7 · qty:{qty}",
        "t": datetime.now().strftime("%H:%M:%S")
    })
    if len(s["decisions"]) > 50:
        s["decisions"] = s["decisions"][:50]
    log(s, f"COMPRA {sym} × {qty} @ ${price:.2f} | convergencia {conv_score}/7", "buy")
    return True

def execute_sell(s, sym, pos, price, reason, conv_score):
    qty  = pos["qty"]
    cost = pos["avg_cost"]
    ret  = (price - cost) / cost
    pnl  = round((price - cost) * qty, 2)
    won  = ret > 0
    features = pos.get("entry_features")
    update_brain(s, sym, won, ret, features)
    s["history"].insert(0, {
        "sym": sym, "ret": round(ret, 4), "pnl": pnl, "won": won,
        "entry": cost, "exit": price, "qty": qty,
        "convergence_score": pos.get("convergence_score"),
        "t": datetime.now().strftime("%Y-%m-%d %H:%M")
    })
    if len(s["history"]) > 500:
        s["history"] = s["history"][:500]
    if trading_client and s.get("mode") == "beta":
        try:
            order = MarketOrderRequest(symbol=sym, qty=qty,
                                       side=OrderSide.SELL, time_in_force=TimeInForce.DAY)
            trading_client.submit_order(order)
        except Exception as e:
            err_str = str(e).lower()
            if "unauthorized" in err_str or "401" in err_str:
                log(s, f"⚠️ Auth error SELL {sym} — reiniciando cliente Alpaca...", "warn")
                init_alpaca()
                try:
                    if trading_client:
                        trading_client.submit_order(order)
                except Exception as e2:
                    log(s, f"Order error SELL {sym} (retry): {e2}", "warn")
            else:
                log(s, f"Order error SELL {sym}: {e}", "warn")
    s["cash"] = round(s["cash"] + qty * price, 2)
    s["positions"][sym] = {"qty": 0, "avg_cost": 0}
    action = "TP" if ret >= s["config"]["tp"]/100 else "SL" if ret <= -s["config"]["sl"]/100 else "VENTA"
    s["decisions"].insert(0, {
        "sym": sym, "action": action, "price": price,
        "detail": f"{'+' if won else ''}{ret*100:.1f}% · conv:{conv_score}/7 · {reason}",
        "t": datetime.now().strftime("%H:%M:%S")
    })
    if len(s["decisions"]) > 50:
        s["decisions"] = s["decisions"][:50]
    log(s, f"{'✅' if won else '❌'} {'GANANCIA' if won else 'PÉRDIDA'} {sym} @ ${price:.2f} | {ret*100:+.1f}% · {reason}", "sell")
    if len(s["history"]) % 5 == 0:
        run_backtest(s)

# -------------------------------------------------------
# MAIN CYCLE
# -------------------------------------------------------
MAX_OPEN_POSITIONS = 3

def run_cycle(s):
    s["cycle"] += 1

    # Update bar history for all symbols
    for sym in STOCK_SYMBOLS:
        fetch_bars_ohlcv(s, sym, limit=5)

    check_drawdown(s)
    if s.get("drawdown_stopped"):
        log(s, "⏸ Ciclo pausado: drawdown stop activo", "warn")
        return

    mode = s.get("mode", "alpha")
    if mode == "beta" and not is_market_open():
        log(s, "⏸ Mercado cerrado", "think")
        return

    # Compute convergence scores for all symbols
    scored     = []
    factor_map = {}
    for sym in STOCK_SYMBOLS:
        bars = s.get("bar_history", {}).get(sym, [])
        if len(bars) < MIN_BARS_CALC:
            bars_needed = MIN_BARS_CALC - len(bars)
            factor_map[sym] = {"score": None, "bars_needed": bars_needed}
            continue
        score, breakdown = calc_convergence(s, sym)
        scored.append((sym, score))
        factor_map[sym] = {"score": score, "factors": breakdown}

    s["factor_scores"] = factor_map

    if not scored:
        log(s, f"Ciclo {s['cycle']}: acumulando datos ({len(s.get('bar_history',{}).get('NVDA',[]))}/{MIN_BARS_CALC} barras)", "think")
        return

    # Rank by convergence score (stock selection)
    scored.sort(key=lambda x: -x[1])
    top_str = " | ".join(f"{sym}:{sc:+d}" for sym, sc in scored[:5])
    log(s, f"Ciclo {s['cycle']} — Rankings: {top_str}", "think")

    # --- EXIT CHECK ---
    for sym, pos in list(s["positions"].items()):
        if pos.get("qty", 0) <= 0:
            continue
        price = gp(s, sym)
        cost  = pos["avg_cost"]
        ret   = (price - cost) / cost
        sl    = -(s["config"]["sl"] / 100)
        tp    =  s["config"]["tp"] / 100

        score = factor_map.get(sym, {}).get("score")
        if score is None:
            continue

        reason = None
        if ret <= sl:
            reason = f"SL {ret*100:.1f}%"
        elif ret >= tp:
            reason = f"TP {ret*100:.1f}%"
        elif score <= CONVERGENCE_EXIT_THRESHOLD:
            reason = f"señal revertida ({score}/7)"

        if reason:
            execute_sell(s, sym, pos, price, reason, score)

    # --- ENTRY CHECK ---
    open_count = sum(1 for p in s["positions"].values() if p.get("qty", 0) > 0)
    total_cap  = s["cash"] + sum(p["qty"] * gp(s, sym)
                                  for sym, p in s["positions"].items() if p.get("qty", 0) > 0)

    for sym, score in scored:
        if open_count >= MAX_OPEN_POSITIONS:
            break
        if score < CONVERGENCE_BUY_THRESHOLD:
            continue
        if s["positions"].get(sym, {}).get("qty", 0) > 0:
            continue

        price    = gp(s, sym)
        features = _extract_features(s, sym)
        sz_pct   = kelly_position_size(s, sym, total_cap)
        ml_mult  = ml_confidence_multiplier(s, sym)
        # Extra boost when convergence is very strong (6-7/7)
        conv_boost = 1.0 + 0.15 * max(0, score - CONVERGENCE_BUY_THRESHOLD)
        invest   = total_cap * sz_pct * ml_mult * conv_boost * s.get("drawdown_scale", 1.0)
        invest   = min(invest, s["cash"] * 0.95)
        qty      = int(invest / price)

        if qty < 1:
            log(s, f"Sin capital suficiente para {sym} (${invest:.0f} < ${price:.0f})", "warn")
            continue

        ok = execute_buy(s, sym, qty, price, score, factor_map[sym]["factors"], features)
        if ok:
            open_count += 1
            # Update scores
            if sym not in s["scores"]:
                s["scores"][sym] = {"score": 50, "trades": 0, "wins": 0}
            s["scores"][sym]["last"] = "compra"

    save_state(s)

# -------------------------------------------------------
# STATE INITIALIZATION
# -------------------------------------------------------
def make_default_state():
    return {
        "running": False, "cycle": 0, "mode": "alpha",
        "cash": 1000.0, "start_cap": 1000.0,
        "wins": 0, "losses": 0,
        "positions":  {sym: {"qty": 0, "avg_cost": 0} for sym in STOCK_SYMBOLS},
        "prices":     dict(BASE_PRICES),
        "scores":     {sym: {"score": 50, "trades": 0, "wins": 0, "last": "—"} for sym in STOCK_SYMBOLS},
        "decisions":  [], "log": [], "history": [], "memory": [],
        "patterns":   [],
        "bar_history": {sym: [] for sym in STOCK_SYMBOLS},
        "factor_scores": {},
        "ml_weights": {}, "ml_bias": {},
        "peak_capital": 1000.0, "current_drawdown": 0.0, "drawdown_stopped": False,
        "backtest_done": False, "backtest_summary": None,
        "config": {"freq": 60, "sl": 4, "tp": 6, "sz": 20, "risk": "balanced",
                   "convergence_threshold": CONVERGENCE_BUY_THRESHOLD},
    }

state = make_default_state()

def init_state():
    global state
    saved = load_state()
    if saved:
        data = saved if isinstance(saved, dict) else json.loads(saved)
        # Migrate missing fields
        defaults = make_default_state()
        for k, v in defaults.items():
            if k not in data:
                data[k] = v
        if "bar_history" not in data:
            data["bar_history"] = {sym: [] for sym in STOCK_SYMBOLS}
        if "factor_scores" not in data:
            data["factor_scores"] = {}
        if "peak_capital"     not in data: data["peak_capital"]     = data.get("cash", 1000.0)
        if "current_drawdown" not in data: data["current_drawdown"] = 0.0
        if "drawdown_stopped" not in data: data["drawdown_stopped"] = False
        if "ml_weights"       not in data: data["ml_weights"]       = {}
        if "ml_bias"          not in data: data["ml_bias"]          = {}
        state = data
        print("✅ Estado restaurado desde DB")
    else:
        print("🆕 Estado inicial creado")
    # Siempre arrancar si hay API keys — nunca depender del estado guardado en DB
    if ALPACA_API_KEY and ALPACA_SECRET_KEY:
        state["running"] = True
        save_state(state)
        print("🔄 Agente iniciado automáticamente (API keys detectadas)")

# -------------------------------------------------------
# AGENT LOOP
# -------------------------------------------------------
agent_lock = threading.Lock()

def agent_loop():
    # Forzar running=True siempre que haya API keys (self-healing)
    if ALPACA_API_KEY and ALPACA_SECRET_KEY:
        state["running"] = True
    while True:
        try:
            # Auto-healing: si se apagó por error, se reactiva solo y guarda en DB
            if not state.get("running") and ALPACA_API_KEY and ALPACA_SECRET_KEY:
                state["running"] = True
                save_state(state)
                print("🔄 Self-healing: agente reactivado y guardado en DB")
            if state.get("running"):
                with agent_lock:
                    run_cycle(state)
        except Exception as e:
            print(f"Cycle error: {e}")
            log(state, f"Error en ciclo: {e}", "warn")
        freq = state.get("config", {}).get("freq", 60)
        time.sleep(freq)

# -------------------------------------------------------
# FLASK ROUTES
# -------------------------------------------------------
@app.route("/")
def index():
    return jsonify({"agent": "trading-agent-3", "strategy": "multi-factor-convergence",
                    "version": "1.0", "factors": FACTOR_NAMES})

@app.route("/dashboard")
def dashboard():
    return send_from_directory(".", "dashboard.html")

@app.route("/comparador")
def comparador():
    return send_from_directory(".", "comparador.html")

@app.route("/state")
def get_state():
    total   = state["cash"] + sum(p["qty"] * gp(state, sym)
                                   for sym, p in state["positions"].items() if p.get("qty", 0) > 0)
    pnl     = round(total - state["start_cap"], 2)
    wr      = round(state["wins"] / (state["wins"] + state["losses"]) * 100) \
              if (state["wins"] + state["losses"]) > 0 else 0
    ml_active = any(
        len([m for m in state.get("memory", []) if m["sym"] == sym]) >= ML_MIN_TRADES
        for sym in STOCK_SYMBOLS
    )
    min_bars = min((len(v) for v in state.get("bar_history", {}).values()), default=0)
    return jsonify({
        "running": state["running"], "cycle": state["cycle"],
        "cash": round(state["cash"], 2), "total": round(total, 2),
        "pnl": pnl, "pnl_pct": round(pnl / state["start_cap"] * 100, 2),
        "wins": state["wins"], "losses": state["losses"], "win_rate": wr,
        "positions": {k: v for k, v in state["positions"].items() if v.get("qty", 0) > 0},
        "decisions": state["decisions"][:20], "log": state["log"][:100],
        "scores":    state["scores"], "prices": state["prices"],
        "patterns":  state.get("patterns", []),
        "config":    state["config"],
        "history_count": len(state["history"]),
        "mode":      state.get("mode", "alpha"),
        "alpaca_connected": trading_client is not None,
        "market_open": is_market_open() if state.get("mode") == "beta" else None,
        "sentiment_ready": len(sentiment_cache) > 0,
        "bars_collected": min_bars,
        "bars_needed":    MIN_BARS_CALC,
        "bars_ready":     min_bars >= MIN_BARS_CALC,
        # Convergence
        "convergence_threshold": CONVERGENCE_BUY_THRESHOLD,
        "factor_scores": state.get("factor_scores", {}),
        # Drawdown
        "current_drawdown":   state.get("current_drawdown", 0),
        "peak_capital":       state.get("peak_capital", state["start_cap"]),
        "drawdown_stopped":   state.get("drawdown_stopped", False),
        "max_drawdown_limit": MAX_DRAWDOWN_PCT * 100,
        # ML
        "ml_active":    ml_active,
        "ml_min_trades": ML_MIN_TRADES,
        "memory_size":  len(state.get("memory", [])),
        "memory_max":   500,
        # Backtest
        "backtest_done":    state.get("backtest_done", False),
        "backtest_summary": state.get("backtest_summary"),
    })

@app.route("/factors")
def get_factors():
    result = {}
    for sym in STOCK_SYMBOLS:
        bars = state.get("bar_history", {}).get(sym, [])
        n    = len(bars)
        if n < MIN_BARS_CALC:
            result[sym] = {"ready": False, "bars": n, "bars_needed": MIN_BARS_CALC}
            continue
        score, breakdown = calc_convergence(state, sym)
        result[sym] = {
            "ready": True, "score": score,
            "threshold": CONVERGENCE_BUY_THRESHOLD,
            "signal": "BUY" if score >= CONVERGENCE_BUY_THRESHOLD else
                      "EXIT" if score <= CONVERGENCE_EXIT_THRESHOLD else "HOLD",
            "factors": {k: {"signal": v["signal"], "detail": v["detail"]}
                        for k, v in breakdown.items()},
        }
    ranked = sorted(
        [(sym, result[sym].get("score") or -99) for sym in STOCK_SYMBOLS],
        key=lambda x: -x[1]
    )
    return jsonify({"factors": result, "ranked": ranked, "threshold": CONVERGENCE_BUY_THRESHOLD})

@app.route("/drawdown")
def get_drawdown():
    total = state["cash"] + sum(p["qty"] * gp(state, sym)
                                 for sym, p in state["positions"].items() if p.get("qty", 0) > 0)
    return jsonify({
        "current_drawdown":   state.get("current_drawdown", 0),
        "peak_capital":       state.get("peak_capital", state["start_cap"]),
        "current_capital":    round(total, 2),
        "max_drawdown_limit": MAX_DRAWDOWN_PCT * 100,
        "drawdown_stopped":   state.get("drawdown_stopped", False),
    })

@app.route("/ml/status")
def ml_status():
    result = {}
    for sym in STOCK_SYMBOLS:
        trades  = [m for m in state.get("memory", []) if m["sym"] == sym]
        wins    = [m for m in trades if m["won"]]
        weights = state.get("ml_weights", {}).get(sym, [0.0] * N_FEATURES)
        result[sym] = {
            "trades":     len(trades),
            "active":     len(trades) >= ML_MIN_TRADES,
            "prob":       round(ml_predict(state, sym), 3),
            "multiplier": ml_confidence_multiplier(state, sym),
            "weights":    [round(w, 4) for w in weights],
            "win_rate":   round(len(wins) / len(trades) * 100) if trades else 0,
        }
    return jsonify({"ml": result, "min_trades_required": ML_MIN_TRADES, "learning_rate": ML_LR})

@app.route("/sentiment")
def get_sentiment():
    with sentiment_lock:
        data = dict(sentiment_cache)
    return jsonify({"sentiment": data, "symbols": len(data)})

@app.route("/sentiment/refresh", methods=["POST"])
@require_auth
def refresh_sentiment():
    t = threading.Thread(target=update_sentiment_cache, daemon=True)
    t.start()
    return jsonify({"ok": True})

@app.route("/start", methods=["POST"])
@require_auth
def start():
    state["running"] = True
    state["drawdown_stopped"] = False
    save_state(state)
    log(state, f"Agente iniciado · Modo: {state.get('mode','alpha').upper()}", "think")
    return jsonify({"ok": True, "running": True})

@app.route("/stop", methods=["POST"])
@require_auth
def stop():
    state["running"] = False
    save_state(state)
    log(state, "Agente detenido", "warn")
    return jsonify({"ok": True, "running": False})

@app.route("/config", methods=["POST"])
@require_auth
def set_config():
    data = request.get_json() or {}
    errors = []
    VALID_RISK = {"conservative", "balanced", "aggressive"}
    ranges = {"freq": (60, 3600), "sl": (0.5, 20), "tp": (0.5, 50), "sz": (1, 50)}
    for k, (lo, hi) in ranges.items():
        if k in data:
            try:
                v = float(data[k])
            except (TypeError, ValueError):
                errors.append(f"{k} debe ser un número"); continue
            if not lo <= v <= hi:
                errors.append(f"{k} debe estar entre {lo} y {hi}"); continue
            state["config"][k] = v
    if "risk" in data:
        if data["risk"] not in VALID_RISK:
            errors.append(f"risk debe ser uno de: {', '.join(VALID_RISK)}")
        else:
            state["config"]["risk"] = data["risk"]
    if "convergence_threshold" in data:
        try:
            v = int(data["convergence_threshold"])
        except (TypeError, ValueError):
            errors.append("convergence_threshold debe ser un entero")
        else:
            if not 1 <= v <= 7:
                errors.append("convergence_threshold debe estar entre 1 y 7")
            else:
                global CONVERGENCE_BUY_THRESHOLD
                CONVERGENCE_BUY_THRESHOLD = v
                state["config"]["convergence_threshold"] = v
    if errors:
        return jsonify({"ok": False, "errors": errors}), 400
    save_state(state)
    return jsonify({"ok": True, "config": state["config"]})

@app.route("/mode", methods=["POST"])
@require_auth
def set_mode():
    data = request.get_json() or {}
    if data.get("mode") in ("alpha", "beta"):
        state["mode"] = data["mode"]
        save_state(state)
    return jsonify({"ok": True, "mode": state.get("mode")})

@app.route("/reset", methods=["POST"])
@require_auth
def reset():
    global state
    state = make_default_state()
    save_state(state)
    return jsonify({"ok": True})

@app.route("/save_report", methods=["POST"])
@require_auth
def save_report():
    data = request.get_json() or {}
    conn = get_db()
    if not conn:
        return jsonify({"ok": False, "error": "no db"}), 500
    try:
        with conn.cursor() as cur:
            resumen = json.dumps({
                "capital":  data.get("capital", 0),
                "pnl_pct":  data.get("pnl_pct", 0),
                "ops":      data.get("ops", 0),
                "win_rate": data.get("win_rate", 0),
                "ciclos":   data.get("ciclos", 0),
            })
            cur.execute("INSERT INTO agent3b_reports (texto, resumen) VALUES (%s, %s::jsonb) RETURNING id",
                        (data.get("texto", ""), resumen))
            report_id = cur.fetchone()["id"]
            cur.execute("SELECT COUNT(*) as total FROM agent3b_reports")
            total = cur.fetchone()["total"]
            conn.commit()
            return jsonify({"ok": True, "id": report_id, "total": total})
    finally:
        conn.close()

@app.route("/reports")
def get_reports():
    conn = get_db()
    if not conn:
        return jsonify({"reports": []})
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, resumen, created_at FROM agent3b_reports ORDER BY id DESC LIMIT 30")
            rows = cur.fetchall()
            return jsonify({"reports": [
                {"id": r["id"],
                 "resumen": r["resumen"],
                 "fecha": r["created_at"].strftime("%d/%m/%Y %H:%M") if r["created_at"] else ""}
                for r in rows
            ]})
    finally:
        conn.close()

# -------------------------------------------------------
# STARTUP
# -------------------------------------------------------
init_alpaca()
init_db()
init_state()

threading.Thread(target=agent_loop,     daemon=True).start()
threading.Thread(target=sentiment_loop, daemon=True).start()

if __name__ == "__main__":
    app.run(debug=False, port=5002)

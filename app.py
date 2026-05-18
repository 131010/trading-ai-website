import os
import json
import time
import logging
import calendar
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, jsonify, render_template
import yfinance as yf
import numpy as np
import pytz

# ── Configuration & Setup ──────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
IST = pytz.timezone("Asia/Kolkata")

WATCHLIST = [
    {"symbol": "ICICIBANK.NS",  "name": "ICICI Bank",      "sector": "Banking",      "beta": 1.2},
    {"symbol": "SUNPHARMA.NS",  "name": "Sun Pharma",       "sector": "Pharma",       "beta": 0.8},
    {"symbol": "DRREDDY.NS",    "name": "Dr. Reddy's",      "sector": "Pharma",       "beta": 0.7},
    {"symbol": "MARUTI.NS",     "name": "Maruti Suzuki",    "sector": "Auto",         "beta": 0.9},
    {"symbol": "COALINDIA.NS",  "name": "Coal India",       "sector": "Energy",       "beta": 0.6},
    {"symbol": "HINDUNILVR.NS", "name": "HUL",              "sector": "FMCG",         "beta": 0.5},
    {"symbol": "ADANIPORTS.NS", "name": "Adani Ports",      "sector": "Infra",        "beta": 1.3},
    {"symbol": "NTPC.NS",       "name": "NTPC",             "sector": "Power",        "beta": 0.7},
    {"symbol": "TCS.NS",        "name": "TCS",              "sector": "IT",           "beta": 0.9},
    {"symbol": "TITAN.NS",      "name": "Titan",            "sector": "Consumer",     "beta": 1.1},
    {"symbol": "RELIANCE.NS",   "name": "Reliance",         "sector": "Conglomerate", "beta": 0.95},
    {"symbol": "HDFCBANK.NS",   "name": "HDFC Bank",        "sector": "Banking",      "beta": 1.0},
    {"symbol": "BAJFINANCE.NS", "name": "Bajaj Finance",    "sector": "NBFC",         "beta": 1.4},
    {"symbol": "INFY.NS",       "name": "Infosys",          "sector": "IT",           "beta": 0.85},
    {"symbol": "AXISBANK.NS",   "name": "Axis Bank",        "sector": "Banking",      "beta": 1.15},
    {"symbol": "WIPRO.NS",      "name": "Wipro",            "sector": "IT",           "beta": 0.8},
    {"symbol": "TECHM.NS",      "name": "Tech Mahindra",    "sector": "IT",           "beta": 1.1},
    {"symbol": "TATAMOTORS.NS", "name": "Tata Motors",      "sector": "Auto",         "beta": 1.5},
    {"symbol": "POWERGRID.NS",  "name": "Power Grid",       "sector": "Power",        "beta": 0.6},
    {"symbol": "CIPLA.NS",      "name": "Cipla",            "sector": "Pharma",       "beta": 0.75},
]

SECTOR_PE = {
    "Banking": 18, "Pharma": 28, "Auto": 20, "Energy": 12,
    "FMCG": 50, "Infra": 22, "Power": 18, "IT": 25,
    "Consumer": 55, "NBFC": 22, "Conglomerate": 20
}

SECTOR_OUTLOOK = {
    "Banking":      ("Positive", "RBI rate-cut cycle benefits NIMs; FII re-entry expected on rupee stabilisation."),
    "Pharma":       ("Strong",   "Weak rupee boosts USD export earnings; sector outperforming in volatile markets."),
    "Auto":         ("Positive", "GST 2.0 auto cuts 5-10%; above-normal monsoon strengthens rural demand."),
    "Energy":       ("Stable",   "High crude boosts domestic coal/gas substitution; power demand rising."),
    "FMCG":         ("Positive", "Above-normal monsoon drives rural volumes; Budget income-tax relief lifts urban spend."),
    "Infra":        ("Positive", "Govt capex thrust; PM Modi diplomatic visits may catalyse trade-corridor deals."),
    "Power":        ("Strong",   "India power demand surging — summer heat, data-centre expansion, EV growth."),
    "IT":           ("Cautious", "Sector beaten down — contrarian opportunity; weak rupee boosts USD revenue."),
    "Consumer":     ("Positive", "Income-tax relief drives discretionary spending; wedding/festive season ahead."),
    "NBFC":         ("Positive", "Credit growth robust; rate-cut cycle boosts cost of funds outlook."),
    "Conglomerate": ("Stable",   "Diversified across energy, retail, telecom; Jio and retail segments strong."),
}

def is_market_open():
    now = datetime.now(IST)
    if now.weekday() >= 5: return False
    mo = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    mc = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return mo <= now <= mc

# ── Original Technical Indicators ──────────────────────────────────────────

def ema(data, span):
    k = 2 / (span + 1)
    r = [data[0]]
    for p in data[1:]: r.append(p * k + r[-1] * (1 - k))
    return np.array(r)

def compute_rsi(prices, period=14):
    if len(prices) < period + 1: return None
    d = np.diff(prices)
    g, l = np.where(d > 0, d, 0.0), np.where(d < 0, -d, 0.0)
    ag, al = np.mean(g[:period]), np.mean(l[:period])
    for i in range(period, len(g)):
        ag = (ag * (period - 1) + g[i]) / period
        al = (al * (period - 1) + l[i]) / period
    return round(100 - 100 / (1 + ag / al) if al else 100, 2)

def compute_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal: return None, None, None
    ef, es = ema(prices, fast), ema(prices, slow)
    ml = ef[-len(es):] - es
    sl = ema(ml, signal)
    hist = ml[-len(sl):] - sl
    return round(float(ml[-1]), 4), round(float(sl[-1]), 4), round(float(hist[-1]), 4)

def compute_bollinger(prices, period=20, num_std=2):
    if len(prices) < period: return None, None, None
    w = np.array(prices[-period:])
    m, s = np.mean(w), np.std(w)
    return round(m - num_std * s, 2), round(m, 2), round(m + num_std * s, 2)

def compute_atr(highs, lows, closes, period=14):
    if len(closes) < period + 1: return None
    trs = [max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1])) for i in range(1, len(closes))]
    return round(np.mean(trs[-period:]), 2)

# ── Original Scoring & Recommendation Engines ─────────────────────────────

def score_stock(d):
    score, reasons = 0, []
    rsi = d.get("rsi")
    if rsi is not None:
        if rsi < 30:   score += 3; reasons.append(f"RSI {rsi} — deeply oversold")
        elif rsi < 45: score += 2; reasons.append(f"RSI {rsi} — bullish setup")
        elif rsi > 75: score -= 2; reasons.append(f"RSI {rsi} — overbought")

    hist = d.get("macd_hist")
    if hist is not None:
        if hist > 0: score += 2; reasons.append("MACD bullish")
        else: score -= 1

    c = d["current_price"]
    if c > d["ma50"] > d["ma200"]: score += 3; reasons.append("Confirmed uptrend")
    
    if d.get("iv_status") == "Undervalued": score += 3; reasons.append("Undervalued")
    return score, reasons

def build_recommendation(d, rank):
    score, _ = score_stock(d)
    c = d["current_price"]
    signal = "BUY" if score >= 4 else "SELL" if score <= -2 else "HOLD"
    confidence = min(95, 55 + abs(score) * 5)
    atr = d.get("atr") or (c * 0.015)
    
    return {
        "rank": rank, "name": d["name"], "symbol": d["symbol"].replace(".NS", ""),
        "sector": d["sector"], "signal": signal, "current_price": c,
        "target_price": round(c + atr * 6, 2) if signal == "BUY" else round(c - atr * 5, 2),
        "stop_loss": round(c - atr * 3, 2), "intrinsic_value": d.get("intrinsic_value"),
        "iv_status": d.get("iv_status", "N/A"), "margin_of_safety": d.get("margin_of_safety"),
        "confidence": confidence, "risk_level": "High" if d.get("beta", 1) > 1.3 else "Medium",
        "holding_period": "6–8 weeks", "upside_pct": 15.0,
        "technical_summary": f"RSI: {d['rsi']}. Price vs MA50: {d['trend']}.",
        "fundamental_summary": f"Sector PE: {SECTOR_PE.get(d['sector'])}x.",
        "situational_summary": SECTOR_OUTLOOK.get(d['sector'])[1],
        "key_risks": "Market volatility.", "score": score
    }

def build_option(stock_data, rank):
    c = stock_data["current_price"]
    strike = round(c / 50) * 50 + 50
    return {
        "rank": rank, "underlying": stock_data["name"], "symbol": stock_data["symbol"].replace(".NS", ""),
        "option_type": "CE", "strike_price": strike, "expiry": "30 Jul 2026",
        "current_stock_price": c, "strategy": "Long Call", "risk_level": "High",
        "target_move_pct": 12.0, "max_loss": "Premium paid", "holding_period": "4–6 weeks",
        "reasoning": f"Bullish momentum in {stock_data['name']}.", "key_risks": "Theta decay."
    }

# ── Data Fetcher ───────────────────────────────────────────────────────────

def fetch_stock(item):
    try:
        tk = yf.Ticker(item["symbol"])
        hist = tk.history(period="12mo", interval="1d")
        if hist.empty: return None
        closes = hist["Close"].tolist()
        highs, lows = hist["High"].tolist(), hist["Low"].tolist()
        c = round(closes[-1], 2)
        
        # Fundamental Data
        iv_data = {"intrinsic_value": None, "margin_of_safety": None, "iv_status": "N/A"}
        try:
            info = tk.info
            eps = info.get("trailingEps") or (c / 20)
            iv = round(eps * SECTOR_PE.get(item["sector"], 20), 2)
            mos = round((iv - c) / c * 100, 1)
            iv_data = {"intrinsic_value": iv, "margin_of_safety": mos, "iv_status": "Undervalued" if mos > 10 else "Fair"}
        except: pass

        return {
            **item, "current_price": c, "rsi": compute_rsi(closes),
            "macd_hist": compute_macd(closes)[2], "ma50": np.mean(closes[-50:]),
            "ma200": np.mean(closes[-200:]), "atr": compute_atr(highs, lows, closes),
            "trend": "Above MA50" if c > np.mean(closes[-50:]) else "Below MA50", **iv_data
        }
    except: return None

# ── Routes ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("Daily_Recomendation.html")

@app.route("/api/market-status")
def market_status():
    return jsonify({"is_open": is_market_open(), "current_time": datetime.now(IST).strftime("%I:%M %p IST")})

@app.route("/api/recommendations")
def recommendations():
    with ThreadPoolExecutor(max_workers=5) as ex:
        raw_stocks = [s for s in list(ex.map(fetch_stock, WATCHLIST)) if s]
    
    scored = sorted(raw_stocks, key=lambda x: score_stock(x)[0], reverse=True)
    stock_recs = [build_recommendation(s, i+1) for i, s in enumerate(scored[:5])]
    opt_recs = [build_option(s, i+1) for i, s in enumerate(scored[:2])]

    return jsonify({
        "generated_at": datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST"),
        "stocks_analyzed": len(raw_stocks),
        "market_summary": f"Strong momentum in {scored[0]['sector']} sector.",
        "stocks": stock_recs,
        "options": opt_recs
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)

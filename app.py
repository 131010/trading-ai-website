import os
import json
import time
import logging
import calendar
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor  # Performance optimization for handling 20 stock iterations
from flask import Flask, jsonify, render_template
import yfinance as yf
import numpy as np
import pytz

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
    if now.weekday() >= 5:
        return False
    mo = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    mc = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return mo <= now <= mc

# ── Technical indicators ────────────────────────────────────────────────────

def ema(data, span):
    k, r = 2 / (span + 1), [data[0]]
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
    trs = [max(highs[i] - lows[i],
               abs(highs[i] - closes[i-1]),
               abs(lows[i]  - closes[i-1]))
           for i in range(1, len(closes))]
    return round(np.mean(trs[-period:]), 2)

def compute_stoch_rsi(prices, period=14):
    rsis = []
    for i in range(period, len(prices)):
        r = compute_rsi(prices[i - period:i + 1])
        if r is not None: rsis.append(r)
    if len(rsis) < 3: return None
    lo, hi = min(rsis[-period:]), max(rsis[-period:])
    return round((rsis[-1] - lo) / (hi - lo) * 100, 1) if hi != lo else 50

# ── Scoring engine ─────────────────────────────────────────────────────────

def score_stock(d):
    score, reasons = 0, []

    # RSI
    rsi = d.get("rsi")
    if rsi is not None:
        if rsi < 30:   score += 3; reasons.append(f"RSI {rsi} — deeply oversold, strong bounce potential")
        elif rsi < 45: score += 2; reasons.append(f"RSI {rsi} — oversold, bullish setup forming")
        elif rsi < 60: score += 1; reasons.append(f"RSI {rsi} — neutral with upward bias")
        elif rsi > 75: score -= 2; reasons.append(f"RSI {rsi} — overbought, caution warranted")
        else:          score -= 1; reasons.append(f"RSI {rsi} — slightly elevated")

    # MACD
    hist = d.get("macd_hist")
    macd_v, sig_v = d.get("macd"), d.get("macd_signal")
    if hist is not None:
        if hist > 0 and macd_v > sig_v:   score += 2; reasons.append("MACD bullish crossover — momentum turning positive")
        elif hist > 0:                     score += 1; reasons.append("MACD histogram positive — mild bullish momentum")
        elif hist < 0 and macd_v < sig_v:  score -= 2; reasons.append("MACD bearish — downward momentum dominant")
        else:                              score -= 1; reasons.append("MACD histogram negative — caution")

    # Moving averages
    c = d["current_price"]; ma20, ma50, ma200 = d["ma20"], d["ma50"], d["ma200"]
    if c > ma50 > ma200:    score += 3; reasons.append("Price above MA50 & MA200 — confirmed uptrend")
    elif c > ma50:          score += 2; reasons.append("Price above MA50 — medium-term bullish")
    elif c > ma200:         score += 1; reasons.append("Price above MA200 — long-term support holding")
    elif c < ma50 < ma200:  score -= 2; reasons.append("Price below MA50 & MA200 — downtrend in play")
    if ma20 > ma50:         score += 1; reasons.append("MA20 above MA50 — short-term acceleration")

    # Volume
    vr = d.get("volume_ratio", 1)
    if vr > 2.0:    score += 2; reasons.append(f"Volume {vr}x avg — strong institutional interest")
    elif vr > 1.3:  score += 1; reasons.append(f"Volume {vr}x avg — above-average participation")
    elif vr < 0.5:  score -= 1; reasons.append("Low volume — weak conviction")

    # Bollinger Bands
    bb_l, bb_m, bb_h = d.get("bb_low"), d.get("bb_mid"), d.get("bb_high")
    if bb_l and bb_h:
        if c < bb_l:         score += 2; reasons.append("Price below lower Bollinger Band — mean-reversion buy signal")
        elif c > bb_h:       score -= 1; reasons.append("Price above upper Bollinger Band — stretched, wait for pullback")
        elif c < bb_m:       score += 1; reasons.append("Price below Bollinger midline — room to expand upward")

    # 52-week position
    pct = d.get("pct_from_52w_high", 0)
    if pct < -30:   score += 2; reasons.append(f"Stock is {abs(pct)}% below 52-week high — deep value zone")
    elif pct < -15: score += 1; reasons.append(f"Stock is {abs(pct)}% below 52-week high — discounted entry")
    elif pct > -5:  score -= 1; reasons.append("Near 52-week high — limited upside headroom short term")

    # Intrinsic value
    iv_status = d.get("iv_status")
    mos = d.get("margin_of_safety")
    if iv_status == "Undervalued":
        score += 3 if (mos or 0) > 20 else 2
        reasons.append(f"Intrinsic value ₹{d.get('intrinsic_value')} — stock undervalued by {mos}%")
    elif iv_status == "Overvalued":
        score -= 2
        reasons.append(f"Trading above intrinsic value by {abs(mos or 0)}% — valuation risk")

    return score, reasons

# ── Recommendation builder ─────────────────────────────────────────────────

def build_recommendation(d, rank):
    score, tech_reasons = score_stock(d)
    c = d["current_price"]

    # Signal
    if score >= 6:   signal, confidence = "BUY",  min(95, 65 + score * 3)
    elif score >= 3: signal, confidence = "BUY",  min(75, 55 + score * 3)
    elif score <= -4: signal, confidence = "SELL", min(80, 55 + abs(score) * 3)
    else:            signal, confidence = "HOLD", 50

    # Target / SL using ATR-based logic
    atr = d.get("atr") or (c * 0.015)
    if signal == "BUY":
        target = round(c + atr * 6, 2)
        sl     = round(c - atr * 3, 2)
        upside = round((target - c) / c * 100, 1)
    elif signal == "SELL":
        target = round(c - atr * 5, 2)
        sl     = round(c + atr * 3, 2)
        upside = round((c - target) / c * 100, 1)
    else:
        target = round(c * 1.06, 2)
        sl     = round(c * 0.94, 2)
        upside = 6.0

    # Intrinsic value
    iv = d.get("intrinsic_value")
    mos = d.get("margin_of_safety")
    iv_str = f"₹{iv} ({d.get('iv_status')}; {mos:+.1f}% vs CMP)" if iv else "Data unavailable"

    # Technical summary
    rsi = d.get("rsi"); macd_h = d.get("macd_hist")
    tech_sum = (
        f"RSI at {rsi} ({'oversold' if rsi and rsi < 40 else 'overbought' if rsi and rsi > 70 else 'neutral'}). "
        f"MACD histogram {'positive — bullish momentum' if macd_h and macd_h > 0 else 'negative — caution'}. "
        f"Price is {d['trend'].lower()} (MA20 ₹{d['ma20']}, MA50 ₹{d['ma50']}). "
        f"Volume at {d.get('volume_ratio',1)}x 20-day average. "
        f"Bollinger midline ₹{d.get('bb_mid')}."
    )

    # Fundamental
    sector = d["sector"]
    fund_sum = (
        f"Sector P/E benchmark: {SECTOR_PE.get(sector, 20)}x. "
        f"Intrinsic value estimate: {iv_str}. "
        f"52-week range: ₹{d['week52_low']}–₹{d['week52_high']}; "
        f"currently {abs(d.get('pct_from_52w_high', 0))}% below year-high."
    )

    # Situational
    outlook_tag, outlook_text = SECTOR_OUTLOOK.get(sector, ("Neutral", "Sector-neutral macro environment."))
    sit_sum = (
        f"Sector outlook: {outlook_tag}. {outlook_text} "
        f"India macro backdrop: Above-normal monsoon (IMD), RBI rate-cut June meeting, "
        f"GST 2.0 reforms, rupee at ₹95+ (boosts export-linked sectors)."
    )

    # Risk
    risk_notes = []
    if d.get("beta", 1) > 1.2: risk_notes.append("high beta stock — amplifies market swings")
    if d.get("rsi", 50) > 70:  risk_notes.append("overbought RSI — pullback risk")
    if d.get("pct_from_52w_high", -10) > -5: risk_notes.append("near 52-week high — limited upside headroom")
    risk_notes.append("crude oil / rupee volatility remains a market-wide risk")
    key_risks = "; ".join(risk_notes[:2]).capitalize() + "."

    risk_level = "High" if d.get("beta", 1) > 1.3 else "Medium"
    holding = "6–8 weeks" if signal in ("BUY", "SELL") else "Monitor 2–4 weeks"

    return {
        "rank": rank,
        "name": d["name"],
        "symbol": d["symbol"].replace(".NS", ""),
        "sector": d["sector"],
        "signal": signal,
        "current_price": c,
        "target_price": target,
        "stop_loss": sl,
        "intrinsic_value": iv,
        "iv_status": d.get("iv_status", "N/A"),
        "margin_of_safety": mos,
        "confidence": confidence,
        "risk_level": risk_level,
        "holding_period": holding,
        "upside_pct": upside,
        "technical_summary": tech_sum,
        "fundamental_summary": fund_sum,
        "situational_summary": sit_sum,
        "key_risks": key_risks,
        "score": score,
    }

def build_option(stock_data, rank):
    c   = stock_data["current_price"]
    rsi = stock_data.get("rsi", 50)
    mh  = stock_data.get("macd_hist", 0)
    tr  = stock_data["trend"]
    sc, _ = score_stock(stock_data)

    # Decide CE or PE
    bullish = sc >= 3 or (rsi and rsi < 45) or (tr == "Uptrend")
    opt_type = "CE" if bullish else "PE"

    # Strike: nearest 50 above CMP for CE, below for PE
    if opt_type == "CE":
        strike = round(c / 50) * 50
        if strike <= c: strike += 50
    else:
        strike = round(c / 50) * 50
        if strike >= c: strike -= 50

    # Expiry — pick next month or month after for 2-3 month horizon
    now = datetime.now(IST)
    month_ahead = now + timedelta(days=45)
    # NSE monthly expiry = last Thursday of month
    y, m = month_ahead.year, month_ahead.month
    # find last Thursday
    last_day = calendar.monthrange(y, m)[1]
    last_thu = max(d for d in range(1, last_day + 1)
                   if datetime(y, m, d).weekday() == 3)
    expiry_dt = datetime(y, m, last_thu)
    expiry_str = expiry_dt.strftime("%d %b %Y")

    atr = stock_data.get("atr") or (c * 0.015)
    expected_move = round(atr * 5 / c * 100, 1)

    if opt_type == "CE":
        reasoning = (
            f"{stock_data['name']} shows a bullish setup: "
            f"RSI at {rsi} ({'oversold recovery' if rsi < 45 else 'neutral-positive'}), "
            f"MACD histogram {'turning positive' if (mh or 0) > 0 else 'improving'}. "
            f"Trend is {tr.lower()}. "
            f"Buying the {strike} CE gives leveraged upside with defined risk. "
            f"Expected move of ~{expected_move}% in underlying over 4–6 weeks. "
            f"{SECTOR_OUTLOOK.get(stock_data['sector'], ('', ''))[1]} "
            f"Use Jun/Jul expiry to avoid near-term theta burn — VIX elevated at ~19."
        )
    else:
        reasoning = (
            f"{stock_data['name']} shows a bearish setup: "
            f"RSI at {rsi} ({'overbought' if rsi > 65 else 'weakening'}), "
            f"MACD histogram negative. Trend is {tr.lower()}. "
            f"Buying the {strike} PE gives downside participation with capped risk. "
            f"Expected move of ~{expected_move}% in underlying over 4–6 weeks. "
            f"Sector outlook: {SECTOR_OUTLOOK.get(stock_data['sector'], ('', 'Sector headwinds.'))[1]} "
            f"Use Jun/Jul expiry for adequate time value."
        )

    return {
        "rank": rank,
        "underlying": stock_data["name"],
        "symbol": stock_data["symbol"].replace(".NS", ""),
        "option_type": opt_type,
        "strike_price": strike,
        "expiry": expiry_str,
        "current_stock_price": c,
        "strategy": "Long Call" if opt_type == "CE" else "Long Put",
        "risk_level": "High",
        "target_move_pct": expected_move,
        "max_loss": "Premium paid",
        "holding_period": "4–6 weeks",
        "reasoning": reasoning,
        "key_risks": "Time decay (theta) erodes value daily; IV crush if volatility drops; always verify live option chain before trading.",
    }

# ── Data fetcher ───────────────────────────────────────────────────────────

def fetch_stock(item):
    try:
        tk   = yf.Ticker(item["symbol"])
        hist = tk.history(period="12mo", interval="1d")
        if hist.empty or len(hist) < 30:
            return None

        closes  = hist["Close"].tolist()
        highs   = hist["High"].tolist()
        lows    = hist["Low"].tolist()
        volumes = hist["Volume"].tolist()
        c = round(closes[-1], 2)

        ma20  = round(np.mean(closes[-20:]), 2)
        ma50  = round(np.mean(closes[-50:]) if len(closes) >= 50 else np.mean(closes), 2)
        ma200 = round(np.mean(closes[-200:]) if len(closes) >= 200 else np.mean(closes), 2)

        rsi = compute_rsi(closes)
        macd, macd_sig, macd_hist = compute_macd(closes)
        bb_l, bb_m, bb_h = compute_bollinger(closes)
        atr  = compute_atr(highs, lows, closes)

        avg_vol = np.mean(volumes[-20:]) if len(volumes) >= 20 else np.mean(volumes)
        vol_ratio = round(volumes[-1] / avg_vol, 2) if avg_vol else 1

        w52h = max(closes[-252:]) if len(closes) >= 252 else max(closes)
        w52l = min(closes[-252:]) if len(closes) >= 252 else min(closes)

        trend = ("Uptrend"   if c > ma50 > ma200 else
                 "Downtrend" if c < ma50 < ma200 else "Sideways")

        # Intrinsic value
        iv_data = {"intrinsic_value": None, "margin_of_safety": None, "iv_status": "N/A"}
        try:
            info = tk.info
            eps  = info.get("trailingEps") or info.get("forwardEps")
            if eps and eps > 0:
                fair_pe = SECTOR_PE.get(item["sector"], 22)
                iv_est  = round(eps * fair_pe, 2)
                mos     = round((iv_est - c) / c * 100, 1)
                status  = ("Undervalued" if mos > 10 else
                           "Overvalued"  if mos < -10 else "Fair Value")
                iv_data = {"intrinsic_value": iv_est, "margin_of_safety": mos, "iv_status": status}
        except Exception:
            pass

        return {
            **item,
            "current_price": c,
            "ma20": ma20, "ma50": ma50, "ma200": ma200,
            "rsi": rsi,
            "macd": macd, "macd_signal": macd_sig, "macd_hist": macd_hist,
            "bb_low": bb_l, "bb_mid": bb_m, "bb_high": bb_h,
            "atr": atr,
            "volume": int(volumes[-1]),
            "avg_volume_20d": int(avg_vol),
            "volume_ratio": vol_ratio,
            "week52_high": round(w52h, 2),
            "week52_low":  round(w52l, 2),
            "pct_from_52w_high": round((c - w52h) / w52h * 100, 1),
            "trend": trend,
            **iv_data,
        }
    except Exception as e:
        logger.error(f"Error fetching {item['symbol']}: {e}")
        return None

# ── Market summary ─────────────────────────────────────────────────────────

def generate_market_summary(stocks):
    buys   = sum(1 for s in stocks if s.get("signal") == "BUY")
    sells  = sum(1 for s in stocks if s.get("signal") == "SELL")
    avg_rsi = round(np.mean([s["rsi"] for s in stocks if s.get("rsi")]), 1)
    up_trend = sum(1 for s in stocks if s.get("trend") == "Uptrend")
    tone = ("broadly bullish" if buys > sells * 2 else
            "mixed with selective opportunities" if buys >= sells else
            "cautious — risk-off mode")
    return (
        f"Market is {tone} across the {len(stocks)} stocks analysed. "
        f"{buys} BUY signals, {sells} SELL signals. "
        f"Average RSI: {avg_rsi} ({'oversold' if avg_rsi < 40 else 'overbought' if avg_rsi > 65 else 'neutral'}). "
        f"{up_trend} of {len(stocks)} stocks in confirmed uptrends. "
        f"Key macro watch: RBI June meeting, crude oil above $105, rupee near ₹95.7."
    )

# ── Routes ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    try:
        # Explicitly tracking your precise template filename mapping
        return render_template("Daily_Recomendation.html")
    except Exception:
        # Fallback response in case the file is missing or misplaced in directory
        return "<h1>Market Dashboard Backend Active</h1><p>API Endpoint: <a href='/api/recommendations'>/api/recommendations</a></p>"

@app.route("/api/market-status")
def market_status():
    now = datetime.now(IST)
    open_ = is_market_open()
    nxt = None
    if not open_:
        d = now + timedelta(days=1)
        while d.weekday() >= 5: d += timedelta(days=1)
        nxt = d.replace(hour=9, minute=15).strftime("%d %b %Y, 9:15 AM IST")
    return jsonify({
        "is_open": open_,
        "current_time": now.strftime("%d %b %Y, %I:%M:%S %p IST"),
        "next_open": nxt,
    })

@app.route("/api/recommendations")
def recommendations():
    try:
        stocks_data = []
        
        # Safe thread context boundary mapping to preserve rate limits on concurrent runs
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(fetch_stock, item) for item in WATCHLIST]
            for future in futures:
                d = future.result()
                if d:
                    stocks_data.append(d)
                time.sleep(0.05)

        if len(stocks_data) < 5:
            return jsonify({"error": "Could not fetch enough market data. Yahoo Finance may be rate-limiting — wait 1 minute and retry."}), 500

        # Score and rank all stocks
        scored = sorted(stocks_data, key=lambda x: score_stock(x)[0], reverse=True)

        # Top 5 for stocks, top 2 (different sectors) for options
        top5 = scored[:5]
        stock_recs = [build_recommendation(s, i + 1) for i, s in enumerate(top5)]

        # Options: pick top 2 from scored list, prefer different sectors
        opts_raw, seen_sectors = [], set()
        for s in scored:
            if s["sector"] not in seen_sectors or len(opts_raw) == 0:
                opts_raw.append(s)
                seen_sectors.add(s["sector"])
            if len(opts_raw) == 2: break
        if len(opts_raw) < 2: opts_raw = scored[:2]
        opt_recs = [build_option(opts_raw[i], i + 1) for i in range(len(opts_raw))]

        return jsonify({
            "generated_at": datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST"),
            "stocks_analyzed": len(stocks_data),
            "market_summary": generate_market_summary(stock_recs),
            "stocks": stock_recs,
            "options": opt_recs,
        })

    except Exception as e:
        logger.error(f"Recommendations error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)

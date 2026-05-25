import os
import json
import time
import logging
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify, render_template
import yfinance as yf
import numpy as np
import pytz
import requests

# ── Configuration & Setup ──────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
IST = pytz.timezone("Asia/Kolkata")

# ── NSE Universe: All major indices combined (~500 quality stocks) ──────────
# These are pulled dynamically from NSE's public JSON endpoints at startup
# and refreshed every 24 hours. No hardcoding needed.

NSE_INDEX_URLS = {
    "NIFTY 500":        "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
    "NIFTY MIDCAP 150": "https://archives.nseindia.com/content/indices/ind_niftymidcap150list.csv",
    "NIFTY SMALLCAP 250":"https://archives.nseindia.com/content/indices/ind_niftysmallcap250list.csv",
}

# Sector PE benchmarks
SECTOR_PE = {
    "Banking": 18, "Pharma": 28, "Auto": 20, "Energy": 12,
    "FMCG": 50, "Infra": 22, "Power": 18, "IT": 25,
    "Consumer": 55, "NBFC": 22, "Conglomerate": 20,
    "Metals": 10, "Chemicals": 25, "Realty": 30, "Telecom": 35,
    "Media": 20, "Healthcare": 30, "Finance": 20, "Services": 22,
    "Default": 20
}

SECTOR_OUTLOOK = {
    "Banking":      ("Positive", "RBI rate-cut cycle benefits NIMs; FII re-entry expected on rupee stabilisation."),
    "Pharma":       ("Strong",   "Weak rupee boosts USD export earnings; sector outperforming in volatile markets."),
    "Auto":         ("Positive", "GST 2.0 auto cuts 5-10%; above-normal monsoon strengthens rural demand."),
    "Energy":       ("Stable",   "High crude boosts domestic coal/gas substitution; power demand rising."),
    "FMCG":         ("Positive", "Above-normal monsoon drives rural volumes; Budget income-tax relief lifts urban spend."),
    "Infra":        ("Positive", "Govt capex thrust; trade-corridor deals in focus."),
    "Power":        ("Strong",   "India power demand surging — summer heat, data-centre expansion, EV growth."),
    "IT":           ("Cautious", "Sector beaten down — contrarian opportunity; weak rupee boosts USD revenue."),
    "Consumer":     ("Positive", "Income-tax relief drives discretionary spending; wedding/festive season ahead."),
    "NBFC":         ("Positive", "Credit growth robust; rate-cut cycle boosts cost of funds outlook."),
    "Conglomerate": ("Stable",   "Diversified exposure; domestic consumption theme intact."),
    "Metals":       ("Stable",   "China demand uncertainty; domestic infra orders supportive."),
    "Chemicals":    ("Positive", "China+1 sourcing shift benefits Indian specialty chemicals."),
    "Realty":       ("Positive", "Affordable housing push; urban demand resilient."),
    "Telecom":      ("Stable",   "ARPU improvement trend; 5G rollout ongoing."),
    "Default":      ("Neutral",  "Monitor macro cues and sector rotation signals."),
}

# ── Global Cache ───────────────────────────────────────────────────────────
_universe_cache = {
    "stocks": [],           # list of {symbol, name, sector, beta}
    "last_fetched": None,   # datetime
    "results": [],          # scored stock data (full)
    "recommendations": None,# final API response dict
    "last_screened": None,  # datetime
    "lock": threading.Lock()
}

UNIVERSE_REFRESH_HOURS = 24   # Re-fetch NSE index CSVs every 24h
SCREEN_REFRESH_MINUTES = 30   # Re-run scoring every 30 minutes


# ── NSE Universe Builder ───────────────────────────────────────────────────

def _nse_headers():
    """NSE requires a referer header."""
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

def _sector_from_industry(industry: str) -> str:
    """Map yfinance industry string to our sector buckets."""
    if not industry:
        return "Default"
    industry = industry.lower()
    mapping = {
        "bank": "Banking", "financial": "Banking", "insurance": "NBFC",
        "nbfc": "NBFC", "microfinance": "NBFC",
        "pharmaceutical": "Pharma", "drug": "Pharma", "biotech": "Pharma",
        "auto": "Auto", "vehicle": "Auto", "tyre": "Auto",
        "software": "IT", "technology": "IT", "it ": "IT",
        "oil": "Energy", "gas": "Energy", "coal": "Energy", "petroleum": "Energy",
        "power": "Power", "utility": "Power", "electricity": "Power",
        "fmcg": "FMCG", "consumer good": "FMCG", "packaged food": "FMCG",
        "cement": "Infra", "construction": "Infra", "infrastructure": "Infra",
        "metal": "Metals", "steel": "Metals", "aluminium": "Metals", "copper": "Metals",
        "chemical": "Chemicals", "fertiliser": "Chemicals", "agrochemical": "Chemicals",
        "realty": "Realty", "real estate": "Realty",
        "telecom": "Telecom", "telecommunication": "Telecom",
        "media": "Media", "entertainment": "Media",
        "hospital": "Healthcare", "diagnostic": "Healthcare",
        "textile": "Consumer", "retail": "Consumer", "jewellery": "Consumer",
        "conglomerate": "Conglomerate", "diversified": "Conglomerate",
    }
    for key, sector in mapping.items():
        if key in industry:
            return sector
    return "Default"

def fetch_nse_universe() -> list:
    """
    Download NSE index constituent CSVs and return a deduplicated list of
    {symbol, name, sector, beta} dicts. Falls back to a 50-stock seed list
    if NSE is unreachable (e.g., during Render cold-start in a restricted network).
    """
    seen = set()
    stocks = []

    for index_name, url in NSE_INDEX_URLS.items():
        try:
            resp = requests.get(url, headers=_nse_headers(), timeout=15)
            resp.raise_for_status()
            lines = resp.text.strip().splitlines()

            # NSE CSVs: first row is header, columns vary by index
            # Common format: Company Name, Industry, Symbol, Series, ISIN Code
            header = [h.strip().strip('"').lower() for h in lines[0].split(",")]
            sym_idx  = next((i for i, h in enumerate(header) if "symbol" in h), None)
            name_idx = next((i for i, h in enumerate(header) if "company" in h or "name" in h), None)
            ind_idx  = next((i for i, h in enumerate(header) if "industry" in h or "sector" in h), None)

            if sym_idx is None:
                logger.warning(f"Could not parse header for {index_name}: {header}")
                continue

            for line in lines[1:]:
                cols = [c.strip().strip('"') for c in line.split(",")]
                if len(cols) <= sym_idx:
                    continue
                raw_sym = cols[sym_idx].strip()
                if not raw_sym or raw_sym in seen:
                    continue
                seen.add(raw_sym)
                name     = cols[name_idx].strip() if name_idx and name_idx < len(cols) else raw_sym
                industry = cols[ind_idx].strip()  if ind_idx  and ind_idx  < len(cols) else ""
                sector   = _sector_from_industry(industry)
                stocks.append({
                    "symbol": f"{raw_sym}.NS",
                    "name":   name,
                    "sector": sector,
                    "beta":   1.0,      # will be overwritten from yfinance info
                })

            logger.info(f"Loaded {index_name}: {len(stocks)} total unique stocks so far")

        except Exception as e:
            logger.warning(f"Failed to fetch {index_name}: {e}")

    if not stocks:
        logger.warning("NSE CSV fetch failed entirely — using seed fallback list")
        stocks = _seed_fallback()

    return stocks


def _seed_fallback() -> list:
    """50-stock seed used only when NSE endpoints are unreachable."""
    seed_symbols = [
        "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","KOTAKBANK",
        "SBIN","BAJFINANCE","BHARTIARTL","ASIANPAINT","ITC","AXISBANK","MARUTI",
        "SUNPHARMA","TITAN","WIPRO","ULTRACEMCO","NTPC","POWERGRID","COALINDIA",
        "TATAMOTORS","TECHM","DRREDDY","ADANIPORTS","CIPLA","ONGC","BAJAJFINSV",
        "DIVISLAB","HCLTECH","BPCL","GRASIM","JSWSTEEL","NESTLEIND","TATASTEEL",
        "TATACONSUM","EICHERMOT","INDUSINDBK","M&M","HDFCLIFE","SBILIFE",
        "BRITANNIA","HEROMOTOCO","PIIND","TORNTPHARM","DABUR","MCDOWELL-N",
        "SIEMENS","ABB","PIDILITIND",
    ]
    return [{"symbol": f"{s}.NS", "name": s, "sector": "Default", "beta": 1.0}
            for s in seed_symbols]


def ensure_universe_loaded():
    """Load (or refresh) the stock universe into cache."""
    with _universe_cache["lock"]:
        last = _universe_cache["last_fetched"]
        if last and (datetime.now() - last) < timedelta(hours=UNIVERSE_REFRESH_HOURS):
            return
        logger.info("Fetching NSE stock universe…")
        stocks = fetch_nse_universe()
        _universe_cache["stocks"] = stocks
        _universe_cache["last_fetched"] = datetime.now()
        logger.info(f"Universe loaded: {len(stocks)} stocks")


# ── Technical Indicators ───────────────────────────────────────────────────

def ema(data, span):
    k = 2 / (span + 1)
    r = [data[0]]
    for p in data[1:]:
        r.append(p * k + r[-1] * (1 - k))
    return np.array(r)

def compute_rsi(prices, period=14):
    if len(prices) < period + 1:
        return None
    d = np.diff(prices)
    g = np.where(d > 0, d, 0.0)
    l = np.where(d < 0, -d, 0.0)
    ag, al = np.mean(g[:period]), np.mean(l[:period])
    for i in range(period, len(g)):
        ag = (ag * (period - 1) + g[i]) / period
        al = (al * (period - 1) + l[i]) / period
    return round(100 - 100 / (1 + ag / al) if al else 100, 2)

def compute_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal:
        return None, None, None
    ef, es = ema(prices, fast), ema(prices, slow)
    ml = ef[-len(es):] - es
    sl = ema(ml, signal)
    hist = ml[-len(sl):] - sl
    return round(float(ml[-1]), 4), round(float(sl[-1]), 4), round(float(hist[-1]), 4)

def compute_bollinger(prices, period=20, num_std=2):
    if len(prices) < period:
        return None, None, None
    w = np.array(prices[-period:])
    m, s = np.mean(w), np.std(w)
    return round(m - num_std * s, 2), round(m, 2), round(m + num_std * s, 2)

def compute_atr(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return None
    trs = [max(highs[i] - lows[i],
               abs(highs[i] - closes[i - 1]),
               abs(lows[i]  - closes[i - 1]))
           for i in range(1, len(closes))]
    return round(np.mean(trs[-period:]), 2)

def compute_adx(highs, lows, closes, period=14):
    """Average Directional Index — trend strength (>25 = trending)."""
    if len(closes) < period * 2 + 1:
        return None
    plus_dm, minus_dm, trs = [], [], []
    for i in range(1, len(closes)):
        h_diff = highs[i] - highs[i - 1]
        l_diff = lows[i - 1] - lows[i]
        plus_dm.append(h_diff if h_diff > l_diff and h_diff > 0 else 0)
        minus_dm.append(l_diff if l_diff > h_diff and l_diff > 0 else 0)
        trs.append(max(highs[i] - lows[i],
                       abs(highs[i] - closes[i - 1]),
                       abs(lows[i]  - closes[i - 1])))
    def smooth(arr, p):
        s = [sum(arr[:p])]
        for v in arr[p:]:
            s.append(s[-1] - s[-1] / p + v)
        return s
    atr_s = smooth(trs, period)
    pdm_s = smooth(plus_dm, period)
    mdm_s = smooth(minus_dm, period)
    dx_vals = []
    for i in range(len(atr_s)):
        pdi = 100 * pdm_s[i] / atr_s[i] if atr_s[i] else 0
        mdi = 100 * mdm_s[i] / atr_s[i] if atr_s[i] else 0
        denom = pdi + mdi
        dx_vals.append(100 * abs(pdi - mdi) / denom if denom else 0)
    return round(np.mean(dx_vals[-period:]), 2)

def compute_volume_spike(volumes):
    """Returns ratio of latest volume vs 20-day avg. >1.5 = spike."""
    if len(volumes) < 21:
        return None
    avg = np.mean(volumes[-21:-1])
    return round(volumes[-1] / avg, 2) if avg else None


# ── Per-Stock Fetcher ──────────────────────────────────────────────────────

def fetch_stock(item):
    try:
        tk = yf.Ticker(item["symbol"])
        hist = tk.history(period="12mo", interval="1d")
        if hist.empty or len(hist) < 50:
            return None

        closes  = hist["Close"].tolist()
        highs   = hist["High"].tolist()
        lows    = hist["Low"].tolist()
        volumes = hist["Volume"].tolist()
        c = round(closes[-1], 2)
        if c <= 0:
            return None

        # --- Fundamental data via yfinance info ---
        iv_data = {"intrinsic_value": None, "margin_of_safety": None, "iv_status": "N/A"}
        beta = item.get("beta", 1.0)
        sector = item.get("sector", "Default")
        name = item.get("name", item["symbol"])
        mkt_cap = None
        try:
            info = tk.fast_info          # faster than tk.info for most fields
            beta = getattr(info, "three_month_average_volume", None) and info.get("beta", beta) or beta
            mkt_cap = getattr(info, "market_cap", None)
            # Richer info for IV
            full_info = tk.info
            beta = full_info.get("beta", beta) or beta
            sector_from_yf = full_info.get("sector", "")
            if sector_from_yf:
                sector = _sector_from_industry(sector_from_yf)
            name = full_info.get("longName", name) or name
            eps = full_info.get("trailingEps") or (c / 20)
            sector_pe = SECTOR_PE.get(sector, SECTOR_PE["Default"])
            iv = round(eps * sector_pe, 2)
            mos = round((iv - c) / c * 100, 1)
            iv_data = {
                "intrinsic_value": iv,
                "margin_of_safety": mos,
                "iv_status": "Undervalued" if mos > 10 else "Overvalued" if mos < -20 else "Fair"
            }
            mkt_cap = full_info.get("marketCap", mkt_cap)
        except Exception:
            eps = c / 20
            sector_pe = SECTOR_PE.get(sector, SECTOR_PE["Default"])
            iv = round(eps * sector_pe, 2)
            mos = round((iv - c) / c * 100, 1)
            iv_data = {"intrinsic_value": iv, "margin_of_safety": mos,
                       "iv_status": "Undervalued" if mos > 10 else "Fair"}

        # --- Technicals ---
        rsi        = compute_rsi(closes)
        macd_line, signal_line, macd_hist = compute_macd(closes)
        bb_lo, bb_mid, bb_hi = compute_bollinger(closes)
        atr        = compute_atr(highs, lows, closes)
        adx        = compute_adx(highs, lows, closes)
        vol_spike  = compute_volume_spike(volumes)
        ma50       = round(np.mean(closes[-50:]), 2)
        ma200      = round(np.mean(closes[-200:]), 2) if len(closes) >= 200 else None
        price_vs_bb = None
        if bb_lo and bb_hi:
            price_vs_bb = round((c - bb_lo) / (bb_hi - bb_lo) * 100, 1) if (bb_hi - bb_lo) > 0 else 50

        # 52-week hi/lo
        week52_hi = round(max(closes[-252:]) if len(closes) >= 252 else max(closes), 2)
        week52_lo = round(min(closes[-252:]) if len(closes) >= 252 else min(closes), 2)

        return {
            **item,
            "name": name,
            "sector": sector,
            "beta": round(float(beta or 1.0), 2),
            "current_price": c,
            "ma50": ma50,
            "ma200": ma200,
            "rsi": rsi,
            "macd_line": macd_line,
            "macd_signal": signal_line,
            "macd_hist": macd_hist,
            "bb_lo": bb_lo, "bb_mid": bb_mid, "bb_hi": bb_hi,
            "price_vs_bb": price_vs_bb,
            "atr": atr,
            "adx": adx,
            "vol_spike": vol_spike,
            "week52_hi": week52_hi,
            "week52_lo": week52_lo,
            "market_cap": mkt_cap,
            "trend": "Above MA50" if c > ma50 else "Below MA50",
            **iv_data
        }
    except Exception as e:
        logger.debug(f"fetch_stock failed for {item['symbol']}: {e}")
        return None


# ── Enhanced Scoring ───────────────────────────────────────────────────────

def score_stock(d):
    """
    Multi-factor scoring. Returns (score: int, reasons: list[str]).
    Score breakdown:
      RSI                 max  +3
      MACD histogram      max  +2
      MA trend            max  +3
      ADX (trend strength)max  +2
      Volume spike        max  +1
      Bollinger position  max  +2
      Intrinsic value     max  +3
      52-week position    max  +1
    Total possible: +17 (BUY threshold: >= 7, SELL: <= 0)
    """
    score, reasons = 0, []
    c = d["current_price"]

    # 1. RSI
    rsi = d.get("rsi")
    if rsi is not None:
        if rsi < 30:
            score += 3; reasons.append(f"RSI {rsi} — deeply oversold, reversal likely")
        elif rsi < 40:
            score += 2; reasons.append(f"RSI {rsi} — oversold territory")
        elif rsi < 50:
            score += 1; reasons.append(f"RSI {rsi} — mild bullish bias")
        elif rsi > 80:
            score -= 3; reasons.append(f"RSI {rsi} — extremely overbought")
        elif rsi > 70:
            score -= 2; reasons.append(f"RSI {rsi} — overbought")

    # 2. MACD histogram
    hist = d.get("macd_hist")
    if hist is not None:
        if hist > 0:
            score += 2; reasons.append("MACD histogram positive — bullish momentum")
        else:
            score -= 1; reasons.append("MACD histogram negative")

    # 3. Moving average trend
    ma50  = d.get("ma50")
    ma200 = d.get("ma200")
    if ma50 and ma200:
        if c > ma50 > ma200:
            score += 3; reasons.append("Golden cross — confirmed uptrend")
        elif c > ma50:
            score += 1; reasons.append("Price above MA50")
        elif c < ma50 < ma200:
            score -= 2; reasons.append("Death cross — confirmed downtrend")
    elif ma50:
        if c > ma50:
            score += 1; reasons.append("Price above MA50")

    # 4. ADX (trend strength)
    adx = d.get("adx")
    if adx is not None:
        if adx > 30:
            score += 2; reasons.append(f"ADX {adx} — strong trend")
        elif adx > 20:
            score += 1; reasons.append(f"ADX {adx} — moderate trend")

    # 5. Volume spike (confirmation)
    vs = d.get("vol_spike")
    if vs is not None and vs >= 1.5:
        score += 1; reasons.append(f"Volume spike {vs}x — institutional activity")

    # 6. Bollinger Band position
    pvb = d.get("price_vs_bb")
    if pvb is not None:
        if pvb < 15:
            score += 2; reasons.append("Near lower Bollinger Band — oversold")
        elif pvb > 85:
            score -= 1; reasons.append("Near upper Bollinger Band — stretched")

    # 7. Intrinsic value / margin of safety
    iv_status = d.get("iv_status")
    mos = d.get("margin_of_safety")
    if iv_status == "Undervalued" and mos is not None:
        if mos > 25:
            score += 3; reasons.append(f"Deep value: {mos}% margin of safety")
        else:
            score += 2; reasons.append(f"Undervalued: {mos}% margin of safety")
    elif iv_status == "Overvalued":
        score -= 1; reasons.append("Trading above intrinsic value")

    # 8. 52-week position (not too extended)
    wk52_hi = d.get("week52_hi")
    wk52_lo = d.get("week52_lo")
    if wk52_hi and wk52_lo and wk52_hi > wk52_lo:
        pct_from_lo = (c - wk52_lo) / (wk52_hi - wk52_lo) * 100
        if pct_from_lo < 25:
            score += 1; reasons.append("Near 52-week low — deep discount")

    return score, reasons


def build_recommendation(d, rank):
    score, reasons = score_stock(d)
    c = d["current_price"]

    signal = "BUY" if score >= 7 else "SELL" if score <= 0 else "HOLD"
    confidence = min(95, 50 + score * 3)
    atr = d.get("atr") or (c * 0.015)

    outlook_label, outlook_text = SECTOR_OUTLOOK.get(d["sector"], SECTOR_OUTLOOK["Default"])

    return {
        "rank": rank,
        "name": d["name"],
        "symbol": d["symbol"].replace(".NS", ""),
        "sector": d["sector"],
        "signal": signal,
        "current_price": c,
        "target_price": round(c + atr * 6, 2) if signal == "BUY" else round(c - atr * 5, 2),
        "stop_loss": round(c - atr * 3, 2),
        "intrinsic_value": d.get("intrinsic_value"),
        "iv_status": d.get("iv_status", "N/A"),
        "margin_of_safety": d.get("margin_of_safety"),
        "week52_hi": d.get("week52_hi"),
        "week52_lo": d.get("week52_lo"),
        "confidence": confidence,
        "risk_level": "High" if d.get("beta", 1) > 1.3 else "Low" if d.get("beta", 1) < 0.7 else "Medium",
        "holding_period": "6–8 weeks",
        "upside_pct": round((d.get("target_price", c) - c) / c * 100, 1) if signal == "BUY" else 0,
        "score": score,
        "reasons": reasons,
        "rsi": d.get("rsi"),
        "adx": d.get("adx"),
        "macd_hist": d.get("macd_hist"),
        "vol_spike": d.get("vol_spike"),
        "atr": atr,
        "beta": d.get("beta"),
        "market_cap": d.get("market_cap"),
        "technical_summary": (
            f"RSI: {d.get('rsi')}. ADX: {d.get('adx')}. "
            f"Price vs MA50: {d.get('trend')}. "
            f"Vol spike: {d.get('vol_spike')}x."
        ),
        "fundamental_summary": (
            f"IV: ₹{d.get('intrinsic_value')} | MoS: {d.get('margin_of_safety')}% | "
            f"Status: {d.get('iv_status')}."
        ),
        "sector_outlook": outlook_label,
        "situational_summary": outlook_text,
        "key_risks": "Market volatility, macro headwinds, sector rotation risk.",
    }


def build_option(stock_data, rank):
    c = stock_data["current_price"]
    strike = round(c / 50) * 50 + 50
    atr = stock_data.get("atr") or (c * 0.015)
    return {
        "rank": rank,
        "underlying": stock_data["name"],
        "symbol": stock_data["symbol"].replace(".NS", ""),
        "option_type": "CE",
        "strike_price": strike,
        "expiry": "30 Jul 2026",
        "current_stock_price": c,
        "strategy": "Long Call",
        "risk_level": "High",
        "target_move_pct": round((atr * 6) / c * 100, 1),
        "max_loss": "Premium paid",
        "holding_period": "4–6 weeks",
        "reasoning": f"Bullish momentum in {stock_data['name']} (score: {stock_data.get('score')}). {SECTOR_OUTLOOK.get(stock_data['sector'], SECTOR_OUTLOOK['Default'])[1]}",
        "key_risks": "Theta decay, IV crush post event.",
    }


# ── Background Screener ────────────────────────────────────────────────────

def run_screening():
    """
    Fetch data for the full universe, score every stock, cache results.
    Called in a background thread so the API stays responsive.
    """
    ensure_universe_loaded()
    universe = _universe_cache["stocks"]

    if not universe:
        logger.error("Universe is empty — cannot screen.")
        return

    logger.info(f"Screening {len(universe)} stocks…")
    t0 = time.time()

    results = []
    # Fetch in parallel — 10 workers is safe for yfinance rate limits
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fetch_stock, item): item for item in universe}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                results.append(res)

    logger.info(f"Fetched {len(results)}/{len(universe)} stocks in {time.time()-t0:.1f}s")

    # Score and sort
    scored = sorted(results, key=lambda x: score_stock(x)[0], reverse=True)

    # Top 10 BUY recommendations, top 20 total
    top20 = scored[:20]
    stock_recs  = [build_recommendation(s, i + 1) for i, s in enumerate(top20)]
    opt_recs    = [build_option(s, i + 1) for i, s in enumerate(scored[:3]) if score_stock(s)[0] >= 7]

    # Sector breakdown of top 20
    sector_count = {}
    for s in top20:
        sector_count[s["sector"]] = sector_count.get(s["sector"], 0) + 1

    response = {
        "generated_at": datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST"),
        "stocks_analyzed": len(results),
        "universe_size": len(universe),
        "market_summary": (
            f"Screened {len(results)} live NSE stocks. "
            f"Top sector: {top20[0]['sector'] if top20 else 'N/A'}. "
            f"Strongest pick: {top20[0]['name'] if top20 else 'N/A'} "
            f"(score {score_stock(top20[0])[0] if top20 else 0})."
        ),
        "sector_breakdown": sector_count,
        "stocks": stock_recs,
        "options": opt_recs,
    }

    with _universe_cache["lock"]:
        _universe_cache["results"] = scored
        _universe_cache["recommendations"] = response
        _universe_cache["last_screened"] = datetime.now()

    logger.info("Screening complete and cached.")


def maybe_refresh_screening():
    """Trigger a background re-screen if cache is stale."""
    last = _universe_cache.get("last_screened")
    if last is None or (datetime.now() - last) > timedelta(minutes=SCREEN_REFRESH_MINUTES):
        t = threading.Thread(target=run_screening, daemon=True)
        t.start()


# ── Market Status ──────────────────────────────────────────────────────────

def is_market_open():
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    mo = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    mc = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return mo <= now <= mc


# ── Flask Routes ───────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("Daily_Recomendation.html")


@app.route("/api/market-status")
def market_status():
    return jsonify({
        "is_open": is_market_open(),
        "current_time": datetime.now(IST).strftime("%I:%M %p IST")
    })


@app.route("/api/recommendations")
def recommendations():
    maybe_refresh_screening()

    cached = _universe_cache.get("recommendations")
    if cached:
        return jsonify(cached)

    # First-ever call — do a quick synchronous screen of just 50 seed stocks
    # while the full background screen runs
    logger.info("No cache yet — running quick seed screen…")
    seed = _seed_fallback()
    with ThreadPoolExecutor(max_workers=10) as ex:
        raw = [s for s in ex.map(fetch_stock, seed) if s]
    scored = sorted(raw, key=lambda x: score_stock(x)[0], reverse=True)
    top = scored[:20]
    return jsonify({
        "generated_at": datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST"),
        "stocks_analyzed": len(raw),
        "universe_size": len(seed),
        "note": "Quick seed screen — full NSE universe screen running in background.",
        "market_summary": f"Seed screen complete. Top pick: {top[0]['name'] if top else 'N/A'}.",
        "stocks": [build_recommendation(s, i + 1) for i, s in enumerate(top)],
        "options": [build_option(s, i + 1) for i, s in enumerate(scored[:3])],
    })


@app.route("/api/universe")
def universe_info():
    """Debug endpoint — shows universe size and last refresh times."""
    return jsonify({
        "universe_size": len(_universe_cache["stocks"]),
        "universe_last_fetched": str(_universe_cache["last_fetched"]),
        "last_screened": str(_universe_cache["last_screened"]),
        "cached_results_count": len(_universe_cache["results"]),
    })


# ── Startup ────────────────────────────────────────────────────────────────

def startup_background():
    """Pre-warm universe and run first screen on startup."""
    time.sleep(2)  # Let Flask bind first
    run_screening()

if __name__ == "__main__":
    # Kick off background screen immediately
    threading.Thread(target=startup_background, daemon=True).start()
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
        debug=False   # Must be False when using background threads
    )

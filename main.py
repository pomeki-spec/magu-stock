from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import ta
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures

app = FastAPI()

@app.get("/dashboard")
def dashboard():
    return FileResponse("index.html")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

TICKERS_US = list(set([
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AVGO","COST","NFLX",
    "TMUS","AMD","PEP","LIN","CSCO","ADBE","TXN","QCOM","INTU","AMAT",
    "AMGN","ISRG","MU","LRCX","KLAC","MRVL","MDLZ","ADP","REGN","PANW",
    "SNPS","CDNS","ORLY","CRWD","ADI","FTNT","MELI","MAR","PYPL","WDAY",
    "ABNB","DXCM","ROST","IDXX","ODFL","FAST","VRSK","BIIB","PCAR","TEAM",
    "ZS","CPRT","PAYX","NXPI","CHTR","DLTR","FANG","ANSS","CTAS","CTSH",
    "MCHP","MRNA","DDOG","EBAY","ENPH","TTWO","XEL","GEHC","ON","GILD",
    "SBUX","BKNG","VRTX","LULU","NTAP","FSLR","CDW","SMCI","CCEP","CEG",
    "MNST","KDP","ILMN","OKTA","ALGN","DOCU","ZM","SGEN","MTCH","ARM",
    "BRK-B","JPM","V","UNH","XOM","JNJ","WMT","MA","PG","HD",
    "CVX","MRK","ABBV","BAC","KO","LLY","TMO","MCD","CRM","ACN",
    "ABT","DHR","NKE","NEE","WFC","PM","T","UPS","MS","RTX",
    "SPGI","BMY","CAT","GS","BLK","SYK","AXP","C","CB","MO",
    "ZTS","CVS","SO","DUK","PLD","TGT","MMM","CI","ITW","HUM",
    "USB","EMR","NSC","AON","EL","HCA","PSA","MCK","WM","ADM",
    "SHW","FCX","ECL","TRV","APD","COF","EW","CARR","IQV","BDX",
    "SPG","GD","NOC","AIG","WELL","CME","MPC","VLO","CCI","CBRE",
    "STZ","YUM","ROP","KEYS","AWK","FIS","LHX","DG","CTVA","TDG",
    "LOW","INTC","IBM","GE","F","GM","PFE","UBER","LYFT","SQ"
]))

TICKERS_KR = [
    "005930.KS","000660.KS","035420.KS","005380.KS","051910.KS",
    "006400.KS","028260.KS","105560.KS","012330.KS","066570.KS",
    "017670.KS","032830.KS","012450.KS","003550.KS","018260.KS"
]

def calculate_classic_score(ticker_data, hist_weekly, hist_daily):
    score = 0
    try:
        if len(hist_weekly) >= 26:
            ema26 = hist_weekly['Close'].ewm(span=26).mean()
            if ema26.iloc[-1] > ema26.iloc[-2]:
                score += 10
        if len(hist_daily) >= 14:
            high = hist_daily['High']
            low = hist_daily['Low']
            close = hist_daily['Close']
            lowest_low = low.rolling(14).min()
            highest_high = high.rolling(14).max()
            stoch_k = 100 * (close - lowest_low) / (highest_high - lowest_low)
            if stoch_k.iloc[-1] < 40:
                score += 10
        if len(hist_daily) >= 5:
            recent_high = hist_daily['High'].iloc[-6:-1].max()
            latest_close = hist_daily['Close'].iloc[-1]
            if latest_close >= recent_high * 0.98:
                score += 10
    except:
        pass
    return score

def calculate_growth_score(info, hist_daily):
    score = 0
    try:
        roe = info.get('returnOnEquity', 0) or 0
        if roe > 0.15:
            score += 10
        debt_equity = info.get('debtToEquity', 999) or 999
        if debt_equity < 100:
            score += 5
        eps_growth = info.get('earningsGrowth', 0) or 0
        if eps_growth > 0.20:
            score += 10
        peg = info.get('pegRatio', 999) or 999
        if 0 < peg < 1.2:
            score += 5
        if len(hist_daily) >= 200:
            ma200 = hist_daily['Close'].rolling(200).mean().iloc[-1]
            current = hist_daily['Close'].iloc[-1]
            if current > ma200:
                score += 5
        if len(hist_daily) >= 14:
            rsi = ta.momentum.RSIIndicator(hist_daily['Close'], window=14).rsi()
            if rsi.iloc[-1] > 50:
                score += 5
    except:
        pass
    return score

def calculate_modern_score(info, hist_daily):
    score = 0
    try:
        rec = info.get('recommendationKey', '') or ''
        if rec in ['strong_buy', 'buy']:
            score += 10
        elif rec == 'hold':
            score += 5
        if len(hist_daily) >= 252:
            year_return = (hist_daily['Close'].iloc[-1] / hist_daily['Close'].iloc[-252] - 1) * 100
            if year_return > 20:
                score += 10
            elif year_return > 0:
                score += 5
        if len(hist_daily) >= 20:
            avg_vol = hist_daily['Volume'].rolling(20).mean().iloc[-1]
            recent_vol = hist_daily['Volume'].iloc[-1]
            if recent_vol > avg_vol * 1.2:
                score += 10
            elif recent_vol > avg_vol:
                score += 5
    except:
        pass
    return score

def get_recommendation(total_score):
    if total_score >= 70: return "Strong Buy"
    elif total_score >= 55: return "Buy"
    elif total_score >= 40: return "Hold"
    else: return "Watch"

def get_portfolio_weight(results):
    buy_stocks = [r for r in results if r['recommendation'] in ['Strong Buy', 'Buy']]
    total_score = sum(r['total_score'] for r in buy_stocks)
    for r in results:
        if r['recommendation'] in ['Strong Buy', 'Buy'] and total_score > 0:
            r['weight'] = round((r['total_score'] / total_score) * 100, 1)
        else:
            r['weight'] = 0
    return results

def fetch_single_stock(ticker, market):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        hist_daily = stock.history(period="1y")
        hist_weekly = stock.history(period="2y", interval="1wk")
        if hist_daily.empty or len(hist_daily) < 20:
            return None
        classic = calculate_classic_score(info, hist_weekly, hist_daily)
        growth = calculate_growth_score(info, hist_daily)
        modern = calculate_modern_score(info, hist_daily)
        total = classic + growth + modern
        current_price = hist_daily['Close'].iloc[-1]
        prev_price = hist_daily['Close'].iloc[-2]
        change_pct = (current_price / prev_price - 1) * 100
        return {
            "ticker": ticker,
            "name": info.get('longName', ticker),
            "price": round(current_price, 2),
            "change_pct": round(change_pct, 2),
            "classic_score": classic,
            "growth_score": growth,
            "modern_score": modern,
            "total_score": total,
            "recommendation": get_recommendation(total),
            "weight": 0,
            "roe": round((info.get('returnOnEquity', 0) or 0) * 100, 1),
            "peg": info.get('pegRatio', 0) or 0,
            "rsi": 0,
        }
    except:
        return None

@app.get("/")
def root():
    return {"status": "MAGU STOCK API 실행 중"}

@app.get("/api/market")
def get_market_data():
    try:
        tickers = {
            "gold": "GC=F", "wti": "CL=F", "usdkrw": "KRW=X",
            "us10y": "^TNX", "vix": "^VIX", "sp500": "^GSPC",
            "nasdaq": "^IXIC", "dow": "^DJI", "russell": "^RUT"
        }
        result = {}
        for key, symbol in tickers.items():
            try:
                t = yf.Ticker(symbol)
                hist = t.history(period="2d")
                if len(hist) >= 2:
                    current = hist['Close'].iloc[-1]
                    prev = hist['Close'].iloc[-2]
                    chg = (current / prev - 1) * 100
                    result[key] = {"value": round(current, 2), "change": round(chg, 2)}
            except:
                result[key] = {"value": 0, "change": 0}
        return result
    except:
        return {}

@app.get("/api/screen/{market}")
def screen_stocks(market: str = "us"):
    tickers = TICKERS_US if market == "us" else TICKERS_KR
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_single_stock, t, market): t for t in tickers}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.append(result)
    results.sort(key=lambda x: x['total_score'], reverse=True)
    results = get_portfolio_weight(results)
    return {
        "market": market,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total_screened": len(results),
        "results": results
    }

# ── 종목 단일 조회 ──────────────────────────────────────────────
@app.get("/api/stock/{ticker}")
def get_stock_score(ticker: str):
    ticker = ticker.upper().strip()
    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        price_check = info.get('regularMarketPrice') or info.get('currentPrice') or info.get('previousClose')
        if not info or not price_check:
            return {"error": f"종목을 찾을 수 없습니다: {ticker}"}

        hist_daily = stock.history(period="1y")
        hist_weekly = stock.history(period="2y", interval="1wk")

        if hist_daily.empty or len(hist_daily) < 20:
            return {"error": "데이터가 부족합니다 (상장 기간이 짧거나 거래 정지)"}

        classic = calculate_classic_score(info, hist_weekly, hist_daily)
        growth  = calculate_growth_score(info, hist_daily)
        modern  = calculate_modern_score(info, hist_daily)
        total   = classic + growth + modern

        current_price = hist_daily['Close'].iloc[-1]
        prev_price    = hist_daily['Close'].iloc[-2]
        change_pct    = (current_price / prev_price - 1) * 100

        rsi_val = 0
        if len(hist_daily) >= 14:
            rsi_val = round(ta.momentum.RSIIndicator(hist_daily['Close'], window=14).rsi().iloc[-1], 1)

        year_return = 0
        if len(hist_daily) >= 252:
            year_return = round((hist_daily['Close'].iloc[-1] / hist_daily['Close'].iloc[-252] - 1) * 100, 1)

        mcap = info.get('marketCap') or 0

        return {
            "ticker": ticker,
            "name": info.get('longName') or info.get('shortName') or ticker,
            "sector": info.get('sector') or info.get('industry') or '—',
            "currency": info.get('currency', 'USD'),
            "price": round(current_price, 2),
            "change_pct": round(change_pct, 2),
            "classic_score": classic,
            "growth_score": growth,
            "modern_score": modern,
            "total_score": total,
            "recommendation": get_recommendation(total),
            "detail": {
                "roe": round((info.get('returnOnEquity') or 0) * 100, 1),
                "debt_equity": round(info.get('debtToEquity') or 0, 1),
                "eps_growth": round((info.get('earningsGrowth') or 0) * 100, 1),
                "peg": round(info.get('pegRatio') or 0, 2),
                "rsi": rsi_val,
                "year_return": year_return,
                "analyst_rec": info.get('recommendationKey') or '—',
                "market_cap": mcap,
            }
        }
    except Exception as e:
        return {"error": f"조회 실패: {str(e)}"}


# ── 백테스트 ────────────────────────────────────────────────────
def score_at_date(hist_daily, hist_weekly, info, cutoff_idx):
    d = hist_daily.iloc[:cutoff_idx]
    w = hist_weekly[hist_weekly.index <= hist_daily.index[cutoff_idx - 1]]
    if len(d) < 20:
        return None
    classic = calculate_classic_score(info, w, d)
    growth  = calculate_growth_score(info, d)
    modern  = calculate_modern_score(info, d)
    return classic, growth, classic + growth + modern

def backtest_single(ticker, hold_days, score_threshold):
    try:
        stock = yf.Ticker(ticker)
        info  = stock.info
        hist  = stock.history(period="2y")
        histw = stock.history(period="3y", interval="1wk")
        if hist.empty or len(hist) < 60:
            return []

        sp500 = yf.Ticker("^GSPC").history(period="2y")
        signals = []

        step = 40
        for i in range(60, len(hist) - hold_days, step):
            result = score_at_date(hist, histw, info, i)
            if result is None:
                continue
            classic, growth, total = result
            if total < score_threshold:
                continue

            entry_price = hist['Close'].iloc[i]
            exit_price  = hist['Close'].iloc[i + hold_days]
            ret = round((exit_price / entry_price - 1) * 100, 2)

            entry_date = hist.index[i]
            exit_date  = hist.index[i + hold_days]
            sp_slice = sp500[(sp500.index >= entry_date) & (sp500.index <= exit_date)]
            sp_ret = 0.0
            if len(sp_slice) >= 2:
                sp_ret = round((sp_slice['Close'].iloc[-1] / sp_slice['Close'].iloc[0] - 1) * 100, 2)

            signals.append({
                "ticker": ticker,
                "signal_date": entry_date.strftime("%Y.%m.%d"),
                "entry_price": round(float(entry_price), 2),
                "exit_price":  round(float(exit_price), 2),
                "return_pct":  ret,
                "sp500_ret":   sp_ret,
                "classic_score": classic,
                "growth_score":  growth,
                "total_score":   total,
                "recommendation": get_recommendation(total),
                "win": ret > 0,
            })
        return signals
    except:
        return []

@app.get("/api/backtest")
def run_backtest(market: str = "us", hold_days: int = 30, score_threshold: int = 55):
    tickers = TICKERS_US[:30] if market == "us" else TICKERS_KR

    all_signals = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(backtest_single, t, hold_days, score_threshold): t for t in tickers}
        for f in concurrent.futures.as_completed(futures):
            all_signals.extend(f.result())

    if not all_signals:
        return {
            "summary": {
                "total_signals": 0,
                "win_rate": 0,
                "avg_return": 0,
                "avg_sp500": 0,
                "alpha": 0,
                "hold_days": hold_days,
                "score_threshold": score_threshold,
                "best_model": "—",
            },
            "band_stats": [],
            "period_returns": [],
            "signals": [],
            "error": "신호 없음 — 점수 기준을 낮추거나 보유 기간을 조정해보세요."
        }

    total    = len(all_signals)
    wins     = sum(1 for s in all_signals if s["win"])
    win_rate = round(wins / total * 100, 1) if total > 0 else 0
    avg_ret  = round(sum(s["return_pct"] for s in all_signals) / total, 2) if total > 0 else 0
    avg_sp   = round(sum(s["sp500_ret"]  for s in all_signals) / total, 2) if total > 0 else 0
    alpha    = round(avg_ret - avg_sp, 2)

    bands = [
        {"label": "70점 이상", "min": 70, "max": 100},
        {"label": "65~69점",   "min": 65, "max": 69},
        {"label": "55~64점",   "min": 55, "max": 64},
        {"label": "40~54점",   "min": 40, "max": 54},
    ]
    band_stats = []
    for b in bands:
        filtered = [s for s in all_signals if b["min"] <= s["total_score"] <= b["max"]]
        if filtered:
            wr = round(sum(1 for s in filtered if s["win"]) / len(filtered) * 100, 1)
        else:
            wr = 0
        band_stats.append({"label": b["label"], "win_rate": wr, "count": len(filtered)})

    period_rets = []
    sample_tickers = tickers[:10]
    for days in [10, 20, 30, 45, 60, 90]:
        day_signals = []
        for t in sample_tickers:
            sigs = backtest_single(t, days, score_threshold)
            day_signals.extend(sigs)
        if day_signals:
            avg = round(sum(s["return_pct"] for s in day_signals) / len(day_signals), 2)
            sp  = round(sum(s["sp500_ret"]  for s in day_signals) / len(day_signals), 2)
        else:
            avg, sp = 0, 0
        period_rets.append({"days": days, "magu": avg, "sp500": sp})

    classic_wins = [s for s in all_signals if s["classic_score"] >= 20 and s["win"]]
    growth_wins  = [s for s in all_signals if s["growth_score"]  >= 30 and s["win"]]
    modern_wins  = [s for s in all_signals if s["total_score"] - s["classic_score"] - s["growth_score"] >= 20 and s["win"]]
    best_model = max(
        [("Classic", len(classic_wins)), ("Growth", len(growth_wins)), ("Modern", len(modern_wins))],
        key=lambda x: x[1]
    )[0]

    all_signals.sort(key=lambda x: abs(x["return_pct"]), reverse=True)

    return {
        "summary": {
            "total_signals": total,
            "win_rate": win_rate,
            "avg_return": avg_ret,
            "avg_sp500": avg_sp,
            "alpha": alpha,
            "hold_days": hold_days,
            "score_threshold": score_threshold,
            "best_model": best_model,
        },
        "band_stats": band_stats,
        "period_returns": period_rets,
        "signals": all_signals[:50],
    }

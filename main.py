from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import ta
import pandas as pd
from datetime import datetime

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

# 스크리닝할 종목 리스트 (미국 + 한국 대표 종목)
# 나스닥 100 + S&P 500 상위 100개
TICKERS_US = list(set([
    # 나스닥 100
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AVGO","COST","NFLX",
    "TMUS","AMD","PEP","LIN","CSCO","ADBE","TXN","QCOM","INTU","AMAT",
    "AMGN","ISRG","MU","LRCX","KLAC","MRVL","MDLZ","ADP","REGN","PANW",
    "SNPS","CDNS","ORLY","CRWD","ADI","FTNT","MELI","MAR","PYPL","WDAY",
    "ABNB","DXCM","ROST","IDXX","ODFL","FAST","VRSK","BIIB","PCAR","TEAM",
    "ZS","CPRT","PAYX","NXPI","CHTR","DLTR","FANG","ANSS","CTAS","CTSH",
    "MCHP","MRNA","DDOG","EBAY","ENPH","TTWO","XEL","GEHC","ON","GILD",
    "SBUX","BKNG","VRTX","LULU","NTAP","FSLR","CDW","SMCI","CCEP","CEG",
    "MNST","KDP","ILMN","OKTA","ALGN","DOCU","ZM","SGEN","MTCH","RIVN",
    "WBD","LCID","GFS","BMRN","CINF","CSGP","SIRI","GEHC","ACGL","ARM",
    # S&P 500 상위 100개
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

print(f"✅ 종목 로드 완료: 총 {len(TICKERS_US)}개")
TICKERS_KR = [
    "005930.KS","000660.KS","035420.KS","005380.KS","051910.KS",
    "006400.KS","028260.KS","105560.KS","012330.KS","066570.KS",
    "017670.KS","032830.KS","012450.KS","003550.KS","018260.KS"
]

def calculate_classic_score(ticker_data, hist_weekly, hist_daily):
    """Classic Model: 엘더 3중 스크린 (30점)"""
    score = 0
    try:
        # 1단계: 26주 EMA 상승 여부 (10점)
        if len(hist_weekly) >= 26:
            ema26 = hist_weekly['Close'].ewm(span=26).mean()
            if ema26.iloc[-1] > ema26.iloc[-2]:
                score += 10

        # 2단계: Stochastic 과매도 구간 (10점)
        if len(hist_daily) >= 14:
            high = hist_daily['High']
            low = hist_daily['Low']
            close = hist_daily['Close']
            lowest_low = low.rolling(14).min()
            highest_high = high.rolling(14).max()
            stoch_k = 100 * (close - lowest_low) / (highest_high - lowest_low)
            if stoch_k.iloc[-1] < 40:  # 과매도 구간
                score += 10

        # 3단계: 최근 5일 내 전고점 돌파 시도 (10점)
        if len(hist_daily) >= 5:
            recent_high = hist_daily['High'].iloc[-6:-1].max()
            latest_close = hist_daily['Close'].iloc[-1]
            if latest_close >= recent_high * 0.98:
                score += 10
    except:
        pass
    return score

def calculate_growth_score(info, hist_daily):
    """Growth Model: 퀀트펀더멘털 (40점)"""
    score = 0
    try:
        # 1단계: ROE > 15% (10점)
        roe = info.get('returnOnEquity', 0) or 0
        if roe > 0.15:
            score += 10

        # 부채비율 < 100% (5점)
        debt_equity = info.get('debtToEquity', 999) or 999
        if debt_equity < 100:
            score += 5

        # 2단계: EPS 성장률 > 20% (10점)
        eps_growth = info.get('earningsGrowth', 0) or 0
        if eps_growth > 0.20:
            score += 10

        # PEG < 1.2 (5점)
        peg = info.get('pegRatio', 999) or 999
        if 0 < peg < 1.2:
            score += 5

        # 3단계: 200일선 위 (5점)
        if len(hist_daily) >= 200:
            ma200 = hist_daily['Close'].rolling(200).mean().iloc[-1]
            current = hist_daily['Close'].iloc[-1]
            if current > ma200:
                score += 5

        # RSI > 50 (5점)
        if len(hist_daily) >= 14:
            rsi = ta.momentum.RSIIndicator(hist_daily['Close'], window=14).rsi()
            if rsi.iloc[-1] > 50:
                score += 5

    except:
        pass
    return score

def calculate_modern_score(info, hist_daily):
    """Modern Model: AI 심리 스크린 (30점)"""
    score = 0
    try:
        # 1단계: 실적 상향 (애널리스트 추천 기반) (10점)
        rec = info.get('recommendationKey', '') or ''
        if rec in ['strong_buy', 'buy']:
            score += 10
        elif rec == 'hold':
            score += 5

        # 2단계: RS - 52주 수익률 상대 강도 (10점)
        if len(hist_daily) >= 252:
            year_return = (hist_daily['Close'].iloc[-1] / hist_daily['Close'].iloc[-252] - 1) * 100
            if year_return > 20:
                score += 10
            elif year_return > 0:
                score += 5

        # 3단계: 거래량 증가 (모멘텀) (10점)
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
    if total_score >= 70:
        return "Strong Buy"
    elif total_score >= 55:
        return "Buy"
    elif total_score >= 40:
        return "Hold"
    else:
        return "Watch"

def get_portfolio_weight(results):
    """점수 비례 포트폴리오 비중 계산"""
    buy_stocks = [r for r in results if r['recommendation'] in ['Strong Buy', 'Buy']]
    total_score = sum(r['total_score'] for r in buy_stocks)
    for r in results:
        if r['recommendation'] in ['Strong Buy', 'Buy'] and total_score > 0:
            r['weight'] = round((r['total_score'] / total_score) * 100, 1)
        else:
            r['weight'] = 0
    return results
@app.get("/api/market")
def get_market_data():
    try:
        tickers = {
            "gold": "GC=F",
            "wti": "CL=F", 
            "usdkrw": "KRW=X",
            "us10y": "^TNX",
            "vix": "^VIX"
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
                    result[key] = {
                        "value": round(current, 2),
                        "change": round(chg, 2)
                    }
            except:
                result[key] = {"value": 0, "change": 0}
        return result
    except:
        return {}
@app.get("/")
def root():
    return {"status": "MAGU STOCK API 실행 중"}

import concurrent.futures

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

    tickers = TICKERS_US if market == "us" else TICKERS_KR
    results = []

    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            hist_daily = stock.history(period="1y")
            hist_weekly = stock.history(period="2y", interval="1wk")

            if hist_daily.empty or len(hist_daily) < 20:
                continue

            classic = calculate_classic_score(info, hist_weekly, hist_daily)
            growth = calculate_growth_score(info, hist_daily)
            modern = calculate_modern_score(info, hist_daily)
            total = classic + growth + modern

            current_price = hist_daily['Close'].iloc[-1]
            prev_price = hist_daily['Close'].iloc[-2]
            change_pct = (current_price / prev_price - 1) * 100

            results.append({
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
            })
        except Exception as e:
            continue

    results.sort(key=lambda x: x['total_score'], reverse=True)
    results = get_portfolio_weight(results)

    return {
        "market": market,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total_screened": len(results),
        "results": results
    }
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

TICKERS_US = [
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
    "ZTS","CVS","SO","DUK","PLD","TGT","MMC","CI","ITW","HUM",
    "USB","EMR","NSC","AON","EL","HCA","PSA","MCK","WM","ADM",
    "SHW","FCX","ECL","TRV","APD","COF","EW","CARR","IQV","BDX",
    "SPG","GD","NOC","AIG","WELL","CME","MPC","VLO","CCI","CBRE",
    "STZ","YUM","ROP","KEYS","AWK","FIS","LHX","DG","CTVA","TDG",
    "LOW","INTC","IBM","GE","F","GM","PFE","UBER","LYFT","SQ"
]

TICKERS_KR = [
    "005930.KS","000660.KS","005380.KS","000270.KS","051910.KS",
    "006400.KS","035420.KS","035720.KS","028260.KS","105560.KS",
    "055550.KS","086790.KS","032830.KS","003550.KS","066570.KS",
    "012330.KS","017670.KS","018260.KS","012450.KS","096770.KS",
    "010950.KS","003670.KS","005490.KS","000810.KS","030200.KS",
    "015760.KS","011200.KS","034730.KS","009150.KS","010130.KS",
    "002380.KS","011170.KS","004020.KS","000100.KS","006800.KS",
    "016360.KS","139480.KS","004170.KS","021240.KS","097950.KS",
    "000080.KS","033780.KS","271560.KS","282330.KS","326030.KS",
    "207940.KS","068270.KS","128940.KS","002270.KS","001040.KS",
    "247540.KQ","086520.KQ","196170.KQ","091990.KQ","035900.KQ",
    "041510.KQ","122870.KQ","263750.KQ","293490.KQ","112040.KQ",
    "067160.KQ","039030.KQ","357780.KQ","096530.KQ","145020.KQ",
    "214150.KQ","151910.KQ","084370.KQ","095340.KQ","039200.KQ",
    "031370.KQ","048410.KQ","058970.KQ","064760.KQ","237690.KQ",
    "022100.KQ","179900.KQ","036570.KQ","241560.KQ","950130.KQ",
]

KR_NAMES = {
    "005930.KS":"삼성전자","000660.KS":"SK하이닉스","005380.KS":"현대차",
    "000270.KS":"기아","051910.KS":"LG화학","006400.KS":"삼성SDI",
    "035420.KS":"NAVER","035720.KS":"카카오","028260.KS":"삼성물산",
    "105560.KS":"KB금융","055550.KS":"신한지주","086790.KS":"하나금융지주",
    "032830.KS":"삼성생명","003550.KS":"LG","066570.KS":"LG전자",
    "012330.KS":"현대모비스","017670.KS":"SK텔레콤","018260.KS":"삼성SDS",
    "012450.KS":"한화에어로스페이스","096770.KS":"SK이노베이션",
    "010950.KS":"S-Oil","003670.KS":"포스코퓨처엠","005490.KS":"POSCO홀딩스",
    "000810.KS":"삼성화재","030200.KS":"KT","015760.KS":"한국전력",
    "011200.KS":"HMM","034730.KS":"SK","009150.KS":"삼성전기",
    "010130.KS":"고려아연","002380.KS":"KCC","011170.KS":"롯데케미칼",
    "004020.KS":"현대제철","000100.KS":"유한양행","006800.KS":"미래에셋증권",
    "016360.KS":"삼성증권","139480.KS":"이마트","004170.KS":"신세계",
    "021240.KS":"코웨이","097950.KS":"CJ제일제당","000080.KS":"하이트진로",
    "033780.KS":"KT&G","271560.KS":"오리온","282330.KS":"BGF리테일",
    "326030.KS":"SK바이오팜","207940.KS":"삼성바이오로직스","068270.KS":"셀트리온",
    "128940.KS":"한미약품","002270.KS":"롯데제과","001040.KS":"CJ",
    "247540.KQ":"에코프로비엠","086520.KQ":"에코프로","196170.KQ":"알테오젠",
    "091990.KQ":"셀트리온헬스케어","035900.KQ":"JYP엔터","041510.KQ":"SM엔터테인먼트",
    "122870.KQ":"와이지엔터테인먼트","263750.KQ":"펄어비스","293490.KQ":"카카오게임즈",
    "112040.KQ":"위메이드","067160.KQ":"아프리카TV","039030.KQ":"이오테크닉스",
    "357780.KQ":"솔브레인","096530.KQ":"씨젠","145020.KQ":"휴젤",
    "214150.KQ":"클래시스","151910.KQ":"한국콜마","084370.KQ":"유진테크",
    "095340.KQ":"ISC","039200.KQ":"오스코텍","031370.KQ":"아이센스",
    "048410.KQ":"현대바이오","058970.KQ":"엠씨넥스","064760.KQ":"티씨케이",
    "237690.KQ":"에스티팜","022100.KQ":"포스코DX","179900.KQ":"유티아이",
    "036570.KQ":"엔씨소프트","241560.KQ":"두산밥캣","950130.KQ":"엑스페릭스",
}

TICKERS_KR_BT = TICKERS_KR[:50]

# ── SPDR 섹터 ETF 정의 ─────────────────────────────────────────
SECTOR_ETFS = [
    {"ticker": "XLK",  "name": "기술",        "name_en": "Technology",             "emoji": "💻"},
    {"ticker": "XLV",  "name": "헬스케어",    "name_en": "Health Care",            "emoji": "🏥"},
    {"ticker": "XLF",  "name": "금융",        "name_en": "Financials",             "emoji": "🏦"},
    {"ticker": "XLE",  "name": "에너지",      "name_en": "Energy",                 "emoji": "⚡"},
    {"ticker": "XLY",  "name": "경기소비재",  "name_en": "Consumer Discretionary", "emoji": "🛍"},
    {"ticker": "XLP",  "name": "필수소비재",  "name_en": "Consumer Staples",       "emoji": "🛒"},
    {"ticker": "XLB",  "name": "소재",        "name_en": "Materials",              "emoji": "⚙️"},
    {"ticker": "XLC",  "name": "통신서비스",  "name_en": "Communication",          "emoji": "📡"},
    {"ticker": "XLI",  "name": "산업재",      "name_en": "Industrials",            "emoji": "🏭"},
    {"ticker": "XLU",  "name": "유틸리티",    "name_en": "Utilities",              "emoji": "💡"},
    {"ticker": "XLRE", "name": "부동산",      "name_en": "Real Estate",            "emoji": "🏢"},
    {"ticker": "SMH",  "name": "반도체",      "name_en": "Semiconductors",         "emoji": "🔬"},
    {"ticker": "XBI",  "name": "바이오텍",    "name_en": "Biotech",                "emoji": "🧬"},
    {"ticker": "ITB",  "name": "주택건설",    "name_en": "Home Construction",      "emoji": "🏠"},
    {"ticker": "XRT",  "name": "소매유통",    "name_en": "Retail",                 "emoji": "🏪"},
    {"ticker": "XHB",  "name": "주택",        "name_en": "Housing",                "emoji": "🏡"},
    {"ticker": "KRE",  "name": "지역은행",    "name_en": "Regional Banks",         "emoji": "🏧"},
    {"ticker": "XME",  "name": "금속광물",    "name_en": "Metals & Mining",        "emoji": "⛏"},
    {"ticker": "XOP",  "name": "석유가스",    "name_en": "Oil & Gas",              "emoji": "🛢"},
    {"ticker": "XAR",  "name": "방산항공",    "name_en": "Aerospace & Defense",    "emoji": "✈️"},
]

# yfinance 섹터명 → ETF 티커 매핑
SECTOR_TO_ETF = {
    "Technology": "XLK",
    "Healthcare": "XLV",
    "Health Care": "XLV",
    "Financial Services": "XLF",
    "Financials": "XLF",
    "Energy": "XLE",
    "Consumer Cyclical": "XLY",
    "Consumer Discretionary": "XLY",
    "Consumer Defensive": "XLP",
    "Consumer Staples": "XLP",
    "Basic Materials": "XLB",
    "Materials": "XLB",
    "Communication Services": "XLC",
    "Industrials": "XLI",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
}

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
        if roe > 0.15: score += 10
        debt_equity = info.get('debtToEquity', 999) or 999
        if debt_equity < 100: score += 5
        eps_growth = info.get('earningsGrowth', 0) or 0
        if eps_growth > 0.20: score += 10
        peg = info.get('pegRatio', 999) or 999
        if 0 < peg < 1.2: score += 5
        if len(hist_daily) >= 200:
            ma200 = hist_daily['Close'].rolling(200).mean().iloc[-1]
            if hist_daily['Close'].iloc[-1] > ma200: score += 5
        if len(hist_daily) >= 14:
            rsi = ta.momentum.RSIIndicator(hist_daily['Close'], window=14).rsi()
            if rsi.iloc[-1] > 50: score += 5
    except:
        pass
    return score

def calculate_modern_score(info, hist_daily):
    score = 0
    try:
        rec = info.get('recommendationKey', '') or ''
        if rec in ['strong_buy', 'buy']: score += 10
        elif rec == 'hold': score += 5
        if len(hist_daily) >= 252:
            year_return = (hist_daily['Close'].iloc[-1] / hist_daily['Close'].iloc[-252] - 1) * 100
            if year_return > 20: score += 10
            elif year_return > 0: score += 5
        if len(hist_daily) >= 20:
            avg_vol = hist_daily['Volume'].rolling(20).mean().iloc[-1]
            recent_vol = hist_daily['Volume'].iloc[-1]
            if recent_vol > avg_vol * 1.2: score += 10
            elif recent_vol > avg_vol: score += 5
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
        name = KR_NAMES.get(ticker) or info.get('longName', ticker)
        sector = info.get('sector') or 'Unknown'
        return {
            "ticker": ticker,
            "name": name,
            "sector": sector,
            "etf": SECTOR_TO_ETF.get(sector, ""),
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
            return {"error": "데이터가 부족합니다"}
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
        name = KR_NAMES.get(ticker) or info.get('longName') or ticker
        return {
            "ticker": ticker, "name": name,
            "sector": info.get('sector') or '—',
            "currency": info.get('currency', 'USD'),
            "price": round(current_price, 2),
            "change_pct": round(change_pct, 2),
            "classic_score": classic, "growth_score": growth,
            "modern_score": modern, "total_score": total,
            "recommendation": get_recommendation(total),
            "detail": {
                "roe": round((info.get('returnOnEquity') or 0) * 100, 1),
                "debt_equity": round(info.get('debtToEquity') or 0, 1),
                "eps_growth": round((info.get('earningsGrowth') or 0) * 100, 1),
                "peg": round(info.get('pegRatio') or 0, 2),
                "rsi": rsi_val, "year_return": year_return,
                "analyst_rec": info.get('recommendationKey') or '—',
                "market_cap": mcap,
            }
        }
    except Exception as e:
        return {"error": f"조회 실패: {str(e)}"}


# ── Smart Money: SPDR 섹터 ETF 기반 트렌드 ─────────────────────
def analyze_etf(etf_info: dict):
    """단일 ETF 분석 — 가격, 수익률, 거래량, 기관보유율, 모멘텀"""
    ticker = etf_info["ticker"]
    try:
        t = yf.Ticker(ticker)
        info = t.info
        hist = t.history(period="1y")

        if hist.empty or len(hist) < 60:
            return None

        price     = float(hist['Close'].iloc[-1])
        price_1d  = float(hist['Close'].iloc[-2]) if len(hist) >= 2 else price
        price_1w  = float(hist['Close'].iloc[-6]) if len(hist) >= 6 else price
        price_1m  = float(hist['Close'].iloc[-22]) if len(hist) >= 22 else price
        price_3m  = float(hist['Close'].iloc[-66]) if len(hist) >= 66 else price
        price_6m  = float(hist['Close'].iloc[-132]) if len(hist) >= 132 else price
        price_1y  = float(hist['Close'].iloc[0])

        ret_1d = round((price / price_1d - 1) * 100, 2)
        ret_1w = round((price / price_1w - 1) * 100, 2)
        ret_1m = round((price / price_1m - 1) * 100, 2)
        ret_3m = round((price / price_3m - 1) * 100, 2)
        ret_6m = round((price / price_6m - 1) * 100, 2)
        ret_1y = round((price / price_1y - 1) * 100, 2)

        # 거래량 모멘텀 (최근 5일 vs 20일 평균)
        vol_5d  = float(hist['Volume'].iloc[-5:].mean())
        vol_20d = float(hist['Volume'].iloc[-20:].mean())
        vol_ratio = round(vol_5d / vol_20d, 2) if vol_20d > 0 else 1.0

        # RSI
        rsi_val = 0.0
        if len(hist) >= 14:
            rsi_val = round(float(ta.momentum.RSIIndicator(hist['Close'], window=14).rsi().iloc[-1]), 1)

        # 52주 고점 대비
        high_52w = float(hist['High'].max())
        from_high = round((price / high_52w - 1) * 100, 1)

        # 기관 보유율
        inst_pct = round(float(info.get('heldPercentInstitutions') or 0) * 100, 1)

        # 트렌드 판단 로직
        # 1개월 수익률 + 거래량 모멘텀 + 3개월 흐름 종합
        score = 0
        if ret_1m > 5:   score += 3
        elif ret_1m > 2: score += 2
        elif ret_1m > 0: score += 1
        elif ret_1m < -5: score -= 3
        elif ret_1m < -2: score -= 2
        elif ret_1m < 0: score -= 1

        if ret_3m > 10:  score += 2
        elif ret_3m > 3: score += 1
        elif ret_3m < -10: score -= 2
        elif ret_3m < -3:  score -= 1

        if vol_ratio > 1.3: score += 2   # 거래량 급증 = 자금 유입
        elif vol_ratio > 1.1: score += 1
        elif vol_ratio < 0.7: score -= 2  # 거래량 급감 = 자금 유출
        elif vol_ratio < 0.9: score -= 1

        if rsi_val > 60: score += 1
        elif rsi_val < 40: score -= 1

        # 트렌드 분류
        if score >= 5:
            trend = "강한유입"
            trend_score = 10
            trend_color = "#15803d"
        elif score >= 3:
            trend = "유입"
            trend_score = 5
            trend_color = "#22c55e"
        elif score >= 1:
            trend = "소폭유입"
            trend_score = 3
            trend_color = "#86efac"
        elif score >= -1:
            trend = "중립"
            trend_score = 0
            trend_color = "#94a3b8"
        elif score >= -3:
            trend = "소폭유출"
            trend_score = -3
            trend_color = "#fca5a5"
        elif score >= -5:
            trend = "유출"
            trend_score = -5
            trend_color = "#ef4444"
        else:
            trend = "강한유출"
            trend_score = -10
            trend_color = "#7f1d1d"

        return {
            "ticker": ticker,
            "name": etf_info["name"],
            "name_en": etf_info["name_en"],
            "emoji": etf_info["emoji"],
            "price": round(price, 2),
            "ret_1d": ret_1d,
            "ret_1w": ret_1w,
            "ret_1m": ret_1m,
            "ret_3m": ret_3m,
            "ret_6m": ret_6m,
            "ret_1y": ret_1y,
            "vol_ratio": vol_ratio,
            "rsi": rsi_val,
            "from_52w_high": from_high,
            "inst_pct": inst_pct,
            "trend": trend,
            "trend_score": trend_score,
            "trend_color": trend_color,
            "momentum_score": score,
        }
    except:
        return None

@app.get("/api/smartmoney")
def get_smart_money():
    """SPDR 섹터 ETF 기반 Smart Money 트렌드 분석"""
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analyze_etf, etf): etf for etf in SECTOR_ETFS}
        for f in concurrent.futures.as_completed(futures):
            r = f.result()
            if r:
                results.append(r)

    # 모멘텀 스코어 기준 정렬 (강한 유입 → 강한 유출)
    results.sort(key=lambda x: x["momentum_score"], reverse=True)

    # 전체 시장 대비 상대 강도 계산
    if results:
        avg_ret_1m = sum(r["ret_1m"] for r in results) / len(results)
        for r in results:
            r["rel_strength"] = round(r["ret_1m"] - avg_ret_1m, 2)

    # S&P500 대비 비교
    try:
        spy = yf.Ticker("SPY").history(period="3mo")
        spy_ret_1m = round(float((spy['Close'].iloc[-1] / spy['Close'].iloc[-22] - 1) * 100), 2) if len(spy) >= 22 else 0
        spy_ret_3m = round(float((spy['Close'].iloc[-1] / spy['Close'].iloc[0] - 1) * 100), 2)
    except:
        spy_ret_1m = 0
        spy_ret_3m = 0

    return {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "spy_ret_1m": spy_ret_1m,
        "spy_ret_3m": spy_ret_3m,
        "sectors": results,
        "total": len(results),
    }


# ── 백테스트 ────────────────────────────────────────────────────
def score_at_date(hist_daily, hist_weekly, info, cutoff_idx):
    d = hist_daily.iloc[:cutoff_idx]
    w = hist_weekly[hist_weekly.index <= hist_daily.index[cutoff_idx - 1]]
    if len(d) < 20:
        return None
    classic = calculate_classic_score(info, w, d)
    growth  = calculate_growth_score(info, d)
    modern  = calculate_modern_score(info, d)
    return int(classic), int(growth), int(classic + growth + modern)

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
        step = 20
        for i in range(60, len(hist) - hold_days, step):
            result = score_at_date(hist, histw, info, i)
            if result is None:
                continue
            classic, growth, total = result
            if total < score_threshold:
                continue
            entry_price = float(hist['Close'].iloc[i])
            exit_price  = float(hist['Close'].iloc[i + hold_days])
            ret = round((exit_price / entry_price - 1) * 100, 2)
            entry_date = hist.index[i]
            exit_date  = hist.index[i + hold_days]
            sp_slice = sp500[(sp500.index >= entry_date) & (sp500.index <= exit_date)]
            sp_ret = 0.0
            if len(sp_slice) >= 2:
                sp_ret = round(float((sp_slice['Close'].iloc[-1] / sp_slice['Close'].iloc[0] - 1) * 100), 2)
            name = KR_NAMES.get(ticker, ticker)
            signals.append({
                "ticker": name,
                "signal_date": entry_date.strftime("%Y.%m.%d"),
                "sell_date": exit_date.strftime("%Y.%m.%d"),
                "entry_price": round(entry_price, 2),
                "exit_price":  round(exit_price, 2),
                "return_pct":  float(ret),
                "sp500_ret":   float(sp_ret),
                "classic_score": int(classic),
                "growth_score":  int(growth),
                "modern_score":  int(total - classic - growth),
                "total_score":   int(total),
                "recommendation": get_recommendation(total),
                "win": bool(ret > 0),
            })
        return signals
    except:
        return []

@app.get("/api/backtest")
def run_backtest(market: str = "us", hold_days: int = 30, score_threshold: int = 55):
    tickers = TICKERS_US[:50] if market == "us" else TICKERS_KR_BT
    all_signals = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(backtest_single, t, hold_days, score_threshold): t for t in tickers}
        for f in concurrent.futures.as_completed(futures):
            all_signals.extend(f.result())

    if not all_signals:
        return {
            "summary": {"total_signals":0,"win_rate":0.0,"avg_return":0.0,
                        "avg_sp500":0.0,"alpha":0.0,"hold_days":hold_days,
                        "score_threshold":score_threshold,"best_model":"—"},
            "band_stats":[],"period_returns":[],"signals":[],"error":"신호 없음"
        }

    total    = len(all_signals)
    wins     = sum(1 for s in all_signals if s["win"])
    win_rate = round(wins / total * 100, 1)
    avg_ret  = round(sum(s["return_pct"] for s in all_signals) / total, 2)
    avg_sp   = round(sum(s["sp500_ret"]  for s in all_signals) / total, 2)
    alpha    = round(avg_ret - avg_sp, 2)

    bands = [
        {"label":"70점 이상","min":70,"max":100},
        {"label":"65~69점",  "min":65,"max":69},
        {"label":"55~64점",  "min":55,"max":64},
        {"label":"40~54점",  "min":40,"max":54},
    ]
    band_stats = []
    for b in bands:
        filtered = [s for s in all_signals if b["min"] <= s["total_score"] <= b["max"]]
        wr = round(sum(1 for s in filtered if s["win"]) / len(filtered) * 100, 1) if filtered else 0.0
        band_stats.append({"label":b["label"],"win_rate":wr,"count":len(filtered)})

    period_rets = [{"days":d,"magu":avg_ret,"sp500":avg_sp} for d in [10,20,30,45,60,90]]

    classic_wins = [s for s in all_signals if s["classic_score"] >= 20 and s["win"]]
    growth_wins  = [s for s in all_signals if s["growth_score"]  >= 30 and s["win"]]
    modern_wins  = [s for s in all_signals if s["modern_score"]  >= 20 and s["win"]]
    best_model = max(
        [("Classic",len(classic_wins)),("Growth",len(growth_wins)),("Modern",len(modern_wins))],
        key=lambda x: x[1]
    )[0]

    all_signals.sort(key=lambda x: x["return_pct"], reverse=True)

    return {
        "summary": {
            "total_signals":total,"win_rate":win_rate,
            "avg_return":avg_ret,"avg_sp500":avg_sp,"alpha":alpha,
            "hold_days":hold_days,"score_threshold":score_threshold,
            "best_model":best_model,
        },
        "band_stats": band_stats,
        "period_returns": period_rets,
        "signals": all_signals[:100],
    }

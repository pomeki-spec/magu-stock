from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import ta
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures
import os
import requests

app = FastAPI()

@app.get("/dashboard")
def dashboard():
    response = FileResponse("index.html")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ══════════════════════════════════════════════════════════════
# 종목 풀 — 4개 시장 완전 독립
# ══════════════════════════════════════════════════════════════

TICKERS_NASDAQ = list(dict.fromkeys([
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AVGO","COST","NFLX",
    "TMUS","AMD","ADBE","TXN","QCOM","INTU","AMAT","AMGN","ISRG","MU",
    "LRCX","KLAC","MRVL","ADP","REGN","PANW","SNPS","CDNS","CRWD","ADI",
    "FTNT","MELI","PYPL","WDAY","ABNB","DXCM","IDXX","FAST","VRSK","BIIB",
    "PCAR","TEAM","ZS","CPRT","PAYX","NXPI","ANSS","CTAS","CTSH","MCHP",
    "MRNA","DDOG","EBAY","ENPH","TTWO","GEHC","ON","GILD","SBUX","BKNG",
    "VRTX","LULU","NTAP","FSLR","CDW","SMCI","CCEP","CEG","MNST","KDP",
    "ILMN","OKTA","ALGN","DOCU","ZM","MTCH","ARM","MDLZ","ORLY","MAR",
    "PLTR","COIN","RBLX","DASH","HOOD","AFRM","UPST","BILL","TOST","GTLB",
    "DUOL","HIMS","RDDT","CAVA","APP","CELH","DKNG","RIVN","CHWY","ETSY",
    "PINS","SNAP","BMBL","LYFT","UBER","SQ","ROKU","SOFI","NU","MSTR",
    "RIOT","MARA","SOUN","BBAI","IONQ","RKLB","ASTS","JOBY","ACHR","LUNR",
    "ARRY","NOVA","STEM","EVGO","CHPT","BLNK","PLUG","BE","FCEL","NKLA",
    "RXRX","BEAM","CRSP","EDIT","NTLA","PACB","ARWR","KYMR","VKTX","NVCR",
    "AMBA","LSCC","SITM","POWI","AOSL","ONTO","ACLS","ICHR","KLIC","MTSI",
    "BROS","SHAK","WING","TXRH","XPOF","FRPT","YETI","BRZE","SMAR","ASAN",
    "MNDY","GTLB","PCVX","RELY","TASK","ALVO","KTOS","AVAV","HLIT","PRFT",
    "CWAN","ALKT","RBRK","AEHR","EVTC","RSKD","IDCC","AEIS","NRDS","GFAI",
]))[:180]

TICKERS_SP500 = list(dict.fromkeys([
    "BRK-B","JPM","V","UNH","XOM","JNJ","WMT","MA","PG","HD",
    "CVX","MRK","ABBV","BAC","KO","LLY","TMO","MCD","CRM","ACN",
    "ABT","DHR","NKE","NEE","WFC","PM","T","UPS","MS","RTX",
    "SPGI","BMY","CAT","GS","BLK","SYK","AXP","C","CB","MO",
    "ZTS","CVS","SO","DUK","PLD","TGT","MMC","CI","ITW","HUM",
    "USB","EMR","NSC","AON","EL","HCA","PSA","MCK","WM","ADM",
    "SHW","FCX","ECL","TRV","APD","COF","EW","CARR","IQV","BDX",
    "SPG","GD","NOC","AIG","WELL","CME","MPC","VLO","CCI","CBRE",
    "STZ","YUM","ROP","KEYS","AWK","FIS","LHX","DG","CTVA","TDG",
    "LOW","INTC","IBM","GE","F","GM","PFE","LIN","PEP","CSCO",
    "MMM","AFL","ALB","AMP","AMT","AZO","BAX","BEN","BSX","BWA",
    "CAG","CAH","CE","CF","CHD","CHRW","CINF","CL","CLX","CMA",
    "CMS","CNP","COO","CPB","CSX","CTLT","D","DAL","DD","DE",
    "DFS","DGX","DHI","DIS","DLTR","DOV","DRI","DTE","DVA","DVN",
    "EA","ED","EFX","EIX","EMN","EOG","EQIX","EQR","ES","ESS",
    "ETN","ETR","EVRG","EXC","EXR","EXPD","EXPE","FDS","FDX","FE",
    "FFIV","FLT","FMC","FOX","FOXA","FRT","FTV","GIS","GL","GPC",
    "HIG","HON","HPQ","HSY","HUM","ICE","IFF","IP","IPG","IRM",
]))[:180]

TICKERS_KOSPI = list(dict.fromkeys([
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
    "011070.KS","161390.KS","009830.KS","000720.KS","002790.KS",
    "008770.KS","010060.KS","001800.KS","004000.KS","006260.KS",
    "009540.KS","000150.KS","003490.KS","005300.KS","007070.KS",
    "007310.KS","008060.KS","009240.KS","010620.KS","011790.KS",
    "012030.KS","014820.KS","015350.KS","017550.KS","018880.KS",
    "023530.KS","024110.KS","025560.KS","026960.KS","028050.KS",
    "029780.KS","030000.KS","031430.KS","032640.KS","033240.KS",
    "034020.KS","034220.KS","036460.KS","037270.KS","042670.KS",
    "042700.KS","044490.KS","047040.KS","051600.KS","051900.KS",
    "055490.KS","057050.KS","064350.KS","069260.KS","071050.KS",
    "078930.KS","079550.KS","081660.KS","086280.KS","088350.KS",
    "090350.KS","096400.KS","103140.KS","108670.KS","180640.KS",
    "185750.KS","192080.KS","267250.KS","272210.KS","278280.KS",
    "316140.KS","323410.KS","352820.KS","377300.KS","000240.KS",
    "000390.KS","000670.KS","000880.KS","001230.KS","001450.KS",
    "001740.KS","002320.KS","002350.KS","002820.KS","003070.KS",
    "003240.KS","003580.KS","004140.KS","004370.KS","004490.KS",
    "004990.KS","005010.KS","005160.KS","005440.KS","005850.KS",
    "006050.KS","006360.KS","006650.KS","007160.KS","007340.KS",
    "008300.KS","008350.KS","008490.KS","009680.KS","009770.KS",
    "010040.KS","010140.KS","010580.KS","010780.KS","011080.KS",
    "011420.KS","011760.KS","012450.KS","012750.KS","013360.KS",
    "014680.KS","015760.KS","016380.KS","017040.KS","017180.KS",
    "018120.KS","019170.KS","020150.KS","021080.KS","022000.KS",
    "024090.KS","025860.KS","027740.KS","029530.KS","030190.KS",
    "032560.KS","033530.KS","034730.KS","036570.KS","037560.KS",
]))[:180]

TICKERS_KOSDAQ = list(dict.fromkeys([
    "247540.KQ","086520.KQ",
    "068760.KQ","091990.KQ",
    "196170.KQ","096530.KQ","145020.KQ","009420.KQ","048410.KQ",
    "237690.KQ","088290.KQ","058850.KQ","039200.KQ","031370.KQ",
    "039030.KQ","357780.KQ","084370.KQ","064760.KQ","095340.KQ",
    "022100.KQ","058970.KQ","214150.KQ","151910.KQ","042700.KQ",
    "078070.KQ","036540.KQ","114840.KQ","101490.KQ","126340.KQ",
    "112610.KQ","140860.KQ","323280.KQ","232140.KQ","065510.KQ",
    "035900.KQ","041510.KQ","122870.KQ","263750.KQ","293490.KQ",
    "112040.KQ","036570.KQ","067160.KQ","095660.KQ","066430.KQ",
    "241560.KQ","179900.KQ","950130.KQ","082270.KQ","091120.KQ",
    "070300.KQ","086040.KQ","039440.KQ","053160.KQ","191410.KQ",
    "228760.KQ","251970.KQ","256840.KQ","319400.KQ","298540.KQ",
    "204840.KQ","036830.KQ","357120.KQ","048910.KQ","060310.KQ",
]))

TICKERS_US = list(dict.fromkeys(TICKERS_NASDAQ + TICKERS_SP500))
TICKERS_KR = list(dict.fromkeys(TICKERS_KOSPI + TICKERS_KOSDAQ))

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
    "011070.KS":"LG이노텍","161390.KS":"한국타이어앤테크놀로지",
    "009830.KS":"한화솔루션","000720.KS":"현대건설","002790.KS":"아모레퍼시픽",
    "008770.KS":"호텔신라","010060.KS":"OCI홀딩스","004000.KS":"롯데정밀화학",
    "006260.KS":"LS","009540.KS":"한진칼","000150.KS":"두산",
    "003490.KS":"대한항공","005300.KS":"롯데칠성","007070.KS":"GS리테일",
    "007310.KS":"오뚜기","009240.KS":"한샘","011790.KS":"SKC",
    "012030.KS":"DB손해보험","023530.KS":"롯데쇼핑","024110.KS":"기업은행",
    "025560.KS":"메리츠화재","028050.KS":"삼성엔지니어링","034020.KS":"두산에너빌리티",
    "034220.KS":"LG디스플레이","036460.KS":"한국가스공사","037270.KS":"YG엔터테인먼트",
    "042670.KS":"HD현대인프라코어","042700.KS":"한미반도체","047040.KS":"대우건설",
    "051600.KS":"한전KPS","051900.KS":"LG생활건강","064350.KS":"현대로템",
    "078930.KS":"GS","079550.KS":"LIG넥스원","086280.KS":"현대글로비스",
    "088350.KS":"한화생명","096400.KS":"BNK금융지주","103140.KS":"풍산",
    "180640.KS":"한진칼","185750.KS":"종근당","267250.KS":"HD현대",
    "272210.KS":"한화시스템","278280.KS":"천보","316140.KS":"우리금융지주",
    "323410.KS":"카카오뱅크","352820.KS":"하이브","377300.KS":"카카오페이",
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
    "068760.KQ":"셀트리온제약","091120.KQ":"레인보우로보틱스","009420.KQ":"한올바이오파마",
    "112610.KQ":"씨에스윈드","082270.KQ":"뉴트리","086040.KQ":"JW중외제약",
    "095660.KQ":"네오위즈","066430.KQ":"티에스이","070300.KQ":"엑스큐어",
}

TICKERS_KR_BT = TICKERS_KR[:50]

SECTOR_ETFS = [
    {"ticker":"XLK","name":"기술","name_en":"Technology","emoji":"💻"},
    {"ticker":"XLV","name":"헬스케어","name_en":"Health Care","emoji":"🏥"},
    {"ticker":"XLF","name":"금융","name_en":"Financials","emoji":"🏦"},
    {"ticker":"XLE","name":"에너지","name_en":"Energy","emoji":"⚡"},
    {"ticker":"XLY","name":"경기소비재","name_en":"Consumer Discretionary","emoji":"🛍"},
    {"ticker":"XLP","name":"필수소비재","name_en":"Consumer Staples","emoji":"🛒"},
    {"ticker":"XLB","name":"소재","name_en":"Materials","emoji":"⚙️"},
    {"ticker":"XLC","name":"통신서비스","name_en":"Communication Services","emoji":"📡"},
    {"ticker":"XLI","name":"산업재","name_en":"Industrials","emoji":"🏭"},
    {"ticker":"XLU","name":"유틸리티","name_en":"Utilities","emoji":"💡"},
    {"ticker":"XLRE","name":"부동산","name_en":"Real Estate","emoji":"🏢"},
]

SECTOR_TO_ETF = {
    "Technology":"XLK","Healthcare":"XLV","Health Care":"XLV",
    "Financial Services":"XLF","Financials":"XLF","Energy":"XLE",
    "Consumer Cyclical":"XLY","Consumer Discretionary":"XLY",
    "Consumer Defensive":"XLP","Consumer Staples":"XLP",
    "Basic Materials":"XLB","Materials":"XLB",
    "Communication Services":"XLC","Industrials":"XLI",
    "Utilities":"XLU","Real Estate":"XLRE",
}

# ══════════════════════════════════════════════════════════════
# 점수 계산 함수
# ══════════════════════════════════════════════════════════════

def score_ema_slope(hist_weekly):
    try:
        if len(hist_weekly) < 30:
            return 0
        ema = hist_weekly['Close'].ewm(span=26).mean()
        slopes = []
        for i in range(1, 5):
            if len(ema) > i:
                s = (ema.iloc[-i] - ema.iloc[-i-1]) / ema.iloc[-i-1] * 100
                slopes.append(s)
        if not slopes:
            return 0
        avg = sum(slopes) / len(slopes)
        if avg >= 1.0:    return 10
        elif avg >= 0.5:  return 8
        elif avg >= 0.2:  return 6
        elif avg >= 0.05: return 4
        elif avg >= 0.0:  return 2
        else:             return 0
    except:
        return 0

def score_stochastic(hist_daily):
    try:
        if len(hist_daily) < 14:
            return 0
        ll = hist_daily['Low'].rolling(14).min()
        hh = hist_daily['High'].rolling(14).max()
        denom = hh - ll
        if denom.iloc[-1] == 0:
            return 0
        k = float(100 * (hist_daily['Close'].iloc[-1] - ll.iloc[-1]) / denom.iloc[-1])
        if 20 <= k <= 40:    return 10
        elif 40 < k <= 50:   return 8
        elif 15 <= k < 20:   return 7
        elif 50 < k <= 65:   return 5
        elif 10 <= k < 15:   return 4
        elif 65 < k <= 80:   return 3
        elif k > 80:         return 1
        else:                return 2
    except:
        return 0

def score_breakout(hist_daily):
    try:
        if len(hist_daily) < 3:
            return 0
        prev_high = float(hist_daily['High'].iloc[-2])
        latest    = float(hist_daily['Close'].iloc[-1])
        ratio = latest / prev_high
        if ratio >= 1.01:    return 10
        elif ratio >= 1.002: return 8
        elif ratio >= 0.998: return 6
        elif ratio >= 0.99:  return 4
        elif ratio >= 0.97:  return 2
        else:                return 0
    except:
        return 0

def calculate_classic_score(info, hist_weekly, hist_daily):
    s1 = score_ema_slope(hist_weekly)
    s2 = score_stochastic(hist_daily)
    s3 = score_breakout(hist_daily)
    if s1 == 0:
        s2 = s2 // 2
        s3 = s3 // 2
    return s1 + s2 + s3

def score_roe(roe):
    if roe >= 0.30:    return 10
    elif roe >= 0.20:  return 8
    elif roe >= 0.15:  return 6
    elif roe >= 0.10:  return 3
    elif roe >= 0.05:  return 1
    else:              return 0

def score_debt(debt_equity):
    if debt_equity <= 0:     return 5
    elif debt_equity <= 30:  return 5
    elif debt_equity <= 60:  return 4
    elif debt_equity <= 100: return 3
    elif debt_equity <= 150: return 2
    elif debt_equity <= 200: return 1
    else:                    return 0

def score_eps_growth(info):
    quarterly = info.get('earningsQuarterlyGrowth', None)
    annual    = info.get('earningsGrowth', None)
    if quarterly is None and annual is None:
        return 0
    primary = quarterly if quarterly is not None else annual
    primary = primary if primary is not None else 0
    if primary >= 0.40:    base = 10
    elif primary >= 0.25:  base = 8
    elif primary >= 0.15:  base = 6
    elif primary >= 0.10:  base = 4
    elif primary >= 0.05:  base = 2
    elif primary > 0:      base = 1
    else:                  base = 0
    if quarterly is not None and annual is not None:
        if quarterly > annual + 0.05:
            base = min(base + 1, 10)
    return base

def score_peg(peg):
    if peg is None or peg <= 0 or peg >= 50:
        return 2
    elif peg <= 0.8:  return 5
    elif peg <= 1.0:  return 4
    elif peg <= 1.5:  return 3
    elif peg <= 2.0:  return 2
    elif peg <= 3.0:  return 1
    else:             return 0

def score_ma200(hist_daily):
    try:
        if len(hist_daily) < 200:
            return 0
        ma200   = float(hist_daily['Close'].rolling(200).mean().iloc[-1])
        current = float(hist_daily['Close'].iloc[-1])
        ratio   = current / ma200
        if ratio >= 1.20:    return 5
        elif ratio >= 1.10:  return 4
        elif ratio >= 1.03:  return 3
        elif ratio >= 1.00:  return 2
        elif ratio >= 0.95:  return 1
        else:                return 0
    except:
        return 0

def score_rsi(hist_daily):
    try:
        if len(hist_daily) < 14:
            return 0
        rsi = float(ta.momentum.RSIIndicator(hist_daily['Close'], window=14).rsi().iloc[-1])
        if 50 <= rsi <= 65:    return 5
        elif 40 <= rsi < 50:   return 4
        elif 65 < rsi <= 75:   return 3
        elif 30 <= rsi < 40:   return 2
        elif 75 < rsi <= 80:   return 1
        else:                  return 0
    except:
        return 0

def calculate_growth_score(info, hist_daily):
    s1 = score_roe(info.get('returnOnEquity', 0) or 0)
    s2 = score_debt(info.get('debtToEquity', 999) or 999)
    s3 = score_eps_growth(info)
    s4 = score_peg(info.get('pegRatio', None))
    s5 = score_ma200(hist_daily)
    s6 = score_rsi(hist_daily)
    return s1 + s2 + s3 + s4 + s5 + s6

def score_analyst(rec):
    if not rec:
        return 0
    mapping = {
        'strong_buy':   10,
        'buy':          7,
        'hold':         4,
        'underperform': 1,
        'sell':         0,
    }
    return mapping.get(rec.lower(), 0)

def score_relative_strength(hist_daily):
    try:
        n = min(len(hist_daily) - 1, 252)
        if n < 60:
            return 0
        stock_ret = float((hist_daily['Close'].iloc[-1] / hist_daily['Close'].iloc[-n] - 1) * 100)
        spy_adj   = 10.0 * (n / 252)
        excess    = stock_ret - spy_adj
        if excess >= 30:    return 10
        elif excess >= 20:  return 8
        elif excess >= 10:  return 6
        elif excess >= 0:   return 4
        elif excess >= -10: return 2
        else:               return 0
    except:
        return 0

def score_obv_momentum(hist_daily):
    try:
        if len(hist_daily) < 20:
            return 0
        close  = hist_daily['Close']
        volume = hist_daily['Volume']
        obv = [0]
        for i in range(1, len(close)):
            if close.iloc[i] > close.iloc[i-1]:
                obv.append(obv[-1] + volume.iloc[i])
            elif close.iloc[i] < close.iloc[i-1]:
                obv.append(obv[-1] - volume.iloc[i])
            else:
                obv.append(obv[-1])
        obv_series = pd.Series(obv, index=close.index)
        obv_ma5    = obv_series.iloc[-5:].mean()
        obv_ma20   = obv_series.iloc[-20:].mean()
        if obv_ma20 == 0:
            return 0
        ratio      = obv_ma5 / obv_ma20
        obv_prev5  = obv_series.iloc[-10:-5].mean()
        obv_curr5  = obv_series.iloc[-5:].mean()
        rising     = obv_curr5 > obv_prev5
        if ratio >= 1.3 and rising:    return 10
        elif ratio >= 1.1 and rising:  return 8
        elif ratio >= 1.0 and rising:  return 6
        elif ratio >= 1.0:             return 4
        elif ratio >= 0.9:             return 2
        else:                          return 0
    except:
        return 0

def calculate_modern_score(info, hist_daily):
    rec = info.get('recommendationKey', '') or ''
    s1  = score_analyst(rec)
    s2  = score_relative_strength(hist_daily)
    s3  = score_obv_momentum(hist_daily)
    return s1 + s2 + s3

def get_recommendation(total_score, classic=None, growth=None, modern=None):
    if classic is not None and growth is not None and modern is not None:
        if classic < 10 or growth < 15 or modern < 8:
            if total_score >= 70:
                return "Hold"
            elif total_score >= 55:
                return "Watch"
            else:
                return "Watch"
    if total_score >= 70:    return "Strong Buy"
    elif total_score >= 55:  return "Buy"
    elif total_score >= 40:  return "Hold"
    else:                    return "Watch"

def get_portfolio_weight(results):
    buy_stocks  = [r for r in results if r['recommendation'] in ['Strong Buy', 'Buy']]
    total_score = sum(r['total_score'] for r in buy_stocks)
    for r in results:
        if r['recommendation'] in ['Strong Buy', 'Buy'] and total_score > 0:
            r['weight'] = round((r['total_score'] / total_score) * 100, 1)
        else:
            r['weight'] = 0
    return results

def fetch_single_stock(ticker, market):
    try:
        stock       = yf.Ticker(ticker)
        info        = stock.info
        hist_daily  = stock.history(period="1y")
        hist_weekly = stock.history(period="2y", interval="1wk")
        if hist_daily.empty or len(hist_daily) < 20:
            return None

        c_ema   = score_ema_slope(hist_weekly)
        c_stoch = score_stochastic(hist_daily)
        c_break = score_breakout(hist_daily)
        classic = c_ema + (c_stoch//2 if c_ema==0 else c_stoch) + (c_break//2 if c_ema==0 else c_break)

        g_roe   = score_roe(info.get('returnOnEquity', 0) or 0)
        g_debt  = score_debt(info.get('debtToEquity', 999) or 999)
        g_eps   = score_eps_growth(info)
        g_peg   = score_peg(info.get('pegRatio', None))
        g_ma200 = score_ma200(hist_daily)
        g_rsi   = score_rsi(hist_daily)
        growth  = g_roe + g_debt + g_eps + g_peg + g_ma200 + g_rsi

        m_anal  = score_analyst(info.get('recommendationKey','') or '')
        m_rs    = score_relative_strength(hist_daily)
        m_obv   = score_obv_momentum(hist_daily)
        modern  = m_anal + m_rs + m_obv
        total   = classic + growth + modern

        current_price = float(hist_daily['Close'].iloc[-1])
        prev_price    = float(hist_daily['Close'].iloc[-2])
        change_pct    = (current_price / prev_price - 1) * 100

        rsi_val = 0.0
        if len(hist_daily) >= 14:
            rsi_val = round(float(ta.momentum.RSIIndicator(hist_daily['Close'], window=14).rsi().iloc[-1]), 1)

        name   = KR_NAMES.get(ticker) or info.get('longName', ticker)
        sector = info.get('sector') or 'Unknown'

        return {
            "ticker":         ticker,
            "name":           name,
            "sector":         sector,
            "etf":            SECTOR_TO_ETF.get(sector, ""),
            "price":          round(current_price, 2),
            "change_pct":     round(change_pct, 2),
            "classic_score":  classic,
            "growth_score":   growth,
            "modern_score":   modern,
            "total_score":    total,
            "recommendation": get_recommendation(total, classic, growth, modern),
            "weight":         0,
            "rsi":            rsi_val,
            "roe":            round((info.get('returnOnEquity', 0) or 0) * 100, 1),
            "peg":            round(info.get('pegRatio', 0) or 0, 2),
            "c_ema":    c_ema,   "c_stoch": c_stoch, "c_break": c_break,
            "g_roe":    g_roe,   "g_debt":  g_debt,  "g_eps":   g_eps,
            "g_peg":    g_peg,   "g_ma200": g_ma200, "g_rsi":   g_rsi,
            "m_anal":   m_anal,  "m_rs":    m_rs,    "m_obv":   m_obv,
        }
    except:
        return None

# ══════════════════════════════════════════════════════════════
# 텐배거 스크리너
# ══════════════════════════════════════════════════════════════

def score_lynch(info, hist_daily):
    score = 0
    detail = {}
    peg = info.get('pegRatio', 999) or 999
    if peg <= 0:          peg_score = 0
    elif peg <= 0.5:      peg_score = 15
    elif peg <= 0.75:     peg_score = 12
    elif peg <= 1.0:      peg_score = 9
    elif peg <= 1.5:      peg_score = 5
    elif peg <= 2.0:      peg_score = 2
    else:                 peg_score = 0
    score += peg_score
    detail['peg'] = round(peg if peg != 999 else 0, 2)
    detail['peg_score'] = peg_score

    eps_growth = info.get('earningsGrowth', 0) or 0
    if eps_growth >= 0.50:   eps_score = 10
    elif eps_growth >= 0.35: eps_score = 8
    elif eps_growth >= 0.25: eps_score = 6
    elif eps_growth >= 0.15: eps_score = 3
    else:                    eps_score = 0
    score += eps_score
    detail['eps_growth'] = round(eps_growth * 100, 1)
    detail['eps_score']  = eps_score

    mcap   = info.get('marketCap', 0) or 0
    mcap_b = mcap / 1e9
    if mcap_b <= 0:        mcap_score = 0
    elif mcap_b <= 0.3:    mcap_score = 10
    elif mcap_b <= 2.0:    mcap_score = 8
    elif mcap_b <= 10.0:   mcap_score = 5
    elif mcap_b <= 50.0:   mcap_score = 2
    else:                  mcap_score = 0
    score += mcap_score
    detail['market_cap_b'] = round(mcap_b, 1)
    detail['mcap_score']   = mcap_score
    return score, detail

def score_oneil(info, hist_daily, hist_weekly):
    score = 0
    detail = {}
    eps_growth = info.get('earningsGrowth', 0) or 0
    quarterly  = info.get('earningsQuarterlyGrowth', 0) or 0
    c_val = max(eps_growth, quarterly)
    if c_val >= 0.50:   c_score = 10
    elif c_val >= 0.35: c_score = 8
    elif c_val >= 0.25: c_score = 6
    elif c_val >= 0.15: c_score = 3
    else:               c_score = 0
    score += c_score
    detail['quarterly_eps_growth'] = round(c_val * 100, 1)
    detail['c_score'] = c_score

    try:
        if len(hist_daily) >= 252:
            high_52w  = float(hist_daily['High'].rolling(252).max().iloc[-1])
            current   = float(hist_daily['Close'].iloc[-1])
            from_high = (current / high_52w - 1) * 100
            detail['from_52w_high'] = round(from_high, 1)
            if from_high >= 0:      n_score = 10
            elif from_high >= -3:   n_score = 8
            elif from_high >= -8:   n_score = 5
            elif from_high >= -15:  n_score = 2
            else:                   n_score = 0
        else:
            n_score = 0
            detail['from_52w_high'] = 0
    except:
        n_score = 0
        detail['from_52w_high'] = 0
    score += n_score
    detail['n_score'] = n_score

    try:
        if len(hist_daily) >= 50:
            avg_50d   = float(hist_daily['Volume'].rolling(50).mean().iloc[-1])
            recent_5  = float(hist_daily['Volume'].iloc[-5:].mean())
            vol_ratio = recent_5 / avg_50d if avg_50d > 0 else 1.0
            detail['vol_ratio_50d'] = round(vol_ratio, 2)
            if vol_ratio >= 2.5:    s_score = 10
            elif vol_ratio >= 2.0:  s_score = 8
            elif vol_ratio >= 1.5:  s_score = 6
            elif vol_ratio >= 1.2:  s_score = 3
            else:                   s_score = 0
        else:
            s_score = 0
            detail['vol_ratio_50d'] = 0
    except:
        s_score = 0
        detail['vol_ratio_50d'] = 0
    score += s_score
    detail['s_score'] = s_score

    try:
        rec = info.get('recommendationKey', '') or ''
        if rec in ['strong_buy']:  l_score = 5
        elif rec in ['buy']:       l_score = 3
        else:                      l_score = 0
    except:
        l_score = 0
    score += l_score
    detail['l_score'] = l_score
    return score, detail

def score_minervini(info, hist_daily):
    score = 0
    detail = {}
    try:
        close   = hist_daily['Close']
        current = float(close.iloc[-1])
        ma150 = ma200 = None
        if len(hist_daily) >= 200:
            ma150 = float(close.rolling(150).mean().iloc[-1])
            ma200 = float(close.rolling(200).mean().iloc[-1])
            detail['ma150'] = round(ma150, 2)
            detail['ma200'] = round(ma200, 2)
            above_both = (current > ma150) and (current > ma200)
            if above_both:
                avg_ma = (ma150 + ma200) / 2
                ratio  = current / avg_ma
                if ratio >= 1.15:   cond1 = 8
                elif ratio >= 1.08: cond1 = 6
                elif ratio >= 1.02: cond1 = 4
                else:               cond1 = 2
            else:
                cond1 = 0
        else:
            cond1 = 0
            detail['ma150'] = 0
            detail['ma200'] = 0
        score += cond1
        detail['above_ma_score'] = cond1

        if ma150 and ma200:
            if ma150 > ma200:
                ratio_ma = ma150 / ma200
                if ratio_ma >= 1.05:   cond2 = 7
                elif ratio_ma >= 1.02: cond2 = 5
                else:                  cond2 = 3
            else:
                cond2 = 0
        else:
            cond2 = 0
        score += cond2
        detail['ma_cross_score'] = cond2

        if len(hist_daily) >= 220:
            ma200_1m_ago = float(close.rolling(200).mean().iloc[-22])
            if ma200 and ma200 > ma200_1m_ago:
                slope_pct = (ma200 / ma200_1m_ago - 1) * 100
                if slope_pct >= 3.0:   cond3 = 7
                elif slope_pct >= 1.5: cond3 = 5
                elif slope_pct >= 0.5: cond3 = 3
                else:                  cond3 = 1
            else:
                cond3 = 0
        else:
            cond3 = 0
        score += cond3
        detail['ma200_slope_score'] = cond3

        if len(hist_daily) >= 252:
            low_52w  = float(hist_daily['Low'].rolling(252).min().iloc[-1])
            from_low = (current / low_52w - 1) * 100
            detail['from_52w_low'] = round(from_low, 1)
            if from_low >= 100:   cond4 = 8
            elif from_low >= 60:  cond4 = 6
            elif from_low >= 30:  cond4 = 4
            elif from_low >= 15:  cond4 = 2
            else:                 cond4 = 0
        else:
            cond4 = 0
            detail['from_52w_low'] = 0
        score += cond4
        detail['from_low_score'] = cond4
    except:
        detail = {'above_ma_score':0,'ma_cross_score':0,'ma200_slope_score':0,'from_low_score':0}
    return score, detail

def fetch_tenbagger_stock(ticker):
    try:
        stock        = yf.Ticker(ticker)
        info         = stock.info
        hist_daily   = stock.history(period="1y")
        hist_weekly  = stock.history(period="2y", interval="1wk")
        if hist_daily.empty or len(hist_daily) < 60:
            return None
        price_check = info.get('regularMarketPrice') or info.get('currentPrice') or info.get('previousClose')
        if not price_check:
            return None

        lynch_score,     lynch_detail     = score_lynch(info, hist_daily)
        oneil_score,     oneil_detail     = score_oneil(info, hist_daily, hist_weekly)
        minervini_score, minervini_detail = score_minervini(info, hist_daily)
        total = lynch_score + oneil_score + minervini_score

        current_price = float(hist_daily['Close'].iloc[-1])
        prev_price    = float(hist_daily['Close'].iloc[-2])
        change_pct    = (current_price / prev_price - 1) * 100

        if total >= 75:   grade = "🔥 최상위"
        elif total >= 60: grade = "⭐ 유망"
        elif total >= 45: grade = "👀 관심"
        else:             grade = "💤 미해당"

        mcap = info.get('marketCap', 0) or 0
        return {
            "ticker":           ticker,
            "name":             info.get('longName', ticker),
            "sector":           info.get('sector', 'Unknown'),
            "market_cap_b":     round(mcap / 1e9, 1),
            "price":            round(current_price, 2),
            "change_pct":       round(change_pct, 2),
            "lynch_score":      lynch_score,
            "oneil_score":      oneil_score,
            "minervini_score":  minervini_score,
            "total_score":      total,
            "grade":            grade,
            "lynch_detail":     lynch_detail,
            "oneil_detail":     oneil_detail,
            "minervini_detail": minervini_detail,
        }
    except:
        return None

TICKERS_TENBAGGER = list(dict.fromkeys([
    "PLTR","AI","SOUN","BBAI","RBRK","CWAN","ALKT","AEIS",
    "AMBA","LSCC","SITM","ONTO","ACLS","ICHR","KLIC","MTSI",
    "AFRM","UPST","BILL","TOST","GTLB","DDOG","ZS","DUOL",
    "HIMS","RDDT","APP","SMAR","ASAN","MNDY","RELY","BRZE",
    "RXRX","BEAM","CRSP","ARWR","KYMR","VKTX","NVCR","INSM",
    "FSLR","ENPH","ARRY","RKLB","ASTS","JOBY","ACHR",
    "CELH","BROS","CAVA","WING","FRPT","YETI",
]))

# ══════════════════════════════════════════════════════════════
# ★ 유동성 모듈 — FRED API 기반
# ══════════════════════════════════════════════════════════════

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
FRED_BASE    = "https://api.stlouisfed.org/fred/series/observations"

def fetch_fred(series_id: str, limit: int = 20) -> list:
    if not FRED_API_KEY:
        return []
    try:
        params = {
            "series_id":        series_id,
            "api_key":          FRED_API_KEY,
            "file_type":        "json",
            "sort_order":       "desc",
            "limit":            limit,
            "observation_start": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
        }
        resp = requests.get(FRED_BASE, params=params, timeout=10)
        if resp.status_code != 200:
            return []
        data = resp.json().get("observations", [])
        result = []
        for obs in data:
            if obs["value"] != ".":
                result.append({"date": obs["date"], "value": float(obs["value"])})
        return result
    except:
        return []

def score_fed_balance(data: list) -> dict:
    label   = "연준 총자산"
    fred_id = "WALCL"
    if len(data) < 2:
        return {"label":label,"fred_id":fred_id,"score":12,"status":"데이터 없음",
                "value":0,"value_unit":"조 달러","change_pct":0,"history":[]}
    latest = data[0]["value"]
    prev4w = data[4]["value"] if len(data) > 4 else data[-1]["value"]
    change_pct = round((latest - prev4w) / prev4w * 100, 3) if prev4w else 0
    if change_pct >= 1.0:      score, status = 25, "강한 QE — 유동성 최대 공급"
    elif change_pct >= 0.3:    score, status = 20, "QE 진행 — 유동성 공급 중"
    elif change_pct >= 0.0:    score, status = 15, "보합 — 유동성 유지"
    elif change_pct >= -0.3:   score, status = 10, "소폭 QT — 유동성 소폭 감소"
    elif change_pct >= -1.0:   score, status = 5,  "QT 진행 — 유동성 회수 중"
    else:                      score, status = 0,  "강한 QT — 유동성 급속 회수"
    return {
        "label": label, "fred_id": fred_id, "score": score, "max_score": 25,
        "status": status, "value": round(latest / 1e6, 2), "value_unit": "조 달러",
        "change_pct": change_pct,
        "history": [{"date": d["date"], "value": round(d["value"] / 1e6, 2)} for d in data[:8]],
    }

def score_rrp(data: list) -> dict:
    label   = "역레포(RRP) 잔액"
    fred_id = "RRPONTSYD"
    if len(data) < 2:
        return {"label":label,"fred_id":fred_id,"score":12,"status":"데이터 없음",
                "value":0,"value_unit":"조 달러","change_pct":0,"history":[]}
    latest = data[0]["value"]
    prev4w = data[4]["value"] if len(data) > 4 else data[-1]["value"]
    change_pct = round((latest - prev4w) / prev4w * 100, 2) if prev4w else 0
    if change_pct <= -30:      score, status = 25, "RRP 급감 — 대규모 유동성 시장 유입"
    elif change_pct <= -10:    score, status = 20, "RRP 감소 — 유동성 시장 유입 중"
    elif change_pct <= 0:      score, status = 15, "RRP 보합/소폭 감소 — 중립"
    elif change_pct <= 10:     score, status = 10, "RRP 소폭 증가 — 유동성 소폭 흡수"
    elif change_pct <= 30:     score, status = 5,  "RRP 증가 — 유동성 흡수 중"
    else:                      score, status = 0,  "RRP 급증 — 유동성 대규모 흡수"
    if latest < 50000:
        score = min(score + 3, 25)
        status += " (RRP 거의 소진)"
    return {
        "label": label, "fred_id": fred_id, "score": score, "max_score": 25,
        "status": status, "value": round(latest / 1e6, 3), "value_unit": "조 달러",
        "change_pct": change_pct,
        "history": [{"date": d["date"], "value": round(d["value"] / 1e6, 3)} for d in data[:8]],
    }

def score_tga(data: list) -> dict:
    label   = "TGA(재무부 계정) 잔액"
    fred_id = "WTREGEN"
    if len(data) < 2:
        return {"label":label,"fred_id":fred_id,"score":12,"status":"데이터 없음",
                "value":0,"value_unit":"조 달러","change_pct":0,"history":[]}
    latest = data[0]["value"]
    prev4w = data[4]["value"] if len(data) > 4 else data[-1]["value"]
    change_pct = round((latest - prev4w) / prev4w * 100, 2) if prev4w else 0
    if change_pct <= -20:      score, status = 25, "TGA 급감 — 정부 대규모 지출, 유동성 공급"
    elif change_pct <= -5:     score, status = 20, "TGA 감소 — 정부 지출, 유동성 유입"
    elif change_pct <= 5:      score, status = 13, "TGA 보합 — 중립"
    elif change_pct <= 20:     score, status = 7,  "TGA 증가 — 세금 흡수, 유동성 축소"
    else:                      score, status = 0,  "TGA 급증 — 대규모 채권 발행, 유동성 흡수"
    return {
        "label": label, "fred_id": fred_id, "score": score, "max_score": 25,
        "status": status, "value": round(latest / 1e6, 3), "value_unit": "조 달러",
        "change_pct": change_pct,
        "history": [{"date": d["date"], "value": round(d["value"] / 1e6, 3)} for d in data[:8]],
    }

def score_mmf(data: list) -> dict:
    label   = "MMF 총잔액"
    fred_id = "WRMFNS"
    if len(data) < 2:
        return {"label":label,"fred_id":fred_id,"score":12,"status":"데이터 없음",
                "value":0,"value_unit":"조 달러","change_pct":0,"history":[]}
    latest = data[0]["value"]
    prev4w = data[4]["value"] if len(data) > 4 else data[-1]["value"]
    change_pct = round((latest - prev4w) / prev4w * 100, 2) if prev4w else 0
    if change_pct <= -2.0:     score, status = 25, "MMF 급감 — 위험자산 선호, 자금 유입"
    elif change_pct <= -0.5:   score, status = 20, "MMF 감소 — 위험선호 전환 중"
    elif change_pct <= 0.5:    score, status = 13, "MMF 보합 — 관망 중립"
    elif change_pct <= 2.0:    score, status = 7,  "MMF 증가 — 안전자산 선호"
    else:                      score, status = 0,  "MMF 급증 — 강한 위험회피, 자금 이탈"
    return {
        "label": label, "fred_id": fred_id, "score": score, "max_score": 25,
        "status": status, "value": round(latest / 1000, 2), "value_unit": "조 달러",
        "change_pct": change_pct,
        "history": [{"date": d["date"], "value": round(d["value"] / 1000, 2)} for d in data[:8]],
    }

def get_liquidity_signal(total_score: int) -> dict:
    if total_score >= 80:
        return {
            "stage": 1, "signal": "적극매수", "emoji": "🟢",
            "color": "#15803d", "bg_color": "#dcfce7", "border_color": "#86efac",
            "description": "유동성이 최대로 공급되고 있습니다. 시장에 돈이 넘칩니다.",
            "action": "STEP 2 스크리닝 결과를 적극 반영하세요. 마구스코어 65점+ 종목 매수 고려.",
            "step2_guide": "✅ 스크리닝 신호 적극 반영 — 분할 매수 진입",
        }
    elif total_score >= 60:
        return {
            "stage": 2, "signal": "매수우호", "emoji": "🔵",
            "color": "#1d4ed8", "bg_color": "#dbeafe", "border_color": "#93c5fd",
            "description": "유동성이 양호합니다. 시장 환경이 매수에 우호적입니다.",
            "action": "STEP 2 스크리닝 결과를 참고하여 선별적으로 매수하세요.",
            "step2_guide": "✅ 스크리닝 신호 참고 — 마구스코어 70점+ 종목 위주로 선별 매수",
        }
    elif total_score >= 40:
        return {
            "stage": 3, "signal": "중립관망", "emoji": "🟡",
            "color": "#b45309", "bg_color": "#fef9c3", "border_color": "#fde68a",
            "description": "유동성 방향이 불확실합니다. 추세 확인이 필요합니다.",
            "action": "신규 매수 자제. 기존 포지션 유지하며 방향 확인 후 판단하세요.",
            "step2_guide": "⚠️ 스크리닝 참고만 — 신규 매수 자제, 기존 보유 종목 유지",
        }
    elif total_score >= 20:
        return {
            "stage": 4, "signal": "매수축소", "emoji": "🟠",
            "color": "#c2410c", "bg_color": "#ffedd5", "border_color": "#fdba74",
            "description": "유동성이 감소하고 있습니다. 위험 관리가 필요합니다.",
            "action": "신규 매수 중단. 보유 종목 비중 축소 및 손절 기준 점검하세요.",
            "step2_guide": "🚫 스크리닝 결과 무시 — 포지션 축소, 현금 비중 확대",
        }
    else:
        return {
            "stage": 5, "signal": "현금보유", "emoji": "🔴",
            "color": "#991b1b", "bg_color": "#fee2e2", "border_color": "#fca5a5",
            "description": "유동성이 심각하게 경색되어 있습니다. 시장 위험이 높습니다.",
            "action": "전량 현금 보유 권고. 스크리닝 결과와 무관하게 매수 금지.",
            "step2_guide": "🔴 스크리닝 결과 무시 — 전량 현금 보유, 매수 금지",
        }

# ══════════════════════════════════════════════════════════════
# API 엔드포인트
# ══════════════════════════════════════════════════════════════

@app.get("/")
def root():
    return {"status": "MAGU STOCK API 실행 중"}

@app.get("/api/market")
def get_market_data():
    try:
        tickers = {
            "gold":"GC=F","wti":"CL=F","usdkrw":"KRW=X",
            "us10y":"^TNX","vix":"^VIX","sp500":"^GSPC",
            "nasdaq":"^IXIC","dow":"^DJI","russell":"^RUT"
        }
        result = {}
        for key, symbol in tickers.items():
            try:
                t    = yf.Ticker(symbol)
                hist = t.history(period="2d")
                if len(hist) >= 2:
                    current = hist['Close'].iloc[-1]
                    prev    = hist['Close'].iloc[-2]
                    chg     = (current / prev - 1) * 100
                    result[key] = {"value": round(current, 2), "change": round(chg, 2)}
            except:
                result[key] = {"value": 0, "change": 0}
        return result
    except:
        return {}

@app.get("/api/screen/{market}")
def screen_stocks(market: str = "nasdaq"):
    market_map = {
        "nasdaq": TICKERS_NASDAQ,
        "sp500":  TICKERS_SP500,
        "kospi":  TICKERS_KOSPI,
        "kosdaq": TICKERS_KOSDAQ,
        "us":     TICKERS_US,
        "kr":     TICKERS_KR,
    }
    tickers  = market_map.get(market, TICKERS_NASDAQ)
    currency = "KRW" if market in ("kospi","kosdaq","kr") else "USD"
    results  = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_single_stock, t, market): t for t in tickers}
        for f in concurrent.futures.as_completed(futures):
            r = f.result()
            if r:
                results.append(r)
    results.sort(key=lambda x: x['total_score'], reverse=True)
    results = get_portfolio_weight(results)
    labels  = {
        "nasdaq":"나스닥","sp500":"S&P500",
        "kospi":"코스피","kosdaq":"코스닥",
        "us":"미국 전체","kr":"한국 전체",
    }
    return {
        "market":         market,
        "market_label":   labels.get(market, market),
        "currency":       currency,
        "updated_at":     datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total_screened": len(results),
        "results":        results,
    }

@app.get("/api/stock/{ticker}")
def get_stock_score(ticker: str):
    ticker = ticker.upper().strip()
    try:
        stock = yf.Ticker(ticker)
        info  = stock.info
        price_check = info.get('regularMarketPrice') or info.get('currentPrice') or info.get('previousClose')
        if not info or not price_check:
            return {"error": f"종목을 찾을 수 없습니다: {ticker}"}
        hist_daily  = stock.history(period="1y")
        hist_weekly = stock.history(period="2y", interval="1wk")
        if hist_daily.empty or len(hist_daily) < 20:
            return {"error": "데이터가 부족합니다"}

        c_ema   = score_ema_slope(hist_weekly)
        c_stoch = score_stochastic(hist_daily)
        c_break = score_breakout(hist_daily)
        classic = c_ema + (c_stoch//2 if c_ema==0 else c_stoch) + (c_break//2 if c_ema==0 else c_break)

        g_roe   = score_roe(info.get('returnOnEquity', 0) or 0)
        g_debt  = score_debt(info.get('debtToEquity', 999) or 999)
        g_eps   = score_eps_growth(info)
        g_peg   = score_peg(info.get('pegRatio', None))
        g_ma200 = score_ma200(hist_daily)
        g_rsi   = score_rsi(hist_daily)
        growth  = g_roe + g_debt + g_eps + g_peg + g_ma200 + g_rsi

        m_anal  = score_analyst(info.get('recommendationKey','') or '')
        m_rs    = score_relative_strength(hist_daily)
        m_obv   = score_obv_momentum(hist_daily)
        modern  = m_anal + m_rs + m_obv
        total   = classic + growth + modern

        current_price = float(hist_daily['Close'].iloc[-1])
        prev_price    = float(hist_daily['Close'].iloc[-2])
        change_pct    = (current_price / prev_price - 1) * 100

        rsi_val = 0.0
        if len(hist_daily) >= 14:
            rsi_val = round(float(ta.momentum.RSIIndicator(hist_daily['Close'], window=14).rsi().iloc[-1]), 1)

        year_return = 0.0
        if len(hist_daily) >= 252:
            year_return = round((hist_daily['Close'].iloc[-1] / hist_daily['Close'].iloc[-252] - 1) * 100, 1)

        name = KR_NAMES.get(ticker) or info.get('longName') or ticker
        return {
            "ticker":         ticker,
            "name":           name,
            "sector":         info.get('sector') or '—',
            "currency":       info.get('currency', 'USD'),
            "price":          round(current_price, 2),
            "change_pct":     round(change_pct, 2),
            "classic_score":  classic,
            "growth_score":   growth,
            "modern_score":   modern,
            "total_score":    total,
            "recommendation": get_recommendation(total, classic, growth, modern),
            "detail": {
                "roe":         round((info.get('returnOnEquity') or 0) * 100, 1),
                "debt_equity": round(info.get('debtToEquity') or 0, 1),
                "eps_growth":  round((info.get('earningsGrowth') or 0) * 100, 1),
                "peg":         round(info.get('pegRatio') or 0, 2),
                "rsi":         rsi_val,
                "year_return": year_return,
                "analyst_rec": info.get('recommendationKey') or '—',
                "market_cap":  info.get('marketCap') or 0,
            }
        }
    except Exception as e:
        return {"error": f"조회 실패: {str(e)}"}

def analyze_etf(etf_info: dict):
    ticker = etf_info["ticker"]
    try:
        t    = yf.Ticker(ticker)
        info = t.info
        hist = t.history(period="6mo")
        if hist.empty or len(hist) < 60:
            return None
        price    = float(hist['Close'].iloc[-1])
        price_1d = float(hist['Close'].iloc[-2]) if len(hist) >= 2 else price
        price_1w = float(hist['Close'].iloc[-6]) if len(hist) >= 6 else price
        price_1m = float(hist['Close'].iloc[-22]) if len(hist) >= 22 else price
        price_3m = float(hist['Close'].iloc[-66]) if len(hist) >= 66 else price
        price_6m = float(hist['Close'].iloc[-132]) if len(hist) >= 132 else price
        price_1y = float(hist['Close'].iloc[0])
        ret_1d = round((price/price_1d-1)*100,2)
        ret_1w = round((price/price_1w-1)*100,2)
        ret_1m = round((price/price_1m-1)*100,2)
        ret_3m = round((price/price_3m-1)*100,2)
        ret_6m = round((price/price_6m-1)*100,2)
        ret_1y = round((price/price_1y-1)*100,2)
        vol_5d    = float(hist['Volume'].iloc[-5:].mean())
        vol_20d   = float(hist['Volume'].iloc[-20:].mean())
        vol_ratio = round(vol_5d/vol_20d,2) if vol_20d>0 else 1.0
        rsi_val = 0.0
        if len(hist) >= 14:
            rsi_val = round(float(ta.momentum.RSIIndicator(hist['Close'],window=14).rsi().iloc[-1]),1)
        high_52w  = float(hist['High'].max())
        from_high = round((price/high_52w-1)*100,1)
        inst_pct  = round(float(info.get('heldPercentInstitutions') or 0)*100,1)
        sc = 0
        if ret_1m>5:sc+=3
        elif ret_1m>2:sc+=2
        elif ret_1m>0:sc+=1
        elif ret_1m<-5:sc-=3
        elif ret_1m<-2:sc-=2
        elif ret_1m<0:sc-=1
        if ret_3m>10:sc+=2
        elif ret_3m>3:sc+=1
        elif ret_3m<-10:sc-=2
        elif ret_3m<-3:sc-=1
        if vol_ratio>1.3:sc+=2
        elif vol_ratio>1.1:sc+=1
        elif vol_ratio<0.7:sc-=2
        elif vol_ratio<0.9:sc-=1
        if rsi_val>60:sc+=1
        elif rsi_val<40:sc-=1
        if sc>=5:        trend,trend_score="강한유입",10
        elif sc>=3:      trend,trend_score="유입",5
        elif sc>=1:      trend,trend_score="소폭유입",3
        elif sc>=-1:     trend,trend_score="중립",0
        elif sc>=-3:     trend,trend_score="소폭유출",-3
        elif sc>=-5:     trend,trend_score="유출",-5
        else:            trend,trend_score="강한유출",-10
        return {
            "ticker":ticker,"name":etf_info["name"],"name_en":etf_info["name_en"],"emoji":etf_info["emoji"],
            "price":round(price,2),"ret_1d":ret_1d,"ret_1w":ret_1w,"ret_1m":ret_1m,
            "ret_3m":ret_3m,"ret_6m":ret_6m,"ret_1y":ret_1y,
            "vol_ratio":vol_ratio,"rsi":rsi_val,"from_52w_high":from_high,
            "inst_pct":inst_pct,"trend":trend,"trend_score":trend_score,"momentum_score":sc,
        }
    except:
        return None

@app.get("/api/smartmoney")
def get_smart_money():
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analyze_etf, etf): etf for etf in SECTOR_ETFS}
        for f in concurrent.futures.as_completed(futures):
            r = f.result()
            if r:
                results.append(r)
    results.sort(key=lambda x: x["momentum_score"], reverse=True)
    if results:
        avg_ret_1m = sum(r["ret_1m"] for r in results) / len(results)
        for r in results:
            r["rel_strength"] = round(r["ret_1m"] - avg_ret_1m, 2)
    try:
        spy        = yf.Ticker("SPY").history(period="3mo")
        spy_ret_1m = round(float((spy['Close'].iloc[-1]/spy['Close'].iloc[-22]-1)*100),2) if len(spy)>=22 else 0
        spy_ret_3m = round(float((spy['Close'].iloc[-1]/spy['Close'].iloc[0]-1)*100),2)
    except:
        spy_ret_1m = spy_ret_3m = 0
    return {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "spy_ret_1m": spy_ret_1m, "spy_ret_3m": spy_ret_3m,
        "sectors": results, "total": len(results),
    }

@app.get("/api/tenbagger")
def get_tenbagger():
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_tenbagger_stock, t): t for t in TICKERS_TENBAGGER}
        for f in concurrent.futures.as_completed(futures, timeout=180):
            try:
                r = f.result(timeout=20)
                if r:
                    results.append(r)
            except:
                continue
    results.sort(key=lambda x: x['total_score'], reverse=True)
    return {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total":      len(results),
        "universe":   f"나스닥 중소형 성장주 {len(TICKERS_TENBAGGER)}개",
        "results":    results,
    }

@app.get("/api/liquidity")
def get_liquidity():
    if not FRED_API_KEY:
        return {
            "error":      "FRED_API_KEY 환경변수가 설정되지 않았습니다.",
            "guide":      "https://fred.stlouisfed.org/docs/api/api_key.html 에서 무료 발급 후 Railway 환경변수에 추가하세요.",
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
    series_map = {
        "walcl": "WALCL",
        "rrp":   "RRPONTSYD",
        "tga":   "WTREGEN",
        "mmf":   "WRMFNS",
    }
    raw = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_fred, sid, 20): key for key, sid in series_map.items()}
        for f in concurrent.futures.as_completed(futures):
            key    = futures[f]
            raw[key] = f.result()

    s_walcl = score_fed_balance(raw.get("walcl", []))
    s_rrp   = score_rrp(raw.get("rrp", []))
    s_tga   = score_tga(raw.get("tga", []))
    s_mmf   = score_mmf(raw.get("mmf", []))

    indicators  = [s_walcl, s_rrp, s_tga, s_mmf]
    total_score = sum(i["score"] for i in indicators)
    signal      = get_liquidity_signal(total_score)

    return {
        "updated_at":  datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total_score": total_score,
        "max_score":   100,
        "signal":      signal,
        "indicators":  indicators,
        "scoring_guide": {
            "80~100": "🟢 적극매수 — 유동성 최대",
            "60~79":  "🔵 매수우호 — 유동성 양호",
            "40~59":  "🟡 중립관망 — 방향 불확실",
            "20~39":  "🟠 매수축소 — 유동성 감소",
            "0~19":   "🔴 현금보유 — 유동성 경색",
        },
    }

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
        stock  = yf.Ticker(ticker)
        info   = stock.info
        hist   = stock.history(period="2y")
        histw  = stock.history(period="3y", interval="1wk")
        if hist.empty or len(hist) < 60:
            return []
        sp500   = yf.Ticker("^GSPC").history(period="2y")
        signals = []
        step    = 20
        for i in range(60, len(hist) - hold_days, step):
            result = score_at_date(hist, histw, info, i)
            if result is None:
                continue
            classic, growth, total = result
            if total < score_threshold:
                continue
            entry_price = float(hist['Close'].iloc[i])
            exit_price  = float(hist['Close'].iloc[i + hold_days])
            ret         = round((exit_price / entry_price - 1) * 100, 2)
            entry_date  = hist.index[i]
            exit_date   = hist.index[i + hold_days]
            sp_slice    = sp500[(sp500.index >= entry_date) & (sp500.index <= exit_date)]
            sp_ret = 0.0
            if len(sp_slice) >= 2:
                sp_ret = round(float((sp_slice['Close'].iloc[-1]/sp_slice['Close'].iloc[0]-1)*100),2)
            name = KR_NAMES.get(ticker, ticker)
            signals.append({
                "ticker":        name,
                "signal_date":   entry_date.strftime("%Y.%m.%d"),
                "sell_date":     exit_date.strftime("%Y.%m.%d"),
                "entry_price":   round(entry_price, 2),
                "exit_price":    round(exit_price, 2),
                "return_pct":    float(ret),
                "sp500_ret":     float(sp_ret),
                "classic_score": int(classic),
                "growth_score":  int(growth),
                "modern_score":  int(total - classic - growth),
                "total_score":   int(total),
                "recommendation":get_recommendation(total, classic, growth, modern),
                "win":           bool(ret > 0),
            })
        return signals
    except:
        return []

@app.get("/api/backtest")
def run_backtest(market: str = "us", hold_days: int = 30, score_threshold: int = 55):
    tickers     = TICKERS_US[:50] if market == "us" else TICKERS_KR_BT
    all_signals = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(backtest_single, t, hold_days, score_threshold): t for t in tickers}
        for f in concurrent.futures.as_completed(futures):
            all_signals.extend(f.result())
    if not all_signals:
        return {
            "summary":{"total_signals":0,"win_rate":0.0,"avg_return":0.0,
                       "avg_sp500":0.0,"alpha":0.0,"hold_days":hold_days,
                       "score_threshold":score_threshold,"best_model":"—"},
            "band_stats":[],"period_returns":[],"signals":[],"error":"신호 없음"
        }
    total    = len(all_signals)
    wins     = sum(1 for s in all_signals if s["win"])
    win_rate = round(wins/total*100,1)
    avg_ret  = round(sum(s["return_pct"] for s in all_signals)/total,2)
    avg_sp   = round(sum(s["sp500_ret"]  for s in all_signals)/total,2)
    alpha    = round(avg_ret-avg_sp,2)
    bands = [
        {"label":"70점 이상","min":70,"max":100},
        {"label":"65~69점","min":65,"max":69},
        {"label":"55~64점","min":55,"max":64},
        {"label":"40~54점","min":40,"max":54},
    ]
    band_stats = []
    for b in bands:
        filtered = [s for s in all_signals if b["min"]<=s["total_score"]<=b["max"]]
        wr = round(sum(1 for s in filtered if s["win"])/len(filtered)*100,1) if filtered else 0.0
        band_stats.append({"label":b["label"],"win_rate":wr,"count":len(filtered)})
    classic_wins = [s for s in all_signals if s["classic_score"]>=20 and s["win"]]
    growth_wins  = [s for s in all_signals if s["growth_score"]>=30  and s["win"]]
    modern_wins  = [s for s in all_signals if s["modern_score"]>=20  and s["win"]]
    best_model   = max(
        [("Classic",len(classic_wins)),("Growth",len(growth_wins)),("Modern",len(modern_wins))],
        key=lambda x: x[1]
    )[0]
    all_signals.sort(key=lambda x: x["return_pct"], reverse=True)
    return {
        "summary":{
            "total_signals":total,"win_rate":win_rate,
            "avg_return":avg_ret,"avg_sp500":avg_sp,"alpha":alpha,
            "hold_days":hold_days,"score_threshold":score_threshold,"best_model":best_model,
        },
        "band_stats":    band_stats,
        "period_returns":[{"days":d,"magu":avg_ret,"sp500":avg_sp} for d in [10,20,30,45,60,90]],
        "signals":       all_signals[:100],
    }

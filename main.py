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

# ── 나스닥 180개 (기술·성장 중심) ──
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

# ── S&P500 180개 (가치·배당·전통 대형주) ──
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

# ── 코스피 180개 ──
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

# ── 코스닥 (yfinance 검증 종목) ──
TICKERS_KOSDAQ = list(dict.fromkeys([
    # 반도체·IT
    "247540.KQ","086520.KQ","039030.KQ","084370.KQ","095340.KQ",
    "064760.KQ","357780.KQ","022100.KQ","241560.KQ","042700.KQ",
    "058970.KQ","067160.KQ","078070.KQ","036540.KQ","114840.KQ",
    "036830.KQ","053160.KQ","191410.KQ","065510.KQ","036010.KQ",
    "232140.KQ","101490.KQ","126340.KQ","256840.KQ","094360.KQ",
    "112610.KQ","319400.KQ","298540.KQ","204840.KQ","058110.KQ",
    # 바이오·제약
    "196170.KQ","091990.KQ","096530.KQ","145020.KQ","068760.KQ",
    "086040.KQ","009420.KQ","048410.KQ","237690.KQ","031370.KQ",
    "095660.KQ","082270.KQ","228760.KQ","173940.KQ","141080.KQ",
    "058850.KQ","111010.KQ","348210.KQ","251970.KQ","016450.KQ",
    "078520.KQ","119850.KQ","066700.KQ","041920.KQ","005290.KQ",
    # 엔터·게임·미디어
    "035900.KQ","041510.KQ","122870.KQ","263750.KQ","293490.KQ",
    "112040.KQ","036570.KQ","179900.KQ","950130.KQ","323280.KQ",
    "140860.KQ","357120.KQ","023160.KQ","047560.KQ","060900.KQ",
    # 소재·화학·에너지
    "214150.KQ","151910.KQ","039200.KQ","070300.KQ","066430.KQ",
    "019170.KQ","079160.KQ","053300.KQ","057030.KQ","045300.KQ",
    "049070.KQ","071280.KQ","078160.KQ","075130.KQ","088290.KQ",
    # 금융·기타 성장주
    "950160.KQ","950140.KQ","950200.KQ","330860.KQ","091120.KQ",
    "093190.KQ","097780.KQ","104460.KQ","109820.KQ","115180.KQ",
    "119860.KQ","123260.KQ","131290.KQ","137400.KQ","155660.KQ",
    "158300.KQ","160600.KQ","163560.KQ","166090.KQ","168490.KQ",
    "170900.KQ","171490.KQ","174900.KQ","176750.KQ","178600.KQ",
    "180060.KQ","182360.KQ","183490.KQ","185490.KQ","187220.KQ",
    "189300.KQ","192820.KQ","194480.KQ","195870.KQ","196300.KQ",
    "199550.KQ","200130.KQ","200880.KQ","206650.KQ","208350.KQ",
    "210980.KQ","215000.KQ","217270.KQ","218410.KQ","220630.KQ",
    "222040.KQ","225570.KQ","226400.KQ","234080.KQ","236810.KQ",
    "239340.KQ","243840.KQ","248070.KQ","251340.KQ","253590.KQ",
]))[:180]

# ── 하위 호환용 (백테스트 등 기존 코드) ──
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
# ★ 개편: 연속 점수 함수들
# ══════════════════════════════════════════════════════════════

def score_ema_slope(hist_weekly):
    """EMA26 기울기 강도 → 0~10점 (단순 상승/하락이 아닌 기울기 크기 반영)"""
    try:
        if len(hist_weekly) < 26:
            return 0
        ema = hist_weekly['Close'].ewm(span=26).mean()
        # 최근 4주 기울기 평균 (% 변화)
        slopes = []
        for i in range(1, 5):
            if len(ema) > i:
                slope = (ema.iloc[-i] - ema.iloc[-i-1]) / ema.iloc[-i-1] * 100
                slopes.append(slope)
        if not slopes:
            return 0
        avg_slope = sum(slopes) / len(slopes)
        if avg_slope >= 1.5:   return 10
        elif avg_slope >= 1.0: return 8
        elif avg_slope >= 0.5: return 6
        elif avg_slope >= 0.2: return 4
        elif avg_slope >= 0.0: return 2
        else:                  return 0
    except:
        return 0

def score_stochastic(hist_daily):
    """스토캐스틱 과매도 강도 → 0~10점"""
    try:
        if len(hist_daily) < 14:
            return 0
        high  = hist_daily['High']
        low   = hist_daily['Low']
        close = hist_daily['Close']
        ll = low.rolling(14).min()
        hh = high.rolling(14).max()
        stoch_k = 100 * (close - ll) / (hh - ll)
        k = stoch_k.iloc[-1]
        if k <= 10:   return 10  # 극단 과매도 → 최강 매수
        elif k <= 20: return 8
        elif k <= 30: return 6
        elif k <= 40: return 4
        elif k <= 50: return 2
        else:         return 0
    except:
        return 0

def score_breakout(hist_daily):
    """전일 고가 돌파 강도 → 0~10점"""
    try:
        if len(hist_daily) < 10:
            return 0
        recent_high = hist_daily['High'].iloc[-10:-1].max()
        latest_close = hist_daily['Close'].iloc[-1]
        ratio = latest_close / recent_high
        if ratio >= 1.03:   return 10  # 3% 이상 강한 돌파
        elif ratio >= 1.01: return 8   # 1~3% 돌파
        elif ratio >= 0.99: return 5   # 고가 근접
        elif ratio >= 0.97: return 2
        else:               return 0
    except:
        return 0

def calculate_classic_score(info, hist_weekly, hist_daily):
    """엘더 3중 스크린 — 연속 점수 (30점 만점)"""
    s1 = score_ema_slope(hist_weekly)      # 0~10
    s2 = score_stochastic(hist_daily)      # 0~10
    s3 = score_breakout(hist_daily)        # 0~10
    return s1 + s2 + s3

def score_roe(roe):
    """ROE 연속 점수 → 0~10점"""
    if roe >= 0.40:   return 10
    elif roe >= 0.30: return 8
    elif roe >= 0.20: return 6
    elif roe >= 0.15: return 4
    elif roe >= 0.10: return 2
    else:             return 0

def score_debt(debt_equity):
    """부채비율 연속 점수 → 0~5점"""
    if debt_equity <= 0:    return 5   # 무부채
    elif debt_equity <= 20: return 5
    elif debt_equity <= 50: return 4
    elif debt_equity <= 80: return 3
    elif debt_equity <= 100:return 2
    elif debt_equity <= 150:return 1
    else:                   return 0

def score_eps_growth(eps_growth):
    """EPS 성장률 연속 점수 → 0~10점"""
    if eps_growth >= 0.50:   return 10
    elif eps_growth >= 0.35: return 8
    elif eps_growth >= 0.25: return 6
    elif eps_growth >= 0.20: return 4
    elif eps_growth >= 0.10: return 2
    else:                    return 0

def score_peg(peg):
    """PEG 연속 점수 → 0~5점 (낮을수록 저평가 성장주)"""
    if peg <= 0:    return 0   # 음수 PEG는 신뢰 불가
    elif peg <= 0.5: return 5
    elif peg <= 0.8: return 4
    elif peg <= 1.0: return 3
    elif peg <= 1.2: return 2
    elif peg <= 1.5: return 1
    else:            return 0

def score_ma200(hist_daily):
    """200일 이동평균 대비 위치 → 0~5점"""
    try:
        if len(hist_daily) < 200:
            return 0
        ma200   = hist_daily['Close'].rolling(200).mean().iloc[-1]
        current = hist_daily['Close'].iloc[-1]
        ratio   = current / ma200
        if ratio >= 1.20:   return 5   # MA200 대비 20%+ 위
        elif ratio >= 1.10: return 4
        elif ratio >= 1.02: return 3
        elif ratio >= 1.00: return 2
        else:               return 0
    except:
        return 0

def score_rsi(hist_daily):
    """RSI 연속 점수 → 0~5점 (50 이상 추세 확인, 과열 감점)"""
    try:
        if len(hist_daily) < 14:
            return 0
        rsi = ta.momentum.RSIIndicator(hist_daily['Close'], window=14).rsi().iloc[-1]
        if 55 <= rsi <= 70:   return 5   # 이상적 구간
        elif 50 <= rsi < 55:  return 3
        elif 70 < rsi <= 75:  return 3   # 약간 과열
        elif 75 < rsi <= 80:  return 1   # 과열 주의
        elif rsi > 80:        return 0   # 과열 위험
        else:                 return 0   # 50 미만 하락 추세
    except:
        return 0

def calculate_growth_score(info, hist_daily):
    """퀀트 펀더멘털 — 연속 점수 (40점 만점)"""
    roe        = info.get('returnOnEquity', 0) or 0
    debt_eq    = info.get('debtToEquity', 999) or 999
    eps_growth = info.get('earningsGrowth', 0) or 0
    peg        = info.get('pegRatio', 999) or 999

    s1 = score_roe(roe)             # 0~10
    s2 = score_debt(debt_eq)        # 0~5
    s3 = score_eps_growth(eps_growth) # 0~10
    s4 = score_peg(peg)             # 0~5
    s5 = score_ma200(hist_daily)    # 0~5
    s6 = score_rsi(hist_daily)      # 0~5
    return s1 + s2 + s3 + s4 + s5 + s6

def score_analyst(rec):
    """애널리스트 추천 연속 점수 → 0~10점"""
    mapping = {
        'strong_buy': 10,
        'buy':        7,
        'hold':       3,
        'underperform': 1,
        'sell':       0,
    }
    return mapping.get(rec.lower() if rec else '', 3)

def score_year_return(hist_daily):
    """52주 수익률 연속 점수 → 0~10점"""
    try:
        if len(hist_daily) < 252:
            return 0
        ret = (hist_daily['Close'].iloc[-1] / hist_daily['Close'].iloc[-252] - 1) * 100
        if ret >= 100:   return 10
        elif ret >= 60:  return 8
        elif ret >= 30:  return 6
        elif ret >= 20:  return 4
        elif ret >= 0:   return 2
        else:            return 0
    except:
        return 0

def score_volume_momentum(hist_daily):
    """거래량 모멘텀 연속 점수 → 0~10점"""
    try:
        if len(hist_daily) < 20:
            return 0
        avg_vol    = hist_daily['Volume'].rolling(20).mean().iloc[-1]
        recent_vol = hist_daily['Volume'].iloc[-5:].mean()  # 최근 5일 평균
        ratio = recent_vol / avg_vol if avg_vol > 0 else 1.0
        if ratio >= 2.0:    return 10  # 거래량 2배 이상
        elif ratio >= 1.5:  return 8
        elif ratio >= 1.3:  return 6
        elif ratio >= 1.2:  return 4
        elif ratio >= 1.0:  return 2
        else:               return 0
    except:
        return 0

def calculate_modern_score(info, hist_daily):
    """AI & 심리 — 연속 점수 (30점 만점)"""
    rec = info.get('recommendationKey', '') or ''
    s1 = score_analyst(rec)                # 0~10
    s2 = score_year_return(hist_daily)     # 0~10
    s3 = score_volume_momentum(hist_daily) # 0~10
    return s1 + s2 + s3

def get_recommendation(total_score):
    if total_score >= 70:   return "Strong Buy"
    elif total_score >= 55: return "Buy"
    elif total_score >= 40: return "Hold"
    else:                   return "Watch"

def get_portfolio_weight(results):
    buy_stocks  = [r for r in results if r['recommendation'] in ['Strong Buy','Buy']]
    total_score = sum(r['total_score'] for r in buy_stocks)
    for r in results:
        if r['recommendation'] in ['Strong Buy','Buy'] and total_score > 0:
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

        classic = calculate_classic_score(info, hist_weekly, hist_daily)
        growth  = calculate_growth_score(info, hist_daily)
        modern  = calculate_modern_score(info, hist_daily)
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
            "recommendation": get_recommendation(total),
            "weight":         0,
            "rsi":            rsi_val,
            "roe":            round((info.get('returnOnEquity', 0) or 0) * 100, 1),
            "peg":            round(info.get('pegRatio', 0) or 0, 2),
        }
    except:
        return None

# ══════════════════════════════════════════════════════════════
# ★ 텐배거 스크리너 — 피터 린치 + 오닐 CANSLIM + 미너비니 SEPA
# ══════════════════════════════════════════════════════════════

def score_lynch(info, hist_daily):
    """
    피터 린치 스타일 — 소형 고성장 저평가 (35점 만점)
    핵심: PEG < 1, 소형주, EPS 폭발 성장, 이익 성장 지속성
    """
    score = 0
    detail = {}

    # 1. PEG 비율 (낮을수록 저평가 성장주) — 15점
    peg = info.get('pegRatio', 999) or 999
    if peg <= 0:
        peg_score = 0
    elif peg <= 0.5:  peg_score = 15
    elif peg <= 0.75: peg_score = 12
    elif peg <= 1.0:  peg_score = 9
    elif peg <= 1.5:  peg_score = 5
    elif peg <= 2.0:  peg_score = 2
    else:             peg_score = 0
    score += peg_score
    detail['peg'] = round(peg if peg != 999 else 0, 2)
    detail['peg_score'] = peg_score

    # 2. EPS 성장률 (폭발적 성장) — 10점
    eps_growth = info.get('earningsGrowth', 0) or 0
    if eps_growth >= 0.50:   eps_score = 10
    elif eps_growth >= 0.35: eps_score = 8
    elif eps_growth >= 0.25: eps_score = 6
    elif eps_growth >= 0.15: eps_score = 3
    else:                    eps_score = 0
    score += eps_score
    detail['eps_growth'] = round(eps_growth * 100, 1)
    detail['eps_score'] = eps_score

    # 3. 시가총액 (소형주 선호) — 10점
    mcap = info.get('marketCap', 0) or 0
    mcap_b = mcap / 1e9  # 십억 달러
    if mcap_b <= 0:        mcap_score = 0
    elif mcap_b <= 0.3:    mcap_score = 10  # 마이크로캡
    elif mcap_b <= 2.0:    mcap_score = 8   # 소형주
    elif mcap_b <= 10.0:   mcap_score = 5   # 중형주
    elif mcap_b <= 50.0:   mcap_score = 2   # 대형주
    else:                  mcap_score = 0   # 초대형 — 린치 스타일엔 불리
    score += mcap_score
    detail['market_cap_b'] = round(mcap_b, 1)
    detail['mcap_score'] = mcap_score

    return score, detail

def score_oneil(info, hist_daily, hist_weekly):
    """
    윌리엄 오닐 CANSLIM — 실적 + 신고가 돌파 + 거래량 폭증 (35점 만점)
    C: Current EPS  A: Annual EPS  N: New High  S: Supply/Volume  L: Leader
    """
    score = 0
    detail = {}

    # C — 분기 EPS 성장 (최근 실적 급등) — 10점
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

    # N — 52주 신고가 돌파 여부 (New High) — 10점
    try:
        if len(hist_daily) >= 252:
            high_52w   = float(hist_daily['High'].rolling(252).max().iloc[-1])
            current    = float(hist_daily['Close'].iloc[-1])
            from_high  = (current / high_52w - 1) * 100
            detail['from_52w_high'] = round(from_high, 1)
            if from_high >= 0:      n_score = 10  # 신고가 돌파!
            elif from_high >= -3:   n_score = 8   # 신고가 바로 아래
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

    # S — 거래량 급증 (Supply & Demand) — 10점
    try:
        if len(hist_daily) >= 50:
            avg_50d  = float(hist_daily['Volume'].rolling(50).mean().iloc[-1])
            recent_5 = float(hist_daily['Volume'].iloc[-5:].mean())
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

    # L — 상대 강도 (Leader vs Laggard): 섹터 내 RS — 5점
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
    """
    마크 미너비니 SEPA — 추세 템플릿 (30점 만점)
    Specific Entry Point Analysis: 이동평균 정렬 + 52주 범위 위치
    """
    score = 0
    detail = {}

    try:
        close = hist_daily['Close']
        current = float(close.iloc[-1])

        # 1. 주가 > 150MA AND 주가 > 200MA — 8점
        ma150 = ma200 = None
        if len(hist_daily) >= 200:
            ma150 = float(close.rolling(150).mean().iloc[-1])
            ma200 = float(close.rolling(200).mean().iloc[-1])
            detail['ma150'] = round(ma150, 2)
            detail['ma200'] = round(ma200, 2)
            above_both = (current > ma150) and (current > ma200)
            if above_both:
                # 얼마나 위에 있는지도 점수화
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

        # 2. 150MA > 200MA (단기 > 장기 = 골든 크로스 구조) — 7점
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

        # 3. 200MA가 최근 상승 추세 (1개월 전 대비) — 7점
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

        # 4. 52주 저점 대비 30% 이상 상승 (강한 바닥 탈출) — 8점
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
    """텐배거 스크리너 — 단일 종목 분석"""
    try:
        stock       = yf.Ticker(ticker)
        info        = stock.info
        hist_daily  = stock.history(period="2y")
        hist_weekly = stock.history(period="3y", interval="1wk")

        if hist_daily.empty or len(hist_daily) < 60:
            return None

        price_check = info.get('regularMarketPrice') or info.get('currentPrice') or info.get('previousClose')
        if not price_check:
            return None

        lynch_score,    lynch_detail    = score_lynch(info, hist_daily)
        oneil_score,    oneil_detail    = score_oneil(info, hist_daily, hist_weekly)
        minervini_score, minervini_detail = score_minervini(info, hist_daily)

        total = lynch_score + oneil_score + minervini_score

        current_price = float(hist_daily['Close'].iloc[-1])
        prev_price    = float(hist_daily['Close'].iloc[-2])
        change_pct    = (current_price / prev_price - 1) * 100

        # 텐배거 등급
        if total >= 75:   grade = "🔥 최상위"
        elif total >= 60: grade = "⭐ 유망"
        elif total >= 45: grade = "👀 관심"
        else:             grade = "💤 미해당"

        name   = info.get('longName', ticker)
        sector = info.get('sector', 'Unknown')
        mcap   = info.get('marketCap', 0) or 0

        return {
            "ticker":          ticker,
            "name":            name,
            "sector":          sector,
            "market_cap_b":    round(mcap / 1e9, 1),
            "price":           round(current_price, 2),
            "change_pct":      round(change_pct, 2),
            "lynch_score":     lynch_score,
            "oneil_score":     oneil_score,
            "minervini_score": minervini_score,
            "total_score":     total,
            "grade":           grade,
            "lynch_detail":    lynch_detail,
            "oneil_detail":    oneil_detail,
            "minervini_detail":minervini_detail,
        }
    except:
        return None

# ══════════════════════════════════════════════════════════════
# ★ 텐배거 전용 종목 풀 — 나스닥 중소형 성장주
#   기존 TICKERS_US(대형주)와 완전 분리. 시총 기준 $10B 이하 위주.
#   섹터: AI/SaaS/바이오/핀테크/클린에너지/우주/소비 성장주
# ══════════════════════════════════════════════════════════════
TICKERS_TENBAGGER = [
    # ── AI · 데이터 · 클라우드 ──
    "PLTR","AI","SOUN","BBAI","IREN","ALAB","IDCC","CWAN",
    "ALKT","AIOT","RBRK","AEHR","EVTC","RSKD","KTOS","CACI",
    "GFAI","AEYE","NRDS","AEIS",

    # ── 반도체 · 하드웨어 중소형 ──
    "AMBA","LSCC","SITM","POWI","AOSL","DIOD","MRAM","QUIK",
    "FORM","ONTO","ACLS","ICHR","KLIC","IPGP","COHU","UCTT",
    "CAMT","AEHR","AXTI","MTSI",

    # ── SaaS · 핀테크 · 결제 ──
    "AFRM","UPST","BILL","TOST","GTLB","DDOG","ZS","CELH",
    "DUOL","HIMS","RDDT","CAVA","APP","SMAR","ASAN","MNDY",
    "TASK","PCVX","ALVO","RELY",

    # ── 바이오 · 헬스케어 성장 ──
    "RXRX","NVCR","BEAM","CRSP","EDIT","NTLA","PACB","ARWR",
    "KYMR","VKTX","HRMY","PRAX","INVA","IMVT","LEGN","KROS",
    "RVMD","INSM","RARE","ACAD",

    # ── 클린에너지 · 배터리 ──
    "FSLR","ENPH","ARRY","SHLS","NOVA","STEM","FLUX","REGI",
    "BE","PLUG","BLDP","HYLN","MKFG","NRGV","EVGO","CHPT",

    # ── 우주 · 방산 · 드론 ──
    "RKLB","ASTS","LUNR","IRDM","SPCE","JOBY","ACHR","LILM",
    "ASTR","MNTS","KTOS","CACI","AVAV","HLIT","PRFT",

    # ── 소비 성장 · 라이프스타일 ──
    "CELH","BROS","CAVA","SHAK","WING","TXRH","PDFS","XPOF",
    "MODG","GOLI","BRZE","FRPT","YETI","BIRD","GOOS",

    # ── 이머징 플랫폼 · 커머스 ──
    "RDDT","SNAP","PINS","BMBL","MTTR","OPEN","OPAD","LOTZ",
    "SPWH","PRTS","VTEX","GLBE","BIGC","PRCH","CLPR",
]

# 중복 제거
TICKERS_TENBAGGER = list(dict.fromkeys(TICKERS_TENBAGGER))


@app.get("/api/tenbagger")
def get_tenbagger():
    """
    텐배거 스크리너 — 나스닥 중소형 전용 (기존 스크리너와 완전 독립)
    피터 린치 + 오닐 CANSLIM + 미너비니 SEPA 종합
    """
    results = []
    # 과부하 방지: max_workers=8, 타임아웃 관리
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_tenbagger_stock, t): t for t in TICKERS_TENBAGGER}
        for f in concurrent.futures.as_completed(futures, timeout=180):
            try:
                r = f.result(timeout=20)
                if r:
                    results.append(r)
            except Exception:
                continue

    results.sort(key=lambda x: x['total_score'], reverse=True)
    return {
        "updated_at":  datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total":       len(results),
        "universe":    f"나스닥 중소형 성장주 {len(TICKERS_TENBAGGER)}개",
        "results":     results,
        "scoring": {
            "lynch":     "35점 — 피터 린치: PEG + EPS성장 + 시총(소형선호)",
            "oneil":     "35점 — 오닐 CANSLIM: 분기EPS + 52주신고가 + 거래량급증 + RS",
            "minervini": "30점 — 미너비니 SEPA: 150/200MA정렬 + MA기울기 + 52주저점탈출",
        }
    }

# ══════════════════════════════════════════════════════════════
# 기존 API 엔드포인트 (유지)
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
    """
    market: nasdaq / sp500 / kospi / kosdaq / us / kr
    """
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

        classic = calculate_classic_score(info, hist_weekly, hist_daily)
        growth  = calculate_growth_score(info, hist_daily)
        modern  = calculate_modern_score(info, hist_daily)
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
            "recommendation": get_recommendation(total),
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
        hist = t.history(period="1y")
        if hist.empty or len(hist) < 60:
            return None

        price    = float(hist['Close'].iloc[-1])
        price_1d = float(hist['Close'].iloc[-2]) if len(hist) >= 2 else price
        price_1w = float(hist['Close'].iloc[-6]) if len(hist) >= 6 else price
        price_1m = float(hist['Close'].iloc[-22]) if len(hist) >= 22 else price
        price_3m = float(hist['Close'].iloc[-66]) if len(hist) >= 66 else price
        price_6m = float(hist['Close'].iloc[-132]) if len(hist) >= 132 else price
        price_1y = float(hist['Close'].iloc[0])

        ret_1d = round((price / price_1d - 1) * 100, 2)
        ret_1w = round((price / price_1w - 1) * 100, 2)
        ret_1m = round((price / price_1m - 1) * 100, 2)
        ret_3m = round((price / price_3m - 1) * 100, 2)
        ret_6m = round((price / price_6m - 1) * 100, 2)
        ret_1y = round((price / price_1y - 1) * 100, 2)

        vol_5d    = float(hist['Volume'].iloc[-5:].mean())
        vol_20d   = float(hist['Volume'].iloc[-20:].mean())
        vol_ratio = round(vol_5d / vol_20d, 2) if vol_20d > 0 else 1.0

        rsi_val = 0.0
        if len(hist) >= 14:
            rsi_val = round(float(ta.momentum.RSIIndicator(hist['Close'], window=14).rsi().iloc[-1]), 1)

        high_52w = float(hist['High'].max())
        from_high = round((price / high_52w - 1) * 100, 1)
        inst_pct  = round(float(info.get('heldPercentInstitutions') or 0) * 100, 1)

        sc = 0
        if ret_1m > 5:    sc += 3
        elif ret_1m > 2:  sc += 2
        elif ret_1m > 0:  sc += 1
        elif ret_1m < -5: sc -= 3
        elif ret_1m < -2: sc -= 2
        elif ret_1m < 0:  sc -= 1
        if ret_3m > 10:   sc += 2
        elif ret_3m > 3:  sc += 1
        elif ret_3m < -10:sc -= 2
        elif ret_3m < -3: sc -= 1
        if vol_ratio > 1.3:   sc += 2
        elif vol_ratio > 1.1: sc += 1
        elif vol_ratio < 0.7: sc -= 2
        elif vol_ratio < 0.9: sc -= 1
        if rsi_val > 60:  sc += 1
        elif rsi_val < 40:sc -= 1

        if sc >= 5:        trend, trend_score = "강한유입",  10
        elif sc >= 3:      trend, trend_score = "유입",       5
        elif sc >= 1:      trend, trend_score = "소폭유입",   3
        elif sc >= -1:     trend, trend_score = "중립",       0
        elif sc >= -3:     trend, trend_score = "소폭유출",  -3
        elif sc >= -5:     trend, trend_score = "유출",      -5
        else:              trend, trend_score = "강한유출", -10

        return {
            "ticker":      ticker,
            "name":        etf_info["name"],
            "name_en":     etf_info["name_en"],
            "emoji":       etf_info["emoji"],
            "price":       round(price, 2),
            "ret_1d":      ret_1d, "ret_1w": ret_1w, "ret_1m": ret_1m,
            "ret_3m":      ret_3m, "ret_6m": ret_6m, "ret_1y": ret_1y,
            "vol_ratio":   vol_ratio,
            "rsi":         rsi_val,
            "from_52w_high": from_high,
            "inst_pct":    inst_pct,
            "trend":       trend,
            "trend_score": trend_score,
            "momentum_score": sc,
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
        spy      = yf.Ticker("SPY").history(period="3mo")
        spy_ret_1m = round(float((spy['Close'].iloc[-1] / spy['Close'].iloc[-22] - 1) * 100), 2) if len(spy) >= 22 else 0
        spy_ret_3m = round(float((spy['Close'].iloc[-1] / spy['Close'].iloc[0] - 1) * 100), 2)
    except:
        spy_ret_1m = spy_ret_3m = 0
    return {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "spy_ret_1m": spy_ret_1m,
        "spy_ret_3m": spy_ret_3m,
        "sectors":    results,
        "total":      len(results),
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
            entry_price  = float(hist['Close'].iloc[i])
            exit_price   = float(hist['Close'].iloc[i + hold_days])
            ret          = round((exit_price / entry_price - 1) * 100, 2)
            entry_date   = hist.index[i]
            exit_date    = hist.index[i + hold_days]
            sp_slice     = sp500[(sp500.index >= entry_date) & (sp500.index <= exit_date)]
            sp_ret       = 0.0
            if len(sp_slice) >= 2:
                sp_ret = round(float((sp_slice['Close'].iloc[-1] / sp_slice['Close'].iloc[0] - 1) * 100), 2)
            name = KR_NAMES.get(ticker, ticker)
            signals.append({
                "ticker":         name,
                "signal_date":    entry_date.strftime("%Y.%m.%d"),
                "sell_date":      exit_date.strftime("%Y.%m.%d"),
                "entry_price":    round(entry_price, 2),
                "exit_price":     round(exit_price, 2),
                "return_pct":     float(ret),
                "sp500_ret":      float(sp_ret),
                "classic_score":  int(classic),
                "growth_score":   int(growth),
                "modern_score":   int(total - classic - growth),
                "total_score":    int(total),
                "recommendation": get_recommendation(total),
                "win":            bool(ret > 0),
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
        {"label":"65~69점","min":65,"max":69},
        {"label":"55~64점","min":55,"max":64},
        {"label":"40~54점","min":40,"max":54},
    ]
    band_stats = []
    for b in bands:
        filtered = [s for s in all_signals if b["min"] <= s["total_score"] <= b["max"]]
        wr = round(sum(1 for s in filtered if s["win"]) / len(filtered) * 100, 1) if filtered else 0.0
        band_stats.append({"label":b["label"],"win_rate":wr,"count":len(filtered)})

    classic_wins = [s for s in all_signals if s["classic_score"] >= 20 and s["win"]]
    growth_wins  = [s for s in all_signals if s["growth_score"]  >= 30 and s["win"]]
    modern_wins  = [s for s in all_signals if s["modern_score"]  >= 20 and s["win"]]
    best_model   = max(
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
        "band_stats":    band_stats,
        "period_returns":[{"days":d,"magu":avg_ret,"sp500":avg_sp} for d in [10,20,30,45,60,90]],
        "signals":       all_signals[:100],
    }

from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import ta
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures
import os
import requests
import psycopg2
import psycopg2.extras
import json
import pytz
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
# DB 연결
# ══════════════════════════════════════════════════════════════

DATABASE_URL = os.environ.get("DATABASE_URL", "")

def get_conn():
    url = DATABASE_URL
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url, sslmode="require")

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS screening_cache (
            ticker         TEXT NOT NULL,
            market         TEXT NOT NULL,
            name           TEXT,
            sector         TEXT,
            etf            TEXT,
            price          FLOAT,
            change_pct     FLOAT,
            classic_score  INT,
            growth_score   INT,
            modern_score   INT,
            total_score    INT,
            recommendation TEXT,
            weight         FLOAT,
            rsi            FLOAT,
            roe            FLOAT,
            peg            FLOAT,
            c_ema          INT,
            c_stoch        INT,
            c_break        INT,
            g_roe          INT,
            g_debt         INT,
            g_eps          INT,
            g_peg          INT,
            g_ma200        INT,
            g_rsi          INT,
            m_anal         INT,
            m_rs           INT,
            m_obv          INT,
            screened_at    TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (ticker, market)
        );
        CREATE INDEX IF NOT EXISTS idx_sc_market ON screening_cache(market);
        CREATE INDEX IF NOT EXISTS idx_sc_score  ON screening_cache(total_score DESC);

        CREATE TABLE IF NOT EXISTS tenbagger_cache (
            ticker           TEXT PRIMARY KEY,
            name             TEXT,
            sector           TEXT,
            market_cap_b     FLOAT,
            price            FLOAT,
            change_pct       FLOAT,
            lynch_score      INT,
            oneil_score      INT,
            minervini_score  INT,
            total_score      INT,
            grade            TEXT,
            lynch_detail     JSONB,
            oneil_detail     JSONB,
            minervini_detail JSONB,
            screened_at      TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS bestpick_records (
            id                SERIAL PRIMARY KEY,
            ticker            TEXT NOT NULL,
            name              TEXT,
            sector            TEXT,
            entry_price       FLOAT NOT NULL,
            total_score       INT,
            classic_score     INT,
            growth_score      INT,
            modern_score      INT,
            recommendation    TEXT,
            consecutive_count INT DEFAULT 1,
            picked_at         DATE NOT NULL DEFAULT CURRENT_DATE,
            market            TEXT DEFAULT 'nasdaq'
        );
        -- 기존 테이블에 market 컬럼이 없으면 추가
        ALTER TABLE bestpick_records ADD COLUMN IF NOT EXISTS market TEXT DEFAULT 'nasdaq';
        UPDATE bestpick_records SET market = 'nasdaq' WHERE market IS NULL;
        -- 기존 (ticker, picked_at) 제약 삭제 후 (ticker, picked_at, market)으로 교체
        ALTER TABLE bestpick_records DROP CONSTRAINT IF EXISTS bestpick_records_ticker_picked_at_key;
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'bestpick_records_ticker_picked_at_market_key'
            ) THEN
                ALTER TABLE bestpick_records ADD CONSTRAINT bestpick_records_ticker_picked_at_market_key UNIQUE (ticker, picked_at, market);
            END IF;
        END $$;
        CREATE INDEX IF NOT EXISTS idx_bp_picked_at ON bestpick_records(picked_at DESC);
        CREATE INDEX IF NOT EXISTS idx_bp_ticker    ON bestpick_records(ticker);
        CREATE INDEX IF NOT EXISTS idx_bp_market    ON bestpick_records(market);

        CREATE TABLE IF NOT EXISTS bestpick_prices (
            id         SERIAL PRIMARY KEY,
            record_id  INT NOT NULL REFERENCES bestpick_records(id) ON DELETE CASCADE,
            ticker     TEXT NOT NULL,
            price_date DATE NOT NULL,
            price      FLOAT NOT NULL,
            return_pct FLOAT,
            UNIQUE (record_id, price_date)
        );
        CREATE INDEX IF NOT EXISTS idx_bpp_record ON bestpick_prices(record_id);
    """)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("DB 초기화 완료")

def cleanup_old_data():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("DELETE FROM screening_cache WHERE screened_at < NOW() - INTERVAL '30 days'")
        cur.execute("DELETE FROM tenbagger_cache WHERE screened_at < NOW() - INTERVAL '30 days'")
        cur.execute("DELETE FROM bestpick_records WHERE picked_at < CURRENT_DATE - INTERVAL '180 days'")
        conn.commit(); cur.close(); conn.close()
        logger.info("cleanup 완료")
    except Exception as e:
        logger.error(f"cleanup 오류: {e}")

# ══════════════════════════════════════════════════════════════
# 종목 풀 — 확장판 (나스닥200, S&P500, 코스피200, 코스닥150)
# ══════════════════════════════════════════════════════════════

def get_sp500_tickers():
    """Wikipedia에서 S&P500 최신 구성종목 크롤링"""
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(url)
        tickers = tables[0]['Symbol'].str.replace('.', '-', regex=False).tolist()
        logger.info(f"S&P500 Wikipedia 크롤링: {len(tickers)}개")
        return tickers
    except Exception as e:
        logger.warning(f"S&P500 크롤링 실패, 하드코딩 폴백: {e}")
        return []

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
    "MNDY","PCVX","RELY","TASK","ALVO","KTOS","AVAV","HLIT","PRFT","CWAN",
    "ALKT","RBRK","AEHR","EVTC","RSKD","IDCC","AEIS","NRDS","GFAI","INTC",
    "CSCO","PEP","CMCSA","VRSN","SWKS","QRVO","MPWR","ENTG","FORM","UCTT",
    "CRUS","SLAB","DIOD","AAON","EXPO","PRGS","PCTY","NCNO","JAMF","APPF",
]))[:200]

TICKERS_SP500_FALLBACK = list(dict.fromkeys([
    "BRK-B","JPM","V","UNH","XOM","JNJ","WMT","MA","PG","HD",
    "CVX","MRK","ABBV","BAC","KO","LLY","TMO","MCD","CRM","ACN",
    "ABT","DHR","NKE","NEE","WFC","PM","T","UPS","MS","RTX",
    "SPGI","BMY","CAT","GS","BLK","SYK","AXP","C","CB","MO",
    "ZTS","CVS","SO","DUK","PLD","TGT","MMC","CI","ITW","HUM",
    "USB","EMR","NSC","AON","EL","HCA","PSA","MCK","WM","ADM",
    "SHW","FCX","ECL","TRV","APD","COF","EW","CARR","IQV","BDX",
    "SPG","GD","NOC","AIG","WELL","CME","MPC","VLO","CCI","CBRE",
    "STZ","YUM","ROP","KEYS","AWK","FIS","LHX","DG","CTVA","TDG",
    "LOW","IBM","GE","F","GM","PFE","LIN","CSCO","MMM","AFL",
    "ALB","AMP","AMT","AZO","BAX","BEN","BSX","BWA","CAG","CAH",
    "CE","CF","CHD","CHRW","CINF","CL","CLX","CMA","CMS","CNP",
    "COO","CPB","CSX","D","DAL","DD","DE","DFS","DGX","DHI",
    "DIS","DLTR","DOV","DRI","DTE","DVA","DVN","EA","ED","EFX",
    "EIX","EMN","EOG","EQIX","EQR","ES","ESS","ETN","ETR","EXC",
    "EXR","EXPD","EXPE","FDS","FDX","FE","FFIV","FLT","FMC","FRT",
    "FTV","GIS","GL","GPC","HIG","HON","HPQ","HSY","ICE","IFF",
    "IP","IPG","IRM","SNA","SWK","SYY","TAP","TFC","TGT","TJX",
    "TPR","TSCO","TSN","TT","TXT","UDR","UHS","ULTA","UNM","URI",
]))

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
    "011420.KS","011760.KS","012750.KS","013360.KS","014680.KS",
    "016380.KS","017040.KS","017180.KS","018120.KS","019170.KS",
    "020150.KS","021080.KS","022000.KS","024090.KS","025860.KS",
    "027740.KS","029530.KS","030190.KS","032560.KS","033530.KS",
    "036570.KS","037560.KS","038530.KS","039130.KS","041650.KS",
    "042660.KS","047050.KS","048270.KS","049770.KS","053210.KS",
    "054210.KS","058430.KS","060980.KS","069620.KS","075580.KS",
    "078520.KS","082740.KS","091810.KS","100840.KS","138040.KS",
    "145990.KS","175330.KS","214390.KS","241560.KS","259960.KS",
]))[:200]

TICKERS_KOSDAQ = list(dict.fromkeys([
    "247540.KQ","086520.KQ","068760.KQ","091990.KQ","196170.KQ",
    "096530.KQ","145020.KQ","009420.KQ","048410.KQ","237690.KQ",
    "088290.KQ","058850.KQ","039200.KQ","031370.KQ","039030.KQ",
    "357780.KQ","084370.KQ","064760.KQ","095340.KQ","022100.KQ",
    "058970.KQ","214150.KQ","151910.KQ","042700.KQ","078070.KQ",
    "036540.KQ","114840.KQ","101490.KQ","126340.KQ","112610.KQ",
    "140860.KQ","323280.KQ","232140.KQ","065510.KQ","035900.KQ",
    "041510.KQ","122870.KQ","263750.KQ","293490.KQ","112040.KQ",
    "036570.KQ","067160.KQ","095660.KQ","066430.KQ","241560.KQ",
    "179900.KQ","950130.KQ","082270.KQ","091120.KQ","070300.KQ",
    "086040.KQ","039440.KQ","053160.KQ","191410.KQ","228760.KQ",
    "251970.KQ","256840.KQ","319400.KQ","298540.KQ","204840.KQ",
    "036830.KQ","357120.KQ","048910.KQ","060310.KQ","053800.KQ",
    "067900.KQ","041830.KQ","078130.KQ","033290.KQ","094970.KQ",
    "058470.KQ","052690.KQ","090460.KQ","092130.KQ","054040.KQ",
    "089590.KQ","083790.KQ","036200.KQ","067570.KQ","042420.KQ",
    "054180.KQ","080000.KQ","066970.KQ","058610.KQ","041920.KQ",
    "048260.KQ","050120.KQ","038500.KQ","053600.KQ","039610.KQ",
    "053580.KQ","093240.KQ","051160.KQ","089180.KQ","091440.KQ",
    "078340.KQ","078520.KQ","045970.KQ","060900.KQ","058430.KQ",
    "083500.KQ","052260.KQ","047310.KQ","099290.KQ","036160.KQ",
    "058820.KQ","081000.KQ","053210.KQ","045180.KQ","066790.KQ",
    "053290.KQ","052710.KQ","049520.KQ","048870.KQ","039350.KQ",
    "067730.KQ","046080.KQ","036000.KQ","052900.KQ","053700.KQ",
    "058380.KQ","041190.KQ","042080.KQ","048550.KQ","037760.KQ",
    "063570.KQ","038870.KQ","039840.KQ","048430.KQ","041440.KQ",
    "050960.KQ","040290.KQ","049080.KQ","036810.KQ","039290.KQ",
    "052400.KQ","038390.KQ","049830.KQ","052600.KQ","037950.KQ",
    "041590.KQ","067000.KQ","043370.KQ","049010.KQ","038830.KQ",
    "054780.KQ","067140.KQ","048310.KQ","036620.KQ","053050.KQ",
]))[:150]

_sp500_cache = []

def get_tickers_sp500():
    global _sp500_cache
    if _sp500_cache:
        return _sp500_cache
    fresh = get_sp500_tickers()
    if fresh:
        _sp500_cache = fresh
        return _sp500_cache
    return TICKERS_SP500_FALLBACK

TICKERS_US = list(dict.fromkeys(TICKERS_NASDAQ + TICKERS_SP500_FALLBACK))
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
    # 코스피 추가
    "001800.KS":"오리온홀딩스","008060.KS":"대덕전자","010620.KS":"현대미포조선",
    "014820.KS":"동원시스템즈","018880.KS":"한온시스템","026960.KS":"동서",
    "029780.KS":"삼성카드","030000.KS":"제일기획","030190.KS":"NICE평가정보",
    "031430.KS":"신세계인터내셔날","036570.KS":"엔씨소프트","037560.KS":"LG헬로비전",
    "039130.KS":"하나투어","042660.KS":"한화오션","047050.KS":"포스코인터내셔날",
    "048270.KS":"오스템임플란트","049770.KS":"동원F&B","053210.KS":"스카이라이프",
    "054210.KS":"포스코DX","060980.KS":"한솔홀딩스","069620.KS":"대웅제약",
    "082740.KS":"HSD엔진","100840.KS":"SNT에너지","138040.KS":"메리츠금융지주",
    "145990.KS":"삼양사","175330.KS":"JB금융지주","214390.KS":"경보제약",
    "259960.KS":"크래프톤","000390.KS":"삼화페인트","000670.KS":"영풍",
    "000880.KS":"한화","001230.KS":"동국제강","001450.KS":"현대해상",
    "001740.KS":"SK네트웍스","002320.KS":"한진","002350.KS":"넥센타이어",
    "002820.KS":"SBS","003070.KS":"코오롱","003240.KS":"태광산업",
    "003580.KS":"HLB생명과학","004140.KS":"동양","004370.KS":"농심",
    "004490.KS":"세방전지","004990.KS":"롯데지주","005160.KS":"동국제강",
    "005440.KS":"현대그린푸드","005850.KS":"에스엘","006360.KS":"GS건설",
    "006650.KS":"대한유화","007160.KS":"사조산업","007340.KS":"DN오토모티브",
    "008300.KS":"효성티앤씨","008350.KS":"남선알미늄","008490.KS":"서흥",
    "009680.KS":"부산은행","009770.KS":"삼원강재","010040.KS":"한국내화",
    "010140.KS":"삼성중공업","010780.KS":"아이에스동서","011080.KS":"두산에너빌리티",
    "011420.KS":"경동나비엔","011760.KS":"현대코퍼레이션","012750.KS":"에스원",
    "014680.KS":"한솔케미칼","016380.KS":"KG동부제철","017040.KS":"광동제약",
    "017180.KS":"한미사이언스","019170.KS":"신풍제약","020150.KS":"일진전기",
    "022000.KS":"오성첨단소재","025860.KS":"남해화학","032640.KS":"LG유플러스",
    "033240.KS":"자화전자","033530.KS":"세아제강지주","038530.KS":"삼일제약",
    "041650.KS":"상신브레이크","053580.KS":"빙그레","058430.KS":"포스코스틸리온",
    "069260.KS":"쌍용C&E","071050.KS":"한국금융지주","081660.KS":"휠라홀딩스",
    "090350.KS":"노루홀딩스","108670.KS":"LG하우시스","241560.KS":"두산밥캣",
    # 코스닥 추가
    "088290.KQ":"이오플로우","058850.KQ":"KH바텍","078070.KQ":"유비케어",
    "036540.KQ":"SFA반도체","114840.KQ":"아이패밀리에스씨","101490.KQ":"에스앤에스텍",
    "126340.KQ":"비나텍","140860.KQ":"파크시스템스","323280.KQ":"태성",
    "232140.KQ":"와이씨","065510.KQ":"휴비스","066970.KQ":"엘앤에프",
    "058610.KQ":"에스피지","039440.KQ":"에스티아이","053160.KQ":"CJ CGV",
    "228760.KQ":"지노믹트리","251970.KQ":"펌텍코리아","256840.KQ":"한국비엔씨",
    "319400.KQ":"현대무벡스","036830.KQ":"솔브레인홀딩스","357120.KQ":"큐라클",
    "048910.KQ":"대원미디어","053800.KQ":"안랩","041830.KQ":"인바디",
    "033290.KQ":"코웰패션","058470.KQ":"리노공업","052690.KQ":"한전기술",
    "090460.KQ":"비에이치","092130.KQ":"이크레더블","083790.KQ":"크리스에프앤씨",
    "036200.KQ":"유니셈","054180.KQ":"PI첨단소재","080000.KQ":"에스엔유",
    "041920.KQ":"동국S&C","048260.KQ":"오스코텍","038500.KQ":"로보스타",
    "053600.KQ":"한국토지신탁","083500.KQ":"에프에스티","052260.KQ":"현대바이오랜드",
    "047310.KQ":"파워로직스","099290.KQ":"테크윙","081000.KQ":"티씨케이",
    "058820.KQ":"CMG제약","053290.KQ":"NE능률","052710.KQ":"아모텍",
    "049520.KQ":"유아이엘","039350.KQ":"이건산업","046080.KQ":"에코바이오",
    "052900.KQ":"엘아이에스","041190.KQ":"우리기술투자","042080.KQ":"가비아",
    "037760.KQ":"코스모화학","063570.KQ":"한국전자금융","038870.KQ":"에코마케팅",
    "039840.KQ":"디오","041440.KQ":"현대에버다임","040290.KQ":"미래컴퍼니",
    "049080.KQ":"파세코","039290.KQ":"후성","052400.KQ":"코나아이",
    "038390.KQ":"레드캡투어","037950.KQ":"엘앤씨바이오","067000.KQ":"조이시티",
    "043370.KQ":"피에이치에이","049010.KQ":"이엔에프테크놀로지","054780.KQ":"하이록코리아",
    "067140.KQ":"두올","048310.KQ":"비트컴퓨터","053050.KQ":"지에스이",
    "067730.KQ":"에스바이오메딕스","191410.KQ":"육일씨엔에쓰","298540.KQ":"더컴퍼니",
    "204840.KQ":"지오엘리먼트","060310.KQ":"3S","078130.KQ":"국일인토트",
    "094970.KQ":"제이엠티","089590.KQ":"제주항공","036000.KQ":"예림당",
    "058380.KQ":"동화일렉트로라이트","048430.KQ":"유라테크","050960.KQ":"수산아이앤티",
    "036810.KQ":"에스엠코어","049830.KQ":"율호","052600.KQ":"나노엔텍",
    "041590.KQ":"플랜티넷","038830.KQ":"이건에너지","036620.KQ":"감성코퍼레이션",
    "067570.KQ":"에이테크솔루션","042420.KQ":"네오위즈홀딩스",
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
# 점수 계산 함수 (기존 그대로)
# ══════════════════════════════════════════════════════════════

def score_ema_slope(hist_weekly):
    try:
        if len(hist_weekly) < 30: return 0
        ema = hist_weekly['Close'].ewm(span=26).mean()
        slopes = [(ema.iloc[-i]-ema.iloc[-i-1])/ema.iloc[-i-1]*100 for i in range(1,5) if len(ema)>i]
        if not slopes: return 0
        avg = sum(slopes)/len(slopes)
        if avg>=1.0: return 10
        elif avg>=0.5: return 8
        elif avg>=0.2: return 6
        elif avg>=0.05: return 4
        elif avg>=0.0: return 2
        else: return 0
    except: return 0

def score_stochastic(hist_daily):
    try:
        if len(hist_daily)<14: return 0
        ll=hist_daily['Low'].rolling(14).min(); hh=hist_daily['High'].rolling(14).max()
        denom=hh-ll
        if denom.iloc[-1]==0: return 0
        k=float(100*(hist_daily['Close'].iloc[-1]-ll.iloc[-1])/denom.iloc[-1])
        if 20<=k<=40: return 10
        elif 40<k<=50: return 8
        elif 15<=k<20: return 7
        elif 50<k<=65: return 5
        elif 10<=k<15: return 4
        elif 65<k<=80: return 3
        elif k>80: return 1
        else: return 2
    except: return 0

def score_breakout(hist_daily):
    try:
        if len(hist_daily)<3: return 0
        prev_high=float(hist_daily['High'].iloc[-2]); latest=float(hist_daily['Close'].iloc[-1])
        ratio=latest/prev_high
        if ratio>=1.01: return 10
        elif ratio>=1.002: return 8
        elif ratio>=0.998: return 6
        elif ratio>=0.99: return 4
        elif ratio>=0.97: return 2
        else: return 0
    except: return 0

def calculate_classic_score(info, hist_weekly, hist_daily):
    s1=score_ema_slope(hist_weekly); s2=score_stochastic(hist_daily); s3=score_breakout(hist_daily)
    if s1==0: s2=s2//2; s3=s3//2
    return s1+s2+s3

def score_roe(roe):
    if roe>=0.30: return 10
    elif roe>=0.20: return 8
    elif roe>=0.15: return 6
    elif roe>=0.10: return 3
    elif roe>=0.05: return 1
    else: return 0

def score_debt(debt_equity):
    if debt_equity<=0: return 5
    elif debt_equity<=30: return 5
    elif debt_equity<=60: return 4
    elif debt_equity<=100: return 3
    elif debt_equity<=150: return 2
    elif debt_equity<=200: return 1
    else: return 0

def score_eps_growth(info):
    quarterly=info.get('earningsQuarterlyGrowth',None); annual=info.get('earningsGrowth',None)
    if quarterly is None and annual is None: return 0
    primary=quarterly if quarterly is not None else annual
    primary=primary if primary is not None else 0
    if primary>=0.40: base=10
    elif primary>=0.25: base=8
    elif primary>=0.15: base=6
    elif primary>=0.10: base=4
    elif primary>=0.05: base=2
    elif primary>0: base=1
    else: base=0
    if quarterly is not None and annual is not None:
        if quarterly>annual+0.05: base=min(base+1,10)
    return base

def score_peg(peg):
    if peg is None or peg<=0 or peg>=50: return 2
    elif peg<=0.8: return 5
    elif peg<=1.0: return 4
    elif peg<=1.5: return 3
    elif peg<=2.0: return 2
    elif peg<=3.0: return 1
    else: return 0

def score_ma200(hist_daily):
    try:
        if len(hist_daily)<200: return 0
        ma200=float(hist_daily['Close'].rolling(200).mean().iloc[-1]); current=float(hist_daily['Close'].iloc[-1])
        ratio=current/ma200
        if ratio>=1.20: return 5
        elif ratio>=1.10: return 4
        elif ratio>=1.03: return 3
        elif ratio>=1.00: return 2
        elif ratio>=0.95: return 1
        else: return 0
    except: return 0

def score_rsi(hist_daily):
    try:
        if len(hist_daily)<14: return 0
        rsi=float(ta.momentum.RSIIndicator(hist_daily['Close'],window=14).rsi().iloc[-1])
        if 50<=rsi<=65: return 5
        elif 40<=rsi<50: return 4
        elif 65<rsi<=75: return 3
        elif 30<=rsi<40: return 2
        elif 75<rsi<=80: return 1
        else: return 0
    except: return 0

def calculate_growth_score(info, hist_daily):
    return (score_roe(info.get('returnOnEquity',0) or 0)
          + score_debt(info.get('debtToEquity',999) or 999)
          + score_eps_growth(info)
          + score_peg(info.get('pegRatio',None))
          + score_ma200(hist_daily)
          + score_rsi(hist_daily))

def score_analyst(rec):
    if not rec: return 0
    return {'strong_buy':10,'buy':7,'hold':4,'underperform':1,'sell':0}.get(rec.lower(),0)

def score_relative_strength(hist_daily):
    try:
        n=min(len(hist_daily)-1,252)
        if n<60: return 0
        stock_ret=float((hist_daily['Close'].iloc[-1]/hist_daily['Close'].iloc[-n]-1)*100)
        excess=stock_ret-10.0*(n/252)
        if excess>=30: return 10
        elif excess>=20: return 8
        elif excess>=10: return 6
        elif excess>=0: return 4
        elif excess>=-10: return 2
        else: return 0
    except: return 0

def score_obv_momentum(hist_daily):
    try:
        if len(hist_daily)<20: return 0
        close=hist_daily['Close']; volume=hist_daily['Volume']
        obv=[0]
        for i in range(1,len(close)):
            if close.iloc[i]>close.iloc[i-1]: obv.append(obv[-1]+volume.iloc[i])
            elif close.iloc[i]<close.iloc[i-1]: obv.append(obv[-1]-volume.iloc[i])
            else: obv.append(obv[-1])
        obv_s=pd.Series(obv,index=close.index)
        obv_ma5=obv_s.iloc[-5:].mean(); obv_ma20=obv_s.iloc[-20:].mean()
        if obv_ma20==0: return 0
        ratio=obv_ma5/obv_ma20; rising=obv_s.iloc[-5:].mean()>obv_s.iloc[-10:-5].mean()
        if ratio>=1.3 and rising: return 10
        elif ratio>=1.1 and rising: return 8
        elif ratio>=1.0 and rising: return 6
        elif ratio>=1.0: return 4
        elif ratio>=0.9: return 2
        else: return 0
    except: return 0

def calculate_modern_score(info, hist_daily):
    rec=info.get('recommendationKey','') or ''
    return score_analyst(rec)+score_relative_strength(hist_daily)+score_obv_momentum(hist_daily)

def get_recommendation(total_score, classic=None, growth=None, modern=None):
    if classic is not None and growth is not None and modern is not None:
        if classic<10 or growth<15 or modern<8:
            return "Hold" if total_score>=70 else "Watch"
    if total_score>=70: return "Strong Buy"
    elif total_score>=55: return "Buy"
    elif total_score>=40: return "Hold"
    else: return "Watch"

def get_portfolio_weight(results):
    buy_stocks=[r for r in results if r['recommendation'] in ['Strong Buy','Buy']]
    total_score=sum(r['total_score'] for r in buy_stocks)
    for r in results:
        if r['recommendation'] in ['Strong Buy','Buy'] and total_score>0:
            r['weight']=round((r['total_score']/total_score)*100,1)
        else: r['weight']=0
    return results

def fetch_single_stock(ticker, market):
    try:
        stock=yf.Ticker(ticker); info=stock.info
        hist_daily=stock.history(period="1y"); hist_weekly=stock.history(period="2y",interval="1wk")
        if hist_daily.empty or len(hist_daily)<20: return None
        c_ema=score_ema_slope(hist_weekly); c_stoch=score_stochastic(hist_daily); c_break=score_breakout(hist_daily)
        classic=c_ema+(c_stoch//2 if c_ema==0 else c_stoch)+(c_break//2 if c_ema==0 else c_break)
        g_roe=score_roe(info.get('returnOnEquity',0) or 0); g_debt=score_debt(info.get('debtToEquity',999) or 999)
        g_eps=score_eps_growth(info); g_peg=score_peg(info.get('pegRatio',None))
        g_ma200=score_ma200(hist_daily); g_rsi=score_rsi(hist_daily)
        growth=g_roe+g_debt+g_eps+g_peg+g_ma200+g_rsi
        m_anal=score_analyst(info.get('recommendationKey','') or '')
        m_rs=score_relative_strength(hist_daily); m_obv=score_obv_momentum(hist_daily)
        modern=m_anal+m_rs+m_obv; total=classic+growth+modern
        current_price=float(hist_daily['Close'].iloc[-1]); prev_price=float(hist_daily['Close'].iloc[-2])
        change_pct=(current_price/prev_price-1)*100
        rsi_val=0.0
        if len(hist_daily)>=14:
            rsi_val=round(float(ta.momentum.RSIIndicator(hist_daily['Close'],window=14).rsi().iloc[-1]),1)
        name=KR_NAMES.get(ticker) or info.get('longName',ticker); sector=info.get('sector') or 'Unknown'
        return {
            "ticker":ticker,"name":name,"sector":sector,"etf":SECTOR_TO_ETF.get(sector,""),
            "market":market,
            "price":round(current_price,2),"change_pct":round(change_pct,2),
            "classic_score":classic,"growth_score":growth,"modern_score":modern,
            "total_score":total,"recommendation":get_recommendation(total,classic,growth,modern),
            "weight":0,"rsi":rsi_val,
            "roe":round((info.get('returnOnEquity',0) or 0)*100,1),"peg":round(info.get('pegRatio',0) or 0,2),
            "c_ema":c_ema,"c_stoch":c_stoch,"c_break":c_break,
            "g_roe":g_roe,"g_debt":g_debt,"g_eps":g_eps,"g_peg":g_peg,"g_ma200":g_ma200,"g_rsi":g_rsi,
            "m_anal":m_anal,"m_rs":m_rs,"m_obv":m_obv,
        }
    except: return None

# ══════════════════════════════════════════════════════════════
# DB 저장 / 조회
# ══════════════════════════════════════════════════════════════

def save_screening_to_db(results: list):
    if not results or not DATABASE_URL: return
    try:
        conn = get_conn(); cur = conn.cursor()
        for r in results:
            cur.execute("""
                INSERT INTO screening_cache
                    (ticker,market,name,sector,etf,price,change_pct,
                     classic_score,growth_score,modern_score,total_score,
                     recommendation,weight,rsi,roe,peg,
                     c_ema,c_stoch,c_break,g_roe,g_debt,g_eps,g_peg,g_ma200,g_rsi,
                     m_anal,m_rs,m_obv,screened_at)
                VALUES
                    (%(ticker)s,%(market)s,%(name)s,%(sector)s,%(etf)s,%(price)s,%(change_pct)s,
                     %(classic_score)s,%(growth_score)s,%(modern_score)s,%(total_score)s,
                     %(recommendation)s,%(weight)s,%(rsi)s,%(roe)s,%(peg)s,
                     %(c_ema)s,%(c_stoch)s,%(c_break)s,%(g_roe)s,%(g_debt)s,%(g_eps)s,
                     %(g_peg)s,%(g_ma200)s,%(g_rsi)s,%(m_anal)s,%(m_rs)s,%(m_obv)s,NOW())
                ON CONFLICT (ticker, market) DO UPDATE SET
                    name=EXCLUDED.name, sector=EXCLUDED.sector, etf=EXCLUDED.etf,
                    price=EXCLUDED.price, change_pct=EXCLUDED.change_pct,
                    classic_score=EXCLUDED.classic_score, growth_score=EXCLUDED.growth_score,
                    modern_score=EXCLUDED.modern_score, total_score=EXCLUDED.total_score,
                    recommendation=EXCLUDED.recommendation, weight=EXCLUDED.weight,
                    rsi=EXCLUDED.rsi, roe=EXCLUDED.roe, peg=EXCLUDED.peg,
                    c_ema=EXCLUDED.c_ema, c_stoch=EXCLUDED.c_stoch, c_break=EXCLUDED.c_break,
                    g_roe=EXCLUDED.g_roe, g_debt=EXCLUDED.g_debt, g_eps=EXCLUDED.g_eps,
                    g_peg=EXCLUDED.g_peg, g_ma200=EXCLUDED.g_ma200, g_rsi=EXCLUDED.g_rsi,
                    m_anal=EXCLUDED.m_anal, m_rs=EXCLUDED.m_rs, m_obv=EXCLUDED.m_obv,
                    screened_at=NOW()
            """, r)
        conn.commit(); cur.close(); conn.close()
        logger.info(f"DB 저장 완료: {len(results)}건 [{results[0]['market']}]")
    except Exception as e:
        logger.error(f"DB 저장 오류: {e}")

def load_screening_from_db(market: str):
    if not DATABASE_URL: return None
    try:
        conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # 25시간 이내 데이터만 사용 (새벽 스크리닝 실패 시 오래된 캐시 방지)
        freshness = "AND screened_at > NOW() - INTERVAL '25 hours'"
        if market in ("nasdaq","sp500","kospi","kosdaq"):
            cur.execute(f"SELECT * FROM screening_cache WHERE market=%s {freshness} ORDER BY total_score DESC", (market,))
        elif market == "us":
            cur.execute(f"SELECT * FROM screening_cache WHERE market IN ('nasdaq','sp500') {freshness} ORDER BY total_score DESC")
        elif market == "kr":
            cur.execute(f"SELECT * FROM screening_cache WHERE market IN ('kospi','kosdaq') {freshness} ORDER BY total_score DESC")
        else:
            cur.execute(f"SELECT * FROM screening_cache WHERE 1=1 {freshness} ORDER BY total_score DESC")
        rows = cur.fetchall(); cur.close(); conn.close()
        if not rows:
            logger.info(f"DB 캐시 없음 또는 만료 ({market}) → 실시간 계산")
            return None
        results = []
        for r in rows:
            d = dict(r)
            if d.get("screened_at"): d["screened_at"] = d["screened_at"].strftime("%Y-%m-%d %H:%M")
            results.append(d)
        return results
    except Exception as e:
        logger.error(f"DB 조회 오류: {e}")
        return None

def save_tenbagger_to_db(results: list):
    if not results or not DATABASE_URL: return
    try:
        conn = get_conn(); cur = conn.cursor()
        for r in results:
            cur.execute("""
                INSERT INTO tenbagger_cache
                    (ticker,name,sector,market_cap_b,price,change_pct,
                     lynch_score,oneil_score,minervini_score,total_score,grade,
                     lynch_detail,oneil_detail,minervini_detail,screened_at)
                VALUES
                    (%(ticker)s,%(name)s,%(sector)s,%(market_cap_b)s,%(price)s,%(change_pct)s,
                     %(lynch_score)s,%(oneil_score)s,%(minervini_score)s,%(total_score)s,%(grade)s,
                     %(lynch_detail)s,%(oneil_detail)s,%(minervini_detail)s,NOW())
                ON CONFLICT (ticker) DO UPDATE SET
                    name=EXCLUDED.name, sector=EXCLUDED.sector, market_cap_b=EXCLUDED.market_cap_b,
                    price=EXCLUDED.price, change_pct=EXCLUDED.change_pct,
                    lynch_score=EXCLUDED.lynch_score, oneil_score=EXCLUDED.oneil_score,
                    minervini_score=EXCLUDED.minervini_score, total_score=EXCLUDED.total_score,
                    grade=EXCLUDED.grade, lynch_detail=EXCLUDED.lynch_detail,
                    oneil_detail=EXCLUDED.oneil_detail, minervini_detail=EXCLUDED.minervini_detail,
                    screened_at=NOW()
            """, {**r,
                  "lynch_detail": json.dumps(r.get("lynch_detail",{})),
                  "oneil_detail": json.dumps(r.get("oneil_detail",{})),
                  "minervini_detail": json.dumps(r.get("minervini_detail",{}))})
        conn.commit(); cur.close(); conn.close()
        logger.info(f"텐배거 DB 저장: {len(results)}건")
    except Exception as e:
        logger.error(f"텐배거 DB 저장 오류: {e}")

# ══════════════════════════════════════════════════════════════
# 새벽 스케줄러 작업
# ══════════════════════════════════════════════════════════════

def run_full_screening_job():
    """매일 KST 04:00 전체 스크리닝 → DB 저장"""
    logger.info("=== MAGU STOCK 새벽 스크리닝 시작 ===")
    start = datetime.now()

    sp500 = get_sp500_tickers()
    if sp500:
        global _sp500_cache
        _sp500_cache = sp500

    markets = {
        "nasdaq": TICKERS_NASDAQ,
        "sp500":  get_tickers_sp500(),
        "kospi":  TICKERS_KOSPI,
        "kosdaq": TICKERS_KOSDAQ,
    }

    for market, tickers in markets.items():
        logger.info(f"[{market}] {len(tickers)}개 스크리닝 중...")
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = {executor.submit(fetch_single_stock, t, market): t for t in tickers}
            for f in concurrent.futures.as_completed(futures, timeout=300):
                try:
                    r = f.result(timeout=30)
                    if r: results.append(r)
                except concurrent.futures.TimeoutError:
                    logger.warning(f"[{market}] 종목 타임아웃 스킵")
                except Exception as e:
                    logger.warning(f"[{market}] 종목 오류 스킵: {e}")
        results.sort(key=lambda x: x['total_score'], reverse=True)
        results = get_portfolio_weight(results)
        save_screening_to_db(results)
        logger.info(f"[{market}] {len(results)}개 완료")

    _run_tenbagger_job()
    cleanup_old_data()
    elapsed = int((datetime.now() - start).total_seconds() // 60)
    logger.info(f"=== 스크리닝 완료: 소요 {elapsed}분 ===")

def _run_tenbagger_job():
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_tenbagger_stock, t): t for t in TICKERS_TENBAGGER}
        for f in concurrent.futures.as_completed(futures, timeout=300):
            try:
                r = f.result(timeout=20)
                if r: results.append(r)
            except: continue
    results.sort(key=lambda x: x['total_score'], reverse=True)
    save_tenbagger_to_db(results)

# ══════════════════════════════════════════════════════════════
# 텐배거 스크리너 (기존 그대로)
# ══════════════════════════════════════════════════════════════

def score_lynch(info, hist_daily):
    score=0; detail={}
    peg=info.get('pegRatio',999) or 999
    if peg<=0: peg_score=0
    elif peg<=0.5: peg_score=15
    elif peg<=0.75: peg_score=12
    elif peg<=1.0: peg_score=9
    elif peg<=1.5: peg_score=5
    elif peg<=2.0: peg_score=2
    else: peg_score=0
    score+=peg_score; detail['peg']=round(peg if peg!=999 else 0,2); detail['peg_score']=peg_score
    eps_growth=info.get('earningsGrowth',0) or 0
    if eps_growth>=0.50: eps_score=10
    elif eps_growth>=0.35: eps_score=8
    elif eps_growth>=0.25: eps_score=6
    elif eps_growth>=0.15: eps_score=3
    else: eps_score=0
    score+=eps_score; detail['eps_growth']=round(eps_growth*100,1); detail['eps_score']=eps_score
    mcap=info.get('marketCap',0) or 0; mcap_b=mcap/1e9
    if mcap_b<=0: mcap_score=0
    elif mcap_b<=0.3: mcap_score=10
    elif mcap_b<=2.0: mcap_score=8
    elif mcap_b<=10.0: mcap_score=5
    elif mcap_b<=50.0: mcap_score=2
    else: mcap_score=0
    score+=mcap_score; detail['market_cap_b']=round(mcap_b,1); detail['mcap_score']=mcap_score
    return score,detail

def score_oneil(info, hist_daily, hist_weekly):
    score=0; detail={}
    eps_growth=info.get('earningsGrowth',0) or 0; quarterly=info.get('earningsQuarterlyGrowth',0) or 0
    c_val=max(eps_growth,quarterly)
    if c_val>=0.50: c_score=10
    elif c_val>=0.35: c_score=8
    elif c_val>=0.25: c_score=6
    elif c_val>=0.15: c_score=3
    else: c_score=0
    score+=c_score; detail['quarterly_eps_growth']=round(c_val*100,1); detail['c_score']=c_score
    try:
        if len(hist_daily)>=252:
            high_52w=float(hist_daily['High'].rolling(252).max().iloc[-1]); current=float(hist_daily['Close'].iloc[-1])
            from_high=(current/high_52w-1)*100; detail['from_52w_high']=round(from_high,1)
            if from_high>=0: n_score=10
            elif from_high>=-3: n_score=8
            elif from_high>=-8: n_score=5
            elif from_high>=-15: n_score=2
            else: n_score=0
        else: n_score=0; detail['from_52w_high']=0
    except: n_score=0; detail['from_52w_high']=0
    score+=n_score; detail['n_score']=n_score
    try:
        if len(hist_daily)>=50:
            avg_50d=float(hist_daily['Volume'].rolling(50).mean().iloc[-1]); recent_5=float(hist_daily['Volume'].iloc[-5:].mean())
            vol_ratio=recent_5/avg_50d if avg_50d>0 else 1.0; detail['vol_ratio_50d']=round(vol_ratio,2)
            if vol_ratio>=2.5: s_score=10
            elif vol_ratio>=2.0: s_score=8
            elif vol_ratio>=1.5: s_score=6
            elif vol_ratio>=1.2: s_score=3
            else: s_score=0
        else: s_score=0; detail['vol_ratio_50d']=0
    except: s_score=0; detail['vol_ratio_50d']=0
    score+=s_score; detail['s_score']=s_score
    rec=info.get('recommendationKey','') or ''
    l_score=5 if rec=='strong_buy' else 3 if rec=='buy' else 0
    score+=l_score; detail['l_score']=l_score
    return score,detail

def score_minervini(info, hist_daily):
    score=0; detail={}
    try:
        close=hist_daily['Close']; current=float(close.iloc[-1]); ma150=ma200=None
        if len(hist_daily)>=200:
            ma150=float(close.rolling(150).mean().iloc[-1]); ma200=float(close.rolling(200).mean().iloc[-1])
            detail['ma150']=round(ma150,2); detail['ma200']=round(ma200,2)
            above_both=(current>ma150) and (current>ma200)
            if above_both:
                ratio=current/((ma150+ma200)/2)
                if ratio>=1.15: cond1=8
                elif ratio>=1.08: cond1=6
                elif ratio>=1.02: cond1=4
                else: cond1=2
            else: cond1=0
        else: cond1=0; detail['ma150']=0; detail['ma200']=0
        score+=cond1; detail['above_ma_score']=cond1
        if ma150 and ma200:
            if ma150>ma200:
                r=ma150/ma200
                if r>=1.05: cond2=7
                elif r>=1.02: cond2=5
                else: cond2=3
            else: cond2=0
        else: cond2=0
        score+=cond2; detail['ma_cross_score']=cond2
        if len(hist_daily)>=220:
            ma200_1m=float(close.rolling(200).mean().iloc[-22])
            if ma200 and ma200>ma200_1m:
                sp=(ma200/ma200_1m-1)*100
                if sp>=3.0: cond3=7
                elif sp>=1.5: cond3=5
                elif sp>=0.5: cond3=3
                else: cond3=1
            else: cond3=0
        else: cond3=0
        score+=cond3; detail['ma200_slope_score']=cond3
        if len(hist_daily)>=252:
            low_52w=float(hist_daily['Low'].rolling(252).min().iloc[-1]); from_low=(current/low_52w-1)*100
            detail['from_52w_low']=round(from_low,1)
            if from_low>=100: cond4=8
            elif from_low>=60: cond4=6
            elif from_low>=30: cond4=4
            elif from_low>=15: cond4=2
            else: cond4=0
        else: cond4=0; detail['from_52w_low']=0
        score+=cond4; detail['from_low_score']=cond4
    except: detail={'above_ma_score':0,'ma_cross_score':0,'ma200_slope_score':0,'from_low_score':0}
    return score,detail

def fetch_tenbagger_stock(ticker):
    try:
        stock=yf.Ticker(ticker); info=stock.info
        hist_daily=stock.history(period="1y"); hist_weekly=stock.history(period="2y",interval="1wk")
        if hist_daily.empty or len(hist_daily)<60: return None
        price_check=info.get('regularMarketPrice') or info.get('currentPrice') or info.get('previousClose')
        if not price_check: return None
        lynch_score,lynch_detail=score_lynch(info,hist_daily)
        oneil_score,oneil_detail=score_oneil(info,hist_daily,hist_weekly)
        minervini_score,minervini_detail=score_minervini(info,hist_daily)
        total=lynch_score+oneil_score+minervini_score
        current_price=float(hist_daily['Close'].iloc[-1]); prev_price=float(hist_daily['Close'].iloc[-2])
        change_pct=(current_price/prev_price-1)*100
        if total>=75: grade="🔥 최상위"
        elif total>=60: grade="⭐ 유망"
        elif total>=45: grade="👀 관심"
        else: grade="💤 미해당"
        mcap=info.get('marketCap',0) or 0
        return {
            "ticker":ticker,"name":info.get('longName',ticker),"sector":info.get('sector','Unknown'),
            "market_cap_b":round(mcap/1e9,1),"price":round(current_price,2),"change_pct":round(change_pct,2),
            "lynch_score":lynch_score,"oneil_score":oneil_score,"minervini_score":minervini_score,
            "total_score":total,"grade":grade,
            "lynch_detail":lynch_detail,"oneil_detail":oneil_detail,"minervini_detail":minervini_detail,
        }
    except: return None

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
# 유동성 모듈 (기존 그대로)
# ══════════════════════════════════════════════════════════════

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
FRED_BASE    = "https://api.stlouisfed.org/fred/series/observations"
_liquidity_cache: dict = {}

def fetch_fred(series_id: str, limit: int = 20):
    if not FRED_API_KEY: return None
    try:
        params = {"series_id":series_id,"api_key":FRED_API_KEY,"file_type":"json",
                  "sort_order":"desc","limit":limit,
                  "observation_start":(datetime.now()-timedelta(days=2200)).strftime("%Y-%m-%d")}
        resp = requests.get(FRED_BASE, params=params, timeout=10)
        if resp.status_code != 200: return None
        result = [{"date":obs["date"],"value":float(obs["value"])}
                  for obs in resp.json().get("observations",[]) if obs["value"]!="."]
        if len(result)>=2: _liquidity_cache[series_id]=result
        return result if result else None
    except: return None

def fetch_fred_cached(series_id: str, limit: int = 20):
    fresh = fetch_fred(series_id, limit)
    if fresh is not None: return fresh, False
    cached = _liquidity_cache.get(series_id)
    return (cached, True) if cached else (None, False)

def _err_ind(label, fred_id, max_score, is_cached):
    return {"label":label,"fred_id":fred_id,"score":None,"max_score":max_score,
            "error":True,"is_cached":is_cached,"status":"⚠️ 데이터 수집 실패 — 점수 산정 제외",
            "value":0,"value_unit":"—","change_pct":0,"history":[],"context":""}

MMF_RETAIL_RATIO = 0.394

def score_net_liquidity(walcl_data, rrp_data, tga_data):
    label="Fed 순유동성 (WALCL − RRP − TGA)"
    if walcl_data is None or rrp_data is None or tga_data is None:
        return {"label":label,"score":None,"max_score":40,"error":True,
                "status":"⚠️ 데이터 부족 — 순유동성 계산 불가","value":0,"value_unit":"조 달러",
                "walcl_t":0,"rrp_t":0,"tga_t":0,"change_t":0,"change_pct":0,"history":[],"context":""}
    walcl=walcl_data[0]["value"]/1e6; rrp=rrp_data[0]["value"]/1e6; tga=tga_data[0]["value"]/1e6
    net=round(walcl-rrp-tga,2)
    walcl_4w=(walcl_data[4]["value"] if len(walcl_data)>4 else walcl_data[-1]["value"])/1e6
    rrp_4w=(rrp_data[4]["value"] if len(rrp_data)>4 else rrp_data[-1]["value"])/1e6
    tga_4w=(tga_data[4]["value"] if len(tga_data)>4 else tga_data[-1]["value"])/1e6
    net_4w=round(walcl_4w-rrp_4w-tga_4w,2)
    change_t=round(net-net_4w,2); change_pct=round((net-net_4w)/abs(net_4w)*100,2) if net_4w!=0 else 0
    if net>=6.0:   level_s,level_d=25,"순유동성 매우 풍부 ($6조+)"
    elif net>=5.0: level_s,level_d=20,"순유동성 풍부 ($5~6조)"
    elif net>=4.0: level_s,level_d=14,f"순유동성 보통 (${net}조)"
    elif net>=3.0: level_s,level_d=8,"순유동성 타이트 ($3~4조)"
    else:          level_s,level_d=2,"순유동성 경색 ($3조 미만)"
    if change_t>=0.3:    dir_s,dir_d=15,f"증가 ({change_t:+.2f}조) — 유동성 공급 가속"
    elif change_t>=0.1:  dir_s,dir_d=12,f"소폭 증가 ({change_t:+.2f}조)"
    elif change_t>=-0.1: dir_s,dir_d=8,"보합"
    elif change_t>=-0.3: dir_s,dir_d=4,f"소폭 감소 ({change_t:+.2f}조)"
    else:                dir_s,dir_d=0,f"감소 ({change_t:+.2f}조) — 유동성 회수"
    return {"label":label,"score":level_s+dir_s,"max_score":40,"error":False,
            "status":f"{level_d} / {dir_d}","value":net,"value_unit":"조 달러",
            "walcl_t":round(walcl,2),"rrp_t":round(rrp,3),"tga_t":round(tga,3),
            "change_t":change_t,"change_pct":change_pct,"history":[],
            "context":f"WALCL ${walcl:.2f}조 − RRP ${rrp:.3f}조 − TGA ${tga:.3f}조 = ${net}조"}

def score_mmf(data, is_cached=False):
    label,fred_id="MMF 총잔액 (소매 기반 전체 추정)","WRMFNS"
    if data is None or len(data)<5: return _err_ind(label,fred_id,20,is_cached)
    latest=data[0]["value"]; prev4w=data[4]["value"] if len(data)>4 else data[-1]["value"]
    prev12w=data[12]["value"] if len(data)>12 else data[-1]["value"]
    total_est_t=round(latest/MMF_RETAIL_RATIO/1000,2); retail_b=round(latest,1)
    change_4w=round((latest-prev4w)/prev4w*100,2) if prev4w>0 else 0
    change_12w=round((latest-prev12w)/prev12w*100,2) if prev12w>0 else 0
    if change_4w<-1.5:   score,status=20,"MMF 빠른 감소 — 위험자산으로 자금 이동, 강한 매수 환경"
    elif change_4w<-0.3: score,status=16,"MMF 감소 전환 — 위험선호 회복, 매수 우호"
    elif change_4w<=0.5:
        if change_12w<-0.5:   score,status=13,"MMF 보합 (중장기 감소 추세) — 완만한 위험선호 회복"
        elif change_12w>1.0:  score,status=6,"MMF 보합이나 중장기 증가 추세 — 위험회피 지속"
        else:                 score,status=10,"MMF 보합 — 대기 자금 유지, 중립"
    elif change_4w<=2.0: score,status=5,"MMF 증가 — 안전자산 선호, 위험회피 강화"
    else:                score,status=2,"MMF 급증 — 강한 위험회피, 공포 자금 대피 중"
    return {"label":label,"fred_id":fred_id,"score":score,"max_score":20,
            "error":False,"is_cached":is_cached,"status":status,
            "value":total_est_t,"value_unit":"조 달러 (추정)","value_retail":retail_b,
            "change_pct":change_4w,
            "history":[{"date":d["date"],"value":round(d["value"]/MMF_RETAIL_RATIO/1000,2)} for d in data[:260]],
            "context":(f"소매 실측: ${retail_b:.0f}B / 전체 추정: ~${total_est_t}조 / 12주 추세: {change_12w:+.1f}%"),
            "note":"기관 MMF(WIMFNS) 2021년 폐기 → 소매 기반 추정. 방향성 신호 기준."}

TGA_SEASONAL_ADJ={1:-100,2:-50,3:0,4:250,5:100,6:0,7:-50,8:-50,9:100,10:0,11:-50,12:-100}

def detail_walcl(data, is_cached=False):
    label,fred_id="연준 총자산 (WALCL)","WALCL"
    if data is None or len(data)<5: return _err_ind(label,fred_id,0,is_cached)
    latest=data[0]["value"]; prev4w=data[4]["value"] if len(data)>4 else data[-1]["value"]
    prev12w=data[12]["value"] if len(data)>12 else data[-1]["value"]
    chg4w=round((latest-prev4w)/prev4w*100,3) if prev4w else 0
    chg12w=round((latest-prev12w)/prev12w*100,3) if prev12w else 0
    mon_b=round((latest-prev4w)/1000,1); total_t=round(latest/1e6,2)
    if chg4w>0.3:    st="QE — 연준 자산 증가, 유동성 공급"
    elif chg4w>0:    st="소폭 증가 — 유동성 유지 (QT 종료 후 정상)"
    elif mon_b>=-25: st="완만한 감소 (월 $250억↓) — 시장 충격 제한적"
    elif mon_b>=-60: st="중간 QT (월 $250~600억) — 유동성 점진적 감소"
    else:            st="강한 QT (월 $600억+) — 유동성 급속 회수"
    return {"label":label,"fred_id":fred_id,"score":None,"max_score":0,
            "error":False,"is_cached":is_cached,"status":st,
            "value":total_t,"value_unit":"조 달러","change_pct":chg4w,"monthly_change_b":mon_b,
            "history":[{"date":d["date"],"value":round(d["value"]/1e6,2)} for d in data[:260]],
            "context":f"4주: {chg4w:+.3f}% / 12주: {chg12w:+.3f}% / 월 변화: ${mon_b:+.0f}B"}

def detail_rrp(data, is_cached=False):
    label,fred_id="역레포(RRP) 잔액","RRPONTSYD"
    if data is None or len(data)<2: return _err_ind(label,fred_id,0,is_cached)
    latest=data[0]["value"]
    # RRP 일별 데이터 → 4주 전 = index 20
    prev4w=data[20]["value"] if len(data)>20 else data[-1]["value"]
    latest_b=round(latest/1000,1)
    # $10B 미만 = 완전 소진 → 변화율 계산 무의미 (분모≈0 → 수천% 왜곡)
    if latest_b <= 10:
        chg_pct=0
        st="완전 소진 — RRP 버퍼 소멸. 지급준비금에 의존하는 단계."
        ctx="피크($2.5조) 대비 100% 소진 — 잔액 $0, 변화율 표시 불가"
    else:
        chg_pct=round((latest-prev4w)/prev4w*100,2) if prev4w>0 else 0
        rising=latest>prev4w; depl=round((1-latest_b/2500)*100,1)
        if latest_b>500:   st="감소 중 → 유동성 유입 (버퍼 충분)" if not rising else "증가 중 → 유동성 흡수"
        elif latest_b>100: st="소진 진행 중 → 유입 지속" if not rising else "소진 단계에서 재증가 → 주의"
        else:              st="거의 소진 — 추가 공급 여력 없음 (중립)"
        ctx=f"피크($2.5조) 대비 {depl}% 소진 / 4주 변화: {chg_pct:+.1f}%"
    return {"label":label,"fred_id":fred_id,"score":None,"max_score":0,
            "error":False,"is_cached":is_cached,"status":st,
            "value":latest_b,"value_unit":"십억 달러","change_pct":chg_pct,
            "history":[{"date":d["date"],"value":round(d["value"]/1000,1)} for d in data[:1300]],
            "context":ctx}

def detail_tga(data, is_cached=False):
    label,fred_id="TGA(재무부 계정) 잔액","WTREGEN"
    if data is None or len(data)<2: return _err_ind(label,fred_id,0,is_cached)
    latest=data[0]["value"]; prev4w=data[4]["value"] if len(data)>4 else data[-1]["value"]
    latest_b=round(latest/1000,1); chg_pct=round((latest-prev4w)/prev4w*100,2) if prev4w>0 else 0
    chg_b=round((latest-prev4w)/1000,1)
    sadj=TGA_SEASONAL_ADJ.get(datetime.now().month,0); eff_b=latest_b-sadj
    sadj_note=f" (계절조정 {sadj:+d}B → 실효 ${eff_b:.0f}B)" if sadj!=0 else ""
    if eff_b<300:    lv="낮음 — 정부 지출, 유동성 공급"
    elif eff_b<500:  lv="정상 수준 ($300~500B)"
    elif eff_b<800:  lv="높음 ($500~800B) — 유동성 소폭 압박"
    elif eff_b<1000: lv="⚠️ 임계점 초과 ($800B+) — 레포 긴축 위험"
    else:            lv="🚨 위험 구간 ($1조+) — 심각한 유동성 압박"
    gap=800-latest_b
    ctx=f"$800B 임계점 ${abs(gap):.0f}B {'여유' if gap>0 else '초과 ⚠️'}"
    return {"label":label,"fred_id":fred_id,"score":None,"max_score":0,
            "error":False,"is_cached":is_cached,
            "status":f"{lv} / 4주 변화 {chg_b:+.0f}B{sadj_note}",
            "value":latest_b,"value_unit":"십억 달러","change_pct":chg_pct,
            "seasonal_adj":sadj,"effective_b":eff_b,
            "history":[{"date":d["date"],"value":round(d["value"]/1000,1)} for d in data[:260]],
            "context":ctx}

def get_liquidity_signal(total_score: int) -> dict:
    if total_score>=75:
        return {"stage":1,"signal":"적극매수","emoji":"🟢","color":"#15803d","bg_color":"#dcfce7","border_color":"#86efac",
                "description":"순유동성이 풍부하고 MMF 자금이 위험자산으로 이동 중입니다.",
                "action":"스크리닝 신호를 적극 반영하세요. 마구스코어 65점+ 종목 분할 매수 고려.",
                "step2_guide":"✅ 스크리닝 신호 적극 반영 — 분할 매수 진입 권장"}
    elif total_score>=55:
        return {"stage":2,"signal":"매수우호","emoji":"🔵","color":"#1d4ed8","bg_color":"#dbeafe","border_color":"#93c5fd",
                "description":"순유동성이 양호합니다. 시장 환경이 매수에 우호적입니다.",
                "action":"스크리닝 결과를 참고하여 선별적으로 매수하세요.",
                "step2_guide":"✅ 스크리닝 신호 참고 — 마구스코어 70점+ 종목 위주 선별 매수"}
    elif total_score>=38:
        return {"stage":3,"signal":"중립관망","emoji":"🟡","color":"#b45309","bg_color":"#fef9c3","border_color":"#fde68a",
                "description":"순유동성 방향이 불확실합니다. 긍정/부정 신호가 혼재합니다.",
                "action":"신규 매수 자제. 기존 포지션 유지하며 방향 확인 후 판단하세요.",
                "step2_guide":"⚠️ 스크리닝 참고만 — 신규 매수 자제, 기존 보유 종목 유지"}
    elif total_score>=20:
        return {"stage":4,"signal":"매수축소","emoji":"🟠","color":"#c2410c","bg_color":"#ffedd5","border_color":"#fdba74",
                "description":"순유동성이 감소하고 있습니다. 위험 관리가 필요합니다.",
                "action":"신규 매수 중단. 보유 종목 비중 축소 및 손절 기준 점검하세요.",
                "step2_guide":"🚫 스크리닝 결과 무시 — 포지션 축소, 현금 비중 확대"}
    else:
        return {"stage":5,"signal":"현금보유","emoji":"🔴","color":"#991b1b","bg_color":"#fee2e2","border_color":"#fca5a5",
                "description":"순유동성이 심각하게 경색되어 있습니다.",
                "action":"전량 현금 보유 권고. 스크리닝 결과와 무관하게 매수 금지.",
                "step2_guide":"🔴 스크리닝 결과 무시 — 전량 현금 보유, 매수 금지"}

# ══════════════════════════════════════════════════════════════
# API 엔드포인트
# ══════════════════════════════════════════════════════════════

@app.get("/")
def root():
    return {"status":"MAGU STOCK API 실행 중","version":"2.0"}

@app.get("/api/market")
def get_market_data():
    try:
        tickers={"gold":"GC=F","wti":"CL=F","usdkrw":"KRW=X","us10y":"^TNX",
                 "vix":"^VIX","sp500":"^GSPC","nasdaq":"^IXIC","dow":"^DJI","russell":"^RUT"}
        result={}
        for key,symbol in tickers.items():
            try:
                t=yf.Ticker(symbol); hist=t.history(period="10d")
                if len(hist)>=2:
                    current=hist['Close'].iloc[-1]; prev=hist['Close'].iloc[-2]
                    row={"value":round(current,2),"change":round((current/prev-1)*100,2)}
                    # VIX: 5일 전 대비 방향성 추가
                    if key=="vix" and len(hist)>=6:
                        prev5=hist['Close'].iloc[-6]
                        row["direction"]="up" if current>prev5 else "down"
                        row["prev5"]=round(prev5,2)
                    result[key]=row
            except: result[key]={"value":0,"change":0}

        # ── 하이일드 스프레드 (HYG vs LQD) ──────────────────────
        try:
            hyg_ticker = yf.Ticker("HYG")
            lqd_ticker = yf.Ticker("LQD")
            hyg = hyg_ticker.history(period="10d")
            lqd = lqd_ticker.history(period="10d")
            if len(hyg)>=6 and len(lqd)>=6:
                hyg_ret  = (hyg['Close'].iloc[-1]/hyg['Close'].iloc[-2]-1)*100
                lqd_ret  = (lqd['Close'].iloc[-1]/lqd['Close'].iloc[-2]-1)*100
                spread_now  = lqd_ret - hyg_ret
                hyg_5d = (hyg['Close'].iloc[-1]/hyg['Close'].iloc[-6]-1)*100
                lqd_5d = (lqd['Close'].iloc[-1]/lqd['Close'].iloc[-6]-1)*100
                spread_5d   = lqd_5d - hyg_5d
                direction = "narrowing" if spread_5d < 0 else "widening"

                # ── 실제 신용 스프레드 bps (HYG yield - LQD yield) ──
                try:
                    hyg_info = hyg_ticker.info
                    lqd_info = lqd_ticker.info
                    hyg_yield = hyg_info.get("trailingAnnualDividendYield") or hyg_info.get("yield")
                    lqd_yield = lqd_info.get("trailingAnnualDividendYield") or lqd_info.get("yield")
                    if hyg_yield and lqd_yield:
                        spread_bps = round((hyg_yield - lqd_yield) * 10000, 0)
                    else:
                        spread_bps = None
                except:
                    spread_bps = None

                result["high_yield"]={
                    "hyg": round(hyg['Close'].iloc[-1],2),
                    "lqd": round(lqd['Close'].iloc[-1],2),
                    "spread_1d": round(spread_now,3),
                    "spread_5d": round(spread_5d,3),
                    "spread_bps": spread_bps,
                    "direction": direction,
                    "signal": "리스크온 🟢" if direction=="narrowing" else "리스크오프 🔴"
                }
        except Exception as e:
            logger.warning(f"하이일드 스프레드 오류: {e}")
            result["high_yield"]={"direction":"unknown","signal":"데이터 없음"}

        return result
    except: return {}


@app.get("/api/fear_greed")
def get_fear_greed():
    """CNN 공포탐욕지수 — CNN 내부 API 직접 호출"""
    try:
        url="https://production.dataviz.cnn.io/index/fearandgreed/graphdata/"
        headers={
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer":"https://edition.cnn.com/markets/fear-and-greed",
            "Accept":"application/json, text/plain, */*",
            "Origin":"https://edition.cnn.com",
        }
        resp=requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data=resp.json()
        fg=data.get("fear_and_greed",{})

        score       = round(float(fg.get("score", 50)), 1)
        rating      = fg.get("rating", "neutral")
        prev_close  = round(float(fg.get("previous_close", score)), 1)
        week_ago    = round(float(fg.get("previous_1_week", score)), 1)
        month_ago   = round(float(fg.get("previous_1_month", score)), 1)

        # 신호 판단
        if score <= 10:   signal, color = "적극 분할 매수 고려", "#15803d"
        elif score <= 25: signal, color = "분할 매수 고려", "#f59e0b"
        elif score <= 45: signal, color = "관망 (중립 하단)", "#b45309"
        elif score <= 55: signal, color = "관망 (중립)", "#6b7280"
        elif score <= 75: signal, color = "신규 매수 자제", "#1d4ed8"
        else:             signal, color = "비중 축소 고려", "#991b1b"

        return {
            "score": score, "rating": rating,
            "signal": signal, "color": color,
            "prev_close": prev_close,
            "week_ago": week_ago,
            "month_ago": month_ago,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M")
        }
    except Exception as e:
        logger.warning(f"CNN 공포탐욕 오류: {e}")
        return {"score": None, "signal": "데이터 수집 실패", "error": str(e)}


@app.get("/api/breadth")
def get_market_breadth():
    """MMTH 자체 계산 — DB 스크리닝 결과 기반 시장 폭 지표"""
    if not DATABASE_URL:
        return {"error":"DATABASE_URL 없음"}
    try:
        conn=get_conn(); cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # 전체 + 마켓별 200일선 위 비율
        cur.execute("""
            SELECT
                market,
                COUNT(*) AS total,
                SUM(CASE WHEN g_ma200 >= 2 THEN 1 ELSE 0 END) AS above_ma200,
                ROUND(SUM(CASE WHEN g_ma200 >= 2 THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct
            FROM screening_cache
            WHERE screened_at > NOW() - INTERVAL '25 hours'
            GROUP BY market
        """)
        rows=[dict(r) for r in cur.fetchall()]
        cur.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN g_ma200 >= 2 THEN 1 ELSE 0 END) AS above_ma200,
                ROUND(SUM(CASE WHEN g_ma200 >= 2 THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct
            FROM screening_cache
            WHERE screened_at > NOW() - INTERVAL '25 hours'
        """)
        total=dict(cur.fetchone())
        cur.close(); conn.close()

        pct=float(total.get("pct") or 0)
        if pct>=70:   signal,color="🟢 시장 폭 양호 — 광범위한 강세","#15803d"
        elif pct>=40: signal,color="🟡 혼조 — 선별적 접근","#b45309"
        elif pct>=20: signal,color="🟠 시장 폭 붕괴 주의","#c2410c"
        else:         signal,color="🔴 극단 공포 — 역발상 매수 고려","#991b1b"

        return {
            "total_stocks": int(total.get("total") or 0),
            "above_ma200":  int(total.get("above_ma200") or 0),
            "pct": pct,
            "signal": signal,
            "color": color,
            "by_market": rows,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M")
        }
    except Exception as e:
        logger.error(f"시장 폭 오류: {e}")
        return {"error":str(e)}

def _market_label(market):
    return {"nasdaq":"나스닥","sp500":"S&P500","kospi":"코스피","kosdaq":"코스닥","us":"미국 전체","kr":"한국 전체"}.get(market,market)

def _currency(market):
    return "KRW" if market in ("kospi","kosdaq","kr") else "USD"

@app.get("/api/screen/{market}")
def screen_stocks(market: str = "nasdaq"):
    """DB 캐시 우선 → 없으면 실시간 계산 (기존 방식)"""
    cached = load_screening_from_db(market)
    if cached:
        updated_at = cached[0].get("screened_at","–") if cached else "–"
        return {"market":market,"market_label":_market_label(market),"currency":_currency(market),
                "updated_at":updated_at,"total_screened":len(cached),"results":cached,"from_cache":True}

    market_map={"nasdaq":TICKERS_NASDAQ,"sp500":get_tickers_sp500(),
                "kospi":TICKERS_KOSPI,"kosdaq":TICKERS_KOSDAQ,"us":TICKERS_US,"kr":TICKERS_KR}
    tickers=market_map.get(market,TICKERS_NASDAQ)
    results=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures={executor.submit(fetch_single_stock,t,market):t for t in tickers}
        for f in concurrent.futures.as_completed(futures):
            r=f.result()
            if r: results.append(r)
    results.sort(key=lambda x:x['total_score'],reverse=True)
    results=get_portfolio_weight(results)
    return {"market":market,"market_label":_market_label(market),"currency":_currency(market),
            "updated_at":datetime.now().strftime("%Y-%m-%d %H:%M"),
            "total_screened":len(results),"results":results,"from_cache":False}

@app.post("/api/screen/run")
def trigger_screening(background_tasks: BackgroundTasks):
    """수동으로 전체 스크리닝 즉시 실행 (백그라운드)"""
    background_tasks.add_task(run_full_screening_job)
    return {"message":"스크리닝 시작됨. 나스닥200+S&P500+코스피200+코스닥150 약 10~15분 소요. /api/screen/status 로 확인하세요."}

@app.get("/api/screen/status")
def screening_status():
    if not DATABASE_URL: return {"error":"DATABASE_URL 없음"}
    try:
        conn=get_conn(); cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT market, COUNT(*) as cnt, MAX(screened_at) as last_run FROM screening_cache GROUP BY market ORDER BY market")
        rows=cur.fetchall(); cur.close(); conn.close()
        return {"status":[dict(r) for r in rows]}
    except Exception as e: return {"error":str(e)}

@app.get("/api/stock/search")
def search_stock_by_name(q: str = ""):
    """한국어 종목명 부분 검색 → 후보 목록 반환"""
    q = q.strip()
    if not q:
        return {"candidates": []}
    q_lower = q.lower()
    candidates = []
    for ticker, name in KR_NAMES.items():
        if q_lower in name.lower() or q_lower in ticker.lower():
            candidates.append({"ticker": ticker, "name": name})
    # 정렬: 정확히 일치 → 시작 일치 → 포함
    def sort_key(c):
        n = c["name"].lower()
        if n == q_lower: return 0
        if n.startswith(q_lower): return 1
        return 2
    candidates.sort(key=sort_key)
    return {"candidates": candidates[:10]}

@app.get("/api/stock/{ticker}")
def get_stock_score(ticker: str):
    ticker=ticker.upper().strip()
    try:
        stock=yf.Ticker(ticker); info=stock.info
        price_check=info.get('regularMarketPrice') or info.get('currentPrice') or info.get('previousClose')
        if not info or not price_check: return {"error":f"종목을 찾을 수 없습니다: {ticker}"}
        hist_daily=stock.history(period="1y"); hist_weekly=stock.history(period="2y",interval="1wk")
        if hist_daily.empty or len(hist_daily)<20: return {"error":"데이터가 부족합니다"}
        c_ema=score_ema_slope(hist_weekly); c_stoch=score_stochastic(hist_daily); c_break=score_breakout(hist_daily)
        classic=c_ema+(c_stoch//2 if c_ema==0 else c_stoch)+(c_break//2 if c_ema==0 else c_break)
        g_roe=score_roe(info.get('returnOnEquity',0) or 0); g_debt=score_debt(info.get('debtToEquity',999) or 999)
        g_eps=score_eps_growth(info); g_peg=score_peg(info.get('pegRatio',None))
        g_ma200=score_ma200(hist_daily); g_rsi=score_rsi(hist_daily)
        growth=g_roe+g_debt+g_eps+g_peg+g_ma200+g_rsi
        m_anal=score_analyst(info.get('recommendationKey','') or '')
        m_rs=score_relative_strength(hist_daily); m_obv=score_obv_momentum(hist_daily)
        modern=m_anal+m_rs+m_obv; total=classic+growth+modern
        current_price=float(hist_daily['Close'].iloc[-1]); prev_price=float(hist_daily['Close'].iloc[-2])
        change_pct=(current_price/prev_price-1)*100
        rsi_val=0.0
        if len(hist_daily)>=14:
            rsi_val=round(float(ta.momentum.RSIIndicator(hist_daily['Close'],window=14).rsi().iloc[-1]),1)
        year_return=0.0
        if len(hist_daily)>=252:
            year_return=round((hist_daily['Close'].iloc[-1]/hist_daily['Close'].iloc[-252]-1)*100,1)
        name=KR_NAMES.get(ticker) or info.get('longName') or ticker
        return {"ticker":ticker,"name":name,"sector":info.get('sector') or '—',
                "currency":info.get('currency','USD'),"price":round(current_price,2),
                "change_pct":round(change_pct,2),"classic_score":classic,
                "growth_score":growth,"modern_score":modern,"total_score":total,
                "recommendation":get_recommendation(total,classic,growth,modern),
                "detail":{"roe":round((info.get('returnOnEquity') or 0)*100,1),
                          "debt_equity":round(info.get('debtToEquity') or 0,1),
                          "eps_growth":round((info.get('earningsGrowth') or 0)*100,1),
                          "peg":round(info.get('pegRatio') or 0,2),
                          "rsi":rsi_val,"year_return":year_return,
                          "analyst_rec":info.get('recommendationKey') or '—',
                          "market_cap":info.get('marketCap') or 0}}
    except Exception as e: return {"error":f"조회 실패: {str(e)}"}

def analyze_etf(etf_info: dict):
    ticker=etf_info["ticker"]
    try:
        t=yf.Ticker(ticker); info=t.info; hist=t.history(period="2y")
        if hist.empty or len(hist)<60: return None
        price=float(hist['Close'].iloc[-1])
        p1d=float(hist['Close'].iloc[-2])  if len(hist)>=2   else price
        p1w=float(hist['Close'].iloc[-6])  if len(hist)>=6   else price
        p1m=float(hist['Close'].iloc[-22]) if len(hist)>=22  else price
        p3m=float(hist['Close'].iloc[-66]) if len(hist)>=66  else price
        p6m=float(hist['Close'].iloc[-132])if len(hist)>=132 else price
        p1y=float(hist['Close'].iloc[-252])if len(hist)>=252 else float(hist['Close'].iloc[0])
        r1d=round((price/p1d-1)*100,2); r1w=round((price/p1w-1)*100,2)
        r1m=round((price/p1m-1)*100,2); r3m=round((price/p3m-1)*100,2)
        r6m=round((price/p6m-1)*100,2); r1y=round((price/p1y-1)*100,2)
        vol5d=float(hist['Volume'].iloc[-5:].mean()); vol20d=float(hist['Volume'].iloc[-20:].mean())
        vol_ratio=round(vol5d/vol20d,2) if vol20d>0 else 1.0
        rsi_val=0.0
        if len(hist)>=14: rsi_val=round(float(ta.momentum.RSIIndicator(hist['Close'],window=14).rsi().iloc[-1]),1)
        high_52w=float(hist['High'].iloc[-252:].max()) if len(hist)>=252 else float(hist['High'].max())
        from_high=round((price/high_52w-1)*100,1)
        inst_pct=round(float(info.get('heldPercentInstitutions') or 0)*100,1)
        sc=0
        if r1m>5:sc+=3
        elif r1m>2:sc+=2
        elif r1m>0:sc+=1
        elif r1m<-5:sc-=3
        elif r1m<-2:sc-=2
        elif r1m<0:sc-=1
        if r3m>10:sc+=2
        elif r3m>3:sc+=1
        elif r3m<-10:sc-=2
        elif r3m<-3:sc-=1
        if vol_ratio>1.3:sc+=2
        elif vol_ratio>1.1:sc+=1
        elif vol_ratio<0.7:sc-=2
        elif vol_ratio<0.9:sc-=1
        if rsi_val>60:sc+=1
        elif rsi_val<40:sc-=1
        if sc>=5:    trend,ts="강한유입",10
        elif sc>=3:  trend,ts="유입",5
        elif sc>=1:  trend,ts="소폭유입",3
        elif sc>=-1: trend,ts="중립",0
        elif sc>=-3: trend,ts="소폭유출",-3
        elif sc>=-5: trend,ts="유출",-5
        else:        trend,ts="강한유출",-10
        return {"ticker":ticker,"name":etf_info["name"],"name_en":etf_info["name_en"],"emoji":etf_info["emoji"],
                "price":round(price,2),"ret_1d":r1d,"ret_1w":r1w,"ret_1m":r1m,
                "ret_3m":r3m,"ret_6m":r6m,"ret_1y":r1y,
                "vol_ratio":vol_ratio,"rsi":rsi_val,"from_52w_high":from_high,
                "inst_pct":inst_pct,"trend":trend,"trend_score":ts,"momentum_score":sc}
    except: return None

@app.get("/api/smartmoney")
def get_smart_money():
    results=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures={executor.submit(analyze_etf,etf):etf for etf in SECTOR_ETFS}
        for f in concurrent.futures.as_completed(futures):
            r=f.result()
            if r: results.append(r)
    results.sort(key=lambda x:x["momentum_score"],reverse=True)
    if results:
        avg=sum(r["ret_1m"] for r in results)/len(results)
        for r in results: r["rel_strength"]=round(r["ret_1m"]-avg,2)
    try:
        spy=yf.Ticker("SPY").history(period="3mo")
        spy_1m=round(float((spy['Close'].iloc[-1]/spy['Close'].iloc[-22]-1)*100),2) if len(spy)>=22 else 0
        spy_3m=round(float((spy['Close'].iloc[-1]/spy['Close'].iloc[0]-1)*100),2)
    except: spy_1m=spy_3m=0
    return {"updated_at":datetime.now().strftime("%Y-%m-%d %H:%M"),
            "spy_ret_1m":spy_1m,"spy_ret_3m":spy_3m,"sectors":results,"total":len(results)}

@app.get("/api/tenbagger")
def get_tenbagger():
    if DATABASE_URL:
        try:
            conn=get_conn(); cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT * FROM tenbagger_cache ORDER BY total_score DESC")
            rows=cur.fetchall(); cur.close(); conn.close()
            if rows:
                results=[]
                for r in rows:
                    d=dict(r)
                    if d.get("screened_at"): d["screened_at"]=d["screened_at"].strftime("%Y-%m-%d %H:%M")
                    results.append(d)
                return {"updated_at":results[0].get("screened_at","–"),
                        "total":len(results),"universe":f"나스닥 중소형 성장주 {len(TICKERS_TENBAGGER)}개",
                        "results":results,"from_cache":True}
        except: pass
    results=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures={executor.submit(fetch_tenbagger_stock,t):t for t in TICKERS_TENBAGGER}
        for f in concurrent.futures.as_completed(futures,timeout=180):
            try:
                r=f.result(timeout=20)
                if r: results.append(r)
            except: continue
    results.sort(key=lambda x:x['total_score'],reverse=True)
    return {"updated_at":datetime.now().strftime("%Y-%m-%d %H:%M"),
            "total":len(results),"universe":f"나스닥 중소형 성장주 {len(TICKERS_TENBAGGER)}개",
            "results":results,"from_cache":False}

@app.get("/api/liquidity")
def get_liquidity():
    if not FRED_API_KEY:
        return {"error":"FRED_API_KEY 환경변수가 설정되지 않았습니다.",
                "guide":"https://fred.stlouisfed.org/docs/api/api_key.html 에서 무료 발급 후 Railway 환경변수에 추가하세요.",
                "updated_at":datetime.now().strftime("%Y-%m-%d %H:%M")}
    series_map={"walcl":"WALCL","rrp":"RRPONTSYD","tga":"WTREGEN","mmf":"WRMFNS"}
    # 차트용: 주별 260개(5년), RRP 일별 1300개(5년)
    limit_map={"walcl":260,"rrp":1300,"tga":260,"mmf":260}
    raw,is_cache={},{}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures={executor.submit(fetch_fred_cached,sid,limit_map[key]):key for key,sid in series_map.items()}
        for f in concurrent.futures.as_completed(futures):
            key=futures[f]; data,cached=f.result()
            raw[key]=data; is_cache[key]=cached
    net_liq=score_net_liquidity(raw.get("walcl"),raw.get("rrp"),raw.get("tga"))
    mmf=score_mmf(raw.get("mmf"),is_cache.get("mmf",False))
    walcl_d=detail_walcl(raw.get("walcl"),is_cache.get("walcl",False))
    rrp_d=detail_rrp(raw.get("rrp"),is_cache.get("rrp",False))
    tga_d=detail_tga(raw.get("tga"),is_cache.get("tga",False))
    scored=[i for i in [net_liq,mmf] if i.get("score") is not None]
    if not scored:
        return {"error":"모든 지표 데이터 수집 실패. FRED API 키 및 네트워크를 확인하세요.",
                "updated_at":datetime.now().strftime("%Y-%m-%d %H:%M")}
    raw_score=sum(i["score"] for i in scored); raw_max=sum(i["max_score"] for i in scored)
    total_score=round(raw_score/raw_max*100) if raw_max>0 else 0
    signal=get_liquidity_signal(total_score)
    for ind in [net_liq,mmf,walcl_d,rrp_d,tga_d]:
        if ind.get("score") is None and ind.get("error"):
            ind["score"]="N/A"; ind["status"]="⚠️ 데이터 없음"
    cached_list=[k.upper() for k,v in is_cache.items() if v]
    data_note=(f"⚠️ 캐시 데이터 사용 중: {', '.join(cached_list)}" if cached_list else "✅ 전체 지표 실시간 데이터")
    return {"updated_at":datetime.now().strftime("%Y-%m-%d %H:%M"),
            "total_score":total_score,"max_score":100,"signal":signal,
            "net_liquidity":net_liq,"mmf":mmf,"indicators":[walcl_d,rrp_d,tga_d],
            "data_quality":data_note,"version":"최종판 — 순유동성(WALCL-RRP-TGA) + MMF",
            "scoring_structure":{"순유동성 (40점)":"WALCL-RRP-TGA / 절대수준 25 + 방향성 15",
                                 "MMF (20점)":"소매 WRMFNS×2.54 추정 / 방향성 기준","합계":"60점 → 100점 환산"},
            "sources":["TradingView: Fed Net Liquidity = WALCL-RRP-TGA",
                       "뉴욕 연준 / BlackRock / Cleveland Fed 공식 문헌 2025",
                       "ICI MMF 공식 데이터 (2026.03 $7.86조)",
                       "Babypips: TGA $800B 임계점","McClellan Financial: RRP 소진 분석"],
            "scoring_guide":{"75~100":"🟢 적극매수","55~74":"🔵 매수우호",
                             "38~54":"🟡 중립관망","20~37":"🟠 매수축소","0~19":"🔴 현금보유"}}

# ══════════════════════════════════════════════════════════════
# 베스트픽 백테스트 — 실제 추적 방식
# ══════════════════════════════════════════════════════════════

def select_bestpick_5(screening_results: list) -> list:
    """총점 상위 + 섹터 분산: 섹터별 최고점 1개씩, 부족하면 총점 순으로 채움"""
    candidates = [r for r in screening_results if r.get("recommendation") in ("Strong Buy", "Buy")]
    if not candidates:
        candidates = sorted(screening_results, key=lambda x: x["total_score"], reverse=True)

    # 섹터별 최고점 1개씩 선택
    seen_sectors = {}
    for r in sorted(candidates, key=lambda x: x["total_score"], reverse=True):
        sector = r.get("sector") or "Unknown"
        if sector not in seen_sectors:
            seen_sectors[sector] = r
        if len(seen_sectors) >= 5:
            break

    picks = list(seen_sectors.values())

    # 5개 미만이면 이미 선택된 종목 제외하고 총점 순으로 채움
    if len(picks) < 5:
        picked_tickers = {p["ticker"] for p in picks}
        for r in sorted(candidates, key=lambda x: x["total_score"], reverse=True):
            if r["ticker"] not in picked_tickers:
                picks.append(r)
                picked_tickers.add(r["ticker"])
            if len(picks) >= 5:
                break

    return picks[:5]


def save_bestpick_to_db(picks: list, market: str = "nasdaq") -> dict:
    """베스트픽 5종목을 DB에 저장. 중복 종목은 consecutive_count만 증가."""
    if not picks or not DATABASE_URL:
        return {"saved": 0, "skipped": 0, "error": "DB 없음"}
    today = datetime.now(pytz.timezone("Asia/Seoul")).date()
    saved = 0; skipped = 0
    try:
        conn = get_conn(); cur = conn.cursor()
        for p in picks:
            ticker = p["ticker"]
            # 오늘 이미 기록됐는지 확인 (market 포함)
            cur.execute("SELECT id FROM bestpick_records WHERE ticker=%s AND picked_at=%s AND market=%s", (ticker, today, market))
            if cur.fetchone():
                skipped += 1
                continue
            # 연속 선정 횟수 계산 (어제 같은 마켓 기록이 있으면 +1)
            cur.execute("""
                SELECT consecutive_count FROM bestpick_records
                WHERE ticker=%s AND picked_at = %s - INTERVAL '1 day' AND market=%s
            """, (ticker, today, market))
            row = cur.fetchone()
            consecutive = (row[0] + 1) if row else 1

            cur.execute("""
                INSERT INTO bestpick_records
                    (ticker, name, sector, entry_price, total_score,
                     classic_score, growth_score, modern_score,
                     recommendation, consecutive_count, picked_at, market)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                ticker, p.get("name", ticker), p.get("sector", "Unknown"),
                p.get("price", 0), p.get("total_score", 0),
                p.get("classic_score", 0), p.get("growth_score", 0),
                p.get("modern_score", 0), p.get("recommendation", ""),
                consecutive, today, market
            ))
            record_id = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO bestpick_prices (record_id, ticker, price_date, price, return_pct)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (record_id, ticker, today, p.get("price", 0), 0.0))
            saved += 1
        conn.commit(); cur.close(); conn.close()
        logger.info(f"베스트픽 저장 [{market}]: {saved}개 신규, {skipped}개 중복 스킵")
        return {"saved": saved, "skipped": skipped}
    except Exception as e:
        logger.error(f"베스트픽 저장 오류: {e}")
        return {"saved": 0, "skipped": 0, "error": str(e)}


def update_bestpick_prices_job():
    """매일 장 마감 후 보유 중인 베스트픽 종목들의 현재가 업데이트"""
    if not DATABASE_URL: return
    try:
        conn = get_conn(); cur = conn.cursor()
        # 아직 6개월 이내 기록된 모든 레코드의 ticker + entry_price 조회
        cur.execute("""
            SELECT id, ticker, entry_price FROM bestpick_records
            WHERE picked_at >= CURRENT_DATE - INTERVAL '180 days'
        """)
        records = cur.fetchall()
        if not records:
            cur.close(); conn.close(); return

        today = datetime.now(pytz.timezone("Asia/Seoul")).date()
        tickers = list({r[1] for r in records})

        # yfinance로 현재가 일괄 조회
        prices = {}
        for ticker in tickers:
            try:
                hist = yf.Ticker(ticker).history(period="2d")
                if not hist.empty:
                    prices[ticker] = float(hist["Close"].iloc[-1])
            except: pass

        for record_id, ticker, entry_price in records:
            current = prices.get(ticker)
            if current is None: continue
            ret = round((current / entry_price - 1) * 100, 2) if entry_price else 0
            cur.execute("""
                INSERT INTO bestpick_prices (record_id, ticker, price_date, price, return_pct)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (record_id, price_date) DO UPDATE SET price=EXCLUDED.price, return_pct=EXCLUDED.return_pct
            """, (record_id, ticker, today, current, ret))

        conn.commit(); cur.close(); conn.close()
        logger.info(f"베스트픽 가격 업데이트 완료: {len(records)}건")
    except Exception as e:
        logger.error(f"베스트픽 가격 업데이트 오류: {e}")


@app.post("/api/bestpick/save")
def save_bestpick(market: str = "nasdaq"):
    """스크리닝 결과에서 베스트픽 5종목을 즉시 선정 후 DB 저장
    DB 캐시가 없으면 실시간 스크리닝으로 fallback"""
    # 1) DB 캐시 우선 조회
    candidates = load_screening_from_db(market)

    # 2) DB 캐시 없으면 실시간 스크리닝 (나스닥 상위 50개만 빠르게)
    if not candidates:
        logger.info(f"[베스트픽] DB 캐시 없음 → 실시간 스크리닝 ({market})")
        market_map = {
            "nasdaq": TICKERS_NASDAQ[:50],
            "sp500":  TICKERS_SP500_FALLBACK[:50],
            "kospi":  TICKERS_KOSPI[:50],
            "kosdaq": TICKERS_KOSDAQ[:50],
        }
        tickers = market_map.get(market, TICKERS_NASDAQ[:50])
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_single_stock, t, market): t for t in tickers}
            for f in concurrent.futures.as_completed(futures, timeout=120):
                try:
                    r = f.result(timeout=20)
                    if r: results.append(r)
                except Exception as e:
                    logger.warning(f"[베스트픽 실시간] 오류: {e}")
        if not results:
            return {"error": "스크리닝 결과를 가져올 수 없습니다. 잠시 후 다시 시도해 주세요."}
        results.sort(key=lambda x: x["total_score"], reverse=True)
        candidates = get_portfolio_weight(results)
        # DB에도 저장 (다음번엔 캐시 사용)
        save_screening_to_db(candidates)

    picks = select_bestpick_5(candidates)
    result = save_bestpick_to_db(picks, market=market)
    return {
        "picks": [{"ticker": p["ticker"], "name": p.get("name"), "sector": p.get("sector"),
                   "total_score": p.get("total_score"), "price": p.get("price"),
                   "recommendation": p.get("recommendation")} for p in picks],
        **result
    }


@app.get("/api/bestpick/history")
def get_bestpick_history(market: str = "nasdaq"):
    """베스트픽 전체 이력 + 현재까지 수익률 추적"""
    if not DATABASE_URL:
        return {"error": "DATABASE_URL 없음"}
    try:
        conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                r.id, r.ticker, r.name, r.sector,
                r.entry_price, r.total_score, r.classic_score, r.growth_score, r.modern_score,
                r.recommendation, r.consecutive_count,
                r.picked_at::text AS picked_at,
                p1.price        AS price_1d,
                p1.return_pct   AS return_1d,
                p7.price        AS price_7d,
                p7.return_pct   AS return_7d,
                p30.price       AS price_30d,
                p30.return_pct  AS return_30d,
                pl.price        AS price_latest,
                pl.return_pct   AS return_latest,
                pl.price_date::text AS latest_date
            FROM bestpick_records r
            LEFT JOIN bestpick_prices p1
                ON p1.record_id = r.id
                AND p1.price_date = r.picked_at + INTERVAL '1 day'
            LEFT JOIN bestpick_prices p7
                ON p7.record_id = r.id
                AND p7.price_date = r.picked_at + INTERVAL '7 days'
            LEFT JOIN bestpick_prices p30
                ON p30.record_id = r.id
                AND p30.price_date = r.picked_at + INTERVAL '30 days'
            LEFT JOIN LATERAL (
                SELECT price, return_pct, price_date
                FROM bestpick_prices
                WHERE record_id = r.id
                ORDER BY price_date DESC LIMIT 1
            ) pl ON TRUE
            WHERE r.picked_at >= CURRENT_DATE - INTERVAL '180 days'
              AND r.market = %s
            ORDER BY r.picked_at DESC, r.total_score DESC
        """, (market,))
        rows = [dict(r) for r in cur.fetchall()]

        # 날짜별 그룹핑
        from collections import defaultdict
        grouped = defaultdict(list)
        for row in rows:
            grouped[row["picked_at"]].append(row)

        # 날짜별 평균 수익률 계산
        summary_by_date = []
        for date, items in sorted(grouped.items(), reverse=True):
            avg_latest = None
            valid = [i["return_latest"] for i in items if i["return_latest"] is not None]
            if valid:
                avg_latest = round(sum(valid) / len(valid), 2)
            summary_by_date.append({
                "date": date,
                "count": len(items),
                "avg_return_latest": avg_latest,
                "picks": items
            })

        # 전체 통계
        all_returns = [r["return_latest"] for r in rows if r["return_latest"] is not None]
        overall_avg = round(sum(all_returns) / len(all_returns), 2) if all_returns else None
        win_count = sum(1 for r in all_returns if r > 0)
        win_rate = round(win_count / len(all_returns) * 100, 1) if all_returns else None

        cur.close(); conn.close()
        return {
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "total_records": len(rows),
            "overall_avg_return": overall_avg,
            "overall_win_rate": win_rate,
            "history": summary_by_date
        }
    except Exception as e:
        logger.error(f"베스트픽 이력 조회 오류: {e}")
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════════
# APScheduler — 매일 KST 04:00 자동 실행
# ══════════════════════════════════════════════════════════════

scheduler = BackgroundScheduler(timezone=pytz.utc)
scheduler.add_job(
    run_full_screening_job,
    CronTrigger(hour=19, minute=0, timezone=pytz.utc),  # UTC 19:00 = KST 04:00
    id="daily_screening",
    replace_existing=True,
    misfire_grace_time=3600
)
# 매일 KST 08:00 (UTC 23:00) — 전날 장 마감 후 베스트픽 가격 업데이트
scheduler.add_job(
    update_bestpick_prices_job,
    CronTrigger(hour=23, minute=0, timezone=pytz.utc),
    id="daily_bestpick_price",
    replace_existing=True,
    misfire_grace_time=3600
)

@app.on_event("startup")
def on_startup():
    if DATABASE_URL:
        try:
            init_db()
            logger.info("DB 연결 및 초기화 완료")
        except Exception as e:
            logger.error(f"DB 초기화 실패: {e}")
    else:
        logger.warning("DATABASE_URL 없음 — 실시간 계산 모드로 동작")
    scheduler.start()
    logger.info("APScheduler 시작 — 매일 KST 04:00 전체 스크리닝 예약됨")

@app.on_event("shutdown")
def on_shutdown():
    scheduler.shutdown()

from fastapi import FastAPI, BackgroundTasks, Request, Response, Cookie
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
import secrets
import hashlib
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import yfinance as yf
import ta
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import concurrent.futures
import os
import requests
import psycopg2
import psycopg2.extras
import json
import math
import re
import pytz
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def sanitize(obj):
    """NaN/Inf float 값을 None으로 변환 — JSON 직렬화 오류 방지"""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, (np.floating, np.float64, np.float32)):
        v = float(obj)
        return None if (math.isnan(v) or math.isinf(v)) else v
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize(v) for v in obj]
    return obj

def safe_json(data):
    """sanitize 후 JSONResponse 반환"""
    return JSONResponse(content=sanitize(data))

# ── Rate Limiter 설정 ──
limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])
app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ── 비밀번호 인증 설정 ──
SITE_PASSWORD = os.environ.get("SITE_PASSWORD", "wisemac2024")
SESSION_TOKEN = hashlib.sha256(SITE_PASSWORD.encode()).hexdigest()

def is_authenticated(session: str = None) -> bool:
    return session == SESSION_TOKEN

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if os.environ.get("BYPASS_AUTH") == "1":
        return await call_next(request)
    public_paths = {"/", "/login", "/api/auth/login", "/api/auth/check"}
    if request.url.path in public_paths:
        return await call_next(request)
    session = request.cookies.get("session")
    if not is_authenticated(session):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return await call_next(request)

@app.post("/api/auth/login")
async def login(request: Request, response: Response):
    try:
        body = await request.json()
        password = body.get("password", "")
    except:
        return JSONResponse({"ok": False, "message": "잘못된 요청"}, status_code=400)

    if password == SITE_PASSWORD:
        response.set_cookie(
            key="session",
            value=SESSION_TOKEN,
            httponly=True,
            max_age=None,   # 세션 쿠키 — 브라우저 닫으면 만료
            samesite="lax"
        )
        return {"ok": True}
    else:
        return JSONResponse({"ok": False, "message": "비밀번호가 틀렸습니다"}, status_code=401)

@app.get("/api/auth/check")
def auth_check(session: str = Cookie(default=None)):
    return {"authenticated": is_authenticated(session)}

@app.get("/dashboard")
def dashboard(session: str = Cookie(default=None)):
    if not is_authenticated(session):
        return RedirectResponse(url="/login", status_code=302)
    response = FileResponse("index.html")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@app.get("/login")
def login_page():
    return FileResponse("login.html")

# ── CORS — 허용 도메인 명시 ──
ALLOWED_ORIGINS = [
    "https://magu-stock-production.up.railway.app",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
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
            ma20_pct       FLOAT,
            from_52w_high  FLOAT,
            vol_ratio      FLOAT,
            screened_at    TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (ticker, market)
        );
        -- 기존 DB에도 컬럼 추가 (재배포 시 자동 적용)
        ALTER TABLE screening_cache ADD COLUMN IF NOT EXISTS ma20_pct      FLOAT;
        ALTER TABLE screening_cache ADD COLUMN IF NOT EXISTS from_52w_high FLOAT;
        ALTER TABLE screening_cache ADD COLUMN IF NOT EXISTS vol_ratio     FLOAT;
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
        ALTER TABLE bestpick_records ADD COLUMN IF NOT EXISTS spy_entry_price FLOAT;
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

        CREATE TABLE IF NOT EXISTS rb_settings (
            id           INT PRIMARY KEY DEFAULT 1,
            total        BIGINT,
            stock        BIGINT,
            target       INT,
            trigger_pct  INT DEFAULT 5,
            last_alert   DATE,
            updated_at   TIMESTAMP DEFAULT NOW()
        );
        -- 리밸런싱 모드 추가 컬럼 (기존 DB 호환)
        ALTER TABLE rb_settings ADD COLUMN IF NOT EXISTS rb_mode        TEXT DEFAULT 'conservative';
        ALTER TABLE rb_settings ADD COLUMN IF NOT EXISTS aggressive_add INT  DEFAULT 10;
        INSERT INTO rb_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

        CREATE TABLE IF NOT EXISTS rb_logs (
            id         SERIAL PRIMARY KEY,
            log_date   VARCHAR(20) NOT NULL,
            log_text   TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS short_volume_cache (
            id              SERIAL PRIMARY KEY,
            ticker          VARCHAR(10) NOT NULL,
            trade_date      DATE NOT NULL,
            short_volume    BIGINT,
            total_volume    BIGINT,
            short_vol_ratio NUMERIC(6,4),
            short_pct_float NUMERIC(6,2),
            short_ratio     NUMERIC(8,2),
            shares_short    BIGINT,
            updated_at      TIMESTAMP DEFAULT NOW(),
            UNIQUE(ticker, trade_date)
        );
        CREATE INDEX IF NOT EXISTS idx_svc_ticker ON short_volume_cache(ticker);
        CREATE INDEX IF NOT EXISTS idx_svc_date   ON short_volume_cache(trade_date DESC);

        CREATE TABLE IF NOT EXISTS ark_holdings_cache (
            id           SERIAL PRIMARY KEY,
            fund         VARCHAR(10) NOT NULL,
            ticker       VARCHAR(20) NOT NULL,
            company      TEXT,
            shares       BIGINT,
            market_value FLOAT,
            weight       FLOAT,
            trade_date   DATE,
            fetched_at   TIMESTAMP DEFAULT NOW(),
            UNIQUE(fund, ticker)
        );
        CREATE INDEX IF NOT EXISTS idx_ark_h_ticker ON ark_holdings_cache(ticker);

        CREATE TABLE IF NOT EXISTS ark_trades_cache (
            id          SERIAL PRIMARY KEY,
            fund        VARCHAR(10) NOT NULL,
            ticker      VARCHAR(20) NOT NULL,
            company     TEXT,
            direction   VARCHAR(10),
            shares      BIGINT,
            etf_percent FLOAT,
            trade_date  DATE,
            fetched_at  TIMESTAMP DEFAULT NOW(),
            UNIQUE(fund, ticker, trade_date, direction)
        );
        CREATE INDEX IF NOT EXISTS idx_ark_t_ticker ON ark_trades_cache(ticker);
        CREATE INDEX IF NOT EXISTS idx_ark_t_date   ON ark_trades_cache(trade_date DESC);

        -- ──────────────────────────────────────────────
        -- 자산 관리: 보유 종목
        -- ──────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS holdings (
            id           SERIAL PRIMARY KEY,
            account      VARCHAR(20) NOT NULL DEFAULT 'main',  -- 'main' | 'sub'
            ticker       VARCHAR(20) NOT NULL,
            name         TEXT,
            sector       TEXT,
            quantity     NUMERIC(14,4) NOT NULL,
            avg_price    NUMERIC(14,4) NOT NULL,
            currency     VARCHAR(3) DEFAULT 'USD',              -- 'USD' | 'KRW'
            memo         TEXT,
            created_at   TIMESTAMP DEFAULT NOW(),
            updated_at   TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_holdings_account ON holdings(account);
        CREATE INDEX IF NOT EXISTS idx_holdings_ticker  ON holdings(ticker);

        -- ──────────────────────────────────────────────
        -- 자산 관리: 월간 자산 스냅샷
        -- ──────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS portfolio_snapshots (
            id            SERIAL PRIMARY KEY,
            snapshot_date DATE NOT NULL UNIQUE,
            total_stock   BIGINT DEFAULT 0,         -- 총 주식 평가 (KRW 환산)
            total_cash    BIGINT DEFAULT 0,         -- 총 현금 (KRW)
            total_assets  BIGINT DEFAULT 0,         -- 합계
            usd_krw       NUMERIC(10,2),            -- 스냅샷 시점 환율
            detail_json   JSONB,                    -- 종목별 상세 (옵션)
            created_at    TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_snapshot_date ON portfolio_snapshots(snapshot_date DESC);

        -- ──────────────────────────────────────────────
        -- 자산 관리: 계좌별 현금 잔고
        -- ──────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS account_cash (
            account     VARCHAR(20) PRIMARY KEY,     -- 'main' | 'sub'
            cash_krw    BIGINT DEFAULT 0,
            cash_usd    NUMERIC(14,2) DEFAULT 0,
            updated_at  TIMESTAMP DEFAULT NOW()
        );
        INSERT INTO account_cash (account) VALUES ('main') ON CONFLICT (account) DO NOTHING;
        INSERT INTO account_cash (account) VALUES ('sub')  ON CONFLICT (account) DO NOTHING;

        -- ──────────────────────────────────────────────
        -- 가격 캐시 (실시간 가격 저장)
        -- ──────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS price_cache (
            ticker     VARCHAR(20) PRIMARY KEY,
            price      NUMERIC(14,4),
            name       TEXT,
            sector     TEXT,
            currency   VARCHAR(3) DEFAULT 'USD',
            cached_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_price_cached_at ON price_cache(cached_at DESC);

        -- ──────────────────────────────────────────────
        -- 환율 캐시
        -- ──────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS fx_cache (
            pair       VARCHAR(10) PRIMARY KEY,
            rate       NUMERIC(10,4),
            cached_at  TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS momentum_cache (
            ticker         TEXT NOT NULL,
            market         TEXT NOT NULL,
            name           TEXT,
            sector         TEXT,
            price          FLOAT,
            change_pct     FLOAT,
            rs_score       INT DEFAULT 0,
            ma_score       INT DEFAULT 0,
            vol_score      INT DEFAULT 0,
            high52_score   INT DEFAULT 0,
            momentum_score INT DEFAULT 0,
            recommendation TEXT,
            ret_1m         FLOAT,
            ret_3m         FLOAT,
            ret_6m         FLOAT,
            ma20_pct       FLOAT,
            from_52w_high  FLOAT,
            vol_ratio      FLOAT,
            screened_at    TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (ticker, market)
        );
        CREATE INDEX IF NOT EXISTS idx_mc_market ON momentum_cache(market);
        CREATE INDEX IF NOT EXISTS idx_mc_score  ON momentum_cache(momentum_score DESC);

        CREATE TABLE IF NOT EXISTS double_confirm_records (
            id                SERIAL PRIMARY KEY,
            ticker            TEXT NOT NULL,
            name              TEXT,
            sector            TEXT,
            entry_price       FLOAT NOT NULL,
            total_score       INT,
            momentum_score    INT,
            combined_score    INT,
            spy_entry_price   FLOAT,
            consecutive_count INT DEFAULT 1,
            picked_at         DATE NOT NULL DEFAULT CURRENT_DATE,
            market            TEXT DEFAULT 'nasdaq',
            UNIQUE (ticker, picked_at, market)
        );
        CREATE INDEX IF NOT EXISTS idx_dc_picked_at ON double_confirm_records(picked_at DESC);
        CREATE INDEX IF NOT EXISTS idx_dc_market    ON double_confirm_records(market);

        CREATE TABLE IF NOT EXISTS double_confirm_prices (
            id         SERIAL PRIMARY KEY,
            record_id  INT NOT NULL REFERENCES double_confirm_records(id) ON DELETE CASCADE,
            ticker     TEXT NOT NULL,
            price_date DATE NOT NULL,
            price      FLOAT NOT NULL,
            return_pct FLOAT,
            UNIQUE (record_id, price_date)
        );
        CREATE INDEX IF NOT EXISTS idx_dcp_record ON double_confirm_prices(record_id);
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
        cur.execute("DELETE FROM bestpick_records WHERE picked_at < CURRENT_DATE - INTERVAL '3 years'")
        cur.execute("DELETE FROM double_confirm_records WHERE picked_at < CURRENT_DATE - INTERVAL '3 years'")
        conn.commit(); cur.close(); conn.close()
        logger.info("cleanup 완료")
    except Exception as e:
        logger.error(f"cleanup 오류: {e}")

# ══════════════════════════════════════════════════════════════
# 종목 풀 — 확장판 (나스닥200, S&P500, 코스피200, 코스닥150)
# ══════════════════════════════════════════════════════════════

def get_sp500_tickers():
    """Wikipedia에서 S&P500 구성종목(~500) 크롤링.
    위키가 기본 UA를 403 차단하므로 브라우저 UA로 받아 파싱한다."""
    import io
    UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    try:
        html = requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
                            headers={"User-Agent": UA}, timeout=20).text
        tables = pd.read_html(io.StringIO(html))
        tickers = (tables[0]['Symbol'].dropna().astype(str)
                   .str.replace('.', '-', regex=False).str.strip().tolist())
        tickers = [t for t in tickers if t]
        logger.info(f"S&P500 크롤링: {len(tickers)}개")
        return tickers
    except Exception as e:
        logger.warning(f"S&P500 크롤링 실패, 폴백 사용: {e}")
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
    # ── 나스닥 커버리지 확장: 상장 우량 중형주 (200 → ~400) ──
    "TER","AMKR","WOLF","SMTC","SYNA","LFUS","VICR","RMBS","CRDO","ALAB",
    "NVMI","CAMT","COHR","LITE","CIEN","VIAV","MXL","INDI","NVTS","ALGM",
    "ADSK","ZI","NTNX","PATH","ESTC","CFLT","DT","PEGA","MANH","TYL",
    "APPN","DOCN","FIVN","BOX","AI","QLYS","TENB","VRNS","RPD","PCOR",
    "FRSH","GEN","BSY","WK","SPSC","BLKB","ALRM","INFA","PD","DBX",
    "DOCS","FROG","TWLO","ZG","YELP","WIX","GLBE","PAYC","DOX","JKHY",
    "ALNY","INCY","BMRN","EXEL","NBIX","UTHR","HALO","SRPT","RARE","IONS",
    "ACAD","MEDP","RGEN","CYTK","ROIV","NTRA","TMDX","EXAS","ARGX","TECH",
    "LNTH","PCRX","SMMT","INSM","ITCI","AXSM","CORT","KRYS","VRNA","VCYT",
    "RXST","COGT","IMVT","KROS","HROW","PODD","DVAX","NVAX","SUPN","AMPH",
    "EXPE","WYNN","PDD","JD","BIDU","TCOM","NTES","BILI","DPZ","PLNT",
    "FOXA","FOX","SIRI","WBD","CHTR","EA","WBA","DLTR","ROST","ODFL",
    "KHC","FANG","LBRDK","CSGP","ZBRA","EXC","XEL","AEP","WTW","CG",
    "GLPI","GFS","FUTU","CART","KSPI","FLEX","CASY","SFM","COKE","SAIA",
    "MDB","NET","SNOW","HUBS","VEEV","OPEN","CVNA","CLSK","WULF","CIFR",
    "AAL","LCID","RUN","SHLS","FLNC","NXT","BTDR","IREN","APLD","CACC",
]))[:400]

# 모멘텀 스크리너 전용 추가 풀 (기존 200에 없는 광통신·반도체·우주·성장주)
TICKERS_MOMENTUM_EXTRA_US = [
    "COHR","LITE","CIEN","VIAV","WDC","STX","PSTG","NTNX",
    "PL","SPCE","LHX","NOC","GD","RTX","BA","HEI","TDG",
    "TTD","SPOT","MDB","NET","SNOW","HUBS","VEEV","ZI","PCVX",
    "VST","CEG","NRG","AES","ETR",
    "IBKR","SCHW","ICE","MSCI","SPGI","MCO",
    "HIMS","TDOC","ACAD","ALNY","NTRA","TMDX","AXON",
    "ANET","SMTC","MTCH","BMBL","SWI","GTLB",
]

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
_mkt_ret_cache = {}  # {벤치마크심볼: {"ret": x, "updated": date}} — 상대강도용 시장 수익률 캐시 (시장별)
_spy_hist_cache = {"data": None, "updated": None}  # 모멘텀 RS 계산용 SPY 1y 히스토리

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
# 한국 유니버스: 로컬(한국)에서 생성한 kr_universe.py(시총 상위) 우선, 없으면 정적 fallback.
# KRX가 해외서버(Railway)를 차단하므로 한국 목록은 로컬에서 추출해 커밋한다.
try:
    import kr_universe as _kru
    if getattr(_kru, "TICKERS_KOSPI_DYN", None):
        TICKERS_KOSPI = list(dict.fromkeys(_kru.TICKERS_KOSPI_DYN))
    if getattr(_kru, "TICKERS_KOSDAQ_DYN", None):
        TICKERS_KOSDAQ = list(dict.fromkeys(_kru.TICKERS_KOSDAQ_DYN))
    logger.info(f"kr_universe.py 적용: 코스피 {len(TICKERS_KOSPI)} · 코스닥 {len(TICKERS_KOSDAQ)}")
except ImportError:
    logger.info("kr_universe.py 없음 → 정적 한국 리스트 사용")

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
    "035760.KQ":"CJ ENM","079160.KQ":"CJ CGV","004840.KS":"CJ대한통운","000120.KS":"CJ씨푸드",
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
# ETF 섹터 자동 추론 (yfinance가 ETF에 sector 미제공 문제 해결)
# ══════════════════════════════════════════════════════════════

# 유명 ETF 하드코딩 매핑 (최우선) — SPDR 섹터 ETF + 레버리지/테마 ETF
ETF_SECTOR_MAP = {
    # SPDR 섹터 ETF
    "XLK":"Technology","XLV":"Healthcare","XLF":"Financial Services",
    "XLE":"Energy","XLY":"Consumer Cyclical","XLP":"Consumer Defensive",
    "XLB":"Basic Materials","XLC":"Communication Services",
    "XLI":"Industrials","XLU":"Utilities","XLRE":"Real Estate",
    # Technology 레버리지/테마
    "TQQQ":"Technology","QQQ":"Technology","QQQM":"Technology","QLD":"Technology",
    "SQQQ":"Technology","PSQ":"Technology",
    "SOXL":"Technology","SOXS":"Technology","SOXX":"Technology","SMH":"Technology","USD":"Technology",
    "TECL":"Technology","TECS":"Technology","FNGU":"Technology","FNGD":"Technology",
    "BULZ":"Technology","WEBL":"Technology","WEBS":"Technology",
    "NVDL":"Technology","NVDX":"Technology","NVDS":"Technology","NVDU":"Technology",
    "MSFU":"Technology","GGLL":"Technology","AMZU":"Technology","AMZD":"Technology",
    "AAPU":"Technology","AAPD":"Technology",
    # Consumer Cyclical 레버리지
    "TSLL":"Consumer Cyclical","TSLS":"Consumer Cyclical","TSLQ":"Consumer Cyclical",
    # Financial Services 레버리지
    "FAS":"Financial Services","FAZ":"Financial Services",
    "DPST":"Financial Services","WDRW":"Financial Services",
    # Energy 레버리지
    "ERX":"Energy","ERY":"Energy","GUSH":"Energy","DRIP":"Energy",
    "NRGU":"Energy","NRGD":"Energy",
    # Healthcare 레버리지
    "CURE":"Healthcare","LABU":"Healthcare","LABD":"Healthcare",
    # Industrials/Defense
    "DFEN":"Industrials","ITA":"Industrials",
    # 광대역 지수 레버리지 → 혼합 성격이나 대표 섹터 없음 → Unknown 유지 (추론 스킵)
    # UPRO, SPXL, SPXU, SPXS 등은 Unknown으로 두고 레버리지 플래그로 처리
    # Real Estate
    "DRN":"Real Estate","DRV":"Real Estate",
    # Communication
    "YCOM":"Communication Services",
    # Utilities
    "UTSL":"Utilities",
    # 중국 테크 레버리지 → Technology
    "YINN":"Technology","YANG":"Technology",
    # Crypto/Digital asset 계열 → Financial Services 성격이지만 별도 처리 필요 시 Unknown
    "ETHU":"Financial Services","BITU":"Financial Services","BITX":"Financial Services",
}

# 이름 패턴 → 섹터 키워드 매핑 (우선순위 순, 먼저 매치된 게 승리)
# 주의: 순서 중요. 더 구체적인 키워드가 먼저 와야 함
ETF_NAME_PATTERNS = [
    # Technology (Semi가 Tech보다 먼저)
    (r"\b(semi|semiconductor|chip)\b",                     "Technology"),
    (r"\b(nasdaq|qqq|nasdaq[\s\-]?100)\b",                 "Technology"),
    (r"\b(software|internet|cloud|fintech|cyber|ai)\b",    "Technology"),
    (r"\btech(nology)?\b",                                 "Technology"),
    # Financial
    (r"\b(bank|regional\s*bank|financial)\b",              "Financial Services"),
    (r"\b(insurance|broker|exchange)\b",                   "Financial Services"),
    # Energy
    (r"\b(oil|gas|energy|petroleum|natural\s*gas)\b",      "Energy"),
    (r"\b(uranium|nuclear)\b",                             "Energy"),
    # Healthcare
    (r"\b(biotech|pharma|pharmaceutical|healthcare|health\s*care|medical)\b", "Healthcare"),
    # Industrials
    (r"\b(aerospace|defense|airline|industrial|transportation)\b", "Industrials"),
    # Real Estate
    (r"\b(real\s*estate|reit|mortgage)\b",                 "Real Estate"),
    # Utilities
    (r"\b(utilit|electric\s*power)\b",                     "Utilities"),
    # Materials
    (r"\b(gold|silver|copper|mining|metal|material)\b",    "Basic Materials"),
    # Consumer
    (r"\b(retail|consumer\s*discretionary|e[\s\-]?commerce|restaurant|leisure)\b", "Consumer Cyclical"),
    (r"\b(consumer\s*staples|food|beverage|household)\b",  "Consumer Defensive"),
    # Communication
    (r"\b(media|telecom|communication|entertainment)\b",   "Communication Services"),
]

# 레버리지 ETF 이름에서 기초자산 티커 추출용 패턴
# 예: "Direxion Daily TSLA Bull 2X Shares" → TSLA
# 예: "GraniteShares 2x Long NVDA Daily ETF" → NVDA
_UNDERLYING_TICKER_RE = re.compile(
    r"\b([A-Z]{2,5})\s+(?:Bull|Bear|Long|Short|Daily|2x|3x|2X|3X)\b"
    r"|\b(?:Daily|Long|Short|2x|3x|2X|3X)\s+([A-Z]{2,5})\b",
    re.IGNORECASE
)

# yfinance 섹터명 정규화 (표기 차이 흡수)
def _normalize_sector(s: str) -> str:
    if not s: return "Unknown"
    s = s.strip()
    alias = {
        "Health Care":"Healthcare",
        "Financials":"Financial Services",
        "Consumer Discretionary":"Consumer Cyclical",
        "Consumer Staples":"Consumer Defensive",
        "Materials":"Basic Materials",
    }
    return alias.get(s, s)

# GICS 11 섹터 (프론트 드롭다운과 동일)
VALID_SECTORS = [
    "Technology","Healthcare","Financial Services","Consumer Cyclical",
    "Consumer Defensive","Communication Services","Industrials","Energy",
    "Utilities","Real Estate","Basic Materials",
]

# ══════════════════════════════════════════════════════════════
# 주요 일정 (매크로 + 실적)
# ══════════════════════════════════════════════════════════════

# 매크로 이벤트 하드코딩 (BLS/Fed/BEA 공식 일정)
# 형식: (날짜 YYYY-MM-DD, 시간 HH:MM ET, 카테고리, 제목, 중요도 1~5)
# 시간이 빈 문자열이면 시간 미정/종일 이벤트
# 일년에 한 번 갱신 필요. 갱신은 BLS/Fed 공식 홈페이지 참조.
MACRO_EVENTS = [
    # ── 2026년 4월 잔여 ──
    ("2026-04-29","14:00","FOMC","FOMC 금리결정 + 성명",5),
    ("2026-04-29","14:30","FOMC","파월 기자회견",5),
    ("2026-04-30","08:30","GDP","Q1 GDP 속보치",4),
    ("2026-04-30","08:30","PCE","3월 PCE / Core PCE",4),
    ("2026-05-01","10:00","ISM","4월 ISM 제조업 PMI",3),
    # ── 2026년 5월 ──
    ("2026-05-08","08:30","NFP","4월 고용보고서 (NFP/실업률)",5),
    ("2026-05-12","08:30","CPI","4월 CPI",5),
    ("2026-05-13","08:30","PPI","4월 PPI",4),
    ("2026-05-15","08:30","RetailSales","4월 소매판매",3),
    ("2026-05-21","14:00","Minutes","4월 FOMC 의사록",3),
    ("2026-05-30","08:30","PCE","4월 PCE / Core PCE",4),
    # ── 2026년 6월 ──
    ("2026-06-06","08:30","NFP","5월 고용보고서",5),
    ("2026-06-11","08:30","CPI","5월 CPI",5),
    ("2026-06-12","08:30","PPI","5월 PPI",4),
    ("2026-06-17","14:00","FOMC","FOMC 금리결정 + SEP + 닷플롯",5),
    ("2026-06-17","14:30","FOMC","파월 기자회견",5),
    ("2026-06-26","08:30","GDP","Q1 GDP 확정치",3),
    ("2026-06-27","08:30","PCE","5월 PCE / Core PCE",4),
    # ── 2026년 7월 ──
    ("2026-07-03","08:30","NFP","6월 고용보고서",5),
    ("2026-07-15","08:30","CPI","6월 CPI",5),
    ("2026-07-16","08:30","PPI","6월 PPI",4),
    ("2026-07-29","14:00","FOMC","FOMC 금리결정 + 성명",5),
    ("2026-07-29","14:30","FOMC","파월 기자회견",5),
    ("2026-07-31","08:30","PCE","6월 PCE / Core PCE",4),
    # ── 2026년 8월 ──
    ("2026-08-01","08:30","NFP","7월 고용보고서",5),
    ("2026-08-12","08:30","CPI","7월 CPI",5),
    ("2026-08-14","08:30","PPI","7월 PPI",4),
    ("2026-08-29","08:30","PCE","7월 PCE / Core PCE",4),
    # 잭슨홀(예년 8월 셋째주, 정확 일정은 매년 6월경 공시)
    ("2026-08-21","09:00","JacksonHole","잭슨홀 심포지엄 (예정)",4),
    # ── 2026년 9월 ──
    ("2026-09-04","08:30","NFP","8월 고용보고서",5),
    ("2026-09-10","08:30","PPI","8월 PPI",4),
    ("2026-09-11","08:30","CPI","8월 CPI",5),
    ("2026-09-16","14:00","FOMC","FOMC 금리결정 + SEP + 닷플롯",5),
    ("2026-09-16","14:30","FOMC","파월 기자회견",5),
    # ── 2026년 10월 ~ 12월 (간소화, 핵심만) ──
    ("2026-10-02","08:30","NFP","9월 고용보고서",5),
    ("2026-10-15","08:30","CPI","9월 CPI",5),
    ("2026-10-28","14:00","FOMC","FOMC 금리결정",5),
    ("2026-11-06","08:30","NFP","10월 고용보고서",5),
    ("2026-11-13","08:30","CPI","10월 CPI",5),
    ("2026-12-04","08:30","NFP","11월 고용보고서",5),
    ("2026-12-10","08:30","CPI","11월 CPI",5),
    ("2026-12-16","14:00","FOMC","FOMC 금리결정 + SEP + 닷플롯",5),
]

# Mag 7 + 추적 대상 (보유 종목과 합쳐서 실적 일정 조회)
MAG7_TICKERS = ["AAPL","MSFT","GOOGL","AMZN","META","NVDA","TSLA"]

# 카테고리별 색상/이모지 (프론트 응답 가공용)
EVENT_STYLE = {
    "FOMC":         {"emoji":"🏛","color":"#dc2626"},  # 적색 (최우선)
    "CPI":          {"emoji":"📊","color":"#dc2626"},
    "PPI":          {"emoji":"📈","color":"#ea580c"},
    "NFP":          {"emoji":"💼","color":"#dc2626"},
    "PCE":          {"emoji":"💵","color":"#ea580c"},
    "GDP":          {"emoji":"📐","color":"#ea580c"},
    "ISM":          {"emoji":"🏭","color":"#d97706"},
    "RetailSales":  {"emoji":"🛒","color":"#d97706"},
    "Minutes":      {"emoji":"📝","color":"#7c3aed"},
    "JacksonHole":  {"emoji":"⛰","color":"#7c3aed"},
    "Earnings":     {"emoji":"💎","color":"#0891b2"},  # 실적은 청록
}

# 실적 일정 캐시 (티커→{date_iso, when, fetched_at})
_earnings_cache = {}
_EARNINGS_CACHE_TTL = 12 * 3600  # 12시간

def _fetch_earnings_date(ticker: str) -> dict:
    """
    yfinance에서 다음 실적 발표일 1건 조회
    반환: {date: 'YYYY-MM-DD', when: 'BMO'/'AMC'/'TBD', source: 'yf'} 또는 None
    """
    import time as _time
    now = _time.time()
    cached = _earnings_cache.get(ticker)
    if cached and (now - cached.get("fetched_at", 0)) < _EARNINGS_CACHE_TTL:
        return cached.get("data")
    try:
        t = yf.Ticker(ticker)
        # 1차: earnings_dates (가장 정확, EPS 추정치 포함)
        try:
            ed = t.earnings_dates
            if ed is not None and not ed.empty:
                # tz-aware 인덱스
                tz = ed.index.tz
                now_dt = datetime.now(tz) if tz else datetime.now()
                future = ed[ed.index >= now_dt].sort_index()
                if not future.empty:
                    next_dt = future.index[0]
                    data = {
                        "date": next_dt.strftime("%Y-%m-%d"),
                        "when": "AMC" if next_dt.hour >= 16 else ("BMO" if next_dt.hour < 9 else "TBD"),
                        "source": "yf"
                    }
                    _earnings_cache[ticker] = {"data": data, "fetched_at": now}
                    return data
        except Exception:
            pass
        # 2차: calendar 폴백
        try:
            cal = t.calendar
            if cal:
                ed_val = cal.get("Earnings Date") if isinstance(cal, dict) else None
                if ed_val:
                    # list/단일 처리
                    dt0 = ed_val[0] if isinstance(ed_val, (list, tuple)) and ed_val else ed_val
                    if hasattr(dt0, "strftime"):
                        data = {"date": dt0.strftime("%Y-%m-%d"), "when": "TBD", "source": "yf-cal"}
                        _earnings_cache[ticker] = {"data": data, "fetched_at": now}
                        return data
        except Exception:
            pass
    except Exception as e:
        logger.debug(f"_fetch_earnings_date {ticker}: {e}")
    # 캐시에 None도 저장 (재시도 폭주 방지, TTL은 동일)
    _earnings_cache[ticker] = {"data": None, "fetched_at": now}
    return None


def infer_etf_sector(ticker: str, long_name: str = "", short_name: str = ""):
    """
    ETF 섹터 자동 추론 — 3단계 폴백
    반환: (sector_or_None, method) — method는 'map'/'pattern'/'underlying'/None
    None 반환 시 수동 입력 필요
    """
    if not ticker:
        return None, None
    tk = ticker.strip().upper()

    # 1단계: 하드코딩 매핑
    if tk in ETF_SECTOR_MAP:
        return ETF_SECTOR_MAP[tk], "map"

    # 2단계: 이름 패턴 매칭
    name_blob = f"{long_name or ''} {short_name or ''}".lower()
    if name_blob.strip():
        for pat, sec in ETF_NAME_PATTERNS:
            if re.search(pat, name_blob, re.IGNORECASE):
                return sec, "pattern"

    # 3단계: 기초자산 티커 추출 → yfinance 섹터 조회
    # longName에서 대문자 티커 후보 추출
    if long_name:
        m = _UNDERLYING_TICKER_RE.search(long_name)
        if m:
            underlying = (m.group(1) or m.group(2) or "").upper()
            # 흔한 오탐 단어 제외
            if underlying and underlying not in {"ETF","USD","FUND","BULL","BEAR","DAILY","LONG","SHORT"}:
                try:
                    info = yf.Ticker(underlying).info or {}
                    sec = _normalize_sector(info.get("sector") or "")
                    if sec and sec != "Unknown":
                        return sec, "underlying"
                except Exception as e:
                    logger.debug(f"infer_etf_sector underlying 조회 실패 {underlying}: {e}")

    return None, None


def resolve_sector(ticker: str, live_info: dict = None) -> dict:
    """
    종목 섹터 해석 — yfinance 우선, 실패 시 ETF 추론
    반환: {sector, method, needs_manual}
      method: 'yfinance'/'map'/'pattern'/'underlying'/'unknown'
      needs_manual: True면 프론트에서 수동 선택 필요
    """
    info = live_info or {}
    raw_sector = info.get("sector") or ""
    long_name  = info.get("longName") or ""
    short_name = info.get("shortName") or ""
    quote_type = (info.get("quoteType") or "").upper()

    # yfinance가 sector를 정상 제공한 경우 (주식 대부분)
    if raw_sector and raw_sector.strip() and raw_sector.strip() != "—":
        return {"sector": _normalize_sector(raw_sector), "method": "yfinance", "needs_manual": False}

    # ETF거나 sector 없는 경우 → 추론 시도
    sec, method = infer_etf_sector(ticker, long_name, short_name)
    if sec:
        return {"sector": sec, "method": method, "needs_manual": False}

    return {"sector": "Unknown", "method": "unknown", "needs_manual": True}


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
        if 20<=k<=40: return 10   # 매수권 (엘더 이론 핵심)
        elif 40<k<=50: return 8
        elif 15<=k<20: return 7
        elif k<10: return 5        # 극단 과매도 → 반등 가능 (기존 2점에서 상향)
        elif 50<k<=65: return 5
        elif 10<=k<15: return 4
        elif 65<k<=80: return 3
        elif k>80: return 1        # 과매수 → 매수 자제
        else: return 2
    except: return 0

def score_breakout(hist_daily):
    try:
        if len(hist_daily)<20: return 0
        # 20일 최고가 대비 현재가 (전일 고가 대비보다 안정적)
        high_20d=float(hist_daily['High'].iloc[-20:-1].max())
        latest=float(hist_daily['Close'].iloc[-1])
        ratio=latest/high_20d
        if ratio>=1.03: return 10   # 20일 고가 3% 이상 돌파
        elif ratio>=1.01: return 8  # 1~3% 돌파
        elif ratio>=0.99: return 6  # 고가 근접 (1% 이내)
        elif ratio>=0.97: return 4  # 고가 대비 1~3% 하락
        elif ratio>=0.93: return 2  # 3~7% 하락
        else: return 0              # 7% 이상 하락
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
    # None 또는 음수는 데이터 없음으로 처리 → 중립 2점
    if debt_equity is None or debt_equity < 0: return 2
    elif debt_equity==0: return 2  # 0도 데이터 없음 가능성 → 중립
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
          + score_debt(info.get('debtToEquity',None))
          + score_eps_growth(info)
          + score_peg(info.get('pegRatio',None))
          + score_ma200(hist_daily)
          + score_rsi(hist_daily))

def score_analyst(rec):
    if not rec: return 2  # 커버리지 없음 → 중립 2점 (한국 소형주 불이익 방지)
    return {'strong_buy':10,'buy':7,'hold':4,'underperform':1,'sell':0,'strong_sell':0}.get(rec.lower(),2)

def _rs_benchmark(market):
    """상대강도 벤치마크 — 시장별 (통화·시장 일치). 한국은 코스피/코스닥 지수, 그 외 S&P500"""
    return {"kospi": "^KS11", "kosdaq": "^KQ11"}.get(market, "^GSPC")

def score_relative_strength(hist_daily, market="nasdaq"):
    """종목 1년 수익률을 '자기 시장 지수' 대비로 평가 (한국 종목을 S&P500과 비교하던 버그 수정)"""
    try:
        n=min(len(hist_daily)-1,252)
        if n<60: return 0
        stock_ret=float((hist_daily['Close'].iloc[-1]/hist_daily['Close'].iloc[-n]-1)*100)
        bench=_rs_benchmark(market)
        today=datetime.now().strftime("%Y-%m-%d")
        c=_mkt_ret_cache.get(bench)
        if c and c.get("updated")==today and c.get("ret") is not None:
            mkt_ret=c["ret"]
        else:
            try:
                idx=yf.Ticker(bench).history(period="1y",timeout=5)
                idx_n=min(len(idx)-1,n)
                if idx_n>=60:
                    mkt_ret=float((idx['Close'].iloc[-1]/idx['Close'].iloc[-idx_n]-1)*100)
                    _mkt_ret_cache[bench]={"ret":mkt_ret,"updated":today}
                else:
                    mkt_ret=10.0*(n/252)
            except:
                mkt_ret=(c or {}).get("ret") if (c or {}).get("ret") is not None else 10.0*(n/252)
        # 벤치마크/종목 수익률이 NaN이면 안전 폴백 (지수 데이터 일시 결측 대비)
        if mkt_ret is None or math.isnan(mkt_ret): mkt_ret=10.0*(n/252)
        if math.isnan(stock_ret): return 0
        excess=stock_ret-mkt_ret
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
        elif ratio>=1.0 and obv_ma20>0: return 4  # OBV 양수 확인 (음수 역전 방지)
        elif ratio>=0.9: return 2
        else: return 0
    except: return 0

def calculate_modern_score(info, hist_daily, market="nasdaq"):
    rec=info.get('recommendationKey','') or ''
    return score_analyst(rec)+score_relative_strength(hist_daily, market)+score_obv_momentum(hist_daily)

def get_recommendation(total_score, classic=None, growth=None, modern=None):
    if classic is not None and growth is not None and modern is not None:
        if classic<10 or growth<15 or modern<10:
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
        c_ema=score_ema_slope(hist_weekly)
        c_stoch=score_stochastic(hist_daily)//(2 if c_ema==0 else 1)
        c_break=score_breakout(hist_daily)//(2 if c_ema==0 else 1)
        classic=c_ema+c_stoch+c_break
        g_roe=score_roe(info.get('returnOnEquity',0) or 0); g_debt=score_debt(info.get('debtToEquity',None))
        g_eps=score_eps_growth(info); g_peg=score_peg(info.get('pegRatio',None))
        g_ma200=score_ma200(hist_daily); g_rsi=score_rsi(hist_daily)
        growth=g_roe+g_debt+g_eps+g_peg+g_ma200+g_rsi
        m_anal=score_analyst(info.get('recommendationKey','') or '')
        m_rs=score_relative_strength(hist_daily, market); m_obv=score_obv_momentum(hist_daily)
        modern=m_anal+m_rs+m_obv; total=classic+growth+modern
        cp=hist_daily['Close'].iloc[-1]; pp=hist_daily['Close'].iloc[-2]
        current_price=float(cp) if not (math.isnan(float(cp)) or math.isinf(float(cp))) else 0.0
        prev_price=float(pp) if not (math.isnan(float(pp)) or math.isinf(float(pp))) else current_price
        change_pct=round((current_price/prev_price-1)*100,2) if prev_price!=0 else 0.0
        rsi_val=0.0
        if len(hist_daily)>=14:
            try:
                rv=float(ta.momentum.RSIIndicator(hist_daily['Close'],window=14).rsi().iloc[-1])
                if not (math.isnan(rv) or math.isinf(rv)): rsi_val=round(rv,1)
            except: pass
        # 진입 타이밍 지표 계산
        ma20_pct=0.0; from_52w_high=0.0; vol_ratio=0.0
        try:
            if len(hist_daily)>=20:
                ma20=float(hist_daily['Close'].rolling(20).mean().iloc[-1])
                if ma20>0: ma20_pct=round((current_price/ma20-1)*100,1)
        except: pass
        try:
            if len(hist_daily)>=20:
                high_52w=float(hist_daily['High'].rolling(min(252,len(hist_daily))).max().iloc[-1])
                if high_52w>0: from_52w_high=round((current_price/high_52w-1)*100,1)
        except: pass
        try:
            if len(hist_daily)>=21:
                avg_vol=float(hist_daily['Volume'].iloc[-21:-1].mean())
                cur_vol=float(hist_daily['Volume'].iloc[-1])
                if avg_vol>0: vol_ratio=round(cur_vol/avg_vol*100,0)
        except: pass
        name=KR_NAMES.get(ticker) or info.get('longName',ticker); sector=info.get('sector') or 'Unknown'
        return {
            "ticker":ticker,"name":name,"sector":sector,"etf":SECTOR_TO_ETF.get(sector,""),
            "market":market,
            "price":round(current_price,2),"change_pct":change_pct,
            "classic_score":classic,"growth_score":growth,"modern_score":modern,
            "total_score":total,"recommendation":get_recommendation(total,classic,growth,modern),
            "weight":0,"rsi":rsi_val,
            "roe":round((info.get('returnOnEquity',0) or 0)*100,1),"peg":round(info.get('pegRatio',0) or 0,2),
            "c_ema":c_ema,"c_stoch":c_stoch,"c_break":c_break,
            "g_roe":g_roe,"g_debt":g_debt,"g_eps":g_eps,"g_peg":g_peg,"g_ma200":g_ma200,"g_rsi":g_rsi,
            "m_anal":m_anal,"m_rs":m_rs,"m_obv":m_obv,
            "ma20_pct":ma20_pct,"from_52w_high":from_52w_high,"vol_ratio":vol_ratio,
        }
    except: return None

# ══════════════════════════════════════════════════════════════
# 모멘텀 스크리너 — 펀더멘탈 무관, 순수 가격·거래량·추세
# ══════════════════════════════════════════════════════════════

def _get_spy_hist():
    today = datetime.now().strftime("%Y-%m-%d")
    global _spy_hist_cache
    if _spy_hist_cache["updated"] == today and _spy_hist_cache["data"] is not None:
        return _spy_hist_cache["data"]
    try:
        spy = yf.Ticker("SPY").history(period="1y", timeout=10)
        if not spy.empty:
            _spy_hist_cache = {"data": spy, "updated": today}
        return _spy_hist_cache["data"]
    except:
        return _spy_hist_cache["data"]

def score_mt_rs(hist):
    """상대강도 복합 (40점): 1M×8 + 3M×17 + 6M×15 — 중기 모멘텀 중심"""
    try:
        spy = _get_spy_hist()
        score = 0
        for days, pts in [(21, 8), (63, 17), (126, 15)]:
            if len(hist) < days: continue
            sr = float((hist['Close'].iloc[-1] / hist['Close'].iloc[-days] - 1) * 100)
            if spy is not None and len(spy) > days:
                mr = float((spy['Close'].iloc[-1] / spy['Close'].iloc[-days] - 1) * 100)
            else:
                mr = round(7.0 * (days / 252), 2)  # 연 7% 기준 기간 비례 보정
            ex = sr - mr
            if ex >= 20: score += pts
            elif ex >= 10: score += int(pts * 0.8)
            elif ex >= 5: score += int(pts * 0.6)
            elif ex >= 0: score += int(pts * 0.4)
        return score
    except: return 0

def score_mt_ma(hist):
    """이동평균 위치 (25점): MA200(12) + MA50(8) + MA20(5) — 장기 추세 우선"""
    try:
        if len(hist) < 50: return 0
        close = float(hist['Close'].iloc[-1]); score = 0
        if close > float(hist['Close'].rolling(20).mean().iloc[-1]): score += 5
        if close > float(hist['Close'].rolling(50).mean().iloc[-1]): score += 8
        if len(hist) >= 200 and close > float(hist['Close'].rolling(200).mean().iloc[-1]): score += 12
        return score
    except: return 0

def score_mt_vol(hist):
    """거래량 모멘텀 (20점): 단기 급증(5d/20d) + 중기 증가 추세(20d/50d) 분리"""
    try:
        if len(hist) < 50: return 0
        v5  = float(hist['Volume'].iloc[-5:].mean())
        v20 = float(hist['Volume'].iloc[-20:].mean())
        v50 = float(hist['Volume'].iloc[-50:].mean())
        if v20 == 0 or v50 == 0: return 0
        r_surge = v5 / v20    # 단기 급증: 최근 5일 vs 20일
        r_trend = v20 / v50   # 중기 추세: 20일 vs 50일
        score = 0
        if r_surge >= 2.0: score += 10
        elif r_surge >= 1.5: score += 8
        elif r_surge >= 1.2: score += 6
        elif r_surge >= 1.0: score += 4
        if r_trend >= 1.3: score += 10
        elif r_trend >= 1.15: score += 7
        elif r_trend >= 1.0: score += 4
        return min(score, 20)
    except: return 0

def score_mt_52w(hist):
    """52주 고점 근접도 (15점)"""
    try:
        if len(hist) < 20: return 0
        close = float(hist['Close'].iloc[-1])
        h52 = float(hist['High'].rolling(min(252, len(hist))).max().iloc[-1])
        if h52 == 0: return 0
        pct = (close / h52 - 1) * 100
        if pct >= -3: return 15
        elif pct >= -7: return 12
        elif pct >= -15: return 8
        elif pct >= -25: return 4
        else: return 0
    except: return 0

def fetch_momentum_stock(ticker, market):
    try:
        stock = yf.Ticker(ticker); info = stock.info
        hist = stock.history(period="1y")
        if hist.empty or len(hist) < 20: return None
        rs = score_mt_rs(hist); ma = score_mt_ma(hist)
        vol = score_mt_vol(hist); h52 = score_mt_52w(hist)
        mtotal = rs + ma + vol + h52
        if mtotal >= 75: rec = "강한 모멘텀"
        elif mtotal >= 60: rec = "모멘텀"
        elif mtotal >= 45: rec = "약한 모멘텀"
        else: rec = "관망"
        cp = float(hist['Close'].iloc[-1]); pp = float(hist['Close'].iloc[-2])
        change_pct = round((cp/pp-1)*100, 2) if pp != 0 else 0.0
        def safe_ret(days):
            try: return round((cp/float(hist['Close'].iloc[-days])-1)*100, 1) if len(hist) >= days else None
            except: return None
        ret_1m = safe_ret(21); ret_3m = safe_ret(63); ret_6m = safe_ret(126)
        ma20_pct = None
        try:
            ma20 = float(hist['Close'].rolling(20).mean().iloc[-1])
            if ma20 > 0: ma20_pct = round((cp/ma20-1)*100, 1)
        except: pass
        from_52w_high = None
        try:
            h52v = float(hist['High'].rolling(min(252, len(hist))).max().iloc[-1])
            if h52v > 0: from_52w_high = round((cp/h52v-1)*100, 1)
        except: pass
        vol_ratio = None
        try:
            avg_vol = float(hist['Volume'].iloc[-21:-1].mean())
            if avg_vol > 0: vol_ratio = round(float(hist['Volume'].iloc[-1])/avg_vol*100, 0)
        except: pass
        name = KR_NAMES.get(ticker) or info.get('longName', ticker)
        sector = info.get('sector') or 'Unknown'
        return {
            "ticker": ticker, "name": name, "sector": sector, "market": market,
            "price": round(cp, 2), "change_pct": change_pct,
            "rs_score": rs, "ma_score": ma, "vol_score": vol, "high52_score": h52,
            "momentum_score": mtotal, "recommendation": rec,
            "ret_1m": ret_1m, "ret_3m": ret_3m, "ret_6m": ret_6m,
            "ma20_pct": ma20_pct, "from_52w_high": from_52w_high, "vol_ratio": vol_ratio,
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
                     m_anal,m_rs,m_obv,ma20_pct,from_52w_high,vol_ratio,screened_at)
                VALUES
                    (%(ticker)s,%(market)s,%(name)s,%(sector)s,%(etf)s,%(price)s,%(change_pct)s,
                     %(classic_score)s,%(growth_score)s,%(modern_score)s,%(total_score)s,
                     %(recommendation)s,%(weight)s,%(rsi)s,%(roe)s,%(peg)s,
                     %(c_ema)s,%(c_stoch)s,%(c_break)s,%(g_roe)s,%(g_debt)s,%(g_eps)s,
                     %(g_peg)s,%(g_ma200)s,%(g_rsi)s,%(m_anal)s,%(m_rs)s,%(m_obv)s,
                     %(ma20_pct)s,%(from_52w_high)s,%(vol_ratio)s,NOW())
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
                    ma20_pct=EXCLUDED.ma20_pct, from_52w_high=EXCLUDED.from_52w_high, vol_ratio=EXCLUDED.vol_ratio,
                    screened_at=NOW()
            """, r)
        conn.commit(); cur.close(); conn.close()
        logger.info(f"DB 저장 완료: {len(results)}건 [{results[0]['market']}]")
    except Exception as e:
        logger.error(f"DB 저장 오류: {e}")

def _cache_freshness_clause() -> str:
    """KST 요일 기준 캐시 유효 기간 — 주말에는 금요일 데이터 그대로 사용"""
    weekday = datetime.now(pytz.timezone("Asia/Seoul")).weekday()  # 0=월 … 6=일
    if weekday == 6:   return "AND screened_at > NOW() - INTERVAL '72 hours'"  # 일요일
    if weekday == 5:   return "AND screened_at > NOW() - INTERVAL '48 hours'"  # 토요일
    return "AND screened_at > NOW() - INTERVAL '25 hours'"                     # 평일

def load_screening_from_db(market: str):
    if not DATABASE_URL: return None
    try:
        conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        freshness = _cache_freshness_clause()
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

def save_momentum_to_db(results: list):
    if not results or not DATABASE_URL: return
    try:
        conn = get_conn(); cur = conn.cursor()
        for r in results:
            cur.execute("""
                INSERT INTO momentum_cache
                    (ticker,market,name,sector,price,change_pct,
                     rs_score,ma_score,vol_score,high52_score,momentum_score,
                     recommendation,ret_1m,ret_3m,ret_6m,ma20_pct,from_52w_high,vol_ratio,screened_at)
                VALUES
                    (%(ticker)s,%(market)s,%(name)s,%(sector)s,%(price)s,%(change_pct)s,
                     %(rs_score)s,%(ma_score)s,%(vol_score)s,%(high52_score)s,%(momentum_score)s,
                     %(recommendation)s,%(ret_1m)s,%(ret_3m)s,%(ret_6m)s,%(ma20_pct)s,%(from_52w_high)s,%(vol_ratio)s,NOW())
                ON CONFLICT (ticker, market) DO UPDATE SET
                    name=EXCLUDED.name, sector=EXCLUDED.sector,
                    price=EXCLUDED.price, change_pct=EXCLUDED.change_pct,
                    rs_score=EXCLUDED.rs_score, ma_score=EXCLUDED.ma_score,
                    vol_score=EXCLUDED.vol_score, high52_score=EXCLUDED.high52_score,
                    momentum_score=EXCLUDED.momentum_score, recommendation=EXCLUDED.recommendation,
                    ret_1m=EXCLUDED.ret_1m, ret_3m=EXCLUDED.ret_3m, ret_6m=EXCLUDED.ret_6m,
                    ma20_pct=EXCLUDED.ma20_pct, from_52w_high=EXCLUDED.from_52w_high,
                    vol_ratio=EXCLUDED.vol_ratio, screened_at=NOW()
            """, r)
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"모멘텀 DB 저장: {len(results)}건 [{results[0]['market']}]")
    except Exception as e:
        logger.error(f"모멘텀 DB 저장 오류: {e}")

def load_momentum_from_db(market: str):
    if not DATABASE_URL: return None
    try:
        conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        freshness = _cache_freshness_clause()
        if market in ("nasdaq","sp500","kospi","kosdaq"):
            cur.execute(f"SELECT * FROM momentum_cache WHERE market=%s {freshness} ORDER BY momentum_score DESC", (market,))
        elif market == "us":
            cur.execute(f"SELECT * FROM momentum_cache WHERE market IN ('nasdaq','sp500') {freshness} ORDER BY momentum_score DESC")
        elif market == "kr":
            cur.execute(f"SELECT * FROM momentum_cache WHERE market IN ('kospi','kosdaq') {freshness} ORDER BY momentum_score DESC")
        else:
            cur.execute(f"SELECT * FROM momentum_cache WHERE 1=1 {freshness} ORDER BY momentum_score DESC")
        rows = cur.fetchall(); cur.close(); conn.close()
        if not rows: return None
        results = []
        for r in rows:
            d = dict(r)
            if d.get("screened_at"): d["screened_at"] = d["screened_at"].strftime("%Y-%m-%d %H:%M")
            results.append(d)
        return results
    except Exception as e:
        logger.error(f"모멘텀 DB 조회 오류: {e}")
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

def _screen_one_market(market, tickers):
    """단일 시장 스크리닝 → DB 저장 (+ 미국은 베스트픽). 반환: 저장 건수"""
    logger.info(f"[{market}] {len(tickers)}개 스크리닝 중...")
    results = []
    # 병렬 10 — yfinance rate limit 회피 (20이면 뒤 시장이 차단당해 일부만 성공)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_single_stock, t, market): t for t in tickers}
        for f in concurrent.futures.as_completed(futures, timeout=900):
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
    if market in ("nasdaq", "sp500"):
        picks = select_bestpick_5(results)
        save_bestpick_to_db(picks, market=market)
    logger.info(f"[{market}] {len(results)}개 완료")
    return len(results)

def _send_screening_telegram():
    """DB 최신 결과로 텔레그램 알림 (한국+미국 종합)"""
    try:
        alert_results = {}
        for market in ["nasdaq", "sp500", "kospi", "kosdaq"]:
            rows = load_screening_from_db(market)
            if rows: alert_results[market] = rows[:5]
        tb_rows = []
        if DATABASE_URL:
            try:
                conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT * FROM tenbagger_cache ORDER BY total_score DESC LIMIT 10")
                tb_rows = [dict(r) for r in cur.fetchall()]
                cur.close(); conn.close()
            except: pass
        send_screening_alert(alert_results, tb_rows)
    except Exception as e:
        logger.error(f"텔레그램 알림 오류: {e}")

def run_kr_screening_job():
    """한국 스크리닝 (KST 01:00) — 미국과 분리해 yfinance rate limit 회피"""
    import time as _t
    logger.info("=== 한국 스크리닝 시작 (KST 01:00) ===")
    start = datetime.now()
    for i, (market, tickers) in enumerate((("kospi", TICKERS_KOSPI), ("kosdaq", TICKERS_KOSDAQ))):
        if i: _t.sleep(90)  # 시장 사이 텀 — rate limit 회복
        _screen_one_market(market, tickers)
    logger.info(f"=== 한국 스크리닝 완료: {int((datetime.now()-start).total_seconds()//60)}분 ===")

def run_us_screening_job():
    """미국 스크리닝 + 텐배거 + 정리 + 알림 (KST 04:00)"""
    import time as _t
    logger.info("=== 미국 스크리닝 시작 (KST 04:00) ===")
    start = datetime.now()
    sp500 = get_sp500_tickers()
    if sp500:
        global _sp500_cache
        _sp500_cache = sp500
    for i, (market, tickers) in enumerate((("nasdaq", TICKERS_NASDAQ), ("sp500", get_tickers_sp500()))):
        if i: _t.sleep(90)  # 나스닥 후 텀 — S&P500이 rate limit에 걸리지 않도록
        _screen_one_market(market, tickers)
    _run_tenbagger_job()
    cleanup_old_data()
    logger.info(f"=== 미국 스크리닝 완료: {int((datetime.now()-start).total_seconds()//60)}분 ===")
    _send_screening_telegram()  # 이 시점엔 한국(01:00)도 완료되어 종합 알림 가능

def run_full_screening_job():
    """전체 스크리닝 (수동 트리거 /api/screen/run · 부팅 catchup용) — 한국+미국 순차"""
    import time as _t
    logger.info("=== MAGU STOCK 전체 스크리닝 시작 ===")
    run_kr_screening_job()
    _t.sleep(120)  # 한국→미국 텀 — rate limit 회복
    run_us_screening_job()

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

def _run_momentum_job():
    """KST 05:30 모멘텀 스크리닝 (펀더멘탈 무관) — 스크리닝과 동일한 rate limit 완화"""
    import time as _t
    logger.info("=== 모멘텀 스크리닝 시작 ===")
    us_pool = list(dict.fromkeys(TICKERS_NASDAQ + TICKERS_MOMENTUM_EXTRA_US))
    sp500_pool = get_tickers_sp500() or TICKERS_SP500_FALLBACK
    markets = {
        "nasdaq": us_pool,
        "sp500":  sp500_pool,
        "kospi":  TICKERS_KOSPI,
        "kosdaq": TICKERS_KOSDAQ,
    }
    for i, (market, tickers) in enumerate(markets.items()):
        if i: _t.sleep(90)  # 시장 사이 텀 — yfinance rate limit 회복
        logger.info(f"[모멘텀/{market}] {len(tickers)}개 스크리닝 중...")
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_momentum_stock, t, market): t for t in tickers}
            for f in concurrent.futures.as_completed(futures, timeout=900):
                try:
                    r = f.result(timeout=30)
                    if r: results.append(r)
                except: continue
        results.sort(key=lambda x: x['momentum_score'], reverse=True)
        save_momentum_to_db(results)
        logger.info(f"[모멘텀/{market}] {len(results)}개 완료")
    logger.info("=== 모멘텀 스크리닝 완료 ===")

# ══════════════════════════════════════════════════════════════
# 공매도 Short Volume (FINRA REGSHO) + Short Interest (yfinance)
# ══════════════════════════════════════════════════════════════

SHORT_VOL_TICKERS = ["SPY", "QQQ", "IWM"]

def fetch_finra_short_volume(tickers: list, max_days_back: int = 5) -> dict:
    """FINRA REGSHO Consolidated NMS 파일에서 Short Volume 파싱
    최근 영업일 파일을 찾아 반환: {ticker: {date, short_volume, total_volume, ratio}}
    """
    import io
    results = {}
    today = datetime.now()
    for delta in range(max_days_back):
        dt = today - timedelta(days=delta)
        if dt.weekday() >= 5:  # 토/일 스킵
            continue
        date_str = dt.strftime("%Y%m%d")
        url = f"https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date_str}.txt"
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                continue
            text = resp.text
            if not text.strip() or "Date|Symbol" not in text:
                continue
            # 파싱: Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
            found = {}
            for line in text.splitlines():
                parts = line.split("|")
                if len(parts) < 5:
                    continue
                sym = parts[1].strip().upper()
                if sym in tickers:
                    try:
                        sv = int(parts[2])
                        tv = int(parts[4])
                        ratio = round(sv / tv, 4) if tv > 0 else None
                        found[sym] = {
                            "trade_date": dt.strftime("%Y-%m-%d"),
                            "short_volume": sv,
                            "total_volume": tv,
                            "short_vol_ratio": ratio
                        }
                    except: pass
            if found:
                results = found
                logger.info(f"[공매도] FINRA {date_str} 파싱 완료: {list(found.keys())}")
                break
        except Exception as e:
            logger.warning(f"[공매도] FINRA {date_str} 요청 실패: {e}")
            continue
    return results

def fetch_short_interest_yf(tickers: list) -> dict:
    """yfinance t.info에서 Short Interest % / Short Ratio / Shares Short 수집"""
    result = {}
    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).info
            result[ticker] = {
                "short_pct_float": round((info.get("shortPercentOfFloat") or 0) * 100, 2),
                "short_ratio":     round(info.get("shortRatio") or 0, 2),
                "shares_short":    info.get("sharesShort") or 0,
            }
        except Exception as e:
            logger.warning(f"[공매도] yfinance {ticker} 실패: {e}")
            result[ticker] = {"short_pct_float": None, "short_ratio": None, "shares_short": None}
    return result

# ══════════════════════════════════════════════════════════════
# ARK Invest 보유/매매 수집
# ══════════════════════════════════════════════════════════════

ARK_FUNDS = ["ARKK", "ARKW", "ARKG", "ARKF", "ARKX", "ARKQ"]

def fetch_ark_holdings() -> dict:
    """ARK 전 펀드 보유 현황 — ticker별 최고 비중 펀드 반환"""
    holdings = {}
    for fund in ARK_FUNDS:
        try:
            url = f"https://arkfunds.io/api/v2/etf/holdings?symbol={fund}"
            resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code != 200:
                logger.warning(f"[ARK] {fund} HTTP {resp.status_code}")
                continue
            data = resp.json()
            trade_date = data.get("date")
            for h in data.get("holdings", []):
                ticker = (h.get("ticker") or "").upper().strip()
                if not ticker:
                    continue
                weight = float(h.get("weight") or 0)
                if ticker not in holdings or weight > holdings[ticker]["weight"]:
                    holdings[ticker] = {
                        "fund": fund, "ticker": ticker,
                        "company": h.get("company", ""),
                        "shares": int(h.get("shares") or 0),
                        "market_value": float(h.get("market_value") or 0),
                        "weight": weight, "trade_date": trade_date,
                    }
        except Exception as e:
            logger.warning(f"[ARK] {fund} holdings 실패: {e}")
    logger.info(f"[ARK] holdings {len(holdings)}개")
    return holdings

def fetch_ark_trades() -> dict:
    """ARK 전 펀드 최근 1일 매매 수집"""
    trades = {}
    for fund in ARK_FUNDS:
        try:
            url = f"https://arkfunds.io/api/v2/etf/trades?symbol={fund}&period=1d"
            resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code != 200:
                continue
            data = resp.json()
            for t in data.get("trades", []):
                ticker = (t.get("ticker") or "").upper().strip()
                if not ticker or ticker in trades:
                    continue
                trades[ticker] = {
                    "fund": fund, "ticker": ticker,
                    "company": t.get("company", ""),
                    "direction": t.get("direction", ""),
                    "shares": int(t.get("shares") or 0),
                    "etf_percent": float(t.get("etf_percent") or 0),
                    "trade_date": t.get("date"),
                }
        except Exception as e:
            logger.warning(f"[ARK] {fund} trades 실패: {e}")
    logger.info(f"[ARK] trades {len(trades)}개")
    return trades

def save_ark_to_db(holdings: dict, trades: dict):
    if not DATABASE_URL:
        return
    try:
        conn = get_conn(); cur = conn.cursor()
        # 성공적으로 가져온 펀드의 기존 보유 내역 전체 삭제 후 재삽입
        # (청산 종목이 DB에 영구 잔류하는 버그 방지)
        fetched_funds = list({h["fund"] for h in holdings.values()})
        if fetched_funds:
            cur.execute("DELETE FROM ark_holdings_cache WHERE fund = ANY(%s)", (fetched_funds,))
        for h in holdings.values():
            cur.execute("""
                INSERT INTO ark_holdings_cache (fund,ticker,company,shares,market_value,weight,trade_date)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (fund,ticker) DO UPDATE SET
                    company=EXCLUDED.company, shares=EXCLUDED.shares,
                    market_value=EXCLUDED.market_value, weight=EXCLUDED.weight,
                    trade_date=EXCLUDED.trade_date, fetched_at=NOW()
            """, (h["fund"],h["ticker"],h["company"],h["shares"],h["market_value"],h["weight"],h["trade_date"]))
        today = datetime.now().strftime("%Y-%m-%d")
        cur.execute("DELETE FROM ark_trades_cache WHERE trade_date::text = %s", (today,))
        for t in trades.values():
            cur.execute("""
                INSERT INTO ark_trades_cache (fund,ticker,company,direction,shares,etf_percent,trade_date)
                VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
            """, (t["fund"],t["ticker"],t["company"],t["direction"],t["shares"],t["etf_percent"],t.get("trade_date") or today))
        conn.commit(); cur.close(); conn.close()
        logger.info(f"[ARK] 저장: holdings {len(holdings)}, trades {len(trades)}")
    except Exception as e:
        logger.error(f"[ARK] 저장 오류: {e}")

def _run_ark_job():
    """ARK 보유/매매 수집 (매일 KST 10:00 = UTC 01:00)"""
    logger.info("[ARK] 수집 시작")
    h = fetch_ark_holdings()
    t = fetch_ark_trades()
    if h:
        save_ark_to_db(h, t)
    else:
        logger.warning("[ARK] holdings 없음")

def _run_short_volume_job():
    """FINRA Short Volume + yfinance Short Interest → DB 저장 (매일 KST 08:00 job에서 호출)"""
    logger.info("[공매도] 수집 시작")
    try:
        sv_data  = fetch_finra_short_volume(SHORT_VOL_TICKERS)
        si_data  = fetch_short_interest_yf(SHORT_VOL_TICKERS)

        if not sv_data and not si_data:
            logger.warning("[공매도] 데이터 없음, 저장 스킵")
            return
        if not DATABASE_URL:
            return

        conn = get_conn(); cur = conn.cursor()
        for ticker in SHORT_VOL_TICKERS:
            sv = sv_data.get(ticker, {})
            si = si_data.get(ticker, {})
            if not sv and not si:
                continue
            trade_date = sv.get("trade_date") or datetime.now().strftime("%Y-%m-%d")
            cur.execute("""
                INSERT INTO short_volume_cache
                (ticker, trade_date, short_volume, total_volume, short_vol_ratio,
                 short_pct_float, short_ratio, shares_short, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s, NOW())
                ON CONFLICT (ticker, trade_date) DO UPDATE SET
                    short_volume    = EXCLUDED.short_volume,
                    total_volume    = EXCLUDED.total_volume,
                    short_vol_ratio = EXCLUDED.short_vol_ratio,
                    short_pct_float = EXCLUDED.short_pct_float,
                    short_ratio     = EXCLUDED.short_ratio,
                    shares_short    = EXCLUDED.shares_short,
                    updated_at      = NOW()
            """, (
                ticker, trade_date,
                sv.get("short_volume"), sv.get("total_volume"), sv.get("short_vol_ratio"),
                si.get("short_pct_float"), si.get("short_ratio"), si.get("shares_short")
            ))
        conn.commit(); cur.close(); conn.close()
        logger.info(f"[공매도] 저장 완료: {list(sv_data.keys())}")
    except Exception as e:
        logger.error(f"[공매도] job 오류: {e}")

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
                elif r>=1.005: cond2=3
                else: cond2=0
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

    # ── AI / 엔터프라이즈 소프트웨어 ─────────────────────────────
    "PLTR","AI","SOUN","BBAI","RBRK","CWAN","ALKT",
    "DDOG","ZS","DUOL","GTLB","SMAR","ASAN","MNDY","BRZE","APP",
    "CFLT","HUBS","DOCN","ESTC","MDB","NCNO","SPSC","PCTY","PAYC",
    "APPN","ALTR","JAMF","WEAVE","DV",

    # ── 반도체 / 반도체 장비 ─────────────────────────────────────
    "AEIS","AMBA","LSCC","SITM","ONTO","ACLS","ICHR","KLIC","MTSI",
    "FORM","WOLF","CAMT","AEHR","RMBS","POWI","DIOD","ALGM","IREN",
    "SMTC","AXTI","PSIX",

    # ── 양자컴퓨팅 ───────────────────────────────────────────────
    "IONQ","RGTI","QUBT","QBTS","QTUM",

    # ── 핀테크 / 금융 혁신 ───────────────────────────────────────
    "AFRM","UPST","BILL","TOST","RELY",
    "FLYW","IREN","HYFM","DAVE","CURO","PRFT","LC","OPEN",
    "SOFI","HOOD","CLOV","STEP","NVEI",

    # ── 사이버보안 ───────────────────────────────────────────────
    "CRWD","S","PANW","CYBR","TENB","QLYS","VRNS","SSTI","RSKD",
    "CODA","MIMECAST","DNLI","EVBG",

    # ── 헬스케어 / 바이오테크 ────────────────────────────────────
    "HIMS","RXRX","BEAM","CRSP","ARWR","KYMR","VKTX","NVCR","INSM",
    "RVMD","PTGX","ACMR","PRAX","TMDX","IMNM","IMVT","AMPH",
    "CLPT","AGIO","VRNA","ALLO","KROS","TVTX",

    # ── 에너지 혁신 / SMR / 청정에너지 ──────────────────────────
    "FSLR","ENPH","ARRY",
    "SMR","OKLO","NNE","BWXT","CEG",
    "STEM","ARRY","BE","PLUG","HYZN","EVGO","CHPT",

    # ── 우주 / 국방테크 / 드론 ───────────────────────────────────
    "RKLB","ASTS","JOBY","ACHR",
    "LUNR","RDW","MNTS","KTOS","HII","RCAT","JOBY",
    "SPIR","PL","SATL",

    # ── 소비재 / 브랜드 성장주 ───────────────────────────────────
    "CELH","BROS","CAVA","WING","FRPT","YETI",
    "LULU","ELF","ONON","BIRK","DKNG","PENN","ACMR",
    "XPOF","PRPL","MESO",

    # ── 데이터 / 클라우드 인프라 ─────────────────────────────────
    "SNOW","NET","DDOG","CFLT","DOCN",
    "TASK","CLNC","AIOT","NTNX","ALTR","PSTG","REYN",

    # ── 로봇 / 자동화 / 모빌리티 ────────────────────────────────
    "RCAT","TNDM","ISRG","AVAV","PRCT","SWBI",
    "RH","TDY","NDSN",

]))

# ══════════════════════════════════════════════════════════════
# 유동성 모듈 (기존 그대로)
# ══════════════════════════════════════════════════════════════

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
FRED_BASE    = "https://api.stlouisfed.org/fred/series/observations"
_liquidity_cache: dict = {}

# ── 텔레그램 알림 ──────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

def send_telegram(message: str):
    """텔레그램 메시지 발송"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.info("텔레그램 미설정 — 알림 스킵")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
        if resp.status_code == 200:
            logger.info("텔레그램 알림 발송 완료")
        else:
            logger.warning(f"텔레그램 발송 실패: {resp.text}")
    except Exception as e:
        logger.error(f"텔레그램 오류: {e}")

def send_screening_alert(market_results: dict, tenbagger_results: list):
    """스크리닝 완료 후 TOP5 텔레그램 발송"""
    kst_now = datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")
    lines = [f"🔔 <b>WISEMAC STOCK 스크리닝 완료</b>", f"📅 {kst_now}\n"]

    market_labels = {"nasdaq":"🇺🇸 나스닥","sp500":"🇺🇸 S&P500","kospi":"🇰🇷 코스피","kosdaq":"🇰🇷 코스닥"}
    for market, results in market_results.items():
        if not results: continue
        top5 = results[:5]
        lines.append(f"<b>{market_labels.get(market, market)} TOP 5</b>")
        for i, r in enumerate(top5, 1):
            lines.append(f"  {i}. {r.get('name', r['ticker'])} ({r['ticker']}) — {r['total_score']}점")
        lines.append("")

    if tenbagger_results:
        top3 = [r for r in tenbagger_results if r.get('total_score', 0) >= 75][:3]
        if top3:
            lines.append("<b>🚀 텐배거 최상위</b>")
            for r in top3:
                lines.append(f"  🔥 {r.get('name', r['ticker'])} ({r['ticker']}) — {r['total_score']}점")

    send_telegram("\n".join(lines))

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
    rrp_4w=(rrp_data[20]["value"] if len(rrp_data)>20 else rrp_data[-1]["value"])/1e6
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
    # 60점 만점 기준 (순유동성 40점 + MMF 20점)
    if total_score>=48:  # 80% 이상
        return {"stage":1,"signal":"적극매수","emoji":"🟢","color":"#15803d","bg_color":"#dcfce7","border_color":"#86efac",
                "description":"순유동성 풍부\nMMF 자금 위험자산 이동",
                "action":"스크리닝 신호를 적극 반영하세요. 마구스코어 65점+ 종목 분할 매수 고려.",
                "step2_guide":"✅ 스크리닝 신호 적극 반영 — 분할 매수 진입 권장"}
    elif total_score>=36:  # 60% 이상
        return {"stage":2,"signal":"매수우호","emoji":"🔵","color":"#1d4ed8","bg_color":"#dbeafe","border_color":"#93c5fd",
                "description":"순유동성 양호\n매수에 우호적인 환경",
                "action":"스크리닝 결과를 참고하여 선별적으로 매수하세요.",
                "step2_guide":"✅ 스크리닝 신호 참고 — 마구스코어 70점+ 종목 위주 선별 매수"}
    elif total_score>=24:  # 40% 이상
        return {"stage":3,"signal":"중립관망","emoji":"🟡","color":"#b45309","bg_color":"#fef9c3","border_color":"#fde68a",
                "description":"방향 불확실\n긍정·부정 신호 혼재",
                "action":"신규 매수 자제. 기존 포지션 유지하며 방향 확인 후 판단하세요.",
                "step2_guide":"⚠️ 스크리닝 참고만 — 신규 매수 자제, 기존 보유 종목 유지"}
    elif total_score>=12:  # 20% 이상
        return {"stage":4,"signal":"매수축소","emoji":"🟠","color":"#c2410c","bg_color":"#ffedd5","border_color":"#fdba74",
                "description":"순유동성 감소 중\n위험 관리 필요",
                "action":"신규 매수 중단. 보유 종목 비중 축소 및 손절 기준 점검하세요.",
                "step2_guide":"🚫 스크리닝 결과 무시 — 포지션 축소, 현금 비중 확대"}
    else:  # 20% 미만
        return {"stage":5,"signal":"현금보유","emoji":"🔴","color":"#991b1b","bg_color":"#fee2e2","border_color":"#fca5a5",
                "description":"순유동성 심각한 경색\n현금 보유 권고",
                "action":"전량 현금 보유 권고. 스크리닝 결과와 무관하게 매수 금지.",
                "step2_guide":"🔴 스크리닝 결과 무시 — 전량 현금 보유, 매수 금지"}

# ══════════════════════════════════════════════════════════════
# API 엔드포인트
# ══════════════════════════════════════════════════════════════

@app.get("/")
def root():
    return RedirectResponse(url="/login", status_code=302)

@app.get("/api/market")
@limiter.limit("30/minute")
def get_market_data(request: Request):
    try:
        tickers={"gold":"GC=F","wti":"CL=F","usdkrw":"KRW=X","us10y":"^TNX",
                 "dxy":"DX-Y.NYB",
                 "vix":"^VIX","sp500":"^GSPC","nasdaq":"^IXIC","dow":"^DJI","russell":"^RUT",
                 "kospi":"^KS11","kosdaq":"^KQ11"}
        result={}
        for key,symbol in tickers.items():
            try:
                t=yf.Ticker(symbol); hist=t.history(period="30d")
                if len(hist)>=2:
                    current=float(hist['Close'].iloc[-1]); prev=float(hist['Close'].iloc[-2])
                    if math.isnan(current) or math.isnan(prev) or prev==0:
                        result[key]={"value":None,"change":None}
                        continue
                    row={"value":round(current,2),"change":round((current/prev-1)*100,2)}
                    if key=="vix" and len(hist)>=6:
                        prev5=float(hist['Close'].iloc[-6])
                        if not math.isnan(prev5):
                            row["direction"]="up" if current>prev5 else "down"
                            row["prev5"]=round(prev5,2)
                    # ── DXY 매크로 컨텍스트 ──────────────────────────
                    if key=="dxy" and len(hist)>=21:
                        prev5=float(hist['Close'].iloc[-6])
                        prev20=float(hist['Close'].iloc[-21])
                        ma20=float(hist['Close'].iloc[-20:].mean())
                        if not (math.isnan(prev5) or math.isnan(prev20) or math.isnan(ma20)):
                            chg_5d=round((current/prev5-1)*100,2)
                            chg_20d=round((current/prev20-1)*100,2)
                            # 강도 판정 (DXY 105 = 강달러 위험, 100 = 중립선)
                            if current>=107:    zone="강달러 위험구간 (107+)"
                            elif current>=105:  zone="강달러 (105~107)"
                            elif current>=102:  zone="달러 강세 (102~105)"
                            elif current>=100:  zone="중립 (100~102)"
                            elif current>=97:   zone="약달러 (97~100)"
                            else:               zone="약달러 위험구간 (97 미만)"
                            row["chg_5d"]=chg_5d
                            row["chg_20d"]=chg_20d
                            row["ma20"]=round(ma20,2)
                            row["above_ma20"]=current>ma20
                            row["direction_5d"]="up" if chg_5d>0 else "down"
                            row["prev5"]=round(prev5,2)
                            row["zone"]=zone
                            # 위험자산 정합성 (DXY 상승 = 위험자산 역풍)
                            row["risk_asset_signal"]=(
                                "역풍 (DXY 상승)" if chg_5d>0.5
                                else "우호 (DXY 하락)" if chg_5d<-0.5
                                else "중립"
                            )
                    result[key]=row
            except: result[key]={"value":None,"change":None}

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

        return safe_json(result)
    except: return safe_json({})


@app.get("/api/fear_greed")
@limiter.limit("30/minute")
def get_fear_greed(request: Request):
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
@limiter.limit("30/minute")
def get_market_breadth(request: Request):
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
                SUM(CASE WHEN g_ma200 > 0 THEN 1 ELSE 0 END) AS above_ma200,
                ROUND(SUM(CASE WHEN g_ma200 > 0 THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct
            FROM screening_cache
            WHERE screened_at > NOW() - INTERVAL '25 hours'
            GROUP BY market
        """)
        rows=[dict(r) for r in cur.fetchall()]
        cur.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN g_ma200 > 0 THEN 1 ELSE 0 END) AS above_ma200,
                ROUND(SUM(CASE WHEN g_ma200 > 0 THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct
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
@limiter.limit("30/minute")
def screen_stocks(request: Request, market: str = "nasdaq"):
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
    return safe_json({"market":market,"market_label":_market_label(market),"currency":_currency(market),
            "updated_at":datetime.now().strftime("%Y-%m-%d %H:%M"),
            "total_screened":len(results),"results":results,"from_cache":False})

@app.post("/api/screen/run")
@limiter.limit("5/minute")
def trigger_screening(request: Request, background_tasks: BackgroundTasks):
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

# ══════════════════════════════════════════════════════════════
# 모멘텀 스크리너 API
# ══════════════════════════════════════════════════════════════

@app.get("/api/momentum/{market}")
@limiter.limit("10/minute")
def get_momentum(request: Request, market: str = "nasdaq"):
    cached = load_momentum_from_db(market)
    if cached:
        return safe_json({"results": cached, "from_cache": True, "updated_at": cached[0].get("screened_at", "")})
    # 캐시 없으면 실시간 계산
    if market in ("nasdaq", "us"):
        tickers = list(dict.fromkeys(TICKERS_NASDAQ + TICKERS_MOMENTUM_EXTRA_US))
    elif market == "sp500":
        tickers = get_tickers_sp500() or TICKERS_SP500_FALLBACK
    elif market == "kospi":
        tickers = TICKERS_KOSPI
    elif market == "kosdaq":
        tickers = TICKERS_KOSDAQ
    else:
        tickers = list(dict.fromkeys(TICKERS_NASDAQ + TICKERS_MOMENTUM_EXTRA_US))
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(fetch_momentum_stock, t, market): t for t in tickers}
        for f in concurrent.futures.as_completed(futures, timeout=300):
            try:
                r = f.result(timeout=30)
                if r: results.append(r)
            except: continue
    results.sort(key=lambda x: x['momentum_score'], reverse=True)
    save_momentum_to_db(results)
    return safe_json({"results": results, "from_cache": False, "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M")})

@app.post("/api/momentum/run")
@limiter.limit("3/minute")
def trigger_momentum(request: Request, background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_momentum_job)
    return {"message": "모멘텀 스크리닝 시작됨. 약 5~10분 소요."}

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
    market = "kospi" if ticker.endswith(".KS") else "kosdaq" if ticker.endswith(".KQ") else "nasdaq"
    try:
        stock=yf.Ticker(ticker); info=stock.info
        price_check=info.get('regularMarketPrice') or info.get('currentPrice') or info.get('previousClose')
        if not info or not price_check: return {"error":f"종목을 찾을 수 없습니다: {ticker}"}
        hist_daily=stock.history(period="1y"); hist_weekly=stock.history(period="2y",interval="1wk")
        if hist_daily.empty or len(hist_daily)<20: return {"error":"데이터가 부족합니다"}
        c_ema=score_ema_slope(hist_weekly)
        c_stoch=score_stochastic(hist_daily)//(2 if c_ema==0 else 1)
        c_break=score_breakout(hist_daily)//(2 if c_ema==0 else 1)
        classic=c_ema+c_stoch+c_break
        g_roe=score_roe(info.get('returnOnEquity',0) or 0); g_debt=score_debt(info.get('debtToEquity',None))
        g_eps=score_eps_growth(info); g_peg=score_peg(info.get('pegRatio',None))
        g_ma200=score_ma200(hist_daily); g_rsi=score_rsi(hist_daily)
        growth=g_roe+g_debt+g_eps+g_peg+g_ma200+g_rsi
        m_anal=score_analyst(info.get('recommendationKey','') or '')
        m_rs=score_relative_strength(hist_daily, market); m_obv=score_obv_momentum(hist_daily)
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
        # 차트용 가격 이력
        closes = [round(float(v), 2) if not pd.isna(v) else None for v in hist_daily['Close']]
        dates = [str(d.date()) if hasattr(d, 'date') else str(d)[:10] for d in hist_daily.index]
        ma20_raw = hist_daily['Close'].rolling(20).mean()
        ma60_raw = hist_daily['Close'].rolling(60).mean()
        ma20 = [round(float(v), 2) if not pd.isna(v) else None for v in ma20_raw]
        ma60 = [round(float(v), 2) if not pd.isna(v) else None for v in ma60_raw]
        return {"ticker":ticker,"name":name,"sector":info.get('sector') or '—',
                "currency":info.get('currency','USD'),"price":round(current_price,2),
                "change_pct":round(change_pct,2),"classic_score":classic,
                "growth_score":growth,"modern_score":modern,"total_score":total,
                "recommendation":get_recommendation(total,classic,growth,modern),
                "chart":{"dates":dates,"closes":closes,"ma20":ma20,"ma60":ma60},
                "detail":{"roe":round((info.get('returnOnEquity') or 0)*100,1),
                          "debt_equity":round(info.get('debtToEquity') or 0,1),
                          "eps_growth":round((info.get('earningsGrowth') or 0)*100,1),
                          "peg":round(info.get('pegRatio') or 0,2),
                          "rsi":rsi_val,"year_return":year_return,
                          "rev_growth":round((info.get('revenueGrowth') or 0)*100,1),
                          "op_margin":round((info.get('operatingMargins') or 0)*100,1),
                          "analyst_rec":info.get('recommendationKey') or '—',
                          "market_cap":info.get('marketCap') or 0}}
    except Exception as e:
        logger.error(f"종목 조회 오류 [{ticker}]: {e}")
        return {"error":f"조회 실패: {str(e)}"}

def analyze_etf(etf_info: dict):
    ticker=etf_info["ticker"]
    try:
        t=yf.Ticker(ticker); info=t.info; hist=t.history(period="2y")
        if hist.empty or len(hist)<60: return None

        def safe_float(val, fallback=None):
            try:
                v = float(val)
                return fallback if (math.isnan(v) or math.isinf(v)) else v
            except: return fallback

        def safe_ret(a, b):
            if a is None or b is None or b == 0: return None
            try:
                v = (a/b-1)*100
                return round(v,2) if not (math.isnan(v) or math.isinf(v)) else None
            except: return None

        price=safe_float(hist['Close'].iloc[-1])
        if price is None: return None

        p1d=safe_float(hist['Close'].iloc[-2]) if len(hist)>=2 else price
        p1w=safe_float(hist['Close'].iloc[-6]) if len(hist)>=6 else price
        p1m=safe_float(hist['Close'].iloc[-22]) if len(hist)>=22 else price
        p3m=safe_float(hist['Close'].iloc[-66]) if len(hist)>=66 else price
        p6m=safe_float(hist['Close'].iloc[-132]) if len(hist)>=132 else price
        p1y=safe_float(hist['Close'].iloc[-252]) if len(hist)>=252 else safe_float(hist['Close'].iloc[0])

        r1d=safe_ret(price,p1d); r1w=safe_ret(price,p1w)
        r1m=safe_ret(price,p1m); r3m=safe_ret(price,p3m)
        r6m=safe_ret(price,p6m); r1y=safe_ret(price,p1y)

        vol5d=safe_float(hist['Volume'].iloc[-5:].mean(),0)
        vol20d=safe_float(hist['Volume'].iloc[-20:].mean(),0)
        vol_ratio=round(vol5d/vol20d,2) if vol20d and vol20d>0 else 1.0

        rsi_val=0.0
        try:
            if len(hist)>=14:
                rv=safe_float(ta.momentum.RSIIndicator(hist['Close'],window=14).rsi().iloc[-1])
                if rv is not None: rsi_val=round(rv,1)
        except: pass

        high_52w_raw=hist['High'].iloc[-252:].max() if len(hist)>=252 else hist['High'].max()
        high_52w=safe_float(high_52w_raw)
        from_high=round((price/high_52w-1)*100,1) if high_52w and high_52w>0 else None
        inst_pct=round(safe_float(info.get('heldPercentInstitutions') or 0, 0)*100,1)

        sc=0
        if r1m is not None:
            if r1m>5:sc+=3
            elif r1m>2:sc+=2
            elif r1m>0:sc+=1
            elif r1m<-5:sc-=3
            elif r1m<-2:sc-=2
            elif r1m<0:sc-=1
        if r3m is not None:
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
    except Exception as e:
        logger.warning(f"analyze_etf 오류 [{ticker}]: {e}")
        return None

@app.get("/api/smartmoney")
@limiter.limit("10/minute")
def get_smart_money(request: Request):
    try:
        results=[]
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures={executor.submit(analyze_etf,etf):etf for etf in SECTOR_ETFS}
            for f in concurrent.futures.as_completed(futures):
                try:
                    r=f.result()
                    if r: results.append(r)
                except: pass
        results.sort(key=lambda x:x.get("momentum_score",0),reverse=True)
        try:
            spy=yf.Ticker("SPY").history(period="2y")
            spy_1m=round(float((spy['Close'].iloc[-1]/spy['Close'].iloc[-22]-1)*100),2) if len(spy)>=22 else 0
            spy_3m=round(float((spy['Close'].iloc[-1]/spy['Close'].iloc[-66]-1)*100),2) if len(spy)>=66 else 0
            spy_6m=round(float((spy['Close'].iloc[-1]/spy['Close'].iloc[-132]-1)*100),2) if len(spy)>=132 else 0
            spy_1y=round(float((spy['Close'].iloc[-1]/spy['Close'].iloc[-252]-1)*100),2) if len(spy)>=252 else 0
        except: spy_1m=spy_3m=spy_6m=spy_1y=0
        if results:
            for r in results:
                r["rel_strength"]=round(r["ret_1m"]-spy_1m,2) if r.get("ret_1m") is not None else None
        return safe_json({"updated_at":datetime.now().strftime("%Y-%m-%d %H:%M"),
                "spy_ret_1m":spy_1m,"spy_ret_3m":spy_3m,"spy_ret_6m":spy_6m,"spy_ret_1y":spy_1y,
                "sectors":results,"total":len(results)})
    except Exception as e:
        logger.error(f"섹터 모멘텀 오류: {e}")
        return safe_json({"error":str(e),"sectors":[],"total":0,"updated_at":datetime.now().strftime("%Y-%m-%d %H:%M")})

def _format_sv_row(d: dict) -> dict:
    return {
        "ticker":          d["ticker"],
        "trade_date":      d["trade_date"].strftime("%Y-%m-%d") if d.get("trade_date") else None,
        "short_volume":    int(d["short_volume"]) if d.get("short_volume") else None,
        "total_volume":    int(d["total_volume"]) if d.get("total_volume") else None,
        "short_vol_ratio": float(d["short_vol_ratio"]) if d.get("short_vol_ratio") is not None else None,
        "short_pct_float": float(d["short_pct_float"]) if d.get("short_pct_float") is not None else None,
        "short_ratio":     float(d["short_ratio"]) if d.get("short_ratio") is not None else None,
        "shares_short":    int(d["shares_short"]) if d.get("shares_short") else None,
        "updated_at":      d["updated_at"].strftime("%Y-%m-%d %H:%M") if d.get("updated_at") else None,
    }

@app.get("/api/short_volume")
@limiter.limit("10/minute")
def get_short_volume(request: Request):
    """SPY/QQQ/IWM 공매도 데이터 — DB 캐시 우선(오늘+전일), 없으면 실시간 수집"""
    # ── DB 캐시: ticker별 최근 2일치 반환 ──────────────────────
    if DATABASE_URL:
        try:
            conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT ticker, trade_date, short_volume, total_volume,
                       short_vol_ratio, short_pct_float, short_ratio, shares_short, updated_at,
                       ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) AS rn
                FROM short_volume_cache
            """)
            rows = cur.fetchall(); cur.close(); conn.close()
            today_map = {}; prev_map = {}
            for r in rows:
                d = dict(r); rn = d.pop("rn")
                t = d["ticker"]
                if rn == 1:   today_map[t] = _format_sv_row(d)
                elif rn == 2: prev_map[t]  = _format_sv_row(d)
            if len(today_map) >= len(SHORT_VOL_TICKERS):
                updated = list(today_map.values())[0]["updated_at"] if today_map else None
                return safe_json({"from_cache": True, "data": today_map, "prev": prev_map, "updated_at": updated})
        except Exception as e:
            logger.warning(f"공매도 캐시 조회 실패, 실시간 수집으로 fallback: {e}")

    # ── fallback: 실시간 수집 ──────────────────────────────────
    try:
        sv_data = fetch_finra_short_volume(SHORT_VOL_TICKERS)
        si_data = fetch_short_interest_yf(SHORT_VOL_TICKERS)
        result  = {}
        for ticker in SHORT_VOL_TICKERS:
            sv = sv_data.get(ticker, {})
            si = si_data.get(ticker, {})
            result[ticker] = {
                "ticker":          ticker,
                "trade_date":      sv.get("trade_date"),
                "short_volume":    sv.get("short_volume"),
                "total_volume":    sv.get("total_volume"),
                "short_vol_ratio": sv.get("short_vol_ratio"),
                "short_pct_float": si.get("short_pct_float"),
                "short_ratio":     si.get("short_ratio"),
                "shares_short":    si.get("shares_short"),
                "updated_at":      datetime.now().strftime("%Y-%m-%d %H:%M"),
            }
        return safe_json({"from_cache": False, "data": result, "prev": {},
                          "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M")})
    except Exception as e:
        logger.error(f"공매도 데이터 오류: {e}")
        return safe_json({"error": str(e), "data": {}, "prev": {},
                          "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M")})

@app.get("/api/ark")
@limiter.limit("10/minute")
def get_ark_data(request: Request):
    """ARK 보유 현황 + 최근 3일 매매 — ticker 기준 반환"""
    result = {"holdings": {}, "trades": {}, "updated_at": None}
    if DATABASE_URL:
        try:
            conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT * FROM ark_holdings_cache WHERE fetched_at >= NOW() - INTERVAL '3 days' ORDER BY weight DESC")
            for r in cur.fetchall():
                d = dict(r); tk = d["ticker"]
                result["holdings"][tk] = {
                    "fund":       d["fund"],
                    "weight":     float(d["weight"]) if d.get("weight") is not None else None,
                    "shares":     int(d["shares"]) if d.get("shares") else None,
                    "trade_date": d["trade_date"].strftime("%Y-%m-%d") if d.get("trade_date") else None,
                }
            cur.execute("SELECT * FROM ark_trades_cache WHERE trade_date >= NOW()::DATE - 3 ORDER BY trade_date DESC")
            seen = set()
            for r in cur.fetchall():
                d = dict(r); tk = d["ticker"]
                if tk not in seen:
                    seen.add(tk)
                    result["trades"][tk] = {
                        "fund":       d["fund"],
                        "direction":  d["direction"],
                        "shares":     int(d["shares"]) if d.get("shares") else None,
                        "trade_date": d["trade_date"].strftime("%Y-%m-%d") if d.get("trade_date") else None,
                    }
            if result["holdings"]:
                result["updated_at"] = next(iter(result["holdings"].values())).get("trade_date")
            cur.close(); conn.close()
            if result["holdings"]:
                return safe_json(result)
        except Exception as e:
            logger.warning(f"[ARK] 캐시 조회 실패, fallback: {e}")
    try:
        h = fetch_ark_holdings(); t = fetch_ark_trades()
        for tk, d in h.items():
            result["holdings"][tk] = {"fund": d["fund"], "weight": d["weight"],
                                      "shares": d["shares"], "trade_date": d["trade_date"]}
        for tk, d in t.items():
            result["trades"][tk]   = {"fund": d["fund"], "direction": d["direction"],
                                      "shares": d["shares"], "trade_date": d["trade_date"]}
        result["updated_at"] = datetime.now().strftime("%Y-%m-%d")
        return safe_json(result)
    except Exception as e:
        logger.error(f"[ARK] API 오류: {e}")
        return safe_json({"error": str(e), "holdings": {}, "trades": {}})

@app.get("/api/tenbagger")
@limiter.limit("5/minute")
def get_tenbagger(request: Request):
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
    results.sort(key=lambda x:x.get('total_score',0),reverse=True)
    return safe_json({"updated_at":datetime.now().strftime("%Y-%m-%d %H:%M"),
            "total":len(results),"universe":f"나스닥 중소형 성장주 {len(TICKERS_TENBAGGER)}개",
            "results":results,"from_cache":False})

@app.get("/api/liquidity")
@limiter.limit("10/minute")
def get_liquidity(request: Request):
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
    total_score=raw_score  # 60점 만점 그대로 표시 (환산 없음)
    signal=get_liquidity_signal(total_score)
    for ind in [net_liq,mmf,walcl_d,rrp_d,tga_d]:
        if ind.get("score") is None and ind.get("error"):
            ind["score"]="N/A"; ind["status"]="⚠️ 데이터 없음"
    cached_list=[k.upper() for k,v in is_cache.items() if v]
    data_note=(f"⚠️ 캐시 데이터 사용 중: {', '.join(cached_list)}" if cached_list else "✅ 전체 지표 실시간 데이터")
    return safe_json({"updated_at":datetime.now().strftime("%Y-%m-%d %H:%M"),
            "total_score":total_score,"max_score":60,"signal":signal,
            "net_liquidity":net_liq,"mmf":mmf,"indicators":[walcl_d,rrp_d,tga_d],
            "data_quality":data_note,"version":"최종판 — 순유동성(WALCL-RRP-TGA) + MMF",
            "scoring_structure":{"순유동성 (40점)":"WALCL-RRP-TGA / 절대수준 25 + 방향성 15",
                                 "MMF (20점)":"소매 WRMFNS 방향성 기준","합계":"60점 만점"},
            "sources":["TradingView: Fed Net Liquidity = WALCL-RRP-TGA",
                       "뉴욕 연준 / BlackRock / Cleveland Fed 공식 문헌 2025",
                       "ICI MMF 공식 데이터 (2026.03 $7.86조)",
                       "Babypips: TGA $800B 임계점","McClellan Financial: RRP 소진 분석"],
            "scoring_guide":{"48~60":"🟢 적극매수","36~47":"🔵 매수우호",
                             "24~35":"🟡 중립관망","12~23":"🟠 매수축소","0~11":"🔴 현금보유"}})

# ══════════════════════════════════════════════════════════════
# 베스트픽 백테스트 — 실제 추적 방식
# ══════════════════════════════════════════════════════════════

def select_bestpick_5(screening_results: list) -> list:
    """점수 순 TOP5 선정 — 카드/트래커/저장 모두 동일 기준"""
    candidates = sorted(screening_results, key=lambda x: x["total_score"], reverse=True)
    return candidates[:5]


def _fetch_spy_price_now() -> float:
    """SPY 현재가 조회 (베스트픽 저장 시 진입가 기록용)"""
    try:
        hist = yf.Ticker("SPY").history(period="2d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except: pass
    return 0.0

def save_bestpick_to_db(picks: list, market: str = "nasdaq") -> dict:
    """베스트픽 5종목을 DB에 저장. 중복 종목은 consecutive_count만 증가."""
    if not picks or not DATABASE_URL:
        return {"saved": 0, "skipped": 0, "error": "DB 없음"}
    today = datetime.now(pytz.timezone("Asia/Seoul")).date()
    spy_entry = _fetch_spy_price_now()
    saved = 0; skipped = 0
    try:
        conn = get_conn(); cur = conn.cursor()
        for p in picks:
            ticker = p["ticker"]
            cur.execute("SELECT id FROM bestpick_records WHERE ticker=%s AND picked_at=%s AND market=%s", (ticker, today, market))
            if cur.fetchone():
                skipped += 1
                continue
            cur.execute("""
                SELECT consecutive_count FROM bestpick_records
                WHERE ticker=%s AND market=%s
                  AND picked_at >= %s - INTERVAL '4 days'
                  AND picked_at < %s
                ORDER BY picked_at DESC LIMIT 1
            """, (ticker, market, today, today))
            row = cur.fetchone()
            consecutive = (row[0] + 1) if row else 1
            cur.execute("""
                INSERT INTO bestpick_records
                    (ticker, name, sector, entry_price, total_score,
                     classic_score, growth_score, modern_score,
                     recommendation, consecutive_count, picked_at, market, spy_entry_price)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                ticker, p.get("name", ticker), p.get("sector", "Unknown"),
                p.get("price", 0), p.get("total_score", 0),
                p.get("classic_score", 0), p.get("growth_score", 0),
                p.get("modern_score", 0), p.get("recommendation", ""),
                consecutive, today, market, spy_entry or None
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


def save_double_confirm_to_db(picks: list, market: str = "nasdaq") -> dict:
    """더블 컨펌 상위 5종목 DB 저장. 중복은 consecutive_count 증가."""
    if not picks or not DATABASE_URL:
        return {"saved": 0, "skipped": 0, "error": "DB 없음"}
    today = datetime.now(pytz.timezone("Asia/Seoul")).date()
    spy_entry = _fetch_spy_price_now()
    saved = 0; skipped = 0
    try:
        conn = get_conn(); cur = conn.cursor()
        for p in picks:
            ticker = p["ticker"]
            cur.execute("SELECT id FROM double_confirm_records WHERE ticker=%s AND picked_at=%s AND market=%s",
                        (ticker, today, market))
            if cur.fetchone():
                skipped += 1; continue
            cur.execute("""
                SELECT consecutive_count FROM double_confirm_records
                WHERE ticker=%s AND market=%s
                  AND picked_at >= %s - INTERVAL '4 days'
                  AND picked_at < %s
                ORDER BY picked_at DESC LIMIT 1
            """, (ticker, market, today, today))
            row = cur.fetchone()
            consecutive = (row[0] + 1) if row else 1
            cur.execute("""
                INSERT INTO double_confirm_records
                    (ticker, name, sector, entry_price, total_score, momentum_score,
                     combined_score, spy_entry_price, consecutive_count, picked_at, market)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                ticker, p.get("name", ticker), p.get("sector", "Unknown"),
                p.get("price", 0), p.get("total_score", 0),
                p.get("momentum_score", 0), p.get("combined_score", 0),
                spy_entry or None, consecutive, today, market
            ))
            record_id = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO double_confirm_prices (record_id, ticker, price_date, price, return_pct)
                VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
            """, (record_id, ticker, today, p.get("price", 0), 0.0))
            saved += 1
        conn.commit(); cur.close(); conn.close()
        logger.info(f"더블컨펌 저장 [{market}]: {saved}개 신규, {skipped}개 중복 스킵")
        return {"saved": saved, "skipped": skipped}
    except Exception as e:
        logger.error(f"더블컨펌 저장 오류: {e}")
        return {"saved": 0, "skipped": 0, "error": str(e)}


def _run_double_confirm_job():
    """KST 05:00 — 베스트픽+모멘텀 동시 통과 상위 5종목 자동 기록"""
    if not DATABASE_URL: return
    logger.info("=== 더블 컨펌 스크리닝 시작 ===")
    for market in ("nasdaq", "sp500"):
        try:
            conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            freshness = "AND screened_at > NOW() - INTERVAL '25 hours'"
            cur.execute(f"""
                SELECT ticker, name, sector, total_score, price
                FROM screening_cache WHERE market=%s AND total_score >= 65 {freshness}
            """, (market,))
            sc = {r["ticker"]: dict(r) for r in cur.fetchall()}
            cur.execute(f"""
                SELECT ticker, momentum_score
                FROM momentum_cache WHERE market=%s AND momentum_score >= 60 {freshness}
            """, (market,))
            mt = {r["ticker"]: dict(r) for r in cur.fetchall()}
            cur.close(); conn.close()

            picks = []
            for ticker in set(sc.keys()) & set(mt.keys()):
                s = sc[ticker]; m = mt[ticker]
                picks.append({
                    "ticker": ticker,
                    "name": s.get("name", ticker),
                    "sector": s.get("sector", "Unknown"),
                    "price": s.get("price", 0),
                    "total_score": s.get("total_score", 0),
                    "momentum_score": m.get("momentum_score", 0),
                    "combined_score": s.get("total_score", 0) + m.get("momentum_score", 0),
                })
            picks.sort(key=lambda x: x["combined_score"], reverse=True)
            top5 = picks[:5]
            if top5:
                save_double_confirm_to_db(top5, market=market)
                logger.info(f"더블컨펌 [{market}] {len(top5)}개 저장")
            else:
                logger.info(f"더블컨펌 [{market}] 교집합 없음 (조건 미충족)")
        except Exception as e:
            logger.error(f"더블컨펌 스크리닝 오류 [{market}]: {e}")


def update_double_confirm_prices_job():
    """장 마감 후 더블 컨펌 종목 현재가 업데이트"""
    if not DATABASE_URL: return
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT id, ticker, entry_price FROM double_confirm_records WHERE picked_at >= CURRENT_DATE - INTERVAL '3 years'")
        records = cur.fetchall()
        cur.close(); conn.close()
        if not records: return

        today = datetime.now(pytz.timezone("Asia/Seoul")).date()
        prices = {}
        for ticker in list({r[1] for r in records}):
            try:
                hist = yf.Ticker(ticker).history(period="2d")
                if not hist.empty:
                    prices[ticker] = float(hist["Close"].iloc[-1])
            except: pass

        conn = get_conn(); cur = conn.cursor()
        for record_id, ticker, entry_price in records:
            current = prices.get(ticker)
            if current is None: continue
            ret = round((current / entry_price - 1) * 100, 2) if entry_price else 0
            cur.execute("""
                INSERT INTO double_confirm_prices (record_id, ticker, price_date, price, return_pct)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (record_id, price_date) DO UPDATE SET price=EXCLUDED.price, return_pct=EXCLUDED.return_pct
            """, (record_id, ticker, today, current, ret))
        conn.commit(); cur.close(); conn.close()
        logger.info(f"더블컨펌 가격 업데이트 완료: {len(records)}건")
    except Exception as e:
        logger.error(f"더블컨펌 가격 업데이트 오류: {e}")


@app.post("/api/bestpick/save_picks")
def save_bestpick_direct(payload: dict):
    """프론트에서 선정된 top5 종목을 직접 받아 DB 저장 — 화면과 트래커 종목 일치 보장"""
    picks = payload.get("picks", [])
    market = payload.get("market", "nasdaq")
    if not picks:
        return {"error": "종목 데이터가 없습니다"}
    result = save_bestpick_to_db(picks, market=market)
    return {
        "picks": [{"ticker": p.get("ticker"), "name": p.get("name"),
                   "sector": p.get("sector"), "total_score": p.get("total_score"),
                   "price": p.get("price"), "recommendation": p.get("recommendation")}
                  for p in picks],
        **result
    }

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


def _fetch_spy_returns() -> dict:
    """SPY 벤치마크 수익률 조회 — 7d/30d/90d/180d"""
    try:
        spy = yf.Ticker("SPY").history(period="1y")
        if spy.empty or len(spy) < 5:
            return {}
        latest = float(spy["Close"].iloc[-1])
        def ret(days):
            if len(spy) <= days: return None
            base = float(spy["Close"].iloc[-days])
            return round((latest / base - 1) * 100, 2) if base else None
        return {
            "spy_7d":   ret(5),
            "spy_30d":  ret(21),
            "spy_90d":  ret(63),
            "spy_180d": ret(126),
            "spy_current": latest,
        }
    except Exception as e:
        logger.warning(f"SPY 수익률 조회 실패: {e}")
        return {}


@app.get("/api/bestpick/history")
def get_bestpick_history(market: str = "nasdaq", date_from: str = None, date_to: str = None):
    """베스트픽 전체 이력 + 현재까지 수익률 추적 + SPY 벤치마크 비교"""
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
                p7.price        AS price_7d,
                p7.return_pct   AS return_7d,
                p30.price       AS price_30d,
                p30.return_pct  AS return_30d,
                p90.price       AS price_90d,
                p90.return_pct  AS return_90d,
                pl.price        AS price_latest,
                pl.return_pct   AS return_latest,
                pl.price_date::text AS latest_date
            FROM bestpick_records r
            LEFT JOIN bestpick_prices p7
                ON p7.record_id = r.id
                AND p7.price_date = r.picked_at + INTERVAL '7 days'
            LEFT JOIN bestpick_prices p30
                ON p30.record_id = r.id
                AND p30.price_date = r.picked_at + INTERVAL '30 days'
            LEFT JOIN bestpick_prices p90
                ON p90.record_id = r.id
                AND p90.price_date = r.picked_at + INTERVAL '90 days'
            LEFT JOIN LATERAL (
                SELECT price, return_pct, price_date
                FROM bestpick_prices
                WHERE record_id = r.id
                ORDER BY price_date DESC LIMIT 1
            ) pl ON TRUE
            WHERE r.market = %s
              AND r.picked_at >= %s::date
              AND r.picked_at <= %s::date
            ORDER BY r.picked_at DESC, r.total_score DESC
        """, (market,
              date_from or (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"),
              date_to   or datetime.now().strftime("%Y-%m-%d")))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()

        # SPY 벤치마크
        spy_rets = _fetch_spy_returns()

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

        # 픽별 개별 알파 계산 (각 픽의 진입 시점 SPY 대비)
        spy_now = spy_rets.get("spy_current") or 0
        spy_data_ok = spy_now > 0
        per_pick_alphas = []
        for r in rows:
            if r.get("return_latest") is None: continue
            spy_entry = r.get("spy_entry_price")
            if spy_entry and spy_entry > 0 and spy_data_ok:
                spy_ret_since_entry = round((spy_now / spy_entry - 1) * 100, 2)
                per_pick_alphas.append(r["return_latest"] - spy_ret_since_entry)
        if per_pick_alphas:
            alpha = round(sum(per_pick_alphas) / len(per_pick_alphas), 2)
        elif overall_avg is not None and spy_rets.get("spy_30d") is not None:
            alpha = round(overall_avg - spy_rets["spy_30d"], 2)
        else:
            alpha = None
        if not spy_data_ok:
            logger.warning("SPY 현재가 조회 실패 — 알파 계산 불가")

        return {
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "total_records": len(rows),
            "overall_avg_return": overall_avg,
            "overall_win_rate": win_rate,
            "spy_benchmark": spy_rets,
            "spy_data_ok": spy_data_ok,
            "alpha_vs_spy": alpha,
            "history": summary_by_date
        }
    except Exception as e:
        logger.error(f"베스트픽 이력 조회 오류: {e}")
        return {"error": str(e)}


@app.get("/api/double_confirm")
def get_double_confirm(market: str = "nasdaq", sc_min: int = 65, mt_min: int = 60):
    """베스트픽 스크리너 + 모멘텀 스크리너 동시 통과 종목 반환"""
    if not DATABASE_URL:
        return {"error": "DATABASE_URL 없음"}
    try:
        conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        freshness = "AND screened_at > NOW() - INTERVAL '25 hours'"

        # 베스트픽 스크리너 — 조건 이상 종목
        if market in ("nasdaq", "sp500", "kospi", "kosdaq"):
            cur.execute(f"""
                SELECT ticker, name, sector, total_score, classic_score, growth_score, modern_score,
                       recommendation, price, change_pct, ma20_pct, from_52w_high, rsi
                FROM screening_cache
                WHERE market = %s AND total_score >= %s {freshness}
            """, (market, sc_min))
        else:
            cur.execute(f"""
                SELECT ticker, name, sector, total_score, classic_score, growth_score, modern_score,
                       recommendation, price, change_pct, ma20_pct, from_52w_high, rsi
                FROM screening_cache
                WHERE market IN ('nasdaq','sp500') AND total_score >= %s {freshness}
            """, (sc_min,))
        sc_rows = {r["ticker"]: dict(r) for r in cur.fetchall()}

        # 모멘텀 스크리너 — 조건 이상 종목
        if market in ("nasdaq", "sp500", "kospi", "kosdaq"):
            cur.execute(f"""
                SELECT ticker, momentum_score, rs_score, ma_score, vol_score, high52_score,
                       recommendation AS mt_rec, ret_1m, ret_3m, ret_6m, vol_ratio
                FROM momentum_cache
                WHERE market = %s AND momentum_score >= %s {freshness}
            """, (market, mt_min))
        else:
            cur.execute(f"""
                SELECT ticker, momentum_score, rs_score, ma_score, vol_score, high52_score,
                       recommendation AS mt_rec, ret_1m, ret_3m, ret_6m, vol_ratio
                FROM momentum_cache
                WHERE market IN ('nasdaq','sp500') AND momentum_score >= %s {freshness}
            """, (mt_min,))
        mt_rows = {r["ticker"]: dict(r) for r in cur.fetchall()}
        cur.close(); conn.close()

        # 교집합
        common = set(sc_rows.keys()) & set(mt_rows.keys())
        results = []
        for ticker in common:
            sc = sc_rows[ticker]; mt = mt_rows[ticker]
            combined = sc_rows[ticker].copy()
            combined.update({
                "momentum_score": mt["momentum_score"],
                "rs_score":       mt["rs_score"],
                "ma_score":       mt["ma_score"],
                "vol_score":      mt["vol_score"],
                "high52_score":   mt["high52_score"],
                "mt_rec":         mt["mt_rec"],
                "ret_1m":         mt["ret_1m"],
                "ret_3m":         mt["ret_3m"],
                "ret_6m":         mt["ret_6m"],
                "vol_ratio":      mt["vol_ratio"],
                "combined_score": sc["total_score"] + mt["momentum_score"],
            })
            results.append(combined)

        results.sort(key=lambda x: x["combined_score"], reverse=True)
        return sanitize({
            "market": market,
            "sc_min": sc_min,
            "mt_min": mt_min,
            "count": len(results),
            "items": results
        })
    except Exception as e:
        logger.error(f"double_confirm 오류: {e}")
        return {"error": str(e)}


@app.get("/api/double_confirm/history")
def get_double_confirm_history(market: str = "nasdaq"):
    """더블 컨펌 이력 + 수익률 추적 + SPY 벤치마크"""
    if not DATABASE_URL:
        return {"error": "DATABASE_URL 없음"}
    try:
        conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                r.id, r.ticker, r.name, r.sector,
                r.entry_price, r.total_score, r.momentum_score, r.combined_score,
                r.spy_entry_price, r.consecutive_count,
                r.picked_at::text AS picked_at,
                p7.return_pct   AS return_7d,
                p30.return_pct  AS return_30d,
                p90.return_pct  AS return_90d,
                pl.return_pct   AS return_latest,
                pl.price_date::text AS latest_date
            FROM double_confirm_records r
            LEFT JOIN double_confirm_prices p7
                ON p7.record_id = r.id
                AND p7.price_date = r.picked_at + INTERVAL '7 days'
            LEFT JOIN double_confirm_prices p30
                ON p30.record_id = r.id
                AND p30.price_date = r.picked_at + INTERVAL '30 days'
            LEFT JOIN double_confirm_prices p90
                ON p90.record_id = r.id
                AND p90.price_date = r.picked_at + INTERVAL '90 days'
            LEFT JOIN LATERAL (
                SELECT return_pct, price_date FROM double_confirm_prices
                WHERE record_id = r.id ORDER BY price_date DESC LIMIT 1
            ) pl ON TRUE
            WHERE r.picked_at >= CURRENT_DATE - INTERVAL '180 days'
              AND r.market = %s
            ORDER BY r.picked_at DESC, r.combined_score DESC
        """, (market,))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()

        spy_rets = _fetch_spy_returns()

        from collections import defaultdict
        grouped = defaultdict(list)
        for row in rows:
            grouped[row["picked_at"]].append(row)

        summary_by_date = []
        for date, items in sorted(grouped.items(), reverse=True):
            valid = [i["return_latest"] for i in items if i["return_latest"] is not None]
            summary_by_date.append({
                "date": date,
                "count": len(items),
                "avg_return_latest": round(sum(valid) / len(valid), 2) if valid else None,
                "picks": items
            })

        all_returns = [r["return_latest"] for r in rows if r["return_latest"] is not None]
        overall_avg = round(sum(all_returns) / len(all_returns), 2) if all_returns else None
        win_rate = round(sum(1 for r in all_returns if r > 0) / len(all_returns) * 100, 1) if all_returns else None

        spy_now = spy_rets.get("spy_current") or 0
        spy_data_ok = spy_now > 0
        per_pick_alphas = []
        for r in rows:
            if r.get("return_latest") is None: continue
            spy_entry = r.get("spy_entry_price")
            if spy_entry and spy_entry > 0 and spy_data_ok:
                spy_ret = round((spy_now / spy_entry - 1) * 100, 2)
                per_pick_alphas.append(r["return_latest"] - spy_ret)
        if per_pick_alphas:
            alpha = round(sum(per_pick_alphas) / len(per_pick_alphas), 2)
        elif overall_avg is not None and spy_rets.get("spy_30d") is not None:
            alpha = round(overall_avg - spy_rets["spy_30d"], 2)
        else:
            alpha = None

        # 스크리너 데이터 존재 여부 (history 없을 때 원인 안내용)
        screener_status = None
        if not rows:
            try:
                conn2 = get_conn(); cur2 = conn2.cursor()
                freshness = "screened_at > NOW() - INTERVAL '25 hours'"
                cur2.execute(f"SELECT COUNT(*) FROM screening_cache WHERE market=%s AND {freshness}", (market,))
                sc_cnt = cur2.fetchone()[0]
                cur2.execute(f"SELECT COUNT(*) FROM momentum_cache WHERE market=%s AND {freshness}", (market,))
                mt_cnt = cur2.fetchone()[0]
                cur2.close(); conn2.close()
                if sc_cnt == 0 and mt_cnt == 0:
                    screener_status = "스크리닝/모멘텀 데이터 없음 — KST 04:00~04:30 이후 자동 기록"
                elif sc_cnt == 0:
                    screener_status = "베스트픽 스크리너 데이터 없음"
                elif mt_cnt == 0:
                    screener_status = "모멘텀 스크리너 데이터 없음"
                else:
                    screener_status = f"교집합 없음 — 베스트픽 {sc_cnt}종목, 모멘텀 {mt_cnt}종목이지만 동시 통과 없음"
            except: pass

        return {
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "total_records": len(rows),
            "overall_avg_return": overall_avg,
            "overall_win_rate": win_rate,
            "spy_benchmark": spy_rets,
            "spy_data_ok": spy_data_ok,
            "alpha_vs_spy": alpha,
            "screener_status": screener_status,
            "history": summary_by_date
        }
    except Exception as e:
        logger.error(f"더블컨펌 이력 조회 오류: {e}")
        return {"error": str(e)}


@app.post("/api/double_confirm/save")
@limiter.limit("3/minute")
def save_double_confirm_manual(request: Request, background_tasks: BackgroundTasks):
    """수동 더블 컨펌 기록 트리거"""
    background_tasks.add_task(_run_double_confirm_job)
    return {"message": "더블 컨펌 스크리닝 시작됨. 잠시 후 이력을 새로고침하세요."}


# ══════════════════════════════════════════════════════════════
# 자산 관리 (Portfolio) API
# ══════════════════════════════════════════════════════════════

def _get_usd_krw(max_age_minutes=10):
    """USD/KRW 환율 조회 — DB 캐시 우선, 만료 시 yfinance 호출"""
    if not DATABASE_URL:
        return _fetch_usd_krw_live()
    try:
        conn = get_conn()
        cur = conn.cursor()
        try:
            cur.execute(f"""
                SELECT rate FROM fx_cache
                WHERE pair = 'USD_KRW' AND cached_at > NOW() - INTERVAL '{int(max_age_minutes)} minutes'
            """)
            row = cur.fetchone()
            if row and row[0]:
                return float(row[0])
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        logger.warning(f"fx_cache 조회 실패: {e}")
    # 캐시 없거나 만료 → 실시간 조회 후 저장
    rate = _fetch_usd_krw_live()
    try:
        conn = get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO fx_cache (pair, rate, cached_at) VALUES ('USD_KRW', %s, NOW())
                ON CONFLICT (pair) DO UPDATE SET rate=EXCLUDED.rate, cached_at=NOW()
            """, (rate,))
            conn.commit()
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        logger.warning(f"fx_cache 저장 실패: {e}")
    return rate

def _fetch_usd_krw_live():
    """yfinance에서 USD/KRW 조회 (실패 시 1400)"""
    try:
        t = yf.Ticker("KRW=X")
        h = t.history(period="2d")
        if not h.empty:
            return float(h['Close'].iloc[-1])
    except Exception:
        pass
    return 1400.0

def _fetch_live_price(ticker: str, max_age_minutes=10):
    """종목 가격 조회 — DB 캐시 우선, 만료 시 yfinance 호출 + 캐시 저장"""
    if not DATABASE_URL:
        return _fetch_live_price_uncached(ticker)
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(f"""
            SELECT price, name, sector, currency FROM price_cache
            WHERE ticker = %s AND cached_at > NOW() - INTERVAL '{int(max_age_minutes)} minutes'
        """, (ticker,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if row and row[0]:
            return {
                "price": float(row[0]),
                "name": row[1] or ticker,
                "sector": row[2] or '—',
                "currency": row[3] or 'USD'
            }
    except Exception as e:
        logger.warning(f"price_cache 조회 실패({ticker}): {e}")
    # 캐시 없거나 만료 → 실시간 조회 + 저장
    data = _fetch_live_price_uncached(ticker)
    if data.get('price'):
        try:
            conn = get_conn(); cur = conn.cursor()
            cur.execute("""
                INSERT INTO price_cache (ticker, price, name, sector, currency, cached_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (ticker) DO UPDATE SET
                    price=EXCLUDED.price, name=EXCLUDED.name,
                    sector=EXCLUDED.sector, currency=EXCLUDED.currency, cached_at=NOW()
            """, (ticker, data['price'], data['name'], data['sector'], data['currency']))
            conn.commit(); cur.close(); conn.close()
        except Exception as e:
            logger.warning(f"price_cache 저장 실패({ticker}): {e}")
    return data

def _fetch_live_price_uncached(ticker: str):
    """
    단일 종목 실시간 가격 + 섹터 (캐시 우회)
    🇺🇸 미국 종목: marketState에 따라 PRE/POST/REGULAR 가격 우선 사용
    🇰🇷 한국 종목 (.KS/.KQ): yfinance가 시간외 필드 거의 안 주므로 일봉 종가 사용
    """
    try:
        t = yf.Ticker(ticker)
        info = t.info or {}
        is_kr = ticker.endswith('.KS') or ticker.endswith('.KQ')
        market_state = (info.get('marketState') or '').upper()
        price = None

        if not is_kr:
            # 미국 종목: 시간외 가격 우선 (0이나 None이면 폴백)
            if market_state == 'PRE':
                p = info.get('preMarketPrice')
                if p:  # 0과 None 모두 falsy → 폴백으로 넘어감
                    price = p
            elif market_state in ('POST', 'POSTPOST'):
                p = info.get('postMarketPrice')
                if p:
                    price = p
            # 그 외 시간대(REGULAR/CLOSED) 또는 위에서 None일 경우 정규장 가격
            if price is None:
                price = info.get('regularMarketPrice') or info.get('currentPrice')
            try:
                price = float(price) if price else None
            except (TypeError, ValueError):
                price = None

        # 폴백: 위에서 못 구했거나 한국 종목이면 일봉 종가
        if price is None:
            try:
                hist = t.history(period="2d")
                if not hist.empty:
                    price = float(hist['Close'].iloc[-1])
            except Exception:
                pass

        return {
            "price": round(price, 4) if price else None,
            "name":  info.get('longName') or info.get('shortName') or ticker,
            "sector": info.get('sector') or '—',
            "currency": info.get('currency') or ('KRW' if is_kr else 'USD'),
            "market_state": market_state if not is_kr else None,
        }
    except Exception as e:
        logger.warning(f"_fetch_live_price_uncached({ticker}) 실패: {e}")
        return {"price": None, "name": ticker, "sector": "—", "currency": "USD", "market_state": None}


@app.get("/api/portfolio/holdings")
@limiter.limit("60/minute")
def get_holdings(request: Request, account: str = "all"):
    """보유 종목 조회 + 실시간 평가금액 계산"""
    if not DATABASE_URL:
        return {"error": "DATABASE_URL 없음"}
    try:
        conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if account in ("main", "sub"):
            cur.execute("SELECT * FROM holdings WHERE account=%s ORDER BY id DESC", (account,))
        else:
            cur.execute("SELECT * FROM holdings ORDER BY account, id DESC")
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()

        usd_krw = _get_usd_krw()
        tickers = list({r['ticker'] for r in rows})
        live_map = {}
        if tickers:
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
                futures = {ex.submit(_fetch_live_price, t): t for t in tickers}
                for f in concurrent.futures.as_completed(futures, timeout=30):
                    t = futures[f]
                    try:
                        live_map[t] = f.result(timeout=10)
                    except Exception:
                        live_map[t] = {"price": None, "name": t, "sector": "—", "currency": "USD"}

        holdings_out = []
        total_stock_krw = 0
        for r in rows:
            live = live_map.get(r['ticker'], {})
            cur_price = live.get('price')
            currency  = r.get('currency') or live.get('currency') or 'USD'
            qty       = float(r.get('quantity') or 0)
            avg       = float(r.get('avg_price') or 0)
            cur_val_local = (cur_price * qty) if cur_price else 0
            cost_local    = avg * qty
            if cur_price is None:
                pnl_local = None
                pnl_pct   = None
            else:
                pnl_local = cur_val_local - cost_local
                pnl_pct   = (pnl_local / cost_local * 100) if cost_local > 0 else 0
            # KRW 환산
            if currency == 'USD':
                cur_val_krw = cur_val_local * usd_krw
                cost_krw    = cost_local * usd_krw
            else:
                cur_val_krw = cur_val_local
                cost_krw    = cost_local
            total_stock_krw += cur_val_krw
            holdings_out.append({
                "id": r['id'], "account": r['account'],
                "ticker": r['ticker'], "name": r.get('name') or live.get('name') or r['ticker'],
                "sector": r.get('sector') or live.get('sector') or '—',
                "quantity": qty, "avg_price": round(avg, 4),
                "current_price": round(cur_price, 4) if cur_price else None,
                "currency": currency,
                "cost_local": round(cost_local, 2),
                "current_value_local": round(cur_val_local, 2),
                "current_value_krw": round(cur_val_krw, 0),
                "pnl_local": round(pnl_local, 2) if pnl_local is not None else None,
                "pnl_pct": round(pnl_pct, 2) if pnl_pct is not None else None,
                "memo": r.get('memo') or '',
            })

        # 현금 합계
        cur2 = None
        cash_map = {"main": {"cash_krw": 0, "cash_usd": 0}, "sub": {"cash_krw": 0, "cash_usd": 0}}
        try:
            conn = get_conn(); cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur2.execute("SELECT * FROM account_cash")
            for c in cur2.fetchall():
                cash_map[c['account']] = {"cash_krw": int(c['cash_krw'] or 0), "cash_usd": float(c['cash_usd'] or 0)}
            cur2.close(); conn.close()
        except Exception as e:
            logger.warning(f"현금 조회 실패: {e}")

        total_cash_krw = sum(c['cash_krw'] + c['cash_usd'] * usd_krw for c in cash_map.values())
        total_assets_krw = total_stock_krw + total_cash_krw

        # 비중 계산 — 전체 자산(주식+현금) 기준
        for h in holdings_out:
            h['weight_pct'] = round(h['current_value_krw'] / total_assets_krw * 100, 2) if total_assets_krw > 0 else 0

        summary = {
            "total_stock_krw": round(total_stock_krw, 0),
            "total_cash_krw":  round(total_cash_krw,  0),
            "total_assets_krw":round(total_assets_krw,0),
            "stock_pct": round(total_stock_krw / total_assets_krw * 100, 2) if total_assets_krw > 0 else 0,
            "cash_pct":  round(total_cash_krw  / total_assets_krw * 100, 2) if total_assets_krw > 0 else 0,
        }

        # 스냅샷 자동 백필 — 빈 평일 보간 (하루 1회만 실행, 사용자 무인지)
        # account="all" 호출일 때만 트리거 (개별 계정 조회 시엔 스킵)
        if account == "all" and total_assets_krw > 0:
            try:
                _backfill_snapshots(summary, usd_krw)
            except Exception as e:
                logger.warning(f"백필 스킵: {e}")

        return {
            "holdings": holdings_out,
            "cash": cash_map,
            "summary": summary,
            "usd_krw": round(usd_krw, 2),
            "updated_at": datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M"),
        }
    except Exception as e:
        logger.error(f"holdings GET 오류: {e}")
        return {"error": str(e)}


@app.post("/api/portfolio/holdings")
@limiter.limit("30/minute")
async def add_holding(request: Request):
    """보유 종목 추가"""
    try:
        payload = await request.json()
        ticker = (payload.get('ticker') or '').strip().upper()
        if not ticker:
            return {"ok": False, "error": "ticker 필수"}
        account  = payload.get('account', 'main')
        quantity = float(payload.get('quantity') or 0)
        avg      = float(payload.get('avg_price') or 0)
        if quantity <= 0 or avg <= 0:
            return {"ok": False, "error": "수량/평균단가는 양수여야 합니다"}

        # 종목 정보 자동 조회 — 통화는 실제 종목 기반으로 결정 (프론트 입력 무시)
        live = _fetch_live_price(ticker)
        name     = payload.get('name') or live.get('name') or ticker
        # 섹터: 프론트에서 명시적으로 넘어온 값 > 자동 추론
        manual_sector = payload.get('sector')
        if manual_sector and manual_sector not in ("—", "Unknown", ""):
            sector = manual_sector
            sector_method = "manual"
        else:
            # live에는 캐시된 값이 있을 수 있으므로 원본 info로 재추론
            try:
                raw_info = yf.Ticker(ticker).info or {}
            except Exception:
                raw_info = {}
            resolved = resolve_sector(ticker, raw_info)
            if resolved["needs_manual"]:
                # 추론 실패 — 프론트에 수동 입력 요청 신호 반환 (저장 안 함)
                return {
                    "ok": False,
                    "needs_manual_sector": True,
                    "ticker": ticker,
                    "name": name,
                    "long_name": raw_info.get("longName") or name,
                    "message": "ETF 섹터 자동 추론 실패 — 수동 선택이 필요합니다"
                }
            sector = resolved["sector"]
            sector_method = resolved["method"]
        # 티커 규칙: .KS/.KQ = KRW, 그 외 = USD (yfinance info 우선)
        if ticker.endswith('.KS') or ticker.endswith('.KQ'):
            currency = 'KRW'
        elif live.get('currency'):
            currency = live['currency']
        else:
            currency = 'USD'
        memo     = payload.get('memo') or ''

        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO holdings (account, ticker, name, sector, quantity, avg_price, currency, memo)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (account, ticker, name, sector, quantity, avg, currency, memo))
        new_id = cur.fetchone()[0]
        conn.commit(); cur.close(); conn.close()
        # 보유 종목 추가 → 일정 페이지 즉시 반영 위해 실적 캐시 무효화
        _earnings_cache.clear()
        return {"ok": True, "id": new_id, "currency": currency, "name": name,
                "sector": sector, "sector_method": sector_method}
    except Exception as e:
        logger.error(f"holdings POST 오류: {e}")
        return {"ok": False, "error": str(e)}


@app.put("/api/portfolio/holdings/{holding_id}")
@limiter.limit("30/minute")
async def update_holding(holding_id: int, request: Request):
    """보유 종목 수정 — sector 필드도 수정 가능"""
    try:
        payload = await request.json()
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            UPDATE holdings SET
                account    = COALESCE(%s, account),
                quantity   = COALESCE(%s, quantity),
                avg_price  = COALESCE(%s, avg_price),
                sector     = COALESCE(%s, sector),
                memo       = COALESCE(%s, memo),
                updated_at = NOW()
            WHERE id = %s
        """, (payload.get('account'),
              payload.get('quantity'),
              payload.get('avg_price'),
              payload.get('sector'),
              payload.get('memo'),
              holding_id))
        conn.commit(); cur.close(); conn.close()
        # 보유 종목 수정 → 캐시 무효화
        _earnings_cache.clear()
        return {"ok": True}
    except Exception as e:
        logger.error(f"holdings PUT 오류: {e}")
        return {"ok": False, "error": str(e)}


@app.delete("/api/portfolio/holdings/{holding_id}")
@limiter.limit("30/minute")
def delete_holding(holding_id: int, request: Request):
    """보유 종목 삭제"""
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("DELETE FROM holdings WHERE id = %s", (holding_id,))
        conn.commit(); cur.close(); conn.close()
        # 보유 종목 삭제 → 캐시 무효화
        _earnings_cache.clear()
        return {"ok": True}
    except Exception as e:
        logger.error(f"holdings DELETE 오류: {e}")
        return {"ok": False, "error": str(e)}


@app.post("/api/portfolio/fix_currencies")
@limiter.limit("5/minute")
def fix_holding_currencies(request: Request):
    """기존 잘못 저장된 통화 일괄 수정 — 티커 기반으로 USD/KRW 재분류"""
    if not DATABASE_URL:
        return {"ok": False, "error": "DATABASE_URL 없음"}
    try:
        conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT id, ticker, currency FROM holdings")
        rows = [dict(r) for r in cur.fetchall()]

        fixed = 0
        for r in rows:
            ticker = r['ticker']
            # 티커 기반 실제 통화 판별
            if ticker.endswith('.KS') or ticker.endswith('.KQ'):
                correct = 'KRW'
            else:
                correct = 'USD'
            if r['currency'] != correct:
                cur.execute("UPDATE holdings SET currency=%s WHERE id=%s", (correct, r['id']))
                fixed += 1
        conn.commit(); cur.close(); conn.close()
        return {"ok": True, "fixed": fixed, "total": len(rows)}
    except Exception as e:
        logger.error(f"fix_currencies 오류: {e}")
        return {"ok": False, "error": str(e)}


@app.post("/api/portfolio/infer_sector")
@limiter.limit("30/minute")
async def api_infer_sector(request: Request):
    """
    종목 추가 전 섹터 사전 추론 — 프론트에서 추가 모달/흐름 분기용
    입력: {"ticker": "TQQQ"}
    반환: {ok, ticker, name, sector, method, needs_manual}
    """
    try:
        payload = await request.json()
        ticker = (payload.get('ticker') or '').strip().upper()
        if not ticker:
            return {"ok": False, "error": "ticker 필수"}
        try:
            info = yf.Ticker(ticker).info or {}
        except Exception as e:
            logger.warning(f"infer_sector info 실패 {ticker}: {e}")
            info = {}
        resolved = resolve_sector(ticker, info)
        name = info.get('longName') or info.get('shortName') or ticker
        return {
            "ok": True,
            "ticker": ticker,
            "name": name,
            "sector": resolved["sector"],
            "method": resolved["method"],
            "needs_manual": resolved["needs_manual"],
        }
    except Exception as e:
        logger.error(f"infer_sector 오류: {e}")
        return {"ok": False, "error": str(e)}


@app.post("/api/portfolio/reclassify_sectors")
@limiter.limit("3/minute")
def reclassify_all_sectors(request: Request):
    """
    저장된 모든 보유 종목 섹터 일괄 재분류
    기존 섹터가 '—' / 'Unknown' / NULL인 종목만 재분류 (수동 설정한 건 보존)
    반환: {ok, total, reclassified, still_unknown, details}
    """
    if not DATABASE_URL:
        return {"ok": False, "error": "DATABASE_URL 없음"}
    try:
        conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT id, ticker, name, sector FROM holdings")
        rows = [dict(r) for r in cur.fetchall()]

        reclassified = 0
        still_unknown = []
        details = []
        for r in rows:
            ticker = r['ticker']
            cur_sector = (r.get('sector') or '').strip()
            # 이미 유효한 섹터가 있으면 스킵 (수동 설정 보존)
            if cur_sector and cur_sector not in ('—', 'Unknown', ''):
                continue
            # yfinance info 조회 + 추론
            try:
                info = yf.Ticker(ticker).info or {}
            except Exception:
                info = {}
            resolved = resolve_sector(ticker, info)
            if resolved["needs_manual"]:
                still_unknown.append({"id": r['id'], "ticker": ticker, "name": r.get('name') or ticker})
                continue
            new_sector = resolved["sector"]
            cur.execute("UPDATE holdings SET sector=%s, updated_at=NOW() WHERE id=%s",
                        (new_sector, r['id']))
            reclassified += 1
            details.append({"ticker": ticker, "sector": new_sector, "method": resolved["method"]})
        conn.commit(); cur.close(); conn.close()
        return {
            "ok": True,
            "total": len(rows),
            "reclassified": reclassified,
            "still_unknown": still_unknown,
            "details": details,
        }
    except Exception as e:
        logger.error(f"reclassify_sectors 오류: {e}")
        return {"ok": False, "error": str(e)}


@app.get("/api/calendar/upcoming")
@limiter.limit("30/minute")
def calendar_upcoming(request: Request, days: int = 30):
    """
    향후 N일 주요 일정 — 매크로 + 실적 (Mag7 + 보유 종목)
    🇰🇷 모든 날짜/시간을 KST(한국 시간) 기준으로 변환해서 반환
    days: 조회 기간 (기본 30일, 최대 90일)
    반환: {ok, today, events: [{date(KST), time(KST 또는 BMO/AMC), category, ..., days_until}]}
    """
    try:
        days = max(1, min(int(days or 30), 90))
        kst_tz = pytz.timezone("Asia/Seoul")
        et_tz  = pytz.timezone("America/New_York")
        today_kst = datetime.now(kst_tz).date()
        # 조회 윈도우: ET 기준으로도 약간 여유롭게 (양 끝 ±1일 이벤트는 KST 변환 후 윈도우 내일 수 있음)
        et_start = today_kst - timedelta(days=2)
        et_cutoff = today_kst + timedelta(days=days + 1)

        events = []

        def _et_to_kst(date_str: str, time_str: str):
            """
            ET 날짜+시간을 KST로 변환.
            time_str이 빈 문자열이면 None 시각 (날짜만 변환, 자정 기준)
            반환: (kst_date_str 'YYYY-MM-DD', kst_time_str 'HH:MM' or '')
            """
            try:
                if time_str and ":" in time_str:
                    hh, mm = [int(x) for x in time_str.split(":")[:2]]
                    et_naive = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=hh, minute=mm)
                else:
                    et_naive = datetime.strptime(date_str, "%Y-%m-%d")
                et_aware = et_tz.localize(et_naive)
                kst = et_aware.astimezone(kst_tz)
                if time_str:
                    return kst.strftime("%Y-%m-%d"), kst.strftime("%H:%M")
                else:
                    return kst.strftime("%Y-%m-%d"), ""
            except Exception:
                return date_str, time_str

        # 1) 매크로 이벤트 — ET → KST 변환
        for et_date_str, et_time_str, category, title, importance in MACRO_EVENTS:
            try:
                et_d = datetime.strptime(et_date_str, "%Y-%m-%d").date()
            except Exception:
                continue
            # 윈도우 사전 필터링 (ET 기준 ±1일 여유)
            if et_d < et_start or et_d > et_cutoff:
                continue

            kst_date, kst_time = _et_to_kst(et_date_str, et_time_str)
            try:
                kd = datetime.strptime(kst_date, "%Y-%m-%d").date()
            except Exception:
                continue
            # KST 기준 윈도우 필터
            if kd < today_kst or kd > today_kst + timedelta(days=days):
                continue

            style = EVENT_STYLE.get(category, {"emoji":"📌","color":"#64748b"})
            events.append({
                "date":       kst_date,
                "time":       kst_time,
                "category":   category,
                "title":      title,
                "ticker":     None,
                "importance": importance,
                "emoji":      style["emoji"],
                "color":      style["color"],
                "days_until": (kd - today_kst).days,
            })

        # 2) 실적 일정 — Mag 7 + 보유 종목
        tickers_to_check = set(MAG7_TICKERS)
        try:
            if DATABASE_URL:
                conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT DISTINCT ticker FROM holdings WHERE currency='USD'")
                for r in cur.fetchall():
                    tk = (r['ticker'] or '').strip().upper()
                    if tk in ETF_SECTOR_MAP:
                        continue
                    tickers_to_check.add(tk)
                cur.close(); conn.close()
        except Exception as e:
            logger.warning(f"calendar_upcoming holdings 조회 실패: {e}")

        # 병렬로 실적 일정 조회 (캐시 활용)
        import concurrent.futures as _cf
        ticker_list = list(tickers_to_check)
        earnings_results = {}
        if ticker_list:
            with _cf.ThreadPoolExecutor(max_workers=8) as ex:
                fut_map = {ex.submit(_fetch_earnings_date, tk): tk for tk in ticker_list}
                for fut in _cf.as_completed(fut_map, timeout=20):
                    tk = fut_map[fut]
                    try:
                        earnings_results[tk] = fut.result()
                    except Exception:
                        earnings_results[tk] = None

        # 실적 이벤트 추가 — yfinance가 ET 기준 날짜 반환하므로 KST 변환
        style_e = EVENT_STYLE["Earnings"]
        for tk, data in earnings_results.items():
            if not data:
                continue
            et_date = data.get("date")
            try:
                _ = datetime.strptime(et_date, "%Y-%m-%d").date()
            except Exception:
                continue

            when = data.get("when") or "TBD"
            when_label = {"BMO":"장전","AMC":"장후","TBD":"미정"}.get(when, when)

            # 실적은 시간 미정(BMO/AMC)이지만 대략 위치를 KST로 환산해서 날짜 결정
            # BMO=ET 7:00 (장전, 일반적), AMC=ET 16:30 (장후), TBD=ET 09:30(시초가 가정)
            proxy_time = {"BMO":"07:00","AMC":"16:30","TBD":"09:30"}.get(when, "09:30")
            kst_date, _kst_time = _et_to_kst(et_date, proxy_time)
            try:
                kd = datetime.strptime(kst_date, "%Y-%m-%d").date()
            except Exception:
                continue
            if kd < today_kst or kd > today_kst + timedelta(days=days):
                continue

            is_mag7 = tk in MAG7_TICKERS
            events.append({
                "date":       kst_date,
                "time":       when,                # BMO/AMC/TBD (시각이 아닌 라벨 코드)
                "time_label": when_label,          # 장전/장후/미정
                "category":   "Earnings",
                "title":      f"{tk} 실적 ({when_label})",
                "ticker":     tk,
                "importance": 5 if is_mag7 else 3,
                "emoji":      "💎" if is_mag7 else "📊",
                "color":      style_e["color"],
                "days_until": (kd - today_kst).days,
                "is_mag7":    is_mag7,
            })

        # 정렬: 날짜 → 시간 (시각 빈 문자열이면 마지막)
        def _sort_key(ev):
            return (ev["date"], ev.get("time") or "99:99")
        events.sort(key=_sort_key)

        return {
            "ok": True,
            "today": today_kst.strftime("%Y-%m-%d"),
            "tz": "KST",
            "days": days,
            "count": len(events),
            "events": events,
        }
    except Exception as e:
        logger.error(f"calendar_upcoming 오류: {e}")
        return {"ok": False, "error": str(e), "events": []}


@app.post("/api/calendar/refresh_earnings")
@limiter.limit("3/minute")
def calendar_refresh_earnings(request: Request):
    """실적 일정 캐시 강제 갱신 (수동 새로고침용)"""
    try:
        _earnings_cache.clear()
        return {"ok": True, "message": "실적 일정 캐시 초기화 완료"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/portfolio/cash")
@limiter.limit("30/minute")
async def update_cash(request: Request):
    """계좌별 현금 잔고 업데이트"""
    try:
        payload = await request.json()
        account  = payload.get('account', 'main')
        cash_krw = int(payload.get('cash_krw') or 0)
        cash_usd = float(payload.get('cash_usd') or 0)
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO account_cash (account, cash_krw, cash_usd, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (account) DO UPDATE SET
                cash_krw = EXCLUDED.cash_krw,
                cash_usd = EXCLUDED.cash_usd,
                updated_at = NOW()
        """, (account, cash_krw, cash_usd))
        conn.commit(); cur.close(); conn.close()
        return {"ok": True}
    except Exception as e:
        logger.error(f"cash POST 오류: {e}")
        return {"ok": False, "error": str(e)}


@app.get("/api/portfolio/snapshots")
@limiter.limit("60/minute")
def get_snapshots(request: Request, months: int = 12, start_date: str = None, end_date: str = None):
    """자산 스냅샷 조회
    - start_date / end_date 지정 시 해당 기간만 조회 (YYYY-MM-DD)
    - 둘 다 없으면 months 기반 최근 N개월
    """
    if not DATABASE_URL:
        return {"error": "DATABASE_URL 없음"}
    try:
        conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if start_date and end_date:
            cur.execute("""
                SELECT * FROM portfolio_snapshots
                WHERE snapshot_date BETWEEN %s AND %s
                ORDER BY snapshot_date ASC
            """, (start_date, end_date))
        elif start_date:
            cur.execute("""
                SELECT * FROM portfolio_snapshots
                WHERE snapshot_date >= %s
                ORDER BY snapshot_date ASC
            """, (start_date,))
        else:
            cur.execute("""
                SELECT * FROM portfolio_snapshots
                WHERE snapshot_date > CURRENT_DATE - (%s * INTERVAL '1 month')
                ORDER BY snapshot_date ASC
            """, (int(months),))
        rows = cur.fetchall(); cur.close(); conn.close()
        out = []
        for r in rows:
            d = dict(r)
            d['snapshot_date'] = d['snapshot_date'].strftime("%Y-%m-%d")
            if 'created_at' in d and d['created_at']:
                d['created_at'] = d['created_at'].strftime("%Y-%m-%d %H:%M")
            out.append(d)
        return {"snapshots": out}
    except Exception as e:
        logger.error(f"snapshots GET 오류: {e}")
        return {"error": str(e)}


@app.post("/api/portfolio/snapshot")
@limiter.limit("10/minute")
def take_snapshot(request: Request):
    """현재 자산 상태를 오늘 날짜로 스냅샷 저장 (수동 or 월말 자동)"""
    if not DATABASE_URL:
        return {"ok": False, "error": "DATABASE_URL 없음"}
    try:
        # 현재 holdings 값 집계
        data = get_holdings(request, account="all")
        if data.get('error'):
            return {"ok": False, "error": data['error']}
        summary = data['summary']
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO portfolio_snapshots (snapshot_date, total_stock, total_cash, total_assets, usd_krw)
            VALUES (CURRENT_DATE, %s, %s, %s, %s)
            ON CONFLICT (snapshot_date) DO UPDATE SET
                total_stock  = EXCLUDED.total_stock,
                total_cash   = EXCLUDED.total_cash,
                total_assets = EXCLUDED.total_assets,
                usd_krw      = EXCLUDED.usd_krw
        """, (summary['total_stock_krw'], summary['total_cash_krw'], summary['total_assets_krw'], data['usd_krw']))
        conn.commit(); cur.close(); conn.close()
        return {"ok": True, "summary": summary}
    except Exception as e:
        logger.error(f"snapshot POST 오류: {e}")
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════
# 리밸런싱 자동 알림 (서버 스케줄 — 화면 안 열어도 매일 점검)
# ══════════════════════════════════════════════════════════════
def _rb_mock_request():
    """get_holdings를 서버 내부에서 호출하기 위한 가짜 Request (slowapi 통과)"""
    from starlette.requests import Request as _SReq
    scope = {"type": "http", "method": "GET", "path": "/", "raw_path": b"/",
             "query_string": b"", "headers": [], "client": ("127.0.0.1", 0),
             "server": ("localhost", 80), "scheme": "http", "app": app}
    return _SReq(scope)

def run_rebalance_check_job(force=False):
    """매일 KST 08:00 — 포트폴리오 비율 자동 점검 → 목표 이탈 시 텔레그램 (하루 1회).
    force=True면 하루 1회 제한 무시 (수동 테스트용)."""
    if not DATABASE_URL:
        return
    kst = pytz.timezone("Asia/Seoul")
    try:
        # 1) 설정 + 오늘 중복 체크
        conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT target, trigger_pct, last_alert FROM rb_settings WHERE id=1")
        s = cur.fetchone(); cur.close(); conn.close()
        s = dict(s) if s else {}
        target  = s.get("target") or 70
        trigger = s.get("trigger_pct") or 5
        last    = s.get("last_alert")
        today   = datetime.now(kst).date()
        if not force and last and last == today:
            logger.info("[리밸런싱] 오늘 이미 알림 발송 — 스킵")
            return

        # 2) 포트폴리오 비율 (현재가 캐시 반영)
        port = get_holdings(_rb_mock_request(), account="all")
        if not isinstance(port, dict) or port.get("error"):
            logger.warning(f"[리밸런싱] 포트폴리오 조회 실패: {port}")
            return
        summ  = port.get("summary", {}) or {}
        total = summ.get("total_assets_krw") or 0
        stock = summ.get("total_stock_krw") or 0
        cash  = summ.get("total_cash_krw") or 0
        if total <= 0:
            logger.info("[리밸런싱] 총자산 0 — 스킵")
            return

        cur_pct = round(stock / total * 100, 1)
        gap = round(cur_pct - target, 1)  # +면 주식 초과, -면 부족
        if abs(gap) < trigger:
            logger.info(f"[리밸런싱] 정상 범위 (이탈 {gap:+.1f}%, 기준 ±{trigger}%) — 알림 안 함")
            return

        # 3) 조정 금액 + 메시지
        amt = abs(stock - total * target / 100)
        def fmt_won(n):
            n = abs(n)
            if n >= 1e8:
                uk = int(n // 1e8); man = int((n % 1e8) // 1e4)
                return f"{uk}억 {man:,}만원" if man else f"{uk}억원"
            if n >= 1e4:
                return f"{int(n // 1e4):,}만원"
            return f"{int(n):,}원"
        now_str = datetime.now(kst).strftime("%Y-%m-%d %H:%M")
        direction = "📉 주식 매도 → 현금 확보" if gap > 0 else "📈 현금 → 주식 매수"
        msg = (f"⚖️ <b>리밸런싱 알림</b>\n📅 {now_str}\n\n"
               f"💼 총 자산: {fmt_won(total)}\n"
               f"📊 현재 주식: {fmt_won(stock)} ({cur_pct}%)\n"
               f"💵 현재 현금: {fmt_won(cash)}\n\n"
               f"🎯 목표 {target}% vs 현재 {cur_pct}% (<b>{gap:+.1f}% 이탈</b>)\n"
               f"{direction}\n💡 조정 필요 금액: <b>{fmt_won(amt)}</b>")
        send_telegram(msg)

        # 4) last_alert 갱신 (하루 1회 보장)
        conn = get_conn(); cur = conn.cursor()
        cur.execute("UPDATE rb_settings SET last_alert=%s WHERE id=1", (today,))
        conn.commit(); cur.close(); conn.close()
        logger.info(f"[리밸런싱] 알림 발송 완료 (이탈 {gap:+.1f}%)")
    except Exception as e:
        logger.error(f"[리밸런싱] 자동 체크 오류: {e}")


# ══════════════════════════════════════════════════════════════
# APScheduler — 매일 KST 04:00 자동 실행
# ══════════════════════════════════════════════════════════════

scheduler = BackgroundScheduler(timezone=pytz.utc)
# 한국 스크리닝 — KST 01:00 (UTC 16:00). 미국과 3시간 분리해 yfinance rate limit 회피
scheduler.add_job(
    run_kr_screening_job,
    CronTrigger(hour=16, minute=0, timezone=pytz.utc),  # UTC 16:00 = KST 01:00
    id="daily_screening_kr",
    replace_existing=True,
    misfire_grace_time=3600
)
# 미국 스크리닝 + 텐배거 + 정리 + 알림 — KST 04:00 (UTC 19:00)
scheduler.add_job(
    run_us_screening_job,
    CronTrigger(hour=19, minute=0, timezone=pytz.utc),  # UTC 19:00 = KST 04:00
    id="daily_screening_us",
    replace_existing=True,
    misfire_grace_time=3600
)
# 리밸런싱 자동 점검 — KST 08:00 (UTC 23:00, 미국 장 마감 후)
scheduler.add_job(
    run_rebalance_check_job,
    CronTrigger(hour=23, minute=0, timezone=pytz.utc),  # UTC 23:00 = KST 08:00
    id="daily_rebalance_check",
    replace_existing=True,
    misfire_grace_time=3600
)
scheduler.add_job(
    _run_momentum_job,
    CronTrigger(hour=20, minute=30, timezone=pytz.utc),  # UTC 20:30 = KST 05:30 (미국 스크리닝 04:00 충분히 후)
    id="momentum_screening",
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
# 매일 KST 09:00 (UTC 00:00) — FINRA Short Volume 수집 (ET 18:00 게시 후 충분한 여유)
scheduler.add_job(
    _run_short_volume_job,
    CronTrigger(hour=0, minute=0, timezone=pytz.utc),
    id="daily_short_volume",
    replace_existing=True,
    misfire_grace_time=3600
)
# 매일 KST 10:00 (UTC 01:00) — ARK 보유/매매 수집
scheduler.add_job(
    _run_ark_job,
    CronTrigger(hour=1, minute=0, timezone=pytz.utc),
    id="daily_ark",
    replace_existing=True,
    misfire_grace_time=3600
)
# 매일 KST 06:30 (UTC 21:30) — 더블 컨펌 자동 기록 (스크리닝 04:00 + 모멘텀 05:30 완료 후)
scheduler.add_job(
    _run_double_confirm_job,
    CronTrigger(hour=21, minute=30, timezone=pytz.utc),
    id="double_confirm_screening",
    replace_existing=True,
    misfire_grace_time=3600
)
# 매일 KST 08:30 (UTC 23:30) — 더블 컨펌 가격 업데이트
scheduler.add_job(
    update_double_confirm_prices_job,
    CronTrigger(hour=23, minute=30, timezone=pytz.utc),
    id="daily_dc_price",
    replace_existing=True,
    misfire_grace_time=3600
)


# ══════════════════════════════════════════════════════════════
# 스냅샷 백필 — 사용자 접속 시 자동으로 빈 날짜 채움
# 옵션 2: 마지막 스냅샷 이후 빠진 평일을 현재 가치로 보간
# ══════════════════════════════════════════════════════════════

# 일일 1회만 실행되도록 메모리 캐시 (KST 날짜 문자열 저장)
_backfill_last_run = {"date": None}

def _backfill_snapshots(summary, usd_krw):
    """
    빈 날짜 자동 백필 (옵션 2 — 보간)
    - DB에서 마지막 스냅샷 날짜 조회
    - 그 다음날 ~ 오늘 사이의 평일 중 누락된 날짜를 현재 자산 가치로 채움
    - 최대 30일까지만 백필 (그 이상 누락은 보간 무의미)
    - 하루 1회만 실행 (반복 호출 시 캐시로 차단)
    """
    if not DATABASE_URL:
        return
    try:
        kst_today = datetime.now(pytz.timezone("Asia/Seoul")).date()
        # 같은 날 이미 백필했으면 스킵
        if _backfill_last_run["date"] == kst_today.isoformat():
            return

        conn = get_conn(); cur = conn.cursor()
        # 마지막 스냅샷 날짜 조회
        cur.execute("SELECT MAX(snapshot_date) FROM portfolio_snapshots")
        row = cur.fetchone()
        last_date = row[0] if row else None

        # 채울 날짜 목록 결정
        if last_date is None:
            # DB가 비어있으면 백필 안 함 (사용자가 처음 기록 버튼을 누르도록)
            cur.close(); conn.close()
            _backfill_last_run["date"] = kst_today.isoformat()
            return

        # 마지막 스냅샷 다음날부터 오늘까지 (오늘 포함)
        start = last_date + timedelta(days=1)
        end   = kst_today
        if start > end:
            # 이미 오늘 또는 미래 데이터까지 있으면 백필 불필요
            cur.close(); conn.close()
            _backfill_last_run["date"] = kst_today.isoformat()
            return

        # 30일 초과 갭이면 마지막 30일분만 채움 (방치된 계정 보호)
        gap_days = (end - start).days + 1
        if gap_days > 30:
            start = end - timedelta(days=29)
            logger.info(f"백필 범위 30일로 제한 (실제 갭: {gap_days}일)")

        # 평일(월~금)만 채움
        dates_to_fill = []
        d = start
        while d <= end:
            if d.weekday() < 5:  # 0=월 ~ 4=금
                dates_to_fill.append(d)
            d += timedelta(days=1)

        if not dates_to_fill:
            cur.close(); conn.close()
            _backfill_last_run["date"] = kst_today.isoformat()
            return

        # 일괄 INSERT (오늘 가치 = 보간값으로 사용)
        # ON CONFLICT DO NOTHING — 사용자가 해당 날짜에 수동 기록한 게 있으면 보존
        filled = 0
        for d in dates_to_fill:
            cur.execute("""
                INSERT INTO portfolio_snapshots
                    (snapshot_date, total_stock, total_cash, total_assets, usd_krw)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (snapshot_date) DO NOTHING
            """, (d, summary['total_stock_krw'], summary['total_cash_krw'],
                  summary['total_assets_krw'], usd_krw))
            if cur.rowcount:
                filled += 1
        conn.commit(); cur.close(); conn.close()

        if filled:
            logger.info(f"스냅샷 백필 완료: {filled}일치 ({dates_to_fill[0]} ~ {dates_to_fill[-1]})")
        _backfill_last_run["date"] = kst_today.isoformat()
    except Exception as e:
        logger.error(f"스냅샷 백필 오류: {e}")


# ── 가격 사전 갱신 (평일 KST 07:00~24:00, 10분마다) ────────────────
def _prefetch_prices_job():
    """보유 종목들의 가격을 백그라운드에서 사전 갱신 → 사용자 접속 시 즉시 응답"""
    try:
        if not DATABASE_URL:
            return
        # 주말에는 실행 안 함 (금요일 종가 고정이므로 불필요)
        kst_now = datetime.now(pytz.timezone("Asia/Seoul"))
        if kst_now.weekday() >= 5:  # 토(5) / 일(6)
            return

        # 보유 종목 티커 목록 조회
        conn = get_conn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT DISTINCT ticker FROM holdings")
            tickers = [r[0] for r in cur.fetchall()]
        finally:
            cur.close()
            conn.close()
        if not tickers:
            return

        # 병렬로 가격 갱신 (캐시 우회하여 실시간 값 가져옴 → DB 저장)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(_fetch_live_price_uncached, t): t for t in tickers}
            results = {}
            for f in concurrent.futures.as_completed(futures, timeout=60):
                tk = futures[f]
                try:
                    results[tk] = f.result(timeout=10)
                except Exception:
                    pass

        # DB 일괄 저장
        conn = get_conn()
        cur = conn.cursor()
        try:
            saved = 0
            for tk, data in results.items():
                if data.get('price'):
                    cur.execute("""
                        INSERT INTO price_cache (ticker, price, name, sector, currency, cached_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (ticker) DO UPDATE SET
                            price=EXCLUDED.price, name=EXCLUDED.name,
                            sector=EXCLUDED.sector, currency=EXCLUDED.currency, cached_at=NOW()
                    """, (tk, data['price'], data['name'], data['sector'], data['currency']))
                    saved += 1

            # 환율도 함께 갱신
            rate = _fetch_usd_krw_live()
            cur.execute("""
                INSERT INTO fx_cache (pair, rate, cached_at) VALUES ('USD_KRW', %s, NOW())
                ON CONFLICT (pair) DO UPDATE SET rate=EXCLUDED.rate, cached_at=NOW()
            """, (rate,))

            conn.commit()
            logger.info(f"가격 사전 갱신 완료: {saved}개 종목, 환율 ₩{rate:.0f}")
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        logger.error(f"가격 사전 갱신 오류: {e}")

# 평일 KST 07:00 ~ 23:59, 10분마다 실행 (주말 제외)
scheduler.add_job(
    _prefetch_prices_job,
    CronTrigger(
        day_of_week='mon-fri',
        hour='7-23',
        minute='*/10',
        timezone=pytz.timezone("Asia/Seoul")
    ),
    id="prefetch_prices",
    replace_existing=True,
    misfire_grace_time=300
)


@app.get("/api/rb/settings")
def get_rb_settings():
    """리밸런싱 설정값 불러오기"""
    try:
        conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM rb_settings WHERE id = 1")
        row = cur.fetchone(); cur.close(); conn.close()
        if row:
            return {"ok": True, "data": dict(row)}
        return {"ok": True, "data": {}}
    except Exception as e:
        logger.error(f"rb_settings GET 오류: {e}")
        return {"ok": False, "error": str(e)}

@app.post("/api/rb/settings")
@limiter.limit("30/minute")
async def save_rb_settings(request: Request):
    """리밸런싱 설정값 저장"""
    try:
        payload = await request.json()
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            UPDATE rb_settings SET
                total          = %(total)s,
                stock          = %(stock)s,
                target         = %(target)s,
                trigger_pct    = %(trigger_pct)s,
                rb_mode        = %(rb_mode)s,
                aggressive_add = %(aggressive_add)s,
                updated_at     = NOW()
            WHERE id = 1
        """, {
            "total":          payload.get("total"),
            "stock":          payload.get("stock"),
            "target":         payload.get("target"),
            "trigger_pct":    payload.get("trigger_pct", 5),
            "rb_mode":        payload.get("rb_mode", "conservative"),
            "aggressive_add": payload.get("aggressive_add", 10),
        })
        conn.commit(); cur.close(); conn.close()
        return {"ok": True}
    except Exception as e:
        logger.error(f"rb_settings POST 오류: {e}")
        return {"ok": False, "error": str(e)}

@app.post("/api/rb/check_now")
@limiter.limit("5/minute")
def rb_check_now(request: Request, background_tasks: BackgroundTasks):
    """[테스트] 리밸런싱 자동 점검 즉시 실행 (force — 하루 1회 제한 무시).
    목표 이탈 시에만 텔레그램 발송."""
    background_tasks.add_task(run_rebalance_check_job, True)
    return {"message": "리밸런싱 점검 실행됨. 목표 이탈 시 텔레그램 발송됩니다 (정상 범위면 발송 안 함)."}

@app.get("/api/rb/last_alert")
def get_rb_last_alert():
    """오늘 알림 발송 여부 확인"""
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT last_alert FROM rb_settings WHERE id = 1")
        row = cur.fetchone(); cur.close(); conn.close()
        last = str(row[0]) if row and row[0] else None
        return {"ok": True, "last_alert": last}
    except Exception as e:
        return {"ok": False, "last_alert": None}

@app.post("/api/rb/last_alert")
def set_rb_last_alert():
    """오늘 날짜로 알림 발송 기록"""
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("UPDATE rb_settings SET last_alert = CURRENT_DATE WHERE id = 1")
        conn.commit(); cur.close(); conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/rb/logs")
def get_rb_logs():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT id, log_date, log_text FROM rb_logs ORDER BY id DESC LIMIT 50")
        rows = cur.fetchall()
        cur.close(); conn.close()
        return {"ok": True, "data": [{"id": r[0], "date": r[1], "text": r[2]} for r in rows]}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/api/rb/logs")
async def add_rb_log(request: Request):
    try:
        body = await request.json()
        log_date = body.get("date", "")
        log_text = body.get("text", "")
        if not log_text:
            return {"ok": False, "error": "text required"}
        conn = get_conn(); cur = conn.cursor()
        cur.execute("INSERT INTO rb_logs (log_date, log_text) VALUES (%s, %s) RETURNING id", (log_date, log_text))
        new_id = cur.fetchone()[0]
        conn.commit(); cur.close(); conn.close()
        return {"ok": True, "id": new_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.put("/api/rb/logs/{log_id}")
async def update_rb_log(log_id: int, request: Request):
    try:
        body = await request.json()
        log_text = body.get("text", "")
        if not log_text:
            return {"ok": False, "error": "text required"}
        conn = get_conn(); cur = conn.cursor()
        cur.execute("UPDATE rb_logs SET log_text = %s WHERE id = %s", (log_text, log_id))
        conn.commit(); cur.close(); conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.delete("/api/rb/logs/{log_id}")
async def delete_rb_log(log_id: int, request: Request):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("DELETE FROM rb_logs WHERE id = %s", (log_id,))
        conn.commit(); cur.close(); conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/api/telegram/rebalance")
@limiter.limit("10/minute")
def telegram_rebalance_alert(request: Request, payload: dict):
    """리밸런싱 트리거 발동 시 프론트에서 호출"""
    try:
        total    = payload.get("total", 0)
        stock    = payload.get("stock", 0)
        cash     = payload.get("cash", 0)
        cur_pct  = payload.get("cur_pct", 0)
        target   = payload.get("target", 70)
        gap      = payload.get("gap", 0)
        action   = payload.get("action", "")
        amt      = payload.get("amt", 0)
        kst_now  = datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")

        def fmt_won(n):
            if n >= 100000000:
                uk = int(n // 100000000)
                rest = int(n % 100000000)
                man = int(rest // 10000)
                return f"{uk}억 {man:,}만원" if man > 0 else f"{uk}억원"
            elif n >= 10000:
                return f"{int(n//10000):,}만원"
            return f"{int(n):,}원"

        direction = "📉 주식 매도 → 현금 확보" if gap > 0 else "📈 현금 → 주식 매수"
        msg = (
            f"⚖️ <b>리밸런싱 알림</b>\n"
            f"📅 {kst_now}\n\n"
            f"💼 총 자산: {fmt_won(total)}\n"
            f"📊 현재 주식: {fmt_won(stock)} ({cur_pct}%)\n"
            f"💵 현재 현금: {fmt_won(cash)}\n\n"
            f"🎯 목표 비율: {target}% vs 현재 {cur_pct}% (<b>{gap:+.1f}% 이탈</b>)\n"
            f"{direction}\n"
            f"💡 조정 필요 금액: <b>{fmt_won(amt)}</b>"
        )
        send_telegram(msg)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/api/telegram/test")
def telegram_test():
    """텔레그램 연결 테스트"""
    send_telegram("✅ WISEMAC STOCK 텔레그램 연결 테스트 성공!")
    return {"ok": True, "token_set": bool(TELEGRAM_TOKEN), "chat_id_set": bool(TELEGRAM_CHAT_ID)}


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

    # ──── 부팅 시 자동 보정 스크리닝 ────
    # Railway 재배포/재시작으로 KST 04:00 스크리닝을 놓친 경우,
    # DB의 최근 스크리닝 시간이 오늘 KST 04:00 이전이면 즉시 한 번 실행
    def _boot_catchup_check():
        try:
            import time as _time
            _time.sleep(10)  # DB 준비 대기
            if not DATABASE_URL:
                return
            conn = get_conn(); cur = conn.cursor()
            cur.execute("SELECT MAX(screened_at) FROM screening_cache")
            row = cur.fetchone()
            cur.close(); conn.close()
            last_run = row[0] if row else None

            now_kst = datetime.now(pytz.timezone("Asia/Seoul"))
            today_4am_kst = now_kst.replace(hour=4, minute=0, second=0, microsecond=0)
            if now_kst.hour < 4:
                today_4am_kst = today_4am_kst - timedelta(days=1)

            need_catchup = False
            if last_run is None:
                need_catchup = True
                logger.info("부팅 보정: DB 비어있음 → 스크리닝 실행")
            else:
                # last_run은 naive UTC timestamp (NOW()가 UTC 반환)
                last_run_utc = last_run if last_run.tzinfo else pytz.utc.localize(last_run)
                last_run_kst = last_run_utc.astimezone(pytz.timezone("Asia/Seoul"))
                if last_run_kst < today_4am_kst:
                    need_catchup = True
                    logger.info(f"부팅 보정: 마지막 스크리닝 {last_run_kst} < 오늘 04:00 KST → 재실행")
                else:
                    logger.info(f"부팅 보정: 마지막 스크리닝 {last_run_kst} — 최신, 스킵")

            if need_catchup:
                import threading
                threading.Thread(target=run_full_screening_job, daemon=True).start()
                logger.info("부팅 보정 스크리닝 백그라운드 시작됨")
        except Exception as e:
            logger.error(f"부팅 보정 스크리닝 체크 실패: {e}")

    import threading
    threading.Thread(target=_boot_catchup_check, daemon=True).start()

@app.on_event("shutdown")
def on_shutdown():
    scheduler.shutdown()

"""
[로컬 전용 — 반드시 한국에서 실행]

코스피/코스닥 시총 상위 종목을 pykrx로 추출해 kr_universe.py 를 생성한다.
KRX는 해외 서버(Railway)를 차단하므로 서버에서 직접 못 만든다 → 한국 IP(로컬)에서
생성한 뒤 커밋·배포하면 main.py 가 자동으로 이 목록을 사용한다.

사용법:
  pip install pykrx
  python generate_kr_universe.py
  git add kr_universe.py
  git commit -m "update KR universe"
  git push

분기/반기마다 한 번씩 다시 실행해 갱신하면 된다.
"""
from pykrx import stock
from datetime import datetime, timedelta

KOSPI_N = 500    # 코스피 시총 상위 N
KOSDAQ_N = 500   # 코스닥 시총 상위 N


def recent_cap(market: str):
    """최근 영업일의 시가총액 DataFrame (최대 10일 역산)"""
    base = datetime.now()
    for i in range(10):
        ds = (base - timedelta(days=i)).strftime("%Y%m%d")
        try:
            df = stock.get_market_cap_by_ticker(ds, market=market)
            if df is not None and not df.empty and "시가총액" in df.columns:
                return df, ds
        except Exception:
            continue
    raise RuntimeError(f"{market} 시총 조회 실패 (최근 10영업일)")


def top_tickers(market: str, n: int, suffix: str):
    df, ds = recent_cap(market)
    rows = df.sort_values("시가총액", ascending=False).head(n)
    codes = [f"{idx}{suffix}" for idx in rows.index]
    print(f"  {market}: {ds} 기준 시총 상위 {len(codes)}개")
    return codes


def main():
    print("KRX 시총 상위 추출 중...")
    kospi = top_tickers("KOSPI", KOSPI_N, ".KS")
    kosdaq = top_tickers("KOSDAQ", KOSDAQ_N, ".KQ")

    with open("kr_universe.py", "w", encoding="utf-8") as f:
        f.write(f"# 자동생성: generate_kr_universe.py (로컬 한국 실행) — {datetime.now():%Y-%m-%d %H:%M}\n")
        f.write(f"TICKERS_KOSPI_DYN = {kospi!r}\n")
        f.write(f"TICKERS_KOSDAQ_DYN = {kosdaq!r}\n")

    print(f"\nkr_universe.py 생성 완료 (코스피 {len(kospi)} + 코스닥 {len(kosdaq)} = {len(kospi)+len(kosdaq)}개)")
    print("→ git add kr_universe.py && git commit && git push 후 Railway 배포하면 적용됩니다.")


if __name__ == "__main__":
    main()

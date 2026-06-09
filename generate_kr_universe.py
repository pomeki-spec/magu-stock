"""
[로컬 실행 권장 — 네이버 금융 크롤링]

코스피/코스닥 시총 상위 종목을 네이버 금융에서 추출해 kr_universe.py 를 생성한다.
(pykrx/finance-datareader는 KRX 인증·서버 변경으로 현재 막혀서 네이버 직접 크롤링 사용)
main.py 가 kr_universe.py 를 자동으로 읽어 한국 유니버스로 사용한다.

사용법:
  python generate_kr_universe.py
  git add kr_universe.py && git commit -m "update KR universe" && git push

분기/반기마다 다시 실행해 갱신.
"""
import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"}
KOSPI_PAGES = 10    # 50종목/페이지 → 500
KOSDAQ_PAGES = 10


def fetch_codes(sosok: int, pages: int):
    """네이버 시총 상위 페이지에서 종목코드 추출 (시총 내림차순). sosok 0=코스피 1=코스닥"""
    out = []
    for p in range(1, pages + 1):
        url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={p}"
        html = requests.get(url, headers=UA, timeout=15).content.decode("euc-kr", "ignore")
        soup = BeautifulSoup(html, "html.parser")
        page_codes = []
        for a in soup.select("table.type_2 a[href*='code=']"):
            m = re.search(r"code=(\d{6})", a.get("href", ""))
            if m:
                page_codes.append(m.group(1))
        page_codes = list(dict.fromkeys(page_codes))
        if not page_codes:
            break  # 마지막 페이지 이후
        out.extend(page_codes)
        time.sleep(0.4)
    return list(dict.fromkeys(out))


def main():
    print("네이버 금융 시총 상위 추출 중...")
    kospi = [c + ".KS" for c in fetch_codes(0, KOSPI_PAGES)]
    kosdaq = [c + ".KQ" for c in fetch_codes(1, KOSDAQ_PAGES)]
    print(f"  코스피: {len(kospi)}개")
    print(f"  코스닥: {len(kosdaq)}개")

    with open("kr_universe.py", "w", encoding="utf-8") as f:
        f.write(f"# 자동생성: generate_kr_universe.py (네이버 금융 시총상위) — {datetime.now():%Y-%m-%d %H:%M}\n")
        f.write(f"TICKERS_KOSPI_DYN = {kospi!r}\n")
        f.write(f"TICKERS_KOSDAQ_DYN = {kosdaq!r}\n")

    print(f"\nkr_universe.py 생성 완료 (코스피 {len(kospi)} + 코스닥 {len(kosdaq)} = {len(kospi)+len(kosdaq)}개)")
    print("→ git add kr_universe.py && git commit && git push 후 배포")


if __name__ == "__main__":
    main()

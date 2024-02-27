# Overview

# Usage-공통
1. Requirements 설치
```bash
conda create -n processor python=3.9
conda activate processor
pip install -r requirements.txt
```

2. config.py에 정보 입력 (for SNU BDAI members)

# SEC
main.py를 동작시켜 필요한 보고서들의 pdf 다운로드 링크가 포함된 csv파일을 생성하는 구조.

이때 S&P500(필요에 따라 다른 기업 리스트의 csv파일) 기업의 ticker와 cik가 포함된 sp500.csv 파일을 필요로 함.

```bash
python main.py
```

이때, main.py를 실행시키려면 SEC_scraper안에서 동작시켜야함에 주의하자.

# DART_scraper
dart_crawler.py를 동작시켜 필요한 보고서들의 pdf 다운로드 링크가 포함된 csv파일을 생성하는 구조.

```bash
python dart_crawler.py
```

이때, dart_crawler.py를 실행시키려면 DART_scraper안에서 동작시켜야함에 주의하자.

# DART_PDF_extractor
보고서 pdf 를 input으로 받으면, 전처리하여 필요한 데이터들이 output으로 나와 postgresql 서버 및 minio 서버에 적재되는 구조.

사용 방법은
<https://github.com/snu-dm/OBSOLETE-DART_pdf_extractor> 참조.

# Exhibition Data Hub

전국 전시 기관 데이터를 수집하고, 기관 홈페이지에서 전시 정보를 추출하는 Python 프로젝트입니다.

## What This Project Does

이 저장소에는 두 개의 주요 수집 스크립트가 있습니다.

1. `src/collect_naver_local.py`
- 네이버 지역 검색 API로 전시/문화 관련 기관 후보를 수집
- 결과 CSV 생성

2. `src/collect_exhibition_events.py`
- 기관 홈페이지(여러 URL 포함)에서 전시명/기간/가격/설명을 추출
- 정적 HTML 파싱 → JS 렌더링(선택) → 이미지 OCR(선택) 순서로 탐색
- 중단 시 체크포인트 저장 지원

## Requirements

- Python 3.14
- (선택) OCR 사용 시 시스템 Tesseract OCR 엔진
- (선택) JS 렌더링 사용 시 Playwright 브라우저

## Setup

### 1) 가상 환경

Windows:

python -m venv .venv
.venv\Scripts\activate

macOS/Linux:

python -m venv .venv
source .venv/bin/activate

### 2) 패키지 설치

pip install -r requirements.txt

### 3) Playwright 브라우저 설치 (JS 렌더링 사용할 때만)

python -m playwright install chromium

### 4) 환경 변수 파일 생성

.env.example을 복사해 .env 파일을 만들고 값을 채웁니다.

필수:
- NAVER_CLIENT_ID
- NAVER_CLIENT_SECRET

선택:
- NAVER_LOCAL_SORT (random 또는 comment, 기본값 random)
- TESSERACT_CMD (PATH로 못 찾을 때 Tesseract 실행 파일 절대 경로)

Windows 예시:

TESSERACT_CMD=C:\Program Files\Tesseract-OCR\tesseract.exe

## Input Files

- docs/regions.md
- docs/keywords.md
- data/test_naver_local_exhibitions.csv (이벤트 추출 기본 입력)

## Script 1: Local Institution Collection

실행:

python src/collect_naver_local.py

출력:

- data/naver_local_exhibitions.csv

설명:

- docs/regions.md와 docs/keywords.md의 조합으로 네이버 지역 검색 API를 호출합니다.
- 중복 기관은 제목/주소/공식 URL 조합으로 제거합니다.

## Script 2: Exhibition Event Extraction

기본 실행:

python src/collect_exhibition_events.py

자주 쓰는 실행 예시:

빠른 테스트:

python src/collect_exhibition_events.py --max-institutions 20

JS 렌더링 포함:

python src/collect_exhibition_events.py --enable-js-render --js-render-timeout-ms 12000

OCR 포함:

python src/collect_exhibition_events.py --enable-image-ocr --max-images-per-page 3

전체 옵션 조합 예시:

python src/collect_exhibition_events.py --enable-js-render --enable-image-ocr --save-every 10

### Main Output Files

- data/extracted_exhibitions.csv
- data/failed_domains.csv

### 주요 옵션 (기본값)

- --input (data/test_naver_local_exhibitions.csv)
- --output (data/extracted_exhibitions.csv)
- --failed-domains-out (data/failed_domains.csv)
- --timeout (8)
- --pause (0.15)
- --max-pages-per-institution (8)
- --max-base-urls-per-institution (3)
- --max-institutions (0, 전체)
- --min-confidence (0.75)
- --save-every (25)
- --enable-js-render
- --js-render-timeout-ms (12000)
- --enable-image-ocr
- --max-images-per-page (3)

## OCR Notes

- Python 패키지: Pillow, pytesseract
- 시스템 엔진: Tesseract OCR 설치 필요
- 코드에서 TESSERACT_CMD 또는 일반 PATH, Windows 기본 설치 경로를 순서대로 탐지합니다.

## Repository Structure

- src: 수집 스크립트
- docs: 지역/키워드 목록
- data: 입력 및 결과 CSV

## Git Ignore Policy

- .env, 캐시, 가상환경, 테스트/스모크 산출물 CSV는 .gitignore로 제외합니다.
- 기본 입력/대표 결과 파일은 필요에 따라 유지할 수 있습니다.
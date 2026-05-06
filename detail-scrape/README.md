# Exhibition Data Hub

전국 전시 기관 데이터를 수집하고, 기관 홈페이지에서 전시 정보를 추출하는 Python 프로젝트입니다.

## What This Project Does

이 저장소에는 세 개의 주요 스크립트가 있습니다.

1. `src/collect_naver_local.py`
- 네이버 지역 검색 API로 전시/문화 관련 기관 후보를 수집
- 배치 처리 지원 (일부 조합만 처리하고 재개 가능)
- 진행 상태를 JSON으로 저장하여 중단 후 자동 재개 지원
- 결과 CSV 생성

2. `src/collect_exhibition_events.py`
- 기관 홈페이지(여러 URL 포함)에서 전시명/기간/가격/설명을 추출
- 정적 HTML 파싱 → JS 렌더링(선택) → 이미지 OCR(선택) 순서로 탐색
- Instagram 프로필/게시물에서 전시 정보 추출 (선택)
- 진행 상태를 JSON으로 저장하여 중단 후 자동 재개 지원
- 중단 시 체크포인트 저장

3. `src/collect_instagram.py` (내부용)
- Playwright 기반 Instagram 추출 모듈
- 인스타그램 프로필/직접 게시물 URL 처리
- 이미지 캡션 및 OCR 텍스트에서 날짜 추출

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

### 3) Playwright 브라우저 설치 (JS 렌더링 또는 Instagram 추출 사용할 때)

python -m playwright install chromium

### 4) 환경 변수 파일 생성

.env.example을 복사해 .env 파일을 만들고 값을 채웁니다.

필수:
- NAVER_CLIENT_ID
- NAVER_CLIENT_SECRET

Instagram 추출용 (선택):
- INSTAGRAM_USER (Instagram 계정명, 로그인 기반 스크래핑용)
- INSTAGRAM_PASS (Instagram 비밀번호)
- INSTAGRAM_PROXY (선택, 프록시 URL: http://host:port)

OCR용 (선택):
- TESSERACT_CMD (PATH로 못 찾을 때 Tesseract 실행 파일 절대 경로)

Naver Local API 검색 순서 (선택):
- NAVER_LOCAL_SORT (random 또는 comment, 기본값 random)

Windows 예시:

TESSERACT_CMD=C:\Program Files\Tesseract-OCR\tesseract.exe
INSTAGRAM_USER=your_username
INSTAGRAM_PASS=your_password

## Input Files

- docs/regions.md
- docs/keywords.md
- data/test_naver_local_exhibitions.csv (이벤트 추출 기본 입력)

## Script 1: Local Institution Collection

기본 실행:

python src/collect_naver_local.py

배치 처리 (첫 5000개 조합만):

python src/collect_naver_local.py --batch-size 5000

특정 지점부터 재개:

python src/collect_naver_local.py --start-index 50000

자동 재개 비활성화:

python src/collect_naver_local.py --no-auto-resume

출력:

- data/naver_local_exhibitions.csv (수집된 기관 목록)
- data/naver_local_progress.json (진행 상태, 자동 재개용)

설명:

- docs/regions.md와 docs/keywords.md의 조합으로 네이버 지역 검색 API를 호출합니다.
- 중복 기관은 제목/주소/공식 URL 조합으로 제거합니다.
- 중단 시에도 진행 상태가 저장되어 다음 실행 시 자동으로 이어집니다.

## Script 2: Exhibition Event Extraction

기본 실행:

python src/collect_exhibition_events.py

### 팀원 실행 가이드

아래 순서대로 세팅하면 각 팀원이 같은 방식으로 실행할 수 있습니다.

1. Python 3.14 설치

Windows 기준으로 Python 3.14를 설치합니다. 설치 후 터미널에서 확인합니다.

python --version

2. 가상환경 생성 및 활성화

프로젝트 루트에서 가상환경을 만들고 활성화합니다.

python -m venv .venv
.venv\Scripts\activate

3. 패키지 설치

requirements.txt에 있는 패키지를 설치합니다.

pip install -r requirements.txt

4. Playwright 브라우저 설치

JS 렌더링이나 Instagram 추출을 사용할 경우 Chromium을 추가 설치합니다.

python -m playwright install chromium

5. 환경 변수 설정

.env 파일에 필요한 값을 넣습니다.

- NAVER_CLIENT_ID
- NAVER_CLIENT_SECRET
- INSTAGRAM_USER (선택)
- INSTAGRAM_PASS (선택)
- INSTAGRAM_PROXY (선택)
- TESSERACT_CMD (선택)

6. 실행 전 확인

입력 CSV와 출력 폴더가 준비되어 있는지 확인합니다.

- 기본 입력: data/test_naver_local_exhibitions.csv
- 기본 출력: data/extracted_exhibitions.csv
- 진행 파일: data/exhibition_extraction_progress.json

7. 분할 실행

기관을 나눠서 각각 다른 범위로 실행합니다. 중간에 끊겨도 각 구간은 진행 파일 기준으로 다시 이어서 실행할 수 있습니다.

사람 1:

python src/collect_exhibition_events.py --start-index 1 --end-index 1756 --progress-file data/exhibition_extraction_progress_1.json

사람 2:

python src/collect_exhibition_events.py --start-index 1757 --end-index 3512 --progress-file data/exhibition_extraction_progress_2.json

사람 3:

python src/collect_exhibition_events.py --start-index 3513 --end-index 5268 --progress-file data/exhibition_extraction_progress_3.json

8. 공통 옵션

필요에 따라 아래 옵션을 같이 붙입니다.

- --enable-js-render : JavaScript 렌더링 사용
- --enable-image-ocr : 이미지 OCR 사용
- --enable-instagram : Instagram 추출 사용
- --no-auto-resume : 자동 재개를 끄고 처음부터 범위만 실행
- --save-every 10 : 더 자주 저장

9. 결과 확인

각 팀원이 만든 CSV를 확인한 뒤, 필요하면 하나의 결과로 합칩니다.

### Notes

- 각 팀원이 같은 출력 파일을 동시에 쓰지 않도록 `--output`과 `--progress-file`을 분리하는 편이 안전합니다.
- 범위를 나눠 실행할 때는 `--start-index`와 `--end-index`를 반드시 같이 지정하는 편이 명확합니다.
- Instagram 추출은 가장 느릴 수 있으므로 필요하지 않으면 끄고, 필요한 구간만 따로 돌리는 편이 좋습니다.

자주 쓰는 실행 예시:

빠른 테스트:

python src/collect_exhibition_events.py --max-institutions 20

JS 렌더링 포함:

python src/collect_exhibition_events.py --enable-js-render --js-render-timeout-ms 12000

OCR 포함:

python src/collect_exhibition_events.py --enable-image-ocr --max-images-per-page 3

Instagram 추출 포함 (긴 딜레이):

python src/collect_exhibition_events.py --enable-instagram --instagram-random-delay-min 10 --instagram-random-delay-max 30

전체 옵션 조합 예시:

python src/collect_exhibition_events.py --enable-js-render --enable-image-ocr --enable-instagram --save-every 10

특정 기관부터 재개:

python src/collect_exhibition_events.py --start-index 1000

특정 범위만 실행:

python src/collect_exhibition_events.py --start-index 1000 --end-index 2000

자동 재개 비활성화:

python src/collect_exhibition_events.py --no-auto-resume

### Main Output Files

- data/extracted_exhibitions.csv (추출된 전시 정보)
- data/failed_domains.csv (실패한 도메인 요약)
- data/exhibition_extraction_progress.json (진행 상태, 자동 재개용)

### 주요 옵션 (기본값)

**입출력:**
- --input (data/test_naver_local_exhibitions.csv)
- --output (data/extracted_exhibitions.csv)
- --failed-domains-out (data/failed_domains.csv)
- --progress-file (data/exhibition_extraction_progress.json)

**진행 제어:**
- --start-index (1, 1-based 기관 번호)
- --end-index (0, 끝까지. 1-based 기관 번호 기준 종료 지점)
- --max-institutions (0, 전체)
- --save-every (25, N개마다 체크포인트 저장)
- --no-auto-resume (자동 재개 비활성화)

**네트워크:**
- --timeout (8초)
- --pause (0.15초, 페이지 간 대기)
- --max-pages-per-institution (8)
- --max-base-urls-per-institution (3)

**추출 옵션:**
- --enable-js-render (JavaScript 렌더링)
- --js-render-timeout-ms (12000)
- --enable-image-ocr (이미지 OCR)
- --max-images-per-page (3)
- --enable-instagram (Instagram 추출)

**Instagram 옵션:**
- --instagram-max-posts (20)
- --instagram-post-delay (4.0초)
- --instagram-profile-delay (8.0초)
- --instagram-random-delay-min (10.0초)
- --instagram-random-delay-max (30.0초)
- --instagram-timeout-ms (10000)
- --instagram-proxy (프록시 URL)
- --instagram-username (.env의 INSTAGRAM_USER)
- --instagram-password (.env의 INSTAGRAM_PASS)

**필터링:**
- --min-confidence (0.75)

### 3인 분할 실행 예시

기관을 세 구간으로 나눠 동시에 실행할 수 있습니다.

사람 1:

python src/collect_exhibition_events.py --start-index 1 --end-index 1756

사람 2:

python src/collect_exhibition_events.py --start-index 1757 --end-index 3512

사람 3:

python src/collect_exhibition_events.py --start-index 3513 --end-index 5268

각 구간은 별도 진행 파일을 쓰고 싶으면 `--progress-file`만 다르게 지정하면 됩니다. 중단된 구간은 마지막 저장 지점부터 다시 이어서 실행할 수 있습니다.

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

# Art Venue Filtering and Exhibition URL Discovery

The DJ branch includes a venue filtering and candidate URL discovery workflow
for the ArtMoa exhibition pipeline.

Current local environment used for this workflow:

```text
OS: Windows
Python: 3.11.2
Shell: Git Bash(MINGW64) or PowerShell
Local data folder: C:\Users\dvkim\OneDrive\바탕 화면\크롤링3
```

No extra runtime packages are required for these added scripts. They use only
the Python standard library and the existing Naver OpenAPI credentials.

Copy `.env.example` to `.env` and fill in:

```text
NAVER_CLIENT_ID=
NAVER_CLIENT_SECRET=
```

Added scripts:

```text
src/classify_art_exhibition_venues.py
src/filter_art_candidates.py
src/discover_exhibition_info_urls.py
src/sort_exhibition_url_candidates.py
```

Workflow:

```bash
python ./src/classify_art_exhibition_venues.py --search-naver-urls --verify-urls -o "C:\Users\dvkim\OneDrive\바탕 화면\크롤링3\naver_local_exhibitions_art_verified.csv"
python ./src/filter_art_candidates.py
python ./src/discover_exhibition_info_urls.py --use-naver-search --top-n 5 --probe-pages 5 --workers 10
python ./src/sort_exhibition_url_candidates.py
```

Large CSV/XLSX outputs and local progress files are ignored by git by default.

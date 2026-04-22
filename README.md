# 🏛️ Exhibition Data Hub

전국 전시회 정보 데이터 수집 프로젝트입니다. 전국의 숨겨진 전시 정보를 수집하는 것을 목표로 합니다.

## 🛠️ Tech Stack
- **Language:** Python (3.14)
- **Editor:** VS Code
- **Data Source:** 네이버 검색 API (지역), 구글 스프레드시트

## 🔐 환경 변수 설정 (네이버 API)

네이버 검색 API 키는 환경 변수로 관리합니다.

1. `.env.example`를 복사해서 `.env` 파일 생성
2. `.env`에 아래 값 입력

```env
NAVER_CLIENT_ID=YOUR_CLIENT_ID
NAVER_CLIENT_SECRET=YOUR_CLIENT_SECRET
```

`.env`는 `.gitignore`에 추가되어 Git에 커밋되지 않습니다.

## � 가상 환경 설정

### 가상 환경 생성 및 활성화

```bash
# 가상 환경 생성
python -m venv venv

# 가상 환경 활성화
# Windows
venv\Scripts\activate
# macOS / Linux
source venv/bin/activate
```

### 의존성 설치

현재 스크립트는 Python 표준 라이브러리만 사용하므로 추가 설치가 필요하지 않습니다.  
만약 추후 라이브러리를 추가할 경우 아래 명령으로 설치하면 됩니다.

```bash
pip install -r requirements.txt
```

## �📊 데이터 수집 스키마 (CSV/Excel 구조)

수집되는 모든 데이터는 CSV 및 Excel로 관리하며, 각 컬럼(열)의 정의는 다음과 같습니다.

| 항목 | 컬럼명(CSV 기준) | 데이터 예시 | 비고 |
| :--- | :--- | :--- | :--- |
| **전시명** | `title` | 완주 로컬 작가 3인전 | |
| **시작일** | `start_date` | 2026-04-15 | YYYY-MM-DD 형식 고정 |
| **종료일** | `end_date` | 2026-05-10 | YYYY-MM-DD 형식 고정 |
| **전체 주소** | `full_address` | 도시 구군 읍면리 ••• | 지번 주소 (address) |
| **도로명 주소** | `road_address` | 서울특별시 중구 을지로15길 6-5 | 도로명 주소 (roadAddress) |
| **대분류** | `region_main` | 도시 | 주소 기반 자동 파싱 |
| **중분류** | `region_sub` | 구군 | 주소 기반 자동 파싱 (광역시는 X) |
| **소분류** | `region_detail` | 읍면리 | 주소 기반 자동 파싱 |
| **URL** | `official_url` | https://••• | 공식 사이트 또는 SNS 정보 |
| **요약 정보** | `summary` | 숲을 주제로 한 서양화 전시 ••• | 전시회 내용 요약 |
| **전화번호** | `tel` | xxx(x)-xxx(x)-xxxx | API에서 비어 있을 수 있음 |
| **전시관 종류** | `category` | 미술관 / 문화예술회관 / 갤러리 등 | 태그 형태로 관리 |
| **입장료 정보** | `price` | 무료 / 유료 / 정보 없음 | 무료/유료 구분(불명확 시 정보 없음) |
| **경도 (X)** | `mapx` | 311277 | WGS84 좌표계 기준 X(경도) |
| **위도 (Y)** | `mapy` | 552097 | WGS84 좌표계 기준 Y(위도) |

## 🧭 주소 파싱 및 좌표 참고

- 기본 구현은 주소 문자열 공백 분리로 `region_main`, `region_sub`, `region_detail`를 채웁니다.
- 정밀도를 높이려면 도로명주소 API 연동 라이브러리(`juso`) 또는 행정구역 코드 데이터(법정동 코드)와 함께 검증하는 방식을 권장합니다.
- 현재 수집 스크립트에는 교체 가능한 함수 `parse_address_parts()`가 포함되어 있어, 추후 정교한 주소 파싱 로직으로 확장할 수 있습니다.

### 좌표계 정보
- **mapx/mapy**: 네이버 지역 검색 API에서 제공하는 WGS84 기준 좌표입니다.
- mapx는 경도(Longitude/X), mapy는 위도(Latitude/Y)에 해당합니다.
- 지도 시각화, 거리 계산 등 공간 분석 시 활용할 수 있습니다.
- 좌표 값이 없는 경우 CSV에서 빈 공간으로 저장됩니다.

## 🔎 네이버 지역 검색 수집 스크립트

블로그 검색 예제를 지역 검색으로 변경한 파이썬 스크립트는 `src/collect_naver_local.py`입니다.

- API 엔드포인트: `https://openapi.naver.com/v1/search/local.json`
- 검색어 형식: `"{지역} {키워드}"`
- 요청 파라미터: `query`(필수), `display`(1~5), `start`(1 고정), `sort`(`random`/`comment`)
- 지역 목록 파일: `docs/regions.md`
- 키워드 목록 파일: `docs/keywords.md`

### 실행 방법

```bash
python src/collect_naver_local.py
```

실행 결과는 `data/naver_local_exhibitions.csv`로 저장됩니다.

### 지역 목록 관리

`docs/regions.md`에 아래처럼 지역을 한 줄씩 작성하면 됩니다.

```md
- 서울
- 부산
- 전주
```

### 키워드 목록 관리

`docs/keywords.md`에 검색 키워드를 한 줄씩 작성하면 됩니다.

```md
- 국립박물관
- 시립미술관
- 아트페어
```

스크립트는 `지역 목록 × 키워드 목록`의 전체 조합으로 검색합니다.

## 📁 폴더 구조
- `/src`: Python 수집 엔진 소스 코드
- `/data`: 지역별 수집 완료 데이터 (CSV, XLSX)
- `/docs`: .md 형식의 지역명 리스트 및 조사 매뉴얼

## 🤖 홈페이지 자동 추출 파이프라인

도메인이 서로 다른 기관 홈페이지에서 전시 정보를 자동 수집하기 위해, 아래 3단계 파이프라인을 제공합니다.

1. 기관 정제: 전시 관련 기관만 선별
2. 페이지 탐색: 홈페이지 + sitemap + 전시 관련 링크 탐색
3. 전시 추출: 전시명/기간/가격(무료/유료) 추출

실행 스크립트는 `src/collect_exhibition_events.py`입니다.

### 실행 예시

빠른 테스트 (기관 20개 제한):

```bash
python src/collect_exhibition_events.py --max-institutions 20
```

전체 실행:

```bash
python src/collect_exhibition_events.py
```

### 출력 파일

- `data/filtered_exhibition_institutions.csv`
	- 전시 관련 기관 목록
- `data/discovered_exhibition_pages.csv`
	- 기관별 전시 후보 페이지 URL
- `data/extracted_exhibitions.csv`
	- 추출된 전시 정보 (전시명, 시작일, 종료일, 가격 유형, 근거 텍스트)
- `data/curated_exhibitions.csv`
	- 신뢰도 기준(`--min-confidence`) 이상인 우선 활용 데이터
- `data/rejected_exhibitions.csv`
	- 신뢰도 기준 미만인 검수 대상 데이터

### 주요 옵션

- `--max-institutions`: 테스트용 기관 수 제한 (0이면 전체)
- `--max-pages-per-institution`: 기관당 탐색 페이지 수 제한
- `--timeout`: 요청 타임아웃(초)
- `--pause`: 페이지 요청 간 대기 시간(초)
- `--min-confidence`: 자동 채택 최소 신뢰도 (기본값 `0.85`)

기관별 중복 통합은 자동으로 수행됩니다. 동일 기관에서 같은 전시가 여러 페이지에서 발견되면,
신뢰도/가격 정보/근거 텍스트 길이를 기준으로 대표 1건으로 합쳐 저장합니다.
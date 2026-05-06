# URL List Scrape

예술 관련 전시장 후보를 분류하고, 각 장소 사이트 안에서 전시정보가 있을 가능성이 높은 URL 후보를 찾는 워크플로우입니다.

## 보안

`NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET`, 개인 PC 경로는 코드에 하드코딩하지 않습니다.

현재 스크립트는 아래 순서로 값을 읽습니다.

```text
1. CLI 옵션: --naver-client-id, --naver-client-secret
2. .env 파일
3. OS 환경변수
```

## 환경 설정

레포 루트에서 `.env.example`을 복사해서 `.env`를 만듭니다.

```bash
cp .env.example .env
```

`.env`에 아래 값을 입력합니다.

```text
NAVER_CLIENT_ID=your_client_id
NAVER_CLIENT_SECRET=your_client_secret
ARTMOA_DATA_DIR=url-list-scrape/data
```

`ARTMOA_DATA_DIR`는 `-i`, `-o` 옵션을 생략했을 때 기본 입출력 CSV를 찾는 폴더입니다.

## 데이터 파일

처음부터 재현하려면 이 원본 입력 CSV가 필요합니다.

```text
url-list-scrape/data/naver_local_exhibitions_mod.csv
```

이 파일은 크롤링3 폴더에서 실제 작업에 사용했던 원본 입력 파일입니다.

아래 파일들은 실행하면서 생성되는 산출물입니다. 기본적으로 다시 만들 수 있으므로 git에는 올리지 않는 것을 권장합니다.

```text
url-list-scrape/data/naver_local_exhibitions_art_verified.csv
url-list-scrape/data/naver_local_exhibitions_art_candidates_yes_maybe.csv
url-list-scrape/data/naver_local_exhibitions_exhibition_url_candidates.csv
url-list-scrape/data/naver_local_exhibitions_exhibition_url_candidates_sorted.csv
```

최종 전달용 파일은 마지막 `sorted.csv`입니다.

## 실행 순서

레포 루트에서 실행합니다.

### 1. 예술 전시장 분류 및 URL 검증

```bash
python ./url-list-scrape/classify_art_exhibition_venues.py --search-naver-urls --verify-urls
```

입력:

```text
url-list-scrape/data/naver_local_exhibitions_mod.csv
```

출력:

```text
url-list-scrape/data/naver_local_exhibitions_art_verified.csv
```

이 단계의 결과는 중간 파일입니다. 다음 단계가 사용합니다.

### 2. yes/maybe 후보만 추출

```bash
python ./url-list-scrape/filter_art_candidates.py
```

입력:

```text
url-list-scrape/data/naver_local_exhibitions_art_verified.csv
```

출력:

```text
url-list-scrape/data/naver_local_exhibitions_art_candidates_yes_maybe.csv
```

이 파일은 예술 장소 후보 목록 확인용으로도 유용합니다.

### 3. 전시정보 후보 URL 탐색

```bash
python ./url-list-scrape/discover_exhibition_info_urls.py --use-naver-search --top-n 5 --probe-pages 5 --workers 10
```

입력:

```text
url-list-scrape/data/naver_local_exhibitions_art_candidates_yes_maybe.csv
```

출력:

```text
url-list-scrape/data/naver_local_exhibitions_exhibition_url_candidates.csv
```

장소별로 `rank`, `candidate_url`, `candidate_type`, `confidence`, `evidence_text`가 생성됩니다.

### 4. 결과 정렬

```bash
python ./url-list-scrape/sort_exhibition_url_candidates.py
```

입력:

```text
url-list-scrape/data/naver_local_exhibitions_exhibition_url_candidates.csv
```

출력:

```text
url-list-scrape/data/naver_local_exhibitions_exhibition_url_candidates_sorted.csv
```

정렬 기준:

```text
1. venue_index 오름차순
2. rank 오름차순
```

## 최종 사용 파일

다음 작업자가 전시정보를 긁을 때 주로 사용할 파일은 이것입니다.

```text
url-list-scrape/data/naver_local_exhibitions_exhibition_url_candidates_sorted.csv
```

`candidate_url`을 `rank` 순서대로 보면 됩니다.

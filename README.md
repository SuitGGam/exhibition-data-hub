# ArtMoa Venue URL Discovery

This repository contains the ArtMoa venue filtering and exhibition-info URL discovery workflow.

## Environment

```text
OS: Windows
Python: 3.11.2
Shell: Git Bash(MINGW64) or PowerShell
```

The scripts use only the Python standard library. No third-party package is required.

## Setup

Create `.env` from the example:

```bash
cp .env.example .env
```

Fill in:

```text
NAVER_CLIENT_ID=your_client_id
NAVER_CLIENT_SECRET=your_client_secret
ARTMOA_DATA_DIR=detail-scrape/data
```

Do not commit `.env`.

## Scripts

```text
detail-scrape/src/classify_art_exhibition_venues.py
detail-scrape/src/filter_art_candidates.py
detail-scrape/src/discover_exhibition_info_urls.py
detail-scrape/src/sort_exhibition_url_candidates.py
```

## Workflow

Put input CSV files in `detail-scrape/data/`, or pass explicit paths with `-i` and `-o`.

1. Classify art-related venues and verify/search URLs:

```bash
python ./detail-scrape/src/classify_art_exhibition_venues.py --search-naver-urls --verify-urls
```

2. Extract `yes` and `maybe` rows:

```bash
python ./detail-scrape/src/filter_art_candidates.py
```

3. Discover ranked exhibition-info URL candidates:

```bash
python ./detail-scrape/src/discover_exhibition_info_urls.py --use-naver-search --top-n 5 --probe-pages 5 --workers 10
```

4. Sort the candidate URL output:

```bash
python ./detail-scrape/src/sort_exhibition_url_candidates.py
```

## Security

Do not hardcode API keys, personal paths, or local machine-specific roots in source files. Use `.env`, environment variables, or command-line options.

If credentials were ever committed to git history, rotate the Naver API key pair before sharing or pushing the branch.

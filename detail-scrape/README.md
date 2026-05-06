# ArtMoa Venue URL Discovery

This branch contains only the ArtMoa venue filtering and exhibition-info URL discovery workflow.

## Environment

```text
OS: Windows
Python: 3.11.2
Shell: Git Bash(MINGW64) or PowerShell
```

The scripts use only the Python standard library. No third-party Python package is required.

## Setup

Create a local `.env` file from the example:

```bash
cp .env.example .env
```

Fill in the credentials in `.env`:

```text
NAVER_CLIENT_ID=your_client_id
NAVER_CLIENT_SECRET=your_client_secret
ARTMOA_DATA_DIR=data
```

Do not commit `.env`. It is ignored by git.

## Files

```text
src/classify_art_exhibition_venues.py
```

Classifies Naver local place rows into `yes`, `maybe`, and `no`, verifies URLs, and optionally compares existing URLs with Naver Search API results.

```text
src/filter_art_candidates.py
```

Extracts only `is_art_venue=yes` and `is_art_venue=maybe` rows.

```text
src/discover_exhibition_info_urls.py
```

Discovers likely exhibition-information URLs for each art venue candidate by combining homepage links, nav/header links, robots.txt, sitemap.xml, common exhibition paths, optional Naver `site:` search, and shallow page title/heading checks.

```text
src/sort_exhibition_url_candidates.py
```

Sorts parallel worker output by `venue_index`, then `rank`.

## Workflow

Put input CSV files in `data/`, or pass explicit paths with `-i` and `-o`.

1. Classify art-related venues and verify/search URLs:

```bash
python ./src/classify_art_exhibition_venues.py --search-naver-urls --verify-urls
```

Default input:

```text
data/naver_local_exhibitions_mod.csv
```

Default output:

```text
data/naver_local_exhibitions_art_verified.csv
```

2. Extract `yes` and `maybe` rows:

```bash
python ./src/filter_art_candidates.py
```

Default output:

```text
data/naver_local_exhibitions_art_candidates_yes_maybe.csv
```

3. Discover ranked exhibition-info URL candidates:

```bash
python ./src/discover_exhibition_info_urls.py --use-naver-search --top-n 5 --probe-pages 5 --workers 10
```

Default output:

```text
data/naver_local_exhibitions_exhibition_url_candidates.csv
```

4. Sort the candidate URL output:

```bash
python ./src/sort_exhibition_url_candidates.py
```

Default output:

```text
data/naver_local_exhibitions_exhibition_url_candidates_sorted.csv
```

## Security

Do not hardcode API keys, personal paths, or local machine-specific roots in source files. Use `.env`, environment variables, or command-line options instead.

If credentials were ever committed to git history, rotate the Naver API key pair before sharing or pushing the branch.

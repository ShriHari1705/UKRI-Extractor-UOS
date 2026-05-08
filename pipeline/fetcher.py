"""
fetcher.py — Fetches paginated UKRI project pages from the API.

Cache strategy (Joe's advice — implement caching so failed runs can resume):
  - Local mode (default): each page saved as cache/ukri_pages/page_NNNN.json
  - S3 mode (USE_S3=true): same files written to s3://<S3_BUCKET>/<S3_PREFIX>/

On a cache hit the API is not called — this means a full 8,660-page run
can be interrupted and resumed without re-fetching completed pages.
"""
import json
import logging
import time
from typing import Generator, Optional

import requests

from config import (
    BASE_URL, API_HEADERS,
    RETRY_MAX, RETRY_DELAY, POLITE_DELAY,
    LOCAL_CACHE, USE_S3, S3_BUCKET, S3_PREFIX,
)
from pipeline.models import UKRIPageResponse

log = logging.getLogger(__name__)


# ── HTTP ───────────────────────────────────────────────────────────────────────

def _get_with_retry(url: str, params: dict = None) -> dict:
    """
    HTTP GET with exponential backoff on transient errors (429, 5xx).
    Raises on persistent failure so the caller can decide how to handle it.
    """
    for attempt in range(1, RETRY_MAX + 1):
        try:
            resp = requests.get(url, params=params, headers=API_HEADERS, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as exc:
            code = exc.response.status_code if exc.response else 0
            if code in (429, 500, 502, 503, 504) and attempt < RETRY_MAX:
                wait = RETRY_DELAY * attempt  # 2s, 4s, …
                log.warning(f"HTTP {code} on attempt {attempt}/{RETRY_MAX} — retrying in {wait}s")
                time.sleep(wait)
            else:
                raise
        except requests.exceptions.RequestException as exc:
            if attempt < RETRY_MAX:
                log.warning(f"Request error on attempt {attempt}/{RETRY_MAX}: {exc}")
                time.sleep(RETRY_DELAY)
            else:
                raise


# ── Local cache ────────────────────────────────────────────────────────────────

def _local_path(page_num: int):
    return LOCAL_CACHE / f"page_{page_num:04d}.json"

def _local_exists(page_num: int) -> bool:
    return _local_path(page_num).exists()

def _local_load(page_num: int) -> dict:
    with open(_local_path(page_num), "r") as f:
        return json.load(f)

def _local_save(page_num: int, data: dict):
    LOCAL_CACHE.mkdir(parents=True, exist_ok=True)
    with open(_local_path(page_num), "w") as f:
        json.dump(data, f, indent=2)


# ── S3 cache ───────────────────────────────────────────────────────────────────

def _s3_key(page_num: int) -> str:
    return f"{S3_PREFIX}/page_{page_num:04d}.json"

def _s3_exists(s3, page_num: int) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=_s3_key(page_num))
        return True
    except Exception:
        return False

def _s3_load(s3, page_num: int) -> dict:
    obj = s3.get_object(Bucket=S3_BUCKET, Key=_s3_key(page_num))
    return json.loads(obj["Body"].read())

def _s3_save(s3, page_num: int, data: dict):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=_s3_key(page_num),
        Body=json.dumps(data, indent=2),
        ContentType="application/json",
    )


# ── Public interface ───────────────────────────────────────────────────────────

def get_page(page_num: int, s3_client=None) -> UKRIPageResponse:
    """
    Fetch one page of UKRI projects, using cache if available.

    Args:
        page_num:   Page number (1-indexed).
        s3_client:  Boto3 S3 client. Required only when USE_S3=true.

    Returns:
        Validated UKRIPageResponse.
    """
    if USE_S3 and s3_client:
        if _s3_exists(s3_client, page_num):
            log.debug(f"S3 cache hit — page {page_num}")
            raw = _s3_load(s3_client, page_num)
        else:
            log.info(f"API → S3: fetching page {page_num}")
            raw = _get_with_retry(f"{BASE_URL}/projects", params={"p": page_num})
            _s3_save(s3_client, page_num, raw)
    else:
        if _local_exists(page_num):
            log.debug(f"Local cache hit — page {page_num}")
            raw = _local_load(page_num)
        else:
            log.info(f"API → local: fetching page {page_num}")
            raw = _get_with_retry(f"{BASE_URL}/projects", params={"p": page_num})
            _local_save(page_num, raw)

    return UKRIPageResponse(**raw)


def fetch_all_pages(
    max_pages: Optional[int] = None,
    s3_client=None,
) -> Generator[UKRIPageResponse, None, None]:
    """
    Generator that yields one UKRIPageResponse per page.
    Discovers total page count from the first response so the loop is self-terminating.

    Args:
        max_pages:  Stop after N pages (useful for dev testing — e.g. --pages 10).
        s3_client:  Boto3 S3 client (required if USE_S3=true).

    Yields:
        UKRIPageResponse for each page.

    Example:
        for page in fetch_all_pages(max_pages=5):
            print(page.page, len(page.project))
    """
    first_page = get_page(1, s3_client=s3_client)
    total = first_page.totalPages
    log.info(
        f"UKRI API: {first_page.totalSize:,} total projects across {total:,} pages "
        f"({'limited to ' + str(max_pages) if max_pages else 'full run'})"
    )
    yield first_page

    limit = min(total, max_pages) if max_pages else total
    for page_num in range(2, limit + 1):
        yield get_page(page_num, s3_client=s3_client)
        time.sleep(POLITE_DELAY)

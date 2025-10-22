#!/usr/bin/env python3
"""Utility to download all available INTERPOL yellow notices with full details.

The public INTERPOL API exposes up to 160 records per result page and silently caps
paginated responses at ~320 records.  To ensure that every notice is collected we
query the catalogue by recursively slicing the demographic search parameters until
each request returns a tractable number of results.  The script then hydrates each
notice with the detailed payload provided by the v2 endpoint and stores the merged
output as a CSV file.

The scraper keeps the following guarantees:
* Covers the full age spectrum (0–120) with recursive binary slicing.
* When a single age bucket still exceeds the API cap the search is subdivided by
  sex, ensuring we never hit the server-side limit.
* Deduplicates notices by ``entity_id`` across every segment to avoid duplicates
  introduced by overlapping search parameters.
* Persists progress to disk so long running executions can be resumed.
"""

from __future__ import annotations

import csv
import json
import math
import ssl
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API_SEARCH_URL = "https://ws-public.interpol.int/notices/v1/yellow"
API_DETAILS_URL = "https://ws-public.interpol.int/notices/v2/yellow"

MAX_RESULTS_PER_PAGE = 160
SEGMENT_THRESHOLD = 320  # maximum result size before slicing the segment further
REQUEST_TIMEOUT = 30
RETRY_LIMIT = 5
BACKOFF_FACTOR = 1.5
PAGE_DELAY = 0.2
DETAIL_DELAY = 0.25

OUTPUT_FILE = Path("data/yellow_notices_full.csv")
PROGRESS_FILE = Path("data/yellow_notices_progress.json")

HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "accept": "application/json, text/plain, */*",
    "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "referer": "https://www.interpol.int/en/How-we-work/Notices/View-Yellow-Notices",
}

SEX_SEGMENTS: Sequence[str] = ("M", "F", "U")

OUTPUT_FIELDS: Sequence[str] = (
    "entity_id",
    "name",
    "forename",
    "birth_name",
    "date_of_birth",
    "place_of_birth",
    "country_of_birth",
    "nationalities",
    "sex",
    "height",
    "weight",
    "eyes_colors",
    "hairs",
    "distinguishing_marks",
    "father_forename",
    "father_name",
    "mother_forename",
    "mother_name",
    "date_of_event",
    "place",
    "country",
    "languages",
    "issuing_country",
    "countries_likely_visited",
    "url",
    "images_url",
    "thumbnail_url",
)


class RequestError(RuntimeError):
    """Raised when the HTTP client cannot complete a request."""


@dataclass
class Segment:
    """Represents a slice of the search space for yellow notices."""

    age_min: int
    age_max: int
    sex: Optional[str] = None

    def to_query(self) -> Dict[str, str]:
        params = {
            "ageMin": str(self.age_min),
            "ageMax": str(self.age_max),
        }
        if self.sex:
            params["sexId"] = self.sex
        return params

    def split(self) -> List["Segment"]:
        """Split the segment into smaller chunks to avoid API caps."""

        if self.age_min < self.age_max:
            mid = (self.age_min + self.age_max) // 2
            return [
                Segment(age_min=self.age_min, age_max=mid, sex=self.sex),
                Segment(age_min=mid + 1, age_max=self.age_max, sex=self.sex),
            ]

        if self.sex is None:
            return [Segment(age_min=self.age_min, age_max=self.age_max, sex=sex) for sex in SEX_SEGMENTS]

        raise RequestError(
            "Unable to split segment further. Consider broadening the slicing strategy."
        )

    def label(self) -> str:
        sex = self.sex or "*"
        return f"age={self.age_min}-{self.age_max}|sex={sex}"


class ProgressTracker:
    """Handles persistence for long running scrape sessions."""

    def __init__(self, progress_file: Path) -> None:
        self.progress_file = progress_file
        self.processed_segments: Set[str] = set()
        self._load()

    def _load(self) -> None:
        if not self.progress_file.exists():
            return
        try:
            with self.progress_file.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (json.JSONDecodeError, OSError):
            return

        self.processed_segments = set(data.get("processed_segments", []))

    def mark_done(self, segment: Segment) -> None:
        self.processed_segments.add(segment.label())
        self._flush()

    def is_done(self, segment: Segment) -> bool:
        return segment.label() in self.processed_segments

    def _flush(self) -> None:
        payload = {
            "timestamp": datetime.utcnow().isoformat(),
            "processed_segments": sorted(self.processed_segments),
        }
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
        with self.progress_file.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)


def http_get_json(url: str, params: Optional[Dict[str, str]] = None) -> Dict[str, object]:
    query = f"{url}?{urlencode(params)}" if params else url
    req = Request(query, headers=HEADERS)
    context = ssl.create_default_context()

    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            with urlopen(req, timeout=REQUEST_TIMEOUT, context=context) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                payload = response.read().decode(charset, errors="replace")
            return json.loads(payload)
        except (HTTPError, URLError) as err:
            if attempt == RETRY_LIMIT:
                raise RequestError(f"HTTP request failed after {RETRY_LIMIT} attempts: {err}") from err
            sleep_for = BACKOFF_FACTOR ** attempt
            time.sleep(sleep_for)
        except json.JSONDecodeError as err:
            raise RequestError(f"Unable to parse JSON payload from {query}") from err
    return {}


def query_total(segment: Segment) -> int:
    params = segment.to_query()
    params.update({"page": "1", "resultPerPage": "1"})
    data = http_get_json(API_SEARCH_URL, params)
    return int(data.get("total", 0))


def fetch_segment(segment: Segment, expected_total: int) -> List[Dict[str, object]]:
    notices: List[Dict[str, object]] = []
    total_pages = max(1, math.ceil(expected_total / MAX_RESULTS_PER_PAGE))

    for page in range(1, total_pages + 1):
        params = segment.to_query()
        params.update({"page": str(page), "resultPerPage": str(MAX_RESULTS_PER_PAGE)})
        data = http_get_json(API_SEARCH_URL, params)
        chunk = data.get("_embedded", {}).get("notices", [])
        notices.extend(chunk)
        time.sleep(PAGE_DELAY)
        if len(chunk) < MAX_RESULTS_PER_PAGE:
            break

    return notices


def clean_entity_id(entity_id: str) -> str:
    return entity_id.replace("/", "-")


def fetch_details(entity_id: str) -> Dict[str, object]:
    detail_url = f"{API_DETAILS_URL}/{clean_entity_id(entity_id)}"
    data = http_get_json(detail_url)
    time.sleep(DETAIL_DELAY)
    return data


def safe_get(container: Dict[str, object], key: str) -> str:
    value = container.get(key)
    if value is None:
        return ""
    if isinstance(value, list):
        return ";".join(str(item) for item in value if item is not None)
    return str(value)


def merge_notice(notice: Dict[str, object], details: Dict[str, object]) -> Dict[str, str]:
    links = details.get("_links", {}) if isinstance(details, dict) else {}

    return {
        "entity_id": str(notice.get("entity_id", "")),
        "name": safe_get(details, "name") or safe_get(notice, "name"),
        "forename": safe_get(details, "forename") or safe_get(notice, "forename"),
        "birth_name": safe_get(details, "birth_name"),
        "date_of_birth": safe_get(details, "date_of_birth") or safe_get(notice, "date_of_birth"),
        "place_of_birth": safe_get(details, "place_of_birth"),
        "country_of_birth": safe_get(details, "country_of_birth_id"),
        "nationalities": safe_get(details, "nationalities") or safe_get(notice, "nationalities"),
        "sex": safe_get(details, "sex_id") or safe_get(notice, "sex_id"),
        "height": safe_get(details, "height"),
        "weight": safe_get(details, "weight"),
        "eyes_colors": safe_get(details, "eyes_colors_id"),
        "hairs": safe_get(details, "hairs_id"),
        "distinguishing_marks": safe_get(details, "distinguishing_marks"),
        "father_forename": safe_get(details, "father_forename"),
        "father_name": safe_get(details, "father_name"),
        "mother_forename": safe_get(details, "mother_forename"),
        "mother_name": safe_get(details, "mother_name"),
        "date_of_event": safe_get(details, "date_of_event"),
        "place": safe_get(details, "place"),
        "country": safe_get(details, "country"),
        "languages": safe_get(details, "languages_spoken_ids"),
        "issuing_country": safe_get(details, "issuing_country"),
        "countries_likely_visited": safe_get(details, "countries_likely_to_be_visited"),
        "url": safe_get(links.get("self", {}) if isinstance(links, dict) else {}, "href"),
        "images_url": safe_get(links.get("images", {}) if isinstance(links, dict) else {}, "href"),
        "thumbnail_url": safe_get(links.get("thumbnail", {}) if isinstance(links, dict) else {}, "href"),
    }


def write_csv(records: Iterable[Dict[str, str]]) -> None:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(OUTPUT_FIELDS))
        writer.writeheader()
        for row in records:
            writer.writerow(row)


def collect_notices() -> List[Dict[str, str]]:
    tracker = ProgressTracker(PROGRESS_FILE)
    pending: List[Segment] = [Segment(age_min=0, age_max=120)]
    seen_ids: Set[str] = set()
    aggregated: List[Dict[str, str]] = []

    while pending:
        segment = pending.pop()
        if tracker.is_done(segment):
            continue

        total = query_total(segment)
        if total == 0:
            tracker.mark_done(segment)
            continue

        if total > SEGMENT_THRESHOLD:
            pending.extend(segment.split())
            continue

        raw_notices = fetch_segment(segment, total)
        print(f"Segment {segment.label()} → {len(raw_notices)} notices")

        for notice in raw_notices:
            entity_id = str(notice.get("entity_id", ""))
            if not entity_id or entity_id in seen_ids:
                continue
            details = fetch_details(entity_id)
            record = merge_notice(notice, details)
            aggregated.append(record)
            seen_ids.add(entity_id)
        tracker.mark_done(segment)

        if len(aggregated) % 200 == 0:
            write_csv(aggregated)

    return aggregated


def run() -> None:
    print("🟡 INTERPOL Yellow Notice Scraper")
    print("=" * 60)
    start = time.time()

    records = collect_notices()
    write_csv(records)

    elapsed = time.time() - start
    try:
        PROGRESS_FILE.unlink()
    except OSError:
        pass
    print("=" * 60)
    print(f"✅ Downloaded {len(records):,} notices")
    print(f"💾 Output saved to {OUTPUT_FILE}")
    print(f"⏱️  Total runtime: {elapsed / 60:.1f} minutes")


if __name__ == "__main__":
    try:
        run()
    except RequestError as err:
        print(f"❌ Scraper failed: {err}")
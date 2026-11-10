"""Small client for the public USAspending.gov API (no API key required).

Handles pagination, retries with backoff, the search API's result cap, and
flattening nested fields so each record becomes one flat dict.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

import requests

BASE_URL = "https://api.usaspending.gov"
# The search endpoints stop returning results after 10,000 rows per query
# (100 pages x 100). Queries that reach it must be split into smaller ones.
DEFAULT_MAX_RESULTS = 10_000
log = logging.getLogger(__name__)


@dataclass
class FetchResult:
    records: list[dict[str, Any]] = field(default_factory=list)
    capped: bool = False  # True if the query hit the result cap and more rows exist


class USAspendingClient:
    def __init__(self, base_url: str = BASE_URL, session: requests.Session | None = None,
                 max_retries: int = 5, backoff_seconds: float = 2.0, timeout: int = 60):
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.timeout = timeout

    def _post(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.post(url, json=payload, timeout=self.timeout)
                # Retry on rate limiting and server errors; fail fast on bad requests.
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise requests.HTTPError(f"retryable status {resp.status_code}", response=resp)
                resp.raise_for_status()
                return resp.json()
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                retryable = status is None or status == 429 or status >= 500
                if not retryable or attempt == self.max_retries:
                    raise
                wait = self.backoff_seconds * 2 ** (attempt - 1)
                log.warning("Attempt %s failed (%s); retrying in %.1fs", attempt, exc, wait)
                time.sleep(wait)
        raise RuntimeError("unreachable")

    def fetch(self, *, endpoint: str, award_type_codes: list[str], fields: list[str],
              start_date: str, end_date: str, date_type: str | None = None,
              sort: str = "Award Amount", page_size: int = 100,
              amount_range: tuple[float | None, float | None] | None = None,
              extra_filters: dict[str, Any] | None = None,
              max_results: int = DEFAULT_MAX_RESULTS,
              max_pages: int | None = None) -> FetchResult:
        """Fetch all records for one query, stopping at the API's result cap.

        `date_type` is only sent when set (the award search accepts it; the transaction
        search filters on action date by default). `amount_range` = (lower, upper),
        either side None for open-ended, is used to split queries that hit the cap.
        """
        period = {"start_date": start_date, "end_date": end_date}
        if date_type:
            period["date_type"] = date_type
        # extra_filters narrows the scope, e.g. {"agencies": [...]} to focus on chosen agencies.
        filters: dict[str, Any] = {**(extra_filters or {}),
                                   "award_type_codes": award_type_codes, "time_period": [period]}
        if amount_range:
            lo, hi = amount_range
            bound = {}
            if lo is not None:
                bound["lower_bound"] = lo
            if hi is not None:
                bound["upper_bound"] = hi
            filters["award_amounts"] = [bound]

        result = FetchResult()
        last_page = max(1, max_results // page_size)
        page = 1
        while True:
            body = self._post(endpoint, {
                "filters": filters, "fields": fields, "page": page,
                "limit": page_size, "sort": sort, "order": "desc",
            })
            results = body.get("results", [])
            result.records.extend(results)
            log.info("page %s: %s records", page, len(results))

            has_next = body.get("page_metadata", {}).get("hasNext", False)
            if not results or not has_next:
                break
            if max_pages and page >= max_pages:
                break
            if page >= last_page:
                result.capped = True
                break
            page += 1
        return result


def _snake(name: str) -> str:
    return name.strip().lower().replace(" ", "_").replace("-", "_")


def flatten_award(record: dict[str, Any]) -> dict[str, Any]:
    """Turn an API record into a flat dict with snake_case keys.

    NAICS / PSC come back either as a plain code or as {"code", "description"};
    both shapes are normalized to `<field>_code` and `<field>_description`.
    """
    flat: dict[str, Any] = {}
    for key, value in record.items():
        col = _snake(key)
        if isinstance(value, dict) and ("code" in value or "description" in value):
            flat[f"{col}_code"] = value.get("code")
            flat[f"{col}_description"] = value.get("description")
        elif col in ("naics", "psc"):
            flat[f"{col}_code"] = value
            flat[f"{col}_description"] = None
        else:
            flat[col] = value
    return flat

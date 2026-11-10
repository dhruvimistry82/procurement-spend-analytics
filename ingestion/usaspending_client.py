"""Small client for the public USAspending.gov API (no API key required).

Handles pagination, retries with backoff, and flattening nested fields so each
award becomes one flat record ready to land as JSON Lines.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Iterator

import requests

BASE_URL = "https://api.usaspending.gov"
log = logging.getLogger(__name__)


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

    def iter_awards(self, *, endpoint: str, award_type_codes: list[str], fields: list[str],
                    start_date: str, end_date: str, date_type: str | None = None,
                    sort: str = "Award Amount", page_size: int = 100,
                    max_pages: int | None = None) -> Iterator[dict[str, Any]]:
        """Yield raw records (awards or transactions) for a date window, following pagination.

        `date_type` is only sent when set: the award search accepts it, the transaction
        search filters on action date by default.
        """
        period = {"start_date": start_date, "end_date": end_date}
        if date_type:
            period["date_type"] = date_type
        page = 1
        while True:
            payload = {
                "filters": {"award_type_codes": award_type_codes, "time_period": [period]},
                "fields": fields,
                "page": page,
                "limit": page_size,
                "sort": sort,
                "order": "desc",
            }
            body = self._post(endpoint, payload)
            results = body.get("results", [])
            log.info("page %s: %s records", page, len(results))
            yield from results

            has_next = body.get("page_metadata", {}).get("hasNext", False)
            if not has_next or not results or (max_pages and page >= max_pages):
                break
            page += 1


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

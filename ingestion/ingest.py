"""Pull awards and transactions from USAspending and land them as JSON Lines files.

Examples
--------
Land one week on your laptop (one file per source per day):
    python -m ingestion.ingest --start 2026-10-01 --end 2026-10-07

Land straight into a Databricks Unity Catalog Volume (uses DATABRICKS_HOST / DATABRICKS_TOKEN):
    python -m ingestion.ingest --start 2026-10-01 --end 2026-10-07 --target volume

How large windows are handled
-----------------------------
The search API returns at most 10,000 rows per query. So each run:
  1. queries one day at a time, and writes each day's file as soon as it's done
     (a failure on day 5 keeps days 1-4);
  2. if a day still hits the cap, re-queries it in award-amount bands, splitting
     any band that is still capped, then removes duplicates from overlapping bands;
  3. if the API keeps failing for a day, records it and moves on. Rerunning the same
     command skips days already landed and retries only the missing ones.
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
import yaml

from ingestion.usaspending_client import FetchResult, USAspendingClient, flatten_award

log = logging.getLogger("ingest")
REPO_ROOT = Path(__file__).resolve().parents[1]

# Amount bands used when a day hits the cap. None = open-ended.
DEFAULT_BANDS = [None, 0, 1_000, 10_000, 25_000, 100_000, 250_000, 1_000_000, 10_000_000, None]
MAX_SPLIT_DEPTH = 8


def load_config(path: Path) -> dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f)


def daterange(start: str, end: str):
    d, last = date.fromisoformat(start), date.fromisoformat(end)
    while d <= last:
        yield d.isoformat()
        d += timedelta(days=1)


def bands_from(edges: list) -> list[tuple]:
    return list(zip(edges[:-1], edges[1:]))


def bisect(band: tuple) -> list[tuple] | None:
    lo, hi = band
    if lo is None or hi is None or hi - lo < 1:
        return None  # open-ended or already narrower than $1: can't split further
    mid = round((lo + hi) / 2, 2)
    return [(lo, mid), (mid, hi)]


def fetch_day(client: USAspendingClient, source: dict[str, Any], day: str,
              max_pages: int | None = None) -> tuple[list[dict[str, Any]], int]:
    """All raw records for one source and one day, splitting by amount if capped.

    Returns (records, number of queries that stayed capped and may be incomplete).
    """
    def run(amount_range=None) -> FetchResult:
        return client.fetch(
            endpoint=source["endpoint"], award_type_codes=source["award_type_codes"],
            fields=source["fields"], start_date=day, end_date=day,
            date_type=source.get("date_type"), sort=source.get("sort", "Award Amount"),
            page_size=source.get("page_size", 100), amount_range=amount_range,
            extra_filters=source.get("filters"), max_pages=max_pages,
        )

    first = run()
    if not first.capped:
        return first.records, 0

    log.warning("%s %s hit the result cap; splitting by amount", source["name"], day)
    records: list[dict[str, Any]] = []
    incomplete = 0
    queue = [(band, 1) for band in bands_from(source.get("split_bands", DEFAULT_BANDS))]
    while queue:
        band, depth = queue.pop(0)
        res = run(band)
        halves = bisect(band) if res.capped and depth < MAX_SPLIT_DEPTH else None
        if halves:
            queue.extend((h, depth + 1) for h in halves)  # re-query this band in two finer halves
            continue
        records.extend(res.records)
        if res.capped:
            incomplete += 1
            log.error("%s %s band %s still capped; data may be incomplete", source["name"], day, band)
    return records, incomplete


def dedupe(records: list[dict[str, Any]], keys: list[str]) -> list[dict[str, Any]]:
    """Band edges overlap, so the same row can appear twice; keep the first."""
    seen, out = set(), []
    for r in records:
        k = tuple(r.get(key) for key in keys)
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out


def to_jsonl(records: list[dict[str, Any]]) -> bytes:
    return "".join(json.dumps(r, default=str) + "\n" for r in records).encode("utf-8")


def is_day_file(name: str, day: str) -> bool:
    """True for files this script lands for `day` (<day>_<run_id>.jsonl), ignoring anything else."""
    return re.fullmatch(rf"{re.escape(day)}_\d{{8}}T\d{{6}}Z\.jsonl", name) is not None


class LocalTarget:
    """Writes landing files to a folder on this machine."""

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir

    def exists(self, source: str, day: str) -> bool:
        return any(is_day_file(p.name, day) for p in (self.base_dir / source).glob(f"{day}_*.jsonl"))

    def write(self, data: bytes, rel_path: str) -> str:
        out = self.base_dir / rel_path
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(data)
        return str(out)


class VolumeTarget:
    """Writes landing files to a Databricks Unity Catalog Volume (DATABRICKS_HOST / DATABRICKS_TOKEN)."""

    def __init__(self, volume_dir: str):
        from databricks.sdk import WorkspaceClient  # imported lazily: optional dependency

        self.ws = WorkspaceClient()
        self.volume_dir = volume_dir.rstrip("/")

    def exists(self, source: str, day: str) -> bool:
        try:
            entries = self.ws.files.list_directory_contents(f"{self.volume_dir}/{source}")
            return any(e.name and is_day_file(e.name, day) for e in entries)
        except Exception:  # folder doesn't exist yet
            return False

    def write(self, data: bytes, rel_path: str) -> str:
        path = f"{self.volume_dir}/{rel_path}"
        self.ws.files.upload(path, io.BytesIO(data), overwrite=False)
        return path


def ingest(client: USAspendingClient, cfg: dict[str, Any], start: str, end: str, target, run_id: str,
           max_pages: int | None = None, resume: bool = True) -> dict[str, Any]:
    """Land every active source for every day in [start, end].

    Each (source, day) is independent: if the API fails for one, it is recorded and the
    run moves on. With resume=True, days that already have a landing file are skipped,
    so rerunning the same command only fetches what's missing.
    """
    summary: dict[str, Any] = {"rows": {}, "skipped": [], "failed": []}
    for day in daterange(start, end):
        for source in cfg["sources"]:
            if not source.get("active", True):
                continue
            name = source["name"]
            if resume and target.exists(name, day):
                log.info("%s %s: already landed, skipping", name, day)
                summary["skipped"].append(f"{name} {day}")
                continue
            try:
                raw, incomplete = fetch_day(client, source, day, max_pages)
            except requests.RequestException as exc:
                log.error("%s %s: FAILED after retries (%s); continuing", name, day, exc)
                summary["failed"].append(f"{name} {day}")
                continue
            rows = []
            for r in dedupe(raw, source.get("dedup_keys", ["generated_internal_id"])):
                rec = flatten_award(r)
                rec.update({"_source": name, "_window_start": day, "_window_end": day,
                            "_run_id": run_id, "_possibly_incomplete": incomplete > 0})
                rows.append(rec)
            summary["rows"][name] = summary["rows"].get(name, 0) + len(rows)
            if not rows:
                log.info("%s %s: no records", name, day)
                continue
            dest = target.write(to_jsonl(rows), f"{name}/{day}_{run_id}.jsonl")
            log.info("%s %s: wrote %s records to %s", name, day, len(rows), dest)
    return summary


def main(argv: list[str] | None = None) -> int:
    yesterday = date.today() - timedelta(days=1)
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", default=(yesterday - timedelta(days=6)).isoformat())
    parser.add_argument("--end", default=yesterday.isoformat())
    parser.add_argument("--config", default=str(REPO_ROOT / "config" / "sources.yml"))
    parser.add_argument("--target", choices=["local", "volume"], default="local")
    parser.add_argument("--max-pages", type=int, default=None, help="cap pages per query (for quick tests)")
    parser.add_argument("--no-resume", action="store_true", help="re-fetch days that were already landed")
    parser.add_argument("--page-delay", type=float, default=0.3, help="seconds to pause between API pages")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(Path(args.config))
    if args.target == "volume":
        target = VolumeTarget(cfg["landing"]["volume_dir"])
    else:
        target = LocalTarget(REPO_ROOT / cfg["landing"]["local_dir"])
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    client = USAspendingClient(page_delay_seconds=args.page_delay)

    summary = ingest(client, cfg, args.start, args.end, target, run_id, args.max_pages,
                     resume=not args.no_resume)
    log.info("rows written: %s", summary["rows"])
    if summary["skipped"]:
        log.info("skipped (already landed): %s", ", ".join(summary["skipped"]))
    if summary["failed"]:
        log.error("FAILED: %s -- rerun the same command to retry just these.", ", ".join(summary["failed"]))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

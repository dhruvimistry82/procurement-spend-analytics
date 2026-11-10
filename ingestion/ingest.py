"""Pull awards from USAspending and land them as JSON Lines files.

Examples
--------
Land one week of contracts on your laptop:
    python -m ingestion.ingest --start 2026-10-01 --end 2026-10-07

Land straight into a Databricks Unity Catalog Volume (uses DATABRICKS_HOST / DATABRICKS_TOKEN):
    python -m ingestion.ingest --start 2026-10-01 --end 2026-10-07 --target volume

Each run writes one immutable file per source, named by window and run time, so
Auto Loader picks up only new files and reruns never overwrite history.
"""
from __future__ import annotations

import argparse
import io
import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from ingestion.usaspending_client import USAspendingClient, flatten_award

log = logging.getLogger("ingest")
REPO_ROOT = Path(__file__).resolve().parents[1]


def load_config(path: Path) -> dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f)


def build_records(client: USAspendingClient, source: dict[str, Any], start: str, end: str,
                  run_id: str, max_pages: int | None = None) -> list[dict[str, Any]]:
    records = []
    for raw in client.iter_awards(
        endpoint=source["endpoint"], award_type_codes=source["award_type_codes"],
        fields=source["fields"], start_date=start, end_date=end,
        date_type=source.get("date_type"), sort=source.get("sort", "Award Amount"),
        page_size=source.get("page_size", 100), max_pages=max_pages,
    ):
        rec = flatten_award(raw)
        rec["_source"] = source["name"]
        rec["_window_start"] = start
        rec["_window_end"] = end
        rec["_run_id"] = run_id
        records.append(rec)
    return records


def to_jsonl(records: list[dict[str, Any]]) -> bytes:
    return "".join(json.dumps(r, default=str) + "\n" for r in records).encode("utf-8")


def write_local(data: bytes, base_dir: Path, rel_path: str) -> str:
    out = base_dir / rel_path
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(data)
    return str(out)


def write_volume(data: bytes, volume_dir: str, rel_path: str) -> str:
    from databricks.sdk import WorkspaceClient  # imported lazily: optional dependency

    path = f"{volume_dir.rstrip('/')}/{rel_path}"
    WorkspaceClient().files.upload(path, io.BytesIO(data), overwrite=False)
    return path


def main(argv: list[str] | None = None) -> None:
    yesterday = date.today() - timedelta(days=1)
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", default=(yesterday - timedelta(days=6)).isoformat())
    parser.add_argument("--end", default=yesterday.isoformat())
    parser.add_argument("--config", default=str(REPO_ROOT / "config" / "sources.yml"))
    parser.add_argument("--target", choices=["local", "volume"], default="local")
    parser.add_argument("--max-pages", type=int, default=None, help="cap pages per source (for quick tests)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(Path(args.config))
    client = USAspendingClient()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    for source in cfg["sources"]:
        if not source.get("active", True):
            log.info("skipping inactive source %s", source["name"])
            continue
        records = build_records(client, source, args.start, args.end, run_id, args.max_pages)
        if not records:
            log.info("%s: no records for %s..%s", source["name"], args.start, args.end)
            continue
        rel = f"{source['name']}/{args.start}_{args.end}_{run_id}.jsonl"
        data = to_jsonl(records)
        if args.target == "volume":
            dest = write_volume(data, cfg["landing"]["volume_dir"], rel)
        else:
            dest = write_local(data, REPO_ROOT / cfg["landing"]["local_dir"], rel)
        log.info("%s: wrote %s records to %s", source["name"], len(records), dest)


if __name__ == "__main__":
    main()

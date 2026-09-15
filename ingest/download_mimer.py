"""
Download production statistics from Svenska kraftnät's Mimer as raw files.

Files are stored exactly as delivered: no parsing, no re-encoding. Every
download is logged to manifest.jsonl with URL, timestamp, size and
SHA-256, so the bronze layer can trace any row back to a specific file.

Some combinations of bidding zone and production type do not exist.
Nuclear in SE1 is one: Mimer responds with only a Summa (total) row and
no data. That is not an error, it is information. SCB writes 0 for the
same combination. Empty responses are therefore stored under empty/ and
logged with status "no_data", but kept out of bronze.

Mimer publishes production data through 2025-03-17. Later periods are
published by eSett and fetched separately.

Run from the repository root:  python ingest/download_mimer.py
"""
import hashlib
import json
import time
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_URL = "https://mimer.svk.se/ProductionConsumption/DownloadText"
OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "mimer"
EMPTY_DIR = OUT_DIR / "empty"
MANIFEST = OUT_DIR / "manifest.jsonl"

# Mimer uses SN1-SN4 for the bidding zones, SCB uses SE1-SE4.
AREAS = {"SN1": "SE1", "SN2": "SE2", "SN3": "SE3", "SN4": "SE4"}

# Codes taken from Mimer's own download links. The production type exists
# only in the URL, never inside the file, which is why the night-time
# check in the silver layer matters.
PRODUCTION_TYPES = {
    "KK": "nuclear",
    "VI": "wind",
    "VA": "hydro",
    "SE": "solar",
    "OK": "thermal",
    "OP": "unspecified",
}

# One file per full year. The end date covers the whole final day.
PERIODS = [
    (date(2021, 1, 1), date(2021, 12, 31)),
    (date(2022, 1, 1), date(2022, 12, 31)),
    (date(2023, 1, 1), date(2023, 12, 31)),
    (date(2024, 1, 1), date(2024, 12, 31)),
    (date(2025, 1, 1), date(2025, 2, 28)),
]

PAUSE_SECONDS = 1.5  # be kind to a public service


def mimer_date(d: date) -> str:
    # Mimer expects US date format in the URL, despite ISO inside the file.
    return d.strftime("%m/%d/%Y 00:00:00")


def log(entry: dict) -> None:
    with MANIFEST.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def download(area: str, sort_id: str, start: date, end: date) -> None:
    name = f"{AREAS[area]}_{PRODUCTION_TYPES[sort_id]}_{start:%Y%m%d}_{end:%Y%m%d}.csv"
    if (OUT_DIR / name).exists() or (EMPTY_DIR / name).exists():
        print(f"already there  {name}")
        return

    url = BASE_URL + "?" + urlencode({
        "PeriodFrom": mimer_date(start),
        "PeriodTo": mimer_date(end),
        "ConstraintAreaId": area,
        "ProductionSortId": sort_id,
    })
    request = Request(url, headers={"User-Agent": "sweden-power-lakehouse/0.1"})
    with urlopen(request, timeout=60) as response:
        body = response.read()

    text = body.decode("utf-8-sig", errors="replace")
    has_header = text.lstrip().startswith("Period;")
    is_empty = text.strip().startswith("Summa;")

    if not has_header and not is_empty:
        raise RuntimeError(f"Unexpected response for {name}: {text[:64]!r}. Check the codes.")

    # Data rows exclude the header, the Summa row and blank lines.
    rows = [
        line for line in text.splitlines()
        if line.strip() and not line.startswith("Period;") and not line.startswith("Summa;")
    ]

    target_dir = OUT_DIR if has_header else EMPTY_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / name).write_bytes(body)

    log({
        "file": name,
        "status": "ok" if has_header else "no_data",
        "source": "mimer",
        "url": url,
        "area_code": area,
        "area": AREAS[area],
        "production_sort_id": sort_id,
        "production_type": PRODUCTION_TYPES[sort_id],
        "period_from": start.isoformat(),
        "period_to": end.isoformat(),
        "downloaded_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "bytes": len(body),
        "data_rows": len(rows),
        "sha256": hashlib.sha256(body).hexdigest(),
    })

    if has_header:
        print(f"downloaded     {name}  ({len(rows):,} rows)")
    else:
        print(f"no data        {name}  (combination does not exist)")
    time.sleep(PAUSE_SECONDS)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for area in AREAS:
        for sort_id in PRODUCTION_TYPES:
            for start, end in PERIODS:
                download(area, sort_id, start, end)


if __name__ == "__main__":
    main()

"""
Hämtar produktionsstatistik från Svenska kraftnäts Mimer som råfiler.

Filerna sparas exakt som de levereras: ingen tolkning, ingen omkodning.
Varje hämtning loggas i manifest.jsonl med URL, tidpunkt, storlek och
SHA-256, så att bronze-lagret kan spåra varje rad till en specifik fil.

Vissa kombinationer av elområde och kraftslag finns inte. Kärnkraft i SE1
är ett exempel: Mimer svarar då med enbart en Summa-rad och inga data.
Det är inte ett fel, utan en uppgift i sig. SCB skriver 0 för samma
kombination. Tomma svar sparas därför under empty/ och registreras i
manifestet med status "no_data", men lämnas utanför bronze.

Mimer har produktionsdata till och med 2025-03-17. Senare perioder
publiceras av eSett och hämtas separat.

Kör från repots rot:  python ingest/download_mimer.py
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

AREAS = {"SN1": "SE1", "SN2": "SE2", "SN3": "SE3", "SN4": "SE4"}

# Koder från Mimers nedladdningslänkar. KK och VI är verifierade.
# Fyll i resten genom att kopiera länken bakom "Spara som CSV" i Mimer.
PRODUCTION_TYPES = {
    "KK": "karnkraft",
    "VI": "vindkraft",
    "VA": "vattenkraft",
    "SE": "solkraft",
    "OK": "ovrig_varmekraft",
    "OP": "uppmatt_ospecificerad",
}

# Hela år per fil. Slutdatumet räknas inklusive hela dygnet.
PERIODS = [
    (date(2021, 1, 1), date(2021, 12, 31)),
    (date(2022, 1, 1), date(2022, 12, 31)),
    (date(2023, 1, 1), date(2023, 12, 31)),
    (date(2024, 1, 1), date(2024, 12, 31)),
    (date(2025, 1, 1), date(2025, 2, 28)),
]

PAUSE_SECONDS = 1.5  # var snäll mot en offentlig tjänst


def mimer_date(d: date) -> str:
    # Mimer vill ha amerikanskt datumformat i URL:en, trots ISO i filen.
    return d.strftime("%m/%d/%Y 00:00:00")


def log(entry: dict) -> None:
    with MANIFEST.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def download(area: str, sort_id: str, start: date, end: date) -> None:
    name = f"{AREAS[area]}_{PRODUCTION_TYPES[sort_id]}_{start:%Y%m%d}_{end:%Y%m%d}.csv"
    if (OUT_DIR / name).exists() or (EMPTY_DIR / name).exists():
        print(f"finns redan  {name}")
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
        raise RuntimeError(f"Oväntat svar för {name}: {text[:64]!r}. Kontrollera koderna.")

    # Datarader räknas exklusive rubrik, Summa-rad och tomma rader.
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
        print(f"hämtad       {name}  ({len(rows):,} rader)")
    else:
        print(f"inga data    {name}  (kombinationen finns inte)")
    time.sleep(PAUSE_SECONDS)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for area in AREAS:
        for sort_id in PRODUCTION_TYPES:
            for start, end in PERIODS:
                download(area, sort_id, start, end)


if __name__ == "__main__":
    main()

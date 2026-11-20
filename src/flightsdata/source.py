"""NYC flights 2013: every departure from JFK, LGA and EWR in 2013 (336,776 flights),
plus airlines, airports, planes and hourly weather.

Original sources: US Bureau of Transportation Statistics (on-time data), FAA (aircraft
registry) and NOAA (weather), packaged as `nycflights13` (CC0). We download the
package from PyPI, check its sha256, and unpack the CSVs.

    python -m flightsdata.source        # -> data/raw/nycflights13/*.csv
"""
from __future__ import annotations

import hashlib
import io
import tarfile
import zipfile
from pathlib import Path

import requests

PYPI = "https://pypi.org/pypi/nycflights13/0.0.3/json"
ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw" / "nycflights13"
TABLES = ["flights", "airlines", "airports", "planes", "weather"]


def download(dest: Path = RAW) -> Path:
    if all((dest / f"{t}.csv").exists() for t in TABLES):
        return dest
    meta = requests.get(PYPI, timeout=30).json()
    sdist = next(u for u in meta["urls"] if u["packagetype"] == "sdist")
    blob = requests.get(sdist["url"], timeout=120).content
    if hashlib.sha256(blob).hexdigest() != sdist["digests"]["sha256"]:
        raise ValueError("sha256 mismatch for nycflights13 sdist")
    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(blob)) as tar:
        for m in tar.getmembers():
            name = Path(m.name).name
            if "/data/" not in m.name or not m.isfile():
                continue
            data = tar.extractfile(m).read()
            if name.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(data)) as z:
                    for inner in z.namelist():
                        if inner.endswith(".csv") and not inner.startswith("__MACOSX"):
                            (dest / Path(inner).name).write_bytes(z.read(inner))
            elif name.endswith(".csv"):
                (dest / name).write_bytes(data)
    return dest


if __name__ == "__main__":
    p = download()
    for t in TABLES:
        print(t, sum(1 for _ in open(p / f"{t}.csv")) - 1, "rows")

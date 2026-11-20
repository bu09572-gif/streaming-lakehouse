"""Turn the 2013 flights file into the stream of changes an airline's operations database
would have seen during the year.

Each flight becomes up to three changes, at the times they really happened:

  insert    the day before departure, 00:00 New York time   status = scheduled
  update    at the actual departure (scheduled + dep_delay)  status = departed
  update    at the actual arrival (scheduled + arr_delay)    status = arrived
  or
  update    at the scheduled departure, if it never left     status = cancelled

The publish time for the schedule (the day before) is my choice. The departure and
arrival times come from the data: scheduled times converted from each airport's time
zone to UTC, plus the recorded delays.
"""
from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW = Path(os.environ.get("RAW_DIR", ROOT / "data" / "raw" / "nycflights13"))
# These four destinations are missing from airports.csv; all are on Atlantic time.
EXTRA_TZ = {"SJU": "America/Puerto_Rico", "BQN": "America/Puerto_Rico",
            "STT": "America/St_Thomas", "PSE": "America/Puerto_Rico"}
COLS = ["flight_id", "carrier", "flight", "tailnum", "origin", "dest", "sched_dep_utc",
        "status", "dep_delay", "arr_delay", "air_time"]


def _local_to_utc(date: pd.Series, hhmm: pd.Series, tz: pd.Series) -> pd.Series:
    hhmm = hhmm.astype(int)
    naive = date + pd.to_timedelta(hhmm // 100, "h") + pd.to_timedelta(hhmm % 100, "min")
    out = pd.Series(pd.NaT, index=date.index, dtype="datetime64[ns, UTC]")
    for zone, idx in tz.groupby(tz).groups.items():
        out.loc[idx] = (naive.loc[idx].dt.tz_localize(zone, ambiguous=True, nonexistent="shift_forward")
                        .dt.tz_convert("UTC"))
    return out


def load_flights(raw: Path = RAW, month: int | None = None) -> pd.DataFrame:
    f = pd.read_csv(raw / "flights.csv", na_values=["NA"], keep_default_na=False,
                    dtype={"tailnum": "string", "carrier": "string"})
    f.insert(0, "flight_id", np.arange(1, len(f) + 1))
    if month:
        f = f[f.month == month].reset_index(drop=True)
    ap = pd.read_csv(raw / "airports.csv").set_index("faa").tzone.to_dict() | EXTRA_TZ
    date = pd.to_datetime(f[["year", "month", "day"]])
    f["sched_dep_utc"] = _local_to_utc(date, f.sched_dep_time, pd.Series("America/New_York", index=f.index))
    sched_arr = _local_to_utc(date, f.sched_arr_time, f.dest.map(ap))
    # scheduled arrival is on the next local day when it would otherwise be before departure
    sched_arr = sched_arr.where(sched_arr > f.sched_dep_utc, sched_arr + pd.Timedelta(days=1))
    f["sched_arr_utc"] = sched_arr
    f["publish_utc"] = (date - pd.Timedelta(days=1)).dt.tz_localize("America/New_York").dt.tz_convert("UTC")
    return f


def build_events(f: pd.DataFrame) -> pd.DataFrame:
    """One row per change, sorted by the time it happens."""
    base = f[["flight_id", "carrier", "flight", "tailnum", "origin", "dest", "sched_dep_utc"]]
    blank = {"dep_delay": np.nan, "arr_delay": np.nan, "air_time": np.nan}

    ins = base.assign(op="i", at=f.publish_utc, status="scheduled", **blank)
    flew = f.dep_delay.notna()
    dep = base[flew].assign(op="u", at=f.sched_dep_utc[flew] + pd.to_timedelta(f.dep_delay[flew], "min"),
                            status="departed", dep_delay=f.dep_delay[flew], arr_delay=np.nan, air_time=np.nan)
    landed = flew & f.arr_delay.notna()
    arr_at = f.sched_arr_utc[landed] + pd.to_timedelta(f.arr_delay[landed], "min")
    arr = base[landed].assign(op="u", at=arr_at, status="arrived", dep_delay=f.dep_delay[landed],
                              arr_delay=f.arr_delay[landed], air_time=f.air_time[landed])
    canc = base[~flew].assign(op="u", at=f.sched_dep_utc[~flew], status="cancelled", **blank)

    ev = pd.concat([ins, dep, arr, canc], ignore_index=True)
    ev["step"] = ev.status.map({"scheduled": 0, "departed": 1, "cancelled": 1, "arrived": 2})
    ev = ev.sort_values(["at", "flight_id", "step"], kind="stable").reset_index(drop=True)
    return ev[["op", "at", *COLS]]


def order_problems(ev: pd.DataFrame) -> int:
    """Flights whose changes, sorted by time, are not in lifecycle order (should be 0)."""
    steps = ev.groupby("flight_id", sort=False).status.agg(list)
    ok = {("scheduled",), ("scheduled", "departed"), ("scheduled", "departed", "arrived"),
          ("scheduled", "cancelled")}
    return int((~steps.map(lambda s: tuple(s) in ok)).sum())

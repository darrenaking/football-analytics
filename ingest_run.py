"""Bulk ingestion driver: ingest selected competitions into per-league Parquet.

Enumerates match_ids by filtering the cached match-context index, then runs each
match through pipeline.match_rows. Per-match errors are caught and logged (some
listed matches may lack event files). Already-written leagues are skipped, so the
run is resumable.

Output: data/{provider}/{competition}__{season}.parquet

Run: .venv/bin/python ingest_run.py
"""
import os
import sys
import time

import polars as pl

import match_context
from pipeline import match_rows, SCHEMA

OUT_DIR = "data"

# (provider, {competition names}, season) to ingest. Men's top-5 leagues:
#   StatsBomb 2015/16  +  Wyscout 2017/18
TARGETS = [
    ("statsbomb", {"Premier League", "La Liga", "Serie A", "Ligue 1", "1. Bundesliga"}, "2015/2016"),
    ("wyscout", {"Premier League", "La Liga", "Serie A", "Ligue 1", "Bundesliga"}, "2017/2018"),
]


def safe(s):
    return (s or "unknown").replace("/", "-").replace(" ", "_").replace(".", "")


def match_ids_for(provider, competitions, season):
    index = match_context._load_or_build(provider, match_context._BUILDERS[provider])
    return sorted(
        mid for mid, ctx in index.items()
        if ctx.get("competition") in competitions and ctx.get("season") == season
    )


def ingest_league(provider, competition, season, match_ids):
    out = os.path.join(OUT_DIR, provider, f"{safe(competition)}__{safe(season)}.parquet")
    if os.path.exists(out) and pl.read_parquet(out, columns=["event_id"]).height > 0:
        print(f"  SKIP (exists, non-empty): {out}", flush=True)
        return
    os.makedirs(os.path.dirname(out), exist_ok=True)

    all_rows, ok, failed, fail_examples = [], 0, 0, []
    t0 = time.time()
    for i, mid in enumerate(match_ids, 1):
        try:
            rows, _, _ = match_rows(provider, mid)
            all_rows.extend(rows)
            ok += 1
        except Exception as ex:
            failed += 1
            if len(fail_examples) < 3:
                fail_examples.append(f"{mid}: {type(ex).__name__} {ex}")
        if i % 20 == 0 or i == len(match_ids):
            rate = i / (time.time() - t0)
            print(f"    {provider}/{competition} {i}/{len(match_ids)} "
                  f"ok={ok} failed={failed} rows={len(all_rows)} ({rate:.1f} match/s)", flush=True)

    df = pl.DataFrame(all_rows, schema=SCHEMA)
    df.write_parquet(out)
    print(f"  WROTE {df.height} rows -> {out} (ok={ok} failed={failed})", flush=True)
    if fail_examples:
        print(f"    sample failures: {fail_examples}", flush=True)


def main():
    grand = 0
    for provider, comps, season in TARGETS:
        # group the flat match list back by competition for per-league files
        index = match_context._load_or_build(provider, match_context._BUILDERS[provider])
        for competition in sorted(comps):
            mids = [m for m in match_ids_for(provider, {competition}, season)]
            if not mids:
                print(f"[{provider}] {competition} {season}: 0 matches, skipping", flush=True)
                continue
            print(f"[{provider}] {competition} {season}: {len(mids)} matches", flush=True)
            ingest_league(provider, competition, season, mids)
            grand += len(mids)
    print(f"\nDONE. Target matches across all leagues: {grand}", flush=True)


if __name__ == "__main__":
    main()

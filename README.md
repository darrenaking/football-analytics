# football-analytics

Football (soccer) event-data pipeline. Ingests open event data from multiple
providers into a single, unified, pitch-relative schema for feature engineering
and experimentation.

## Data sources

- **StatsBomb Open Data** — richest schema (carries, pressures, 360); selective competitions.
- **Wyscout / Pappalardo open dataset** — broad league coverage (2017/18 top-5, WC2018, Euro2016).

Both are normalized with [kloppy](https://github.com/PySport/kloppy), which preserves
the original provider record via `raw_event`.

## Design decisions

- **Normalization layer:** kloppy (faithful typed model; keeps `raw_event` so nothing is lost).
- **Coordinates:** stored pitch-relative `0–1` (kloppy system). Open event data is inherently
  pitch-relative and real pitch dimensions aren't provided, so absolute meters are an
  approximation — computed on the fly (× 105 / × 68) only when a feature needs them.
- **Unmapped events:** useful StatsBomb events (Block, Dispossessed, Foul Won, Dribbled Past)
  are promoted to first-class types; true admin/broadcast events are dropped; everything else
  is kept as `GENERIC` with its original `raw_type` recorded.
- **Storage:** Parquet, with `raw_event_json` retained so nothing is permanently lost.

## Schema (26 columns)

`provider, match_id, competition, season, match_date, event_id, period_id, seconds,
total_seconds, minute, event_type, raw_type, team_id, team_name, team_side, player_id,
player_name, start_x, start_y, end_x, end_y, result, body_part, set_piece,
qualifiers_json, raw_event_json`

## Usage

```bash
python3 -m venv .venv
.venv/bin/python -m pip install kloppy polars
.venv/bin/python pipeline.py   # writes per-match + combined Parquet
```

## Status

Proof-of-concept: two matches (one per provider) ingest end-to-end into the unified
schema. Match context (competition/season/date) is currently supplied per-match;
automated resolution across all matches is the next step.

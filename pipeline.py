"""Ingestion PoC: StatsBomb + Wyscout -> kloppy (0-1 coords) -> one Parquet schema.

Decisions encoded here (see project memory):
  - coordinates stored pitch-relative 0-1 (kloppy system), as recorded
  - promote useful unmapped events to proper type labels (StatsBomb only)
  - drop only true admin/broadcast events; keep all other football events
  - keep raw_event as a JSON string column so nothing is lost

Run: .venv/bin/python pipeline.py
"""
import json
import urllib.request
import polars as pl
from kloppy import statsbomb, wyscout

import match_context

_WY_PLAYERS_URL = "https://raw.githubusercontent.com/koenvo/wyscout-soccer-match-event-dataset/main/raw_data/players.json"
_wy_names_cache = None


def wyscout_player_names():
    """playerId -> name map for the Wyscout open dataset (names aren't in the
    event files). Fetched once and memoized; at scale this should be cached to disk."""
    global _wy_names_cache
    if _wy_names_cache is None:
        req = urllib.request.Request(_WY_PLAYERS_URL, headers={"User-Agent": "football-pipeline"})
        with urllib.request.urlopen(req, timeout=60) as r:
            players = json.load(r)
        _wy_names_cache = {
            str(p["wyId"]): (p.get("shortName") or f"{p.get('firstName','')} {p.get('lastName','')}".strip())
            for p in players
        }
    return _wy_names_cache

# --- per-provider config: the only parts that differ between sources ---------
PROVIDERS = {
    "statsbomb": {
        # StatsBomb raw event -> type.name
        "raw_type": lambda raw: (raw.get("type") or {}).get("name"),
        "names": None,  # StatsBomb event files already carry player names inline
        # unmapped ('generic') raw types worth promoting to first-class labels
        "promote": {
            "Block": "BLOCK",
            "Dispossessed": "DISPOSSESSED",
            "Foul Won": "FOUL_WON",
            "Dribbled Past": "DRIBBLED_PAST",
        },
        # true admin/broadcast events with no football meaning -> dropped
        "drop": {
            "Camera On", "Camera off", "Half Start", "Half End",
            "Starting XI", "Injury Stoppage", "Referee Ball-Drop",
        },
        "load": lambda mid: statsbomb.load(
            event_data=f"https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/{mid}.json",
            lineup_data=f"https://raw.githubusercontent.com/statsbomb/open-data/master/data/lineups/{mid}.json",
            coordinates="kloppy",
        ),
    },
    "wyscout": {
        # Wyscout v2 raw event -> eventName (no promote/drop needed: maps cleanly,
        # and every event is football — only "Goalkeeper leaving line" stays generic)
        "raw_type": lambda raw: raw.get("eventName"),
        "names": wyscout_player_names,  # join playerId -> name (not in event files)
        "promote": {},
        "drop": set(),
        "load": lambda mid: wyscout.load_open_data(match_id=int(mid), coordinates="kloppy"),
    },
}

SCHEMA = [
    "provider", "match_id", "competition", "season", "match_date",
    "event_id", "period_id", "seconds", "total_seconds", "minute",
    "event_type", "raw_type", "team_id", "team_name", "team_side",
    "player_id", "player_name",
    "start_x", "start_y", "end_x", "end_y", "result", "body_part", "set_piece",
    "qualifiers_json", "raw_event_json",
]

# Match context (competition/season/date) isn't in the event files; it's resolved
# automatically per match_id from each provider's match-metadata index.
# See match_context.py.


def enum_val(x):
    """Return the .value of an enum-ish object, else the object/None."""
    v = getattr(x, "value", x)
    return getattr(v, "value", v)


def flatten(event, cfg, provider, match_id, names, context):
    raw = getattr(event, "raw_event", None) or {}
    kind = event.event_type.value          # e.g. 'PASS', 'generic'
    rtype = cfg["raw_type"](raw)           # original provider label

    if kind == "generic":
        if rtype in cfg["drop"]:
            return None
        event_type = cfg["promote"].get(rtype, "GENERIC")
    else:
        event_type = kind

    end = getattr(event, "receiver_coordinates", None) or getattr(event, "end_coordinates", None)
    coords = event.coordinates

    body_part = set_piece = None
    qual = {}
    for q in (event.qualifiers or []):
        name = type(q).__name__
        val = enum_val(q.value)
        qual[name] = val
        if name == "BodyPartQualifier":
            body_part = val
        elif name == "SetPieceQualifier":
            set_piece = val

    team, player = event.team, event.player
    t = getattr(event, "timestamp", None)
    player_id = str(player.player_id) if player else None
    player_name = (player.name if player else None) or (names.get(player_id) if names and player_id else None)

    within = t.total_seconds() if t is not None else None
    period_start = event.period.start_timestamp.total_seconds() if event.period else 0
    total_seconds = (period_start + within) if within is not None else None
    team_side = enum_val(team.ground) if team and team.ground else None

    return {
        "provider": provider,
        "match_id": str(match_id),
        "competition": context.get("competition"),
        "season": context.get("season"),
        "match_date": context.get("match_date"),
        "event_id": str(event.event_id),
        "period_id": event.period.id if event.period else None,
        "seconds": within,
        "total_seconds": total_seconds,
        "minute": int(total_seconds // 60) if total_seconds is not None else None,
        "event_type": event_type,
        "raw_type": rtype,
        "team_id": str(team.team_id) if team else None,
        "team_name": team.name if team else None,
        "team_side": team_side,
        "player_id": player_id,
        "player_name": player_name,
        "start_x": coords.x if coords else None,
        "start_y": coords.y if coords else None,
        "end_x": end.x if end else None,
        "end_y": end.y if end else None,
        "result": enum_val(getattr(event, "result", None)),
        "body_part": body_part,
        "set_piece": set_piece,
        "qualifiers_json": json.dumps(qual) if qual else None,
        "raw_event_json": json.dumps(raw, default=str),
    }


def ingest(provider, match_id):
    cfg = PROVIDERS[provider]
    ds = cfg["load"](match_id)
    names = cfg["names"]() if cfg["names"] else None
    context = match_context.resolve(provider, match_id)
    total = len(ds.events)
    rows, dropped = [], 0
    for e in ds.events:
        r = flatten(e, cfg, provider, match_id, names, context)
        (rows.append(r) if r is not None else None)
        dropped += r is None
    df = pl.DataFrame(rows, schema=SCHEMA)
    out = f"events_{provider}_{match_id}.parquet"
    df.write_parquet(out)
    print(f"[{provider} {match_id}] loaded={total} dropped={dropped} written={df.height} -> {out}")
    return df


def report(provider, df):
    print(f"\n--- {provider}: event_type counts ---")
    print(df.group_by("event_type").len().sort("len", descending=True))
    named = df.filter(pl.col("player_name").is_not_null()).height
    print(f"rows with player_name: {named}/{df.height}")


def main():
    sb = ingest("statsbomb", "8658")
    wy = ingest("wyscout", "2058002")

    # both must share the exact same schema for a unified store
    assert sb.columns == wy.columns == SCHEMA, "schema mismatch between providers"
    print("\nschema identical across both providers ✓")

    combined = pl.concat([sb, wy])
    combined.write_parquet("events_combined.parquet")
    print(f"combined: {combined.height} rows -> events_combined.parquet")

    report("statsbomb", sb)
    report("wyscout", wy)


if __name__ == "__main__":
    main()

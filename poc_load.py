"""Proof-of-concept: load a single match from each source via kloppy.

Run: .venv/bin/python poc_load.py
"""
from kloppy import statsbomb, wyscout


def summarize(name, dataset):
    events = dataset.events
    types = {}
    for e in events:
        types[e.event_type.value] = types.get(e.event_type.value, 0) + 1
    print(f"\n=== {name} ===")
    print(f"records: {len(events)}")
    md = dataset.metadata
    teams = [t.name for t in md.teams]
    print(f"teams: {teams}")
    print(f"periods: {len(md.periods)}")
    top = sorted(types.items(), key=lambda kv: -kv[1])[:8]
    print("top event types:", top)
    # show one pass event with its richness
    for e in events:
        if e.event_type.value == "PASS":
            print("sample PASS:")
            print(f"  coordinates: {e.coordinates}")
            print(f"  result: {e.result}")
            print(f"  qualifiers: {[type(q).__name__ for q in (e.qualifiers or [])]}")
            print(f"  raw_event keys present: {bool(getattr(e, 'raw_event', None))}")
            break


# StatsBomb: 2018 World Cup final (match 8658) — loaded straight from GitHub
sb = statsbomb.load(
    event_data="https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/8658.json",
    lineup_data="https://raw.githubusercontent.com/statsbomb/open-data/master/data/lineups/8658.json",
    coordinates="statsbomb",
)
summarize("StatsBomb 8658 (2018 WC Final)", sb)

# Wyscout: built-in open-data helper
wy = wyscout.load_open_data(match_id=2058002, coordinates="wyscout")
summarize("Wyscout open_data 2058002", wy)

print("\nOK: both sources loaded through kloppy's unified model.")

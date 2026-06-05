"""Probe: which provider event types does kloppy leave as 'generic'?

Run: .venv/bin/python probe_generic.py
"""
from collections import Counter
from kloppy import statsbomb, wyscout


def probe(name, dataset, raw_type_fn):
    generic_raw = Counter()
    mapped_raw = Counter()
    for e in dataset.events:
        raw = getattr(e, "raw_event", None) or {}
        rt = raw_type_fn(raw)
        if e.event_type.value == "generic":
            generic_raw[rt] += 1
        else:
            mapped_raw[e.event_type.value] += 1
    print(f"\n=== {name} ===")
    print(f"GENERIC (unmapped) raw types: {dict(generic_raw.most_common())}")
    print(f"MAPPED typed events:          {dict(mapped_raw.most_common())}")


# StatsBomb raw event -> its 'type' dict has a 'name'
sb = statsbomb.load(
    event_data="https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/8658.json",
    lineup_data="https://raw.githubusercontent.com/statsbomb/open-data/master/data/lineups/8658.json",
    coordinates="statsbomb",
)
probe("StatsBomb 8658", sb, lambda raw: (raw.get("type") or {}).get("name", "?"))

# Wyscout raw event -> 'type'/'primary' naming varies by version
wy = wyscout.load_open_data(match_id=2058002, coordinates="wyscout")


def wy_type(raw):
    t = raw.get("type")
    if isinstance(t, dict):
        return t.get("primary") or t.get("name") or "?"
    return raw.get("eventName") or t or "?"


probe("Wyscout 2058002", wy, wy_type)

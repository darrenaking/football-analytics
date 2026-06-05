"""Verify kloppy's normalized ('kloppy') coordinate system: range + orientation.

Run: .venv/bin/python verify_coords.py
"""
from kloppy import statsbomb, wyscout


def coord_stats(name, dataset):
    xs, ys = [], []
    for e in dataset.events:
        c = e.coordinates
        if c is None:
            continue
        xs.append(c.x)
        ys.append(c.y)
    print(f"\n=== {name} ===")
    print(f"  n with coords: {len(xs)}")
    print(f"  x range: {min(xs):.4f} .. {max(xs):.4f}")
    print(f"  y range: {min(ys):.4f} .. {max(ys):.4f}")
    md = dataset.metadata
    cs = md.coordinate_system
    print(f"  coordinate_system: {type(cs).__name__}")
    try:
        print(f"  pitch_dimensions: {cs.pitch_dimensions}")
    except Exception as ex:
        print(f"  pitch_dimensions: <{ex}>")


for label, loader in [
    ("StatsBomb 8658 -> kloppy", lambda: statsbomb.load(
        event_data="https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/8658.json",
        lineup_data="https://raw.githubusercontent.com/statsbomb/open-data/master/data/lineups/8658.json",
        coordinates="kloppy",
    )),
    ("Wyscout 2058002 -> kloppy", lambda: wyscout.load_open_data(
        match_id=2058002, coordinates="kloppy",
    )),
]:
    coord_stats(label, loader())

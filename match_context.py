"""Automated match-context resolution: match_id -> {competition, season, match_date}.

Builds a per-provider index from each source's match-metadata files and caches it
to .cache/ (gitignored) so the network build happens once.

  - StatsBomb: competitions.json enumerates (competition_id, season_id); each
    matches/{cid}/{sid}.json lists matches with date + competition/season names.
  - Wyscout: raw_data/matches.zip holds one file per competition; competition name
    and season label are mapped from competitionId.
"""
import io
import json
import os
import urllib.request
import zipfile

CACHE_DIR = ".cache"
_SB_RAW = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
_WY_MATCHES_ZIP = "https://raw.githubusercontent.com/koenvo/wyscout-soccer-match-event-dataset/main/raw_data/matches.zip"

# Wyscout competitionId -> (competition name, season label). The open dataset is a
# single season per competition (2017/18 leagues, WC2018, Euro2016).
_WY_COMPS = {
    364: ("Premier League", "2017/2018"),
    412: ("Ligue 1", "2017/2018"),
    426: ("Bundesliga", "2017/2018"),
    524: ("Serie A", "2017/2018"),
    795: ("La Liga", "2017/2018"),
    28: ("FIFA World Cup", "2018"),
    102: ("UEFA Euro", "2016"),
}

_cache = {}


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "football-pipeline"})
    return urllib.request.urlopen(req, timeout=60).read()


def _load_or_build(provider, build_fn):
    if provider in _cache:
        return _cache[provider]
    path = os.path.join(CACHE_DIR, f"{provider}_match_index.json")
    if os.path.exists(path):
        with open(path) as f:
            _cache[provider] = json.load(f)
        return _cache[provider]
    index = build_fn()
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(path, "w") as f:
        json.dump(index, f)
    _cache[provider] = index
    return index


def _build_statsbomb():
    comps = json.loads(_get(f"{_SB_RAW}/competitions.json"))
    index = {}
    for c in comps:
        cid, sid = c["competition_id"], c["season_id"]
        try:
            matches = json.loads(_get(f"{_SB_RAW}/matches/{cid}/{sid}.json"))
        except Exception:
            continue  # season listed but file not present
        for m in matches:
            index[str(m["match_id"])] = {
                "competition": m["competition"]["competition_name"],
                "season": m["season"]["season_name"],
                "match_date": m.get("match_date"),
            }
    return index


def _build_wyscout():
    z = zipfile.ZipFile(io.BytesIO(_get(_WY_MATCHES_ZIP)))
    index = {}
    for name in z.namelist():
        for m in json.loads(z.read(name)):
            comp, season = _WY_COMPS.get(m.get("competitionId"), (None, None))
            index[str(m["wyId"])] = {
                "competition": comp,
                "season": season,
                "match_date": (m.get("dateutc") or "")[:10] or None,
            }
    return index


_BUILDERS = {"statsbomb": _build_statsbomb, "wyscout": _build_wyscout}


def resolve(provider, match_id):
    """Return {competition, season, match_date} for a match_id, or {} if unknown."""
    index = _load_or_build(provider, _BUILDERS[provider])
    return index.get(str(match_id), {})


if __name__ == "__main__":
    for prov, mid in [("statsbomb", "8658"), ("wyscout", "2058002")]:
        idx = _load_or_build(prov, _BUILDERS[prov])
        print(f"{prov}: index size = {len(idx)} | {mid} -> {resolve(prov, mid)}")

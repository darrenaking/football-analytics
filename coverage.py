"""Coverage map: what competitions/seasons/matches are available per provider.

Aggregates the cached match-context indexes (built by match_context.py) into a
per-(provider, competition, season) match count. Reflects matches listed in each
provider's metadata; StatsBomb per-match event availability is generally 1:1 with
this listing but not guaranteed for every single match.

Run: .venv/bin/python coverage.py
"""
import polars as pl
import match_context

rows = []
for provider in ("statsbomb", "wyscout"):
    index = match_context._load_or_build(provider, match_context._BUILDERS[provider])
    for mid, ctx in index.items():
        rows.append({
            "provider": provider,
            "competition": ctx.get("competition"),
            "season": ctx.get("season"),
        })

df = pl.DataFrame(rows)
summary = (
    df.group_by(["provider", "competition", "season"])
      .len()
      .rename({"len": "matches"})
      .sort(["provider", "competition", "season"])
)

with pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_str_lengths=60):
    print(summary)

print("\n--- totals by provider ---")
print(df.group_by("provider").len().rename({"len": "matches"}).sort("provider"))
print(f"\ngrand total matches: {df.height}")

summary.write_csv("coverage.csv")
print("wrote coverage.csv")

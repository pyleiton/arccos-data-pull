# Arccos Golf Data Pull

Export your complete Arccos Golf history into a local [DuckDB](https://duckdb.org/) database, enriched with weather, elevation, and sunrise/sunset data. Optionally pulls GHIN handicap and score history too.

## What you get

Every round you've played with Arccos, stored in a queryable analytical database:

| Table | Contents |
|---|---|
| `rounds` | Round metadata + weather + sunrise/sunset |
| `holes` | Per-hole stats (GIR, fairways, putts, up-and-down) |
| `shots` | Every shot with GPS coordinates and elevation |
| `sga_analysis` | Strokes Gained Analysis at multiple goal handicaps |
| `sga_by_distance` | SGA broken down by distance slabs |
| `sga_by_terrain` | SGA broken down by lie/terrain |
| `sga_by_hole_shape` | SGA by hole shape (dogleg left/right, straight) |
| `sga_putting_by_hole` | Putting SGA per hole |
| `courses` | Course metadata (location, par, tees, slopes, ratings) |
| `course_holes` | Per-hole par, handicap index, tee distances |
| `clubs` | Your bag (club names, make, model, loft, perceived distance) |
| `player_profile` | Handicap, total rounds, goal HCP |
| `ghin_scores` | Official GHIN posted scores with differentials |
| `ghin_handicap_history` | Handicap index revision history |
| `ghin_hole_scores` | Hole-by-hole GHIN score detail |

### Enrichment

Each round is enriched with data from free APIs:

- **Weather** ([Visual Crossing](https://www.visualcrossing.com/)) -- temperature, wind, humidity, UV index, precipitation, cloud cover, pressure, and more, averaged over the hours you played
- **Elevation** ([Open Topo Data](https://www.opentopodata.org/)) -- start and end elevation for every shot
- **Sunrise/Sunset** ([SunriseSunset.io](https://sunrisesunset.io/)) -- dawn, dusk, golden hour, day length, moon phase

## Setup

```bash
pip install duckdb requests python-dotenv
```

Copy the example environment file and fill in your credentials:

```bash
cp .env.example .env
```

At minimum you need `ARCCOS_EMAIL` and `ARCCOS_PASSWORD`. The weather API key is optional but recommended (free tier gives 1,000 requests/day).

## Usage

```bash
# Pull all rounds
python arccos_data_pull.py

# Pull only 2025 rounds
python arccos_data_pull.py --year 2025

# Pull a specific date
python arccos_data_pull.py --date 2025-07-04

# Limit to 10 new rounds
python arccos_data_pull.py --max 10

# Re-pull everything (ignore existing data)
python arccos_data_pull.py --fresh
```

The script is **resumable** -- if it's interrupted, re-run it and it picks up where it left off, skipping rounds already in the database.

## Querying your data

DuckDB is a columnar analytics database. You can query the `.duckdb` file directly from Python, the DuckDB CLI, or any tool that supports DuckDB.

```sql
-- Scoring trend by month
SELECT strftime(start_time, '%Y-%m') AS month,
       count(*) AS rounds,
       round(avg(over_under), 1) AS avg_over_par
FROM rounds
GROUP BY 1 ORDER BY 1;

-- Best strokes gained rounds
SELECT r.course_name, r.start_time::date, s.overall_sga
FROM rounds r
JOIN sga_analysis s ON r.round_id = s.round_id
WHERE s.goal_hcp = 0
ORDER BY s.overall_sga DESC
LIMIT 10;

-- How wind affects your score
SELECT CASE WHEN avg_wind_mph < 5 THEN 'Calm (<5)'
            WHEN avg_wind_mph < 10 THEN 'Light (5-10)'
            WHEN avg_wind_mph < 15 THEN 'Moderate (10-15)'
            ELSE 'Windy (15+)' END AS wind,
       count(*) AS rounds,
       round(avg(over_under), 1) AS avg_over_par
FROM rounds
WHERE avg_wind_mph IS NOT NULL
GROUP BY 1 ORDER BY 1;
```

## API keys

| Service | Required | Free tier | Sign up |
|---|---|---|---|
| Arccos | Yes | N/A (your account) | -- |
| Visual Crossing | No | 1,000 records/day | [visualcrossing.com](https://www.visualcrossing.com/sign-up) |
| Open Topo Data | No | No key needed | -- |
| SunriseSunset.io | No | No key needed | -- |
| GHIN | No | N/A (your account) | -- |

## License

MIT

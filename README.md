# Arccos Golf Data Pull

Export your complete Arccos Golf history into a local [PostgreSQL](https://www.postgresql.org/) database, enriched with weather, elevation, and sunrise/sunset data. Optionally pulls GHIN handicap and score history too. Includes pre-built views for use with [Metabase](https://www.metabase.com/) dashboards.

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
| `course_hole_tees` | Per-hole tee distances by tee box |
| `clubs` | Your bag (club names, make, model, loft, perceived distance) |
| `player_profile` | Handicap, total rounds, goal HCP |
| `ghin_scores` | Official GHIN posted scores with differentials |
| `ghin_handicap_history` | Handicap index revision history |
| `ghin_hole_scores` | Hole-by-hole GHIN score detail |
| `sga_categories` | Reference table for Metabase filter dropdowns |

### Metabase Views

The script automatically creates views optimized for Metabase dashboards:

| View | Contents |
|---|---|
| `v_round_summary` | Every 18-hole round with score, weather, strokes gained, GIR%, fairway% in one row |
| `v_hole_detail` | Hole-by-hole stats with par, score, GIR, fairways, tee distance |
| `v_shot_detail` | Every shot with correct club names mapped from sensor IDs |
| `v_handicap_history` | Clean GHIN history (filters out invalid 999 placeholders) |

### Computed Columns

| Column | Table | Purpose |
|---|---|---|
| `course_and_tee` | `rounds` | Combined course name + tee name for Metabase dropdown filters |

### Enrichment

Each round is enriched with data from free APIs:

- **Weather** ([Visual Crossing](https://www.visualcrossing.com/)) -- temperature, wind, humidity, UV index, precipitation, cloud cover, pressure, and more, averaged over the hours you played
- **Elevation** ([Open Topo Data](https://www.opentopodata.org/)) -- start and end elevation for every shot
- **Sunrise/Sunset** ([SunriseSunset.io](https://sunrisesunset.io/)) -- dawn, dusk, golden hour, day length, moon phase

## Setup

### Prerequisites

- **Docker Desktop** -- runs PostgreSQL and Metabase
- **Python 3.10+** with pip

### Start PostgreSQL

```bash
docker run -d --name arccos-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:18
```

### Install Python dependencies

```bash
pip install psycopg2-binary requests python-dotenv
```

For GHIN integration (optional):

```bash
pip install playwright
playwright install chromium
```

### Configure environment

Copy the example environment file and fill in your credentials:

```bash
cp .env.example .env
```

At minimum you need `ARCCOS_EMAIL` and `ARCCOS_PASSWORD`. Add PostgreSQL settings if not using defaults:

```
ARCCOS_EMAIL=your@email.com
ARCCOS_PASSWORD=yourpassword

# PostgreSQL (defaults shown)
PG_HOST=localhost
PG_PORT=5432
PG_USER=postgres
PG_PASSWORD=postgres
PG_DATABASE=arccos

# Optional
VISUAL_CROSSING_API_KEY=your_key_here
GHIN_EMAIL=your@email.com
GHIN_PASSWORD=yourpassword
```

## Usage

```bash
# Pull all rounds
python arccos_data_pull_postgres.py

# Pull only 2025 rounds
python arccos_data_pull_postgres.py --year 2025

# Pull a specific date
python arccos_data_pull_postgres.py --date 2025-07-04

# Limit to 10 new rounds
python arccos_data_pull_postgres.py --max 10

# Re-pull everything (ignore existing data)
python arccos_data_pull_postgres.py --fresh

# Test with a single round
python arccos_data_pull_postgres.py --max 1 --fresh --date 2026-04-05
```

The script is **resumable** -- if it's interrupted, re-run it and it picks up where it left off, skipping rounds already in the database.

## Metabase Setup

Start Metabase via Docker:

```bash
docker run -d --name metabase -p 3000:3000 metabase/metabase
```

Open http://localhost:3000 and connect to your database:

| Setting | Value |
|---|---|
| Database type | PostgreSQL |
| Host | `host.docker.internal` |
| Port | `5432` |
| Database | `arccos` |
| User | `postgres` |
| Password | `postgres` |

The pre-built views (`v_round_summary`, `v_hole_detail`, etc.) will appear as tables you can query and chart immediately.

## Querying your data

```sql
-- Scoring trend by month
SELECT TO_CHAR(start_time, 'YYYY-MM') AS month,
       COUNT(*) AS rounds,
       ROUND(AVG(over_under)::numeric, 1) AS avg_over_par
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
       COUNT(*) AS rounds,
       ROUND(AVG(over_under)::numeric, 1) AS avg_over_par
FROM rounds
WHERE avg_wind_mph IS NOT NULL
GROUP BY 1 ORDER BY 1;
```

## Files

| File | Purpose |
|---|---|
| `arccos_data_pull_postgres.py` | Main data pull script |
| `sql/schema.sql` | Database schema (apply before first run) |
| `.env` | Configuration (not committed) |

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

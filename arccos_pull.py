"""
Arccos Golf Data Pull Script
Pulls all round/shot data from Arccos API into DuckDB, enriched with weather data.
"""

import argparse
import os
import requests
import duckdb
import json
import time
import sys
from dotenv import load_dotenv
from getpass import getpass

load_dotenv()

# ─── Configuration ───────────────────────────────────────────────────────────

AUTH_BASE = "https://authentication.arccosgolf.com"
API_BASE = "https://api.arccosgolf.com"
HISTORY_WEATHER_BASE = "https://archive-api.open-meteo.com/v1/archive"
ELEVATION_API_BASE = "https://api.open-meteo.com/v1/elevation"
SUNRISE_API_BASE = "https://api.sunrisesunset.io/json"
ROUNDS_PAGE_SIZE = 50

# ─── Auth ────────────────────────────────────────────────────────────────────

def login(email: str, password: str) -> dict:
    """Authenticate and return {userId, accessKey, token}."""
    print("Logging in...")
    # Step 1: Get access key
    resp = requests.post(f"{AUTH_BASE}/accessKeys", json={
        "email": email,
        "password": password,
        "signedInByFacebook": "F"
    })
    resp.raise_for_status()
    keys = resp.json()
    user_id = keys["userId"]
    access_key = keys["accessKey"]
    print(f"  Got access key for user {user_id}")

    # Step 2: Get JWT token
    resp = requests.post(f"{AUTH_BASE}/tokens", json={
        "accessKey": access_key,
        "userId": user_id
    })
    resp.raise_for_status()
    token_data = resp.json()
    token = token_data["token"]
    print(f"  Got JWT token (expires in ~3 hours)")

    return {"userId": user_id, "accessKey": access_key, "token": token}


def api_get(path: str, token: str, params: dict = None) -> dict:
    """Make an authenticated GET request to the Arccos API."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json;charset=utf-8",
    }
    resp = requests.get(f"{API_BASE}{path}", headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()


# ─── Data Pull ───────────────────────────────────────────────────────────────

def pull_all_rounds(user_id: str, token: str) -> list:
    """Pull the full list of rounds (paginated)."""
    all_rounds = []
    offset = 0
    while True:
        print(f"  Fetching rounds offset={offset}...")
        data = api_get(
            f"/v2/users/{user_id}/rounds",
            token,
            params={"limit": ROUNDS_PAGE_SIZE, "offSet": offset, "roundType": "flagship"}
        )
        rounds = data.get("rounds", [])
        if not rounds:
            break
        all_rounds.extend(rounds)
        if len(rounds) < ROUNDS_PAGE_SIZE:
            break
        offset += ROUNDS_PAGE_SIZE
        time.sleep(0.2)
    print(f"  Found {len(all_rounds)} rounds total")
    return all_rounds


def pull_round_detail(user_id: str, round_id: int, token: str) -> dict:
    """Pull full round detail including holes and shots."""
    return api_get(f"/users/{user_id}/rounds/{round_id}", token)


def pull_sga_analysis(user_id: str, round_id: int, token: str, goal_hcp: int = -5) -> dict:
    """Pull strokes gained analysis for a round."""
    try:
        return api_get(
            f"/sga/getDashboardAnalysis/{user_id}",
            token,
            params={"goalHcp": goal_hcp, "roundId": round_id}
        )
    except requests.HTTPError:
        return None


def pull_course(course_id: int, course_version: int, token: str) -> dict:
    """Pull course info."""
    return api_get(
        f"/courses/{course_id}",
        token,
        params={"courseVersion": course_version, "width": 1024, "height": 342}
    )


def pull_bag(user_id: str, bag_id: int, token: str) -> dict:
    """Pull bag/club info."""
    return api_get(f"/users/{user_id}/bags/{bag_id}", token)


# ─── Weather Enrichment ─────────────────────────────────────────────────────

def _avg(vals):
    clean = [v for v in vals if v is not None]
    return round(sum(clean) / len(clean), 1) if clean else None

def fetch_weather(lat: float, lon: float, date_str: str,
                  start_hour: int = None, end_hour: int = None) -> dict:
    """Fetch historical weather from Open-Meteo for a given date and location.
    If start_hour/end_hour provided, averages only over round hours."""
    try:
        resp = requests.get(HISTORY_WEATHER_BASE, params={
            "latitude": lat,
            "longitude": lon,
            "start_date": date_str,
            "end_date": date_str,
            "hourly": ",".join([
                "temperature_2m", "relative_humidity_2m", "dew_point_2m",
                "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
                "precipitation", "weather_code",
                "cloud_cover", "surface_pressure",
                "soil_moisture_0_to_7cm",
            ]),
            "temperature_unit": "fahrenheit",
            "wind_speed_unit": "mph",
            "timezone": "auto"
        }, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        hourly = data.get("hourly", {})
        if not hourly or not hourly.get("time"):
            return None

        # Slice to round hours if available, otherwise use full day
        if start_hour is not None and end_hour is not None:
            sl = slice(max(0, start_hour), min(24, end_hour + 1))
        else:
            sl = slice(None)

        def get(key):
            return hourly.get(key, [])[sl]

        return {
            "avg_temp_f": _avg(get("temperature_2m")),
            "avg_humidity_pct": _avg(get("relative_humidity_2m")),
            "avg_dew_point_f": _avg(get("dew_point_2m")),
            "avg_wind_mph": _avg(get("wind_speed_10m")),
            "avg_wind_dir": _avg(get("wind_direction_10m")),
            "max_wind_gust_mph": max((v for v in get("wind_gusts_10m") if v is not None), default=None),
            "total_precip_in": round(sum(v for v in get("precipitation") if v is not None) / 25.4, 2) if get("precipitation") else None,
            "avg_cloud_cover_pct": _avg(get("cloud_cover")),
            "avg_pressure_hpa": _avg(get("surface_pressure")),
            "avg_soil_moisture": _avg(get("soil_moisture_0_to_7cm")),
            "weather_code": _mode(get("weather_code")),
        }
    except Exception as e:
        print(f"    Weather fetch failed: {e}")
        return None


def _mode(vals):
    """Most frequent non-None value."""
    clean = [v for v in vals if v is not None]
    if not clean:
        return None
    from collections import Counter
    return Counter(clean).most_common(1)[0][0]


# ─── Elevation Enrichment ──────────────────────────────────────────────────

def fetch_elevations(coords: list[tuple[float, float]]) -> list[float]:
    """Fetch elevations for a batch of (lat, lon) pairs from Open-Meteo.
    Returns list of elevations in feet. API accepts up to 100 coords per call."""
    if not coords:
        return []
    elevations = []
    # Batch in groups of 100 (API limit)
    for i in range(0, len(coords), 100):
        batch = coords[i:i+100]
        lats = ",".join(f"{c[0]:.6f}" for c in batch)
        lons = ",".join(f"{c[1]:.6f}" for c in batch)
        try:
            resp = requests.get(ELEVATION_API_BASE, params={
                "latitude": lats,
                "longitude": lons,
            }, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            # API returns meters, convert to feet
            for elev_m in data.get("elevation", []):
                if elev_m is not None:
                    elevations.append(round(elev_m * 3.28084, 1))
                else:
                    elevations.append(None)
        except Exception as e:
            print(f"    Elevation fetch failed for batch: {e}")
            elevations.extend([None] * len(batch))
        if i + 100 < len(coords):
            time.sleep(0.2)
    return elevations


def enrich_elevations(detail: dict) -> None:
    """Backfill start/end elevation on every shot in a round detail dict."""
    # Collect all unique coords we need
    coords = []
    coord_index = []  # (hole_idx, shot_idx, 'start'|'end')
    for hi, hole in enumerate(detail.get("holes", [])):
        for si, shot in enumerate(hole.get("shots", [])):
            slat, slon = shot.get("startLat"), shot.get("startLong")
            elat, elon = shot.get("endLat"), shot.get("endLong")
            if slat and slon:
                coords.append((slat, slon))
                coord_index.append((hi, si, "start"))
            if elat and elon:
                coords.append((elat, elon))
                coord_index.append((hi, si, "end"))

    if not coords:
        return

    elevations = fetch_elevations(coords)

    for (hi, si, pos), elev in zip(coord_index, elevations):
        shot = detail["holes"][hi]["shots"][si]
        if pos == "start":
            shot["startAltitude"] = elev
        else:
            shot["endAltitude"] = elev


# ─── Sunrise/Sunset Enrichment ─────────────────────────────────────────────

def fetch_sunrise_sunset(lat: float, lon: float, date_str: str) -> dict:
    """Fetch sunrise/sunset data from SunriseSunset.io."""
    try:
        resp = requests.get(SUNRISE_API_BASE, params={
            "lat": lat,
            "lng": lon,
            "date": date_str,
        }, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "OK":
            return None
        r = data.get("results", {})
        return {
            "sunrise": r.get("sunrise"),
            "sunset": r.get("sunset"),
            "dawn": r.get("dawn"),
            "dusk": r.get("dusk"),
            "day_length_hrs": _parse_day_length(r.get("day_length")),
            "solar_noon": r.get("solar_noon"),
            "golden_hour": r.get("golden_hour"),
        }
    except Exception as e:
        print(f"    Sunrise/sunset fetch failed: {e}")
        return None


def _parse_day_length(dl_str: str) -> float:
    """Parse day_length like '12:34:56' into decimal hours."""
    if not dl_str:
        return None
    try:
        parts = dl_str.split(":")
        return round(int(parts[0]) + int(parts[1]) / 60 + int(parts[2]) / 3600, 2)
    except Exception:
        return None


# ─── GHIN Integration ────────────────────────────────────────────────────────

GHIN_API_BASE = "https://api2.ghin.com/api/v1"

def ghin_login(email: str, password: str) -> dict:
    """Login to GHIN via browser to handle captcha, intercept the JWT from
    the login API response, and return {token, golfer_id}."""
    from playwright.sync_api import sync_playwright

    print("  Launching browser for GHIN login...")
    token = None
    golfer_id = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()

        # Intercept the login API response to grab the JWT
        # Attach to context level so it catches all pages/redirects
        def handle_response(response):
            nonlocal token, golfer_id
            if "golfer_login.json" in response.url and response.status == 200:
                try:
                    data = response.json()
                    gu = data.get("golfer_user", {})
                    token = gu.get("golfer_user_token")
                    golfer_id = gu.get("golfer_id")
                    print(f"  Intercepted JWT from login response")
                except Exception:
                    pass

        context.on("response", handle_response)

        page = context.new_page()
        page.goto("https://www.ghin.com/login")
        page.wait_for_load_state("networkidle")
        time.sleep(2)

        # Dismiss cookie banner if present
        try:
            reject_btn = page.locator('button:has-text("Reject All")')
            if reject_btn.is_visible(timeout=3000):
                reject_btn.click()
                print("  Dismissed cookie banner")
                time.sleep(1)
        except Exception:
            pass

        # Fill in credentials
        try:
            inputs = page.locator('input[type="text"], input[type="email"]')
            email_input = inputs.first
            email_input.wait_for(state="visible", timeout=10000)
            email_input.fill(email)
            time.sleep(0.5)

            pw_input = page.locator('input[type="password"]').first
            pw_input.fill(password)
            time.sleep(0.5)

            login_btn = page.locator('button:has-text("LOG IN"), button:has-text("Log In"), button:has-text("Sign In"), button[type="submit"]').first
            login_btn.click()
        except Exception as e:
            print(f"  Could not auto-fill login form: {e}")
            print("  Please log in manually in the browser window...")

        # Wait for the JWT to be captured (up to 30s)
        print("  Waiting for login to complete...")
        for _ in range(60):
            if token:
                break
            time.sleep(0.5)

        # Fallback: if interceptor missed it, check localStorage/sessionStorage
        if not token:
            print("  Interceptor missed response, checking browser storage...")
            try:
                stored = page.evaluate("""() => {
                    // Check localStorage and sessionStorage for JWT
                    for (const store of [localStorage, sessionStorage]) {
                        for (let i = 0; i < store.length; i++) {
                            const key = store.key(i);
                            const val = store.getItem(key);
                            if (val && val.startsWith('ey') && val.includes('.')) {
                                return {token: val, key: key};
                            }
                        }
                    }
                    return null;
                }""")
                if stored:
                    token = stored["token"]
                    print(f"  Found JWT in browser storage (key: {stored['key']})")
                    # Decode golfer_id from JWT
                    import base64
                    payload = token.split(".")[1]
                    payload += "=" * (4 - len(payload) % 4)
                    data = json.loads(base64.urlsafe_b64decode(payload))
                    golfer_id = int(data.get("sub", 0))
            except Exception as e:
                print(f"  Storage check failed: {e}")

        browser.close()

    if not token:
        raise RuntimeError("GHIN login failed — no JWT captured from browser")

    print(f"  GHIN login OK (golfer_id={golfer_id})")
    return {"token": token, "golfer_id": golfer_id}


def ghin_get(path: str, token: str, params: dict = None) -> dict:
    """Authenticated GET to GHIN API."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    if params is None:
        params = {}
    params["source"] = "GHINcom"
    resp = requests.get(f"{GHIN_API_BASE}{path}", headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()


def pull_ghin_scores(golfer_id: int, token: str) -> list:
    """Pull all GHIN scores (paginated)."""
    all_scores = []
    offset = 0
    limit = 50
    while True:
        data = ghin_get("/scores.json", token, params={
            "golfer_id": golfer_id,
            "offset": offset,
            "limit": limit,
            "statuses": "Validated",
        })
        scores = data.get("scores", [])
        all_scores.extend(scores)
        total = data.get("total_count", 0)
        if not scores or offset + limit >= total:
            break
        offset += limit
        time.sleep(0.2)
    return all_scores


def pull_ghin_handicap_history(golfer_id: int, token: str) -> list:
    """Pull handicap revision history."""
    data = ghin_get(f"/golfers/{golfer_id}/handicap_history.json", token, params={
        "revCount": 0,
        "date_begin": "2020-01-01",
        "date_end": "2030-12-31",
    })
    return data.get("handicap_revisions", [])


def pull_ghin_course_details(course_id: str, token: str) -> dict:
    """Pull course tee/rating details from GHIN."""
    return ghin_get(f"/crsCourseMethods.asmx/GetCourseDetails.json", token, params={
        "courseId": course_id,
        "include_altered_tees": "true",
    })


# ─── DuckDB Schema & Storage ────────────────────────────────────────────────

def init_db(db_path: str) -> duckdb.DuckDBPyConnection:
    """Create DuckDB and initialize schema."""
    db = duckdb.connect(db_path)

    db.execute("""
        CREATE TABLE IF NOT EXISTS rounds (
            round_id INTEGER PRIMARY KEY,
            round_uuid VARCHAR,
            user_id VARCHAR,
            course_id INTEGER,
            course_version INTEGER,
            course_name VARCHAR,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            last_modified TIMESTAMP,
            tee_id INTEGER,
            num_holes INTEGER,
            num_shots INTEGER,
            par INTEGER,
            over_under INTEGER,
            is_ended VARCHAR,
            ball_make_id INTEGER,
            ball_model_id INTEGER,
            drive_hcp DOUBLE,
            approach_hcp DOUBLE,
            chip_hcp DOUBLE,
            sand_hcp DOUBLE,
            putt_hcp DOUBLE,
            notes VARCHAR,
            -- weather enrichment
            avg_temp_f DOUBLE,
            avg_humidity_pct DOUBLE,
            avg_wind_mph DOUBLE,
            avg_wind_dir DOUBLE,
            total_precip_in DOUBLE,
            avg_dew_point_f DOUBLE,
            max_wind_gust_mph DOUBLE,
            avg_cloud_cover_pct DOUBLE,
            avg_pressure_hpa DOUBLE,
            avg_soil_moisture DOUBLE,
            weather_code INTEGER,
            -- sunrise/sunset enrichment
            sunrise VARCHAR,
            sunset VARCHAR,
            dawn VARCHAR,
            dusk VARCHAR,
            day_length_hrs DOUBLE,
            solar_noon VARCHAR,
            golden_hour VARCHAR
        )
    """)

    # Migrate: add new columns to existing tables
    new_round_cols = {
        "avg_dew_point_f": "DOUBLE", "max_wind_gust_mph": "DOUBLE",
        "avg_cloud_cover_pct": "DOUBLE", "avg_pressure_hpa": "DOUBLE",
        "avg_soil_moisture": "DOUBLE", "weather_code": "INTEGER",
        "sunrise": "VARCHAR", "sunset": "VARCHAR", "dawn": "VARCHAR",
        "dusk": "VARCHAR", "day_length_hrs": "DOUBLE",
        "solar_noon": "VARCHAR", "golden_hour": "VARCHAR",
    }
    for col, dtype in new_round_cols.items():
        try:
            db.execute(f"ALTER TABLE rounds ADD COLUMN {col} {dtype}")
        except Exception:
            pass  # column already exists

    db.execute("""
        CREATE TABLE IF NOT EXISTS holes (
            round_id INTEGER,
            hole_id INTEGER,
            num_shots INTEGER,
            is_gir VARCHAR,
            putts INTEGER,
            is_fairway VARCHAR,
            is_fairway_right VARCHAR,
            is_fairway_left VARCHAR,
            is_sand_save_chance VARCHAR,
            is_sand_save VARCHAR,
            is_up_down_chance VARCHAR,
            is_up_down VARCHAR,
            approach_shot_id INTEGER,
            pin_lat DOUBLE,
            pin_long DOUBLE,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            score_override INTEGER,
            PRIMARY KEY (round_id, hole_id)
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS shots (
            round_id INTEGER,
            hole_id INTEGER,
            shot_id INTEGER,
            shot_uuid VARCHAR,
            club_type INTEGER,
            club_id INTEGER,
            start_lat DOUBLE,
            start_long DOUBLE,
            end_lat DOUBLE,
            end_long DOUBLE,
            distance DOUBLE,
            is_half_swing VARCHAR,
            start_altitude DOUBLE,
            end_altitude DOUBLE,
            shot_time TIMESTAMP,
            should_ignore VARCHAR,
            num_penalties INTEGER,
            user_start_terrain_override INTEGER,
            should_consider_putt_as_chip VARCHAR,
            tour_quality VARCHAR,
            PRIMARY KEY (round_id, hole_id, shot_id)
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS sga_analysis (
            round_id INTEGER,
            overall_sga DOUBLE,
            drive_sga DOUBLE,
            approach_sga DOUBLE,
            short_game_sga DOUBLE,
            putting_sga DOUBLE,
            raw_json JSON,
            PRIMARY KEY (round_id)
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS courses (
            course_id INTEGER PRIMARY KEY,
            course_version INTEGER,
            course_name VARCHAR,
            raw_json JSON
        )
    """)

    # GHIN tables
    db.execute("""
        CREATE TABLE IF NOT EXISTS ghin_scores (
            id INTEGER PRIMARY KEY,
            golfer_id INTEGER,
            played_at DATE,
            course_name VARCHAR,
            course_id VARCHAR,
            tee_name VARCHAR,
            tee_set_id VARCHAR,
            adjusted_gross_score INTEGER,
            net_score INTEGER,
            course_rating DOUBLE,
            slope_rating INTEGER,
            differential DOUBLE,
            unadjusted_differential DOUBLE,
            pcc INTEGER,
            course_handicap VARCHAR,
            score_type VARCHAR,
            number_of_holes INTEGER,
            posted_at TIMESTAMP,
            used BOOLEAN,
            penalty VARCHAR,
            penalty_type VARCHAR,
            raw_json JSON
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS ghin_handicap_history (
            id VARCHAR,
            rev_date DATE,
            handicap_index DOUBLE,
            low_hi DOUBLE,
            hard_cap VARCHAR,
            soft_cap VARCHAR,
            PRIMARY KEY (id)
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS ghin_hole_scores (
            ghin_score_id INTEGER,
            hole_number INTEGER,
            par INTEGER,
            raw_score INTEGER,
            adjusted_gross_score INTEGER,
            stroke_allocation INTEGER,
            x_hole BOOLEAN,
            PRIMARY KEY (ghin_score_id, hole_number)
        )
    """)

    return db


def store_round(db: duckdb.DuckDBPyConnection, rd: dict,
                weather: dict = None, sun: dict = None):
    """Insert or replace a round and its holes/shots."""
    w = weather or {}
    s = sun or {}

    db.execute("""
        INSERT OR REPLACE INTO rounds VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,
            $29,$30,$31,$32,$33,$34,
            $35,$36,$37,$38,$39,$40,$41
        )
    """, [
        rd["roundId"], rd.get("roundUUID"), rd.get("userId"),
        rd["courseId"], rd.get("courseVersion"), rd.get("courseName"),
        rd.get("startTime"), rd.get("endTime"), rd.get("lastModifiedTime"),
        rd.get("teeId"), rd.get("noOfHoles"), rd.get("noOfShots"),
        rd.get("par"),
        rd.get("overUnder"),
        rd.get("isEnded"), rd.get("ballMakeId"), rd.get("ballModelId"),
        rd.get("driveHcp"), rd.get("approachHcp"), rd.get("chipHcp"),
        rd.get("sandHcp"), rd.get("puttHcp"), rd.get("notes"),
        # weather
        w.get("avg_temp_f"), w.get("avg_humidity_pct"),
        w.get("avg_wind_mph"), w.get("avg_wind_dir"), w.get("total_precip_in"),
        w.get("avg_dew_point_f"), w.get("max_wind_gust_mph"),
        w.get("avg_cloud_cover_pct"), w.get("avg_pressure_hpa"),
        w.get("avg_soil_moisture"), w.get("weather_code"),
        # sunrise/sunset
        s.get("sunrise"), s.get("sunset"), s.get("dawn"), s.get("dusk"),
        s.get("day_length_hrs"), s.get("solar_noon"), s.get("golden_hour"),
    ])

    for hole in rd.get("holes", []):
        db.execute("""
            INSERT OR REPLACE INTO holes VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
        """, [
            rd["roundId"], hole["holeId"], hole.get("noOfShots"),
            hole.get("isGir"), hole.get("putts"),
            hole.get("isFairWay"), hole.get("isFairWayRight"), hole.get("isFairWayLeft"),
            hole.get("isSandSaveChance"), hole.get("isSandSave"),
            hole.get("isUpDownChance"), hole.get("isUpDown"),
            hole.get("approachShotId"),
            hole.get("pinLat"), hole.get("pinLong"),
            hole.get("startTime"), hole.get("endTime"),
            hole.get("scoreOverride"),
        ])

        for shot in hole.get("shots", []):
            db.execute("""
                INSERT OR REPLACE INTO shots VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
            """, [
                rd["roundId"], hole["holeId"], shot["shotId"],
                shot.get("shotUUID"), shot.get("clubType"), shot.get("clubId"),
                shot.get("startLat"), shot.get("startLong"),
                shot.get("endLat"), shot.get("endLong"),
                shot.get("distance"), shot.get("isHalfSwing"),
                shot.get("startAltitude"), shot.get("endAltitude"),
                shot.get("shotTime"), shot.get("shouldIgnore"),
                shot.get("noOfPenalties"), shot.get("userStartTerrainOverride"),
                shot.get("shouldConsiderPuttAsChip"), shot.get("tourQuality"),
            ])


def store_sga(db: duckdb.DuckDBPyConnection, round_id: int, sga: dict):
    """Store SGA analysis."""
    if not sga:
        return
    section = sga.get("overall", {}).get("overallSection", {})

    db.execute("""
        INSERT OR REPLACE INTO sga_analysis VALUES ($1,$2,$3,$4,$5,$6,$7)
    """, [
        round_id,
        section.get("sga"),
        section.get("drivingSga"),
        section.get("approachSga"),
        section.get("shortSga"),
        section.get("puttingSga"),
        json.dumps(sga),
    ])


def store_course(db: duckdb.DuckDBPyConnection, course: dict, course_id: int):
    """Store course info."""
    db.execute("""
        INSERT OR REPLACE INTO courses VALUES ($1,$2,$3,$4)
    """, [
        course_id,
        course.get("courseVersion"),
        course.get("courseName", course.get("name")),
        json.dumps(course),
    ])


def store_ghin_score(db: duckdb.DuckDBPyConnection, s: dict):
    """Store a GHIN score and its hole details."""
    db.execute("""
        INSERT OR REPLACE INTO ghin_scores VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22
        )
    """, [
        s["id"], s.get("golfer_id"), s.get("played_at"),
        s.get("course_name"), s.get("course_id"),
        s.get("tee_name"), s.get("tee_set_id"),
        s.get("adjusted_gross_score"), s.get("net_score"),
        s.get("course_rating"), s.get("slope_rating"),
        s.get("differential"), s.get("unadjusted_differential"),
        s.get("pcc"), s.get("course_handicap"),
        s.get("score_type"), s.get("number_of_holes"),
        s.get("posted_at"), s.get("used"),
        s.get("penalty"), s.get("penalty_type"),
        json.dumps(s),
    ])
    for h in s.get("hole_details", []):
        db.execute("""
            INSERT OR REPLACE INTO ghin_hole_scores VALUES ($1,$2,$3,$4,$5,$6,$7)
        """, [
            s["id"], h.get("hole_number"), h.get("par"),
            h.get("raw_score"), h.get("adjusted_gross_score"),
            h.get("stroke_allocation"), h.get("x_hole"),
        ])


def store_ghin_handicap(db: duckdb.DuckDBPyConnection, rev: dict):
    """Store a handicap revision."""
    db.execute("""
        INSERT OR REPLACE INTO ghin_handicap_history VALUES ($1,$2,$3,$4,$5,$6)
    """, [
        rev.get("ID"), rev.get("RevDate", "")[:10],
        float(rev["Value"]) if rev.get("Value") else None,
        float(rev["LowHI"]) if rev.get("LowHI") else None,
        rev.get("Hard_Cap"), rev.get("Soft_Cap"),
    ])


# ─── Filtering ───────────────────────────────────────────────────────────────

def filter_rounds(rounds_list: list, year: int = None, date: str = None) -> list:
    """Filter rounds by year or specific date."""
    if date:
        return [r for r in rounds_list if r.get("startTime", "").startswith(date)]
    if year:
        prefix = str(year)
        return [r for r in rounds_list if r.get("startTime", "").startswith(prefix)]
    return rounds_list


# ─── CLI ─────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Pull Arccos golf data into DuckDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python arccos_pull.py                      # pull all rounds (or respect .env MAX_ROUNDS)
  python arccos_pull.py --max 1              # pull just 1 round to test
  python arccos_pull.py --year 2025          # pull only 2025 rounds
  python arccos_pull.py --date 2026-04-04    # pull round(s) from a specific date
  python arccos_pull.py --goal-hcp 0         # use scratch as SGA baseline
  python arccos_pull.py --year 2025 --goal-hcp 10  # combine filters
        """,
    )
    parser.add_argument("--max", type=int, default=None,
                        help="Max new rounds to pull (default: 0=all, or .env ARCCOS_MAX_ROUNDS)")
    parser.add_argument("--year", type=int, default=None,
                        help="Only pull rounds from this year (e.g. 2025)")
    parser.add_argument("--date", type=str, default=None,
                        help="Only pull round(s) from this date (YYYY-MM-DD)")
    parser.add_argument("--goal-hcp", type=int, default=None,
                        help="Goal handicap for SGA analysis (default: .env or -5)")
    parser.add_argument("--db", type=str, default=None,
                        help="DuckDB file path (default: .env or arccos.duckdb)")
    parser.add_argument("--fresh", action="store_true",
                        help="Ignore existing DB data and re-pull everything matched")
    return parser.parse_args()


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    # Resolve config: CLI args > .env > defaults
    db_file = args.db or os.getenv("ARCCOS_DB_FILE", "arccos.duckdb")
    max_rounds = args.max if args.max is not None else int(os.getenv("ARCCOS_MAX_ROUNDS", "0"))
    goal_hcp = args.goal_hcp if args.goal_hcp is not None else int(os.getenv("ARCCOS_GOAL_HCP", "-5"))

    print("=" * 60)
    print("  Arccos Golf Data Pull")
    print("=" * 60)
    print()

    # Show active filters
    filters = []
    if args.year:
        filters.append(f"year={args.year}")
    if args.date:
        filters.append(f"date={args.date}")
    if max_rounds:
        filters.append(f"max={max_rounds}")
    filters.append(f"goal-hcp={goal_hcp}")
    print(f"  Config: {', '.join(filters)}")
    print()

    # Auth
    email = os.getenv("ARCCOS_EMAIL") or input("Email: ").strip()
    password = os.getenv("ARCCOS_PASSWORD") or getpass("Password: ")
    if not password:
        print("Error: ARCCOS_PASSWORD not set in .env and no password provided.")
        sys.exit(1)
    auth = login(email, password)
    user_id = auth["userId"]
    token = auth["token"]
    print()

    # Init DB
    db = init_db(db_file)
    print(f"Database: {db_file}")
    print()

    # Check what we already have
    existing = set()
    if not args.fresh:
        try:
            rows = db.execute("SELECT round_id FROM rounds").fetchall()
            existing = {r[0] for r in rows}
            if existing:
                print(f"Already have {len(existing)} rounds in DB (use --fresh to re-pull)")
        except Exception:
            pass

    # Pull round list
    print("Pulling round list...")
    rounds_list = pull_all_rounds(user_id, token)

    # Apply filters
    rounds_list = filter_rounds(rounds_list, year=args.year, date=args.date)
    if args.year or args.date:
        filter_desc = args.date or str(args.year)
        print(f"  After filtering for {filter_desc}: {len(rounds_list)} rounds")

    # Determine how many we'll actually pull
    to_pull = [r for r in rounds_list if r["roundId"] not in existing]
    if max_rounds:
        to_pull = to_pull[:max_rounds]
    pull_target = len(to_pull)
    print(f"  Will pull {pull_target} new round(s)" +
          (f" (of {len(rounds_list)} total, limited by --max {max_rounds})" if max_rounds else ""))
    print()

    # Track courses we've already fetched
    fetched_courses = set()
    try:
        rows = db.execute("SELECT course_id FROM courses").fetchall()
        fetched_courses = {r[0] for r in rows}
    except Exception:
        pass

    new_count = 0
    for i, r in enumerate(rounds_list):
        round_id = r["roundId"]
        course_id = r["courseId"]

        if round_id in existing:
            print(f"  [{i+1}/{len(rounds_list)}] Round {round_id} — already in DB, skipping")
            continue

        if max_rounds and new_count >= max_rounds:
            print(f"  Reached max rounds limit ({max_rounds}), stopping.")
            break

        new_count += 1
        print(f"  [{new_count}/{pull_target}] Round {round_id} ({r.get('courseName', '?')}, {r.get('startTime', '?')[:10]})")

        # Pull full detail
        detail = pull_round_detail(user_id, round_id, token)
        time.sleep(0.15)

        # Pull SGA
        sga = pull_sga_analysis(user_id, round_id, token, goal_hcp=goal_hcp)
        time.sleep(0.15)

        # Get GPS and time info for enrichment
        first_hole = (detail.get("holes") or [{}])[0] if detail.get("holes") else {}
        first_shot = (first_hole.get("shots") or [{}])[0] if first_hole.get("shots") else {}
        lat = first_shot.get("startLat")
        lon = first_shot.get("startLong")
        start = detail.get("startTime", "")
        end = detail.get("endTime", "")
        date_str = start[:10] if start else None

        # Parse round hours for weather slicing
        start_hour = int(start[11:13]) if len(start) > 13 else None
        end_hour = int(end[11:13]) if len(end) > 13 else None

        # 1. Weather enrichment
        weather = None
        if lat and lon and date_str:
            print(f"    Weather for {date_str} at ({lat:.2f}, {lon:.2f})...")
            weather = fetch_weather(lat, lon, date_str, start_hour, end_hour)
            if weather:
                print(f"    {weather['avg_temp_f']}F, {weather['avg_wind_mph']}mph wind, "
                      f"{weather['avg_cloud_cover_pct']}% cloud, "
                      f"{weather['total_precip_in']}\" rain")
            time.sleep(0.2)

        # 2. Elevation backfill on every shot
        print(f"    Fetching elevations for {detail.get('noOfShots', '?')} shots...")
        enrich_elevations(detail)
        time.sleep(0.2)

        # 3. Sunrise/sunset
        sun = None
        if lat and lon and date_str:
            print(f"    Fetching sunrise/sunset...")
            sun = fetch_sunrise_sunset(lat, lon, date_str)
            if sun:
                print(f"    Sunrise {sun['sunrise']}, Sunset {sun['sunset']}, "
                      f"Day {sun['day_length_hrs']}hrs")
            time.sleep(0.2)

        # Merge fields that only exist in the list response
        for key in ("par", "driveHcp", "approachHcp", "chipHcp", "sandHcp", "puttHcp"):
            if key not in detail or detail[key] is None:
                detail[key] = r.get(key)

        # Store
        store_round(db, detail, weather, sun)
        if sga:
            store_sga(db, round_id, sga)

        # Course (once per course)
        if course_id not in fetched_courses:
            try:
                course = pull_course(course_id, r.get("courseVersion", 1), token)
                store_course(db, course, course_id)
                fetched_courses.add(course_id)
                time.sleep(0.15)
            except Exception as e:
                print(f"    Course fetch failed: {e}")

    print()
    print("=" * 60)
    print(f"  Done! Pulled {new_count} new Arccos rounds.")
    print()

    # ─── GHIN Pull ───────────────────────────────────────────────────────
    ghin_email = os.getenv("GHIN_EMAIL")
    ghin_password = os.getenv("GHIN_PASSWORD")
    if ghin_email and ghin_password:
        print("=" * 60)
        print("  GHIN Data Pull")
        print("=" * 60)
        print()
        try:
            ghin_auth = ghin_login(ghin_email, ghin_password)
            ghin_token = ghin_auth["token"]
            ghin_id = ghin_auth["golfer_id"]

            # Scores
            print("  Pulling GHIN scores...")
            ghin_scores = pull_ghin_scores(ghin_id, ghin_token)
            print(f"  Found {len(ghin_scores)} scores")
            for s in ghin_scores:
                store_ghin_score(db, s)
            time.sleep(0.2)

            # Handicap history
            print("  Pulling handicap history...")
            revisions = pull_ghin_handicap_history(ghin_id, ghin_token)
            print(f"  Found {len(revisions)} revisions")
            for rev in revisions:
                store_ghin_handicap(db, rev)

            print()
            print(f"  GHIN: {len(ghin_scores)} scores, {len(revisions)} handicap revisions stored")
            print("=" * 60)
            print()
        except Exception as e:
            print(f"  GHIN pull failed: {e}")
            print()
    else:
        print("  (Skipping GHIN — set GHIN_EMAIL and GHIN_PASSWORD in .env)")
        print()

    # Summary
    print("=" * 60)
    print("  Database Summary")
    print("=" * 60)
    count = db.execute("SELECT COUNT(*) FROM rounds").fetchone()[0]
    if count > 0:
        summary = db.execute("""
            SELECT
                COUNT(*) as rounds,
                SUM(num_shots) as total_shots,
                MIN(start_time) as earliest,
                MAX(start_time) as latest,
                ROUND(AVG(over_under), 1) as avg_over_under
            FROM rounds
        """).fetchone()
        print(f"  Arccos rounds:      {summary[0]}")
        print(f"  Total shots:        {summary[1]}")
        print(f"  Date range:         {str(summary[2])[:10]} to {str(summary[3])[:10]}")
        print(f"  Avg over/under:     {summary[4]:+.1f}")

    ghin_count = db.execute("SELECT COUNT(*) FROM ghin_scores").fetchone()[0]
    if ghin_count > 0:
        gs = db.execute("""
            SELECT COUNT(*), MIN(played_at), MAX(played_at),
                   ROUND(AVG(differential), 1)
            FROM ghin_scores
        """).fetchone()
        hcp = db.execute("""
            SELECT handicap_index FROM ghin_handicap_history
            ORDER BY rev_date DESC LIMIT 1
        """).fetchone()
        print(f"  GHIN scores:        {gs[0]} ({gs[1]} to {gs[2]})")
        print(f"  Avg differential:   {gs[3]}")
        if hcp:
            print(f"  Current HCP index:  {hcp[0]}")

    print()
    print(f"  Database saved to: {db_file}")
    print("=" * 60)

    db.close()


if __name__ == "__main__":
    main()

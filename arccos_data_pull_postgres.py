"""
Arccos Golf Data Pull Script (PostgreSQL version)
Pulls all round/shot data from Arccos API into PostgreSQL, enriched with weather data.
"""

import argparse
import logging
import logging.handlers
import os
import requests
import psycopg2
from psycopg2.extras import execute_values
import json
import time
import sys
from pathlib import Path
from dotenv import load_dotenv
from getpass import getpass

load_dotenv()

# ─── Logging ─────────────────────────────────────────────────────────────────

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

log = logging.getLogger("arccos_data_pull")
log.setLevel(logging.INFO)

_log_fmt = logging.Formatter("%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

_log_stdout = logging.StreamHandler(sys.stdout)
_log_stdout.setFormatter(_log_fmt)
log.addHandler(_log_stdout)

_log_file = logging.handlers.TimedRotatingFileHandler(
    LOG_DIR / "arccos_data_pull.log",
    when="midnight",
    backupCount=90,
    encoding="utf-8",
)
_log_file.suffix = "%Y-%m-%d.log"
_log_file.setFormatter(_log_fmt)
log.addHandler(_log_file)

# ─── Configuration ───────────────────────────────────────────────────────────

AUTH_BASE = "https://authentication.arccosgolf.com"
API_BASE = "https://api.arccosgolf.com"
ELEVATION_API_BASE = "https://api.opentopodata.org/v1/srtm90m"
SUNRISE_API_BASE = "https://api.sunrisesunset.io/json"
ROUNDS_PAGE_SIZE = 50


# ─── Rate Limit / Retry ─────────────────────────────────────────────────────

def request_with_retry(method: str, url: str, max_retries: int = 5,
                       base_delay: float = 1.0, **kwargs) -> requests.Response:
    """Make an HTTP request with retry on 429/5xx errors.
    Creates a fresh session on each retry to reset connection-level rate limits."""
    kwargs.setdefault("timeout", 15)
    for attempt in range(max_retries):
        # Use a fresh Session per attempt to get a new TCP connection
        with requests.Session() as session:
            resp = session.request(method, url, **kwargs)
        if resp.status_code == 429 or resp.status_code >= 500:
            delay = min(base_delay * (2 ** attempt), 30)
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = max(delay, float(retry_after))
                except ValueError:
                    pass
            log.warning(f"Rate limited ({resp.status_code}), new connection in {delay:.0f}s (attempt {attempt+1}/{max_retries})")
            time.sleep(delay)
            continue
        return resp
    # Return last response even if still failing
    return resp

# ─── Auth ────────────────────────────────────────────────────────────────────

def login(email: str, password: str) -> dict:
    """Authenticate and return {userId, accessKey, token}."""
    log.info("Logging in...")
    # Step 1: Get access key
    resp = request_with_retry("POST", f"{AUTH_BASE}/accessKeys", json={
        "email": email,
        "password": password,
        "signedInByFacebook": "F"
    })
    resp.raise_for_status()
    keys = resp.json()
    user_id = keys["userId"]
    access_key = keys["accessKey"]
    log.info(f"  Got access key for user {user_id}")

    # Step 2: Get JWT token
    resp = request_with_retry("POST", f"{AUTH_BASE}/tokens", json={
        "accessKey": access_key,
        "userId": user_id
    })
    resp.raise_for_status()
    token_data = resp.json()
    token = token_data["token"]
    log.info(f"  Got JWT token (expires in ~3 hours)")

    return {"userId": user_id, "accessKey": access_key, "token": token}


def api_get(path: str, token: str, params: dict = None) -> dict:
    """Make an authenticated GET request to the Arccos API."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json;charset=utf-8",
    }
    resp = request_with_retry("GET", f"{API_BASE}{path}", headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()


# ─── Data Pull ───────────────────────────────────────────────────────────────

def pull_all_rounds(user_id: str, token: str) -> list:
    """Pull the full list of rounds (paginated)."""
    all_rounds = []
    offset = 0
    while True:
        log.info(f"  Fetching rounds offset={offset}...")
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
    log.info(f"  Found {len(all_rounds)} rounds total")
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

VISUAL_CROSSING_BASE = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

def fetch_weather(lat: float, lon: float, date_str: str,
                  start_hour: int = None, end_hour: int = None,
                  round_id: int = None,
                  con=None, cur=None) -> dict:
    """Fetch weather from Visual Crossing for a given date and location.
    Checks DB cache first. If start_hour/end_hour provided, averages only
    over round hours. Raw response is cached in weather_cache table."""

    # Check cache first
    if cur and round_id:
        try:
            cur.execute(
                "SELECT raw_response FROM weather_cache WHERE round_id=%s",
                [round_id]
            )
            cached = cur.fetchone()
            if cached and cached[0]:
                log.info(f"    Weather cached, skipping API call")
                raw = cached[0]
                if isinstance(raw, str):
                    data = json.loads(raw)
                else:
                    data = raw  # JSONB returns dict directly
                return _parse_weather_response(data, start_hour, end_hour)
        except Exception:
            pass

    api_key = os.getenv("VISUAL_CROSSING_API_KEY", "")
    if not api_key:
        log.info("    VISUAL_CROSSING_API_KEY not set, skipping weather")
        return None
    try:
        resp = request_with_retry("GET",
            f"{VISUAL_CROSSING_BASE}/{lat},{lon}/{date_str}",
            params={
                "key": api_key,
                "unitGroup": "us",
                "include": "hours",
            })
        resp.raise_for_status()
        data = resp.json()

        # Cache raw response in DB
        if cur and con and round_id:
            try:
                cur.execute("""
                    INSERT INTO weather_cache (round_id, lat, lon, date, start_hour, end_hour, raw_response)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (round_id) DO UPDATE SET
                        lat=EXCLUDED.lat,
                        lon=EXCLUDED.lon,
                        date=EXCLUDED.date,
                        start_hour=EXCLUDED.start_hour,
                        end_hour=EXCLUDED.end_hour,
                        raw_response=EXCLUDED.raw_response
                """, [round_id, lat, lon, date_str, start_hour, end_hour, json.dumps(data)])
                con.commit()
            except Exception:
                con.rollback()

        return _parse_weather_response(data, start_hour, end_hour)
    except Exception as e:
        log.error(f"Weather fetch failed: {e}")
        return None


def _parse_weather_response(data: dict, start_hour: int = None, end_hour: int = None) -> dict:
    """Parse a Visual Crossing API response into our weather dict."""
    try:
        day = data.get("days", [{}])[0]
        hours = day.get("hours", [])
        if not hours:
            return None

        # Slice to round hours if available
        if start_hour is not None and end_hour is not None:
            hours = [h for h in hours
                     if start_hour <= int(h["datetime"].split(":")[0]) <= end_hour]

        def havg(key):
            vals = [h.get(key) for h in hours if h.get(key) is not None]
            return round(sum(vals) / len(vals), 1) if vals else None

        def hmax(key):
            vals = [h.get(key) for h in hours if h.get(key) is not None]
            return max(vals) if vals else None

        return {
            "avg_temp_f": havg("temp"),
            "avg_feels_like_f": havg("feelslike"),
            "temp_max_f": day.get("tempmax"),
            "temp_min_f": day.get("tempmin"),
            "avg_humidity_pct": havg("humidity"),
            "avg_dew_point_f": havg("dew"),
            "avg_wind_mph": havg("windspeed"),
            "avg_wind_dir": havg("winddir"),
            "max_wind_gust_mph": hmax("windgust"),
            "total_precip_in": round(sum(h.get("precip", 0) or 0 for h in hours), 2),
            "precip_prob_pct": havg("precipprob"),
            "precip_type": ",".join(day["preciptype"]) if day.get("preciptype") else None,
            "snow_in": day.get("snow"),
            "avg_cloud_cover_pct": havg("cloudcover"),
            "avg_pressure_hpa": havg("pressure"),
            "avg_visibility_mi": havg("visibility"),
            "avg_uv_index": havg("uvindex"),
            "avg_solar_radiation": havg("solarradiation"),
            "solar_energy": day.get("solarenergy"),
            "severe_risk": day.get("severerisk"),
            "moon_phase": day.get("moonphase"),
            "feels_like_max_f": day.get("feelslikemax"),
            "feels_like_min_f": day.get("feelslikemin"),
            "precip_cover_pct": day.get("precipcover"),
            "snow_depth_in": day.get("snowdepth"),
            "weather_conditions": day.get("conditions"),
            "weather_description": day.get("description"),
            "weather_icon": day.get("icon"),
        }
    except Exception as e:
        log.error(f"Weather parse failed: {e}")
        return None


# ─── Elevation Enrichment ──────────────────────────────────────────────────

def fetch_elevations(coords: list[tuple[float, float]]) -> list[float]:
    """Fetch elevations for a batch of (lat, lon) pairs from Open Topo Data.
    Returns list of elevations in feet. API accepts up to 100 coords per call,
    rate limited to 1 call/sec."""
    if not coords:
        return []
    elevations = []
    # Batch in groups of 100 (API limit)
    for i in range(0, len(coords), 100):
        batch = coords[i:i+100]
        locations = "|".join(f"{c[0]:.6f},{c[1]:.6f}" for c in batch)
        try:
            resp = request_with_retry("GET", ELEVATION_API_BASE, params={
                "locations": locations,
            })
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "OK":
                log.error(f"Elevation API error: {data.get('error', 'unknown')}")
                elevations.extend([None] * len(batch))
                continue
            for result in data.get("results", []):
                elev_m = result.get("elevation")
                if elev_m is not None:
                    elevations.append(round(elev_m * 3.28084, 1))
                else:
                    elevations.append(None)
        except Exception as e:
            log.error(f"Elevation fetch failed for batch: {e}")
            elevations.extend([None] * len(batch))
        # Open Topo Data: 1 call/sec rate limit
        time.sleep(1.1)
    return elevations


def enrich_elevations(detail: dict) -> None:
    """Backfill start/end elevation on every shot in a round detail dict."""
    # Collect all unique coords we need
    coords = []
    coord_index = []  # (hole_idx, shot_idx, 'start'|'end')
    for hi, hole in enumerate(detail.get("holes") or []):
        if hole is None:
            continue
        for si, shot in enumerate(hole.get("shots") or []):
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
        resp = request_with_retry("GET", SUNRISE_API_BASE, params={
            "lat": lat,
            "lng": lon,
            "date": date_str,
        })
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
            "first_light": r.get("first_light"),
            "last_light": r.get("last_light"),
            "sun_altitude": r.get("sun_altitude"),
            "sunrise_azimuth": r.get("sunrise_azimuth"),
            "sunset_azimuth": r.get("sunset_azimuth"),
            "moon_illumination": r.get("moon_illumination"),
            "moon_phase_name": r.get("moon_phase"),
            "moonrise": r.get("moonrise"),
            "moonset": r.get("moonset"),
        }
    except Exception as e:
        log.error(f"Sunrise/sunset fetch failed: {e}")
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

    log.info("  Launching browser for GHIN login...")
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
                    log.info(f"  Intercepted JWT from login response")
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
                log.info("  Dismissed cookie banner")
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
            log.warning(f"Could not auto-fill login form: {e}")
            log.info("Please log in manually in the browser window...")

        # Wait for the JWT to be captured (up to 30s)
        log.info("  Waiting for login to complete...")
        for _ in range(60):
            if token:
                break
            time.sleep(0.5)

        # Fallback: if interceptor missed it, check localStorage/sessionStorage
        if not token:
            log.info("  Interceptor missed response, checking browser storage...")
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
                    log.info(f"  Found JWT in browser storage (key: {stored['key']})")
                    # Decode golfer_id from JWT
                    import base64
                    payload = token.split(".")[1]
                    payload += "=" * (4 - len(payload) % 4)
                    data = json.loads(base64.urlsafe_b64decode(payload))
                    golfer_id = int(data.get("sub", 0))
            except Exception as e:
                log.error(f"Storage check failed: {e}")

        browser.close()

    if not token:
        raise RuntimeError("GHIN login failed — no JWT captured from browser")

    log.info(f"  GHIN login OK (golfer_id={golfer_id})")
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
    resp = request_with_retry("GET", f"{GHIN_API_BASE}{path}", headers=headers, params=params)
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


# ─── PostgreSQL Schema & Storage ───────────────────────────────────────────

def init_db(pg_host: str = None, pg_port: int = None, pg_user: str = None,
            pg_password: str = None, pg_database: str = None) -> tuple:
    """Connect to PostgreSQL and initialize schema. Returns (connection, cursor)."""
    host = pg_host or os.getenv("PG_HOST", "localhost")
    port = pg_port or int(os.getenv("PG_PORT", "5432"))
    user = pg_user or os.getenv("PG_USER", "postgres")
    password = pg_password or os.getenv("PG_PASSWORD", "postgres")
    database = pg_database or os.getenv("PG_DATABASE", "arccos")

    con = psycopg2.connect(
        host=host, port=port,
        user=user, password=password,
        dbname=database
    )
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS rounds (
            round_id INTEGER PRIMARY KEY,
            round_uuid TEXT,
            user_id TEXT,
            course_id INTEGER,
            course_version INTEGER,
            course_name TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            last_modified TIMESTAMP,
            tee_id INTEGER,
            num_holes INTEGER,
            num_shots INTEGER,
            par INTEGER,
            over_under INTEGER,
            is_ended TEXT,
            ball_make_id INTEGER,
            ball_model_id INTEGER,
            drive_hcp DOUBLE PRECISION,
            approach_hcp DOUBLE PRECISION,
            chip_hcp DOUBLE PRECISION,
            sand_hcp DOUBLE PRECISION,
            putt_hcp DOUBLE PRECISION,
            notes TEXT,
            -- weather enrichment (Visual Crossing)
            avg_temp_f DOUBLE PRECISION,
            avg_feels_like_f DOUBLE PRECISION,
            temp_max_f DOUBLE PRECISION,
            temp_min_f DOUBLE PRECISION,
            avg_humidity_pct DOUBLE PRECISION,
            avg_dew_point_f DOUBLE PRECISION,
            avg_wind_mph DOUBLE PRECISION,
            avg_wind_dir DOUBLE PRECISION,
            max_wind_gust_mph DOUBLE PRECISION,
            total_precip_in DOUBLE PRECISION,
            precip_prob_pct DOUBLE PRECISION,
            precip_type TEXT,
            snow_in DOUBLE PRECISION,
            avg_cloud_cover_pct DOUBLE PRECISION,
            avg_pressure_hpa DOUBLE PRECISION,
            avg_visibility_mi DOUBLE PRECISION,
            avg_uv_index DOUBLE PRECISION,
            avg_solar_radiation DOUBLE PRECISION,
            solar_energy DOUBLE PRECISION,
            severe_risk DOUBLE PRECISION,
            moon_phase DOUBLE PRECISION,
            feels_like_max_f DOUBLE PRECISION,
            feels_like_min_f DOUBLE PRECISION,
            precip_cover_pct DOUBLE PRECISION,
            snow_depth_in DOUBLE PRECISION,
            weather_conditions TEXT,
            weather_description TEXT,
            weather_icon TEXT,
            -- sunrise/sunset enrichment
            sunrise TEXT,
            sunset TEXT,
            dawn TEXT,
            dusk TEXT,
            day_length_hrs DOUBLE PRECISION,
            solar_noon TEXT,
            golden_hour TEXT,
            first_light TEXT,
            last_light TEXT,
            sun_altitude DOUBLE PRECISION,
            sunrise_azimuth DOUBLE PRECISION,
            sunset_azimuth DOUBLE PRECISION,
            moon_illumination DOUBLE PRECISION,
            moon_phase_name TEXT,
            moonrise TEXT,
            moonset TEXT
        )
    """)

    # Migrate: add new columns to existing tables
    new_round_cols = {
        "avg_feels_like_f": "DOUBLE PRECISION", "temp_max_f": "DOUBLE PRECISION",
        "temp_min_f": "DOUBLE PRECISION",
        "avg_dew_point_f": "DOUBLE PRECISION", "max_wind_gust_mph": "DOUBLE PRECISION",
        "precip_prob_pct": "DOUBLE PRECISION", "precip_type": "TEXT", "snow_in": "DOUBLE PRECISION",
        "avg_cloud_cover_pct": "DOUBLE PRECISION", "avg_pressure_hpa": "DOUBLE PRECISION",
        "avg_visibility_mi": "DOUBLE PRECISION", "avg_uv_index": "DOUBLE PRECISION",
        "avg_solar_radiation": "DOUBLE PRECISION", "solar_energy": "DOUBLE PRECISION",
        "severe_risk": "DOUBLE PRECISION", "moon_phase": "DOUBLE PRECISION",
        "feels_like_max_f": "DOUBLE PRECISION", "feels_like_min_f": "DOUBLE PRECISION",
        "precip_cover_pct": "DOUBLE PRECISION", "snow_depth_in": "DOUBLE PRECISION",
        "weather_conditions": "TEXT", "weather_description": "TEXT",
        "weather_icon": "TEXT",
        "sunrise": "TEXT", "sunset": "TEXT", "dawn": "TEXT",
        "dusk": "TEXT", "day_length_hrs": "DOUBLE PRECISION",
        "solar_noon": "TEXT", "golden_hour": "TEXT",
        "first_light": "TEXT", "last_light": "TEXT",
        "sun_altitude": "DOUBLE PRECISION", "sunrise_azimuth": "DOUBLE PRECISION",
        "sunset_azimuth": "DOUBLE PRECISION", "moon_illumination": "DOUBLE PRECISION",
        "moon_phase_name": "TEXT", "moonrise": "TEXT", "moonset": "TEXT",
    }
    for col, dtype in new_round_cols.items():
        cur.execute(f"ALTER TABLE rounds ADD COLUMN IF NOT EXISTS {col} {dtype}")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS holes (
            round_id INTEGER,
            hole_id INTEGER,
            num_shots INTEGER,
            is_gir TEXT,
            putts INTEGER,
            is_fairway TEXT,
            is_fairway_right TEXT,
            is_fairway_left TEXT,
            is_sand_save_chance TEXT,
            is_sand_save TEXT,
            is_up_down_chance TEXT,
            is_up_down TEXT,
            approach_shot_id INTEGER,
            pin_lat DOUBLE PRECISION,
            pin_long DOUBLE PRECISION,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            score_override INTEGER,
            PRIMARY KEY (round_id, hole_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS shots (
            round_id INTEGER,
            hole_id INTEGER,
            shot_id INTEGER,
            shot_uuid TEXT,
            club_type INTEGER,
            club_id INTEGER,
            start_lat DOUBLE PRECISION,
            start_long DOUBLE PRECISION,
            end_lat DOUBLE PRECISION,
            end_long DOUBLE PRECISION,
            distance DOUBLE PRECISION,
            is_half_swing TEXT,
            start_altitude DOUBLE PRECISION,
            end_altitude DOUBLE PRECISION,
            shot_time TIMESTAMP,
            should_ignore TEXT,
            num_penalties INTEGER,
            user_start_terrain_override INTEGER,
            should_consider_putt_as_chip TEXT,
            tour_quality TEXT,
            PRIMARY KEY (round_id, hole_id, shot_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sga_analysis (
            round_id INTEGER,
            goal_hcp INTEGER,
            -- overall
            overall_sga DOUBLE PRECISION,
            drive_sga DOUBLE PRECISION,
            approach_sga DOUBLE PRECISION,
            short_game_sga DOUBLE PRECISION,
            putting_sga DOUBLE PRECISION,
            -- historic comparisons
            historic_drive_sga DOUBLE PRECISION,
            historic_approach_sga DOUBLE PRECISION,
            historic_short_sga DOUBLE PRECISION,
            historic_putting_sga DOUBLE PRECISION,
            -- traditional stats
            pace_of_play TEXT,
            avg_drive_distance DOUBLE PRECISION,
            longest_drive DOUBLE PRECISION,
            avg_approach_distance DOUBLE PRECISION,
            total_putts INTEGER,
            zero_putts INTEGER,
            one_putts INTEGER,
            two_putts INTEGER,
            three_putts INTEGER,
            gir_hit INTEGER,
            gir_total INTEGER,
            fairways_hit INTEGER,
            fairways_total INTEGER,
            up_and_down_success INTEGER,
            up_and_down_total INTEGER,
            total_distance DOUBLE PRECISION,
            -- score analysis
            par3_avg_score DOUBLE PRECISION,
            par3_sga DOUBLE PRECISION,
            par4_avg_score DOUBLE PRECISION,
            par4_sga DOUBLE PRECISION,
            par5_avg_score DOUBLE PRECISION,
            par5_sga DOUBLE PRECISION,
            birdie_pct DOUBLE PRECISION,
            par_pct DOUBLE PRECISION,
            bogey_pct DOUBLE PRECISION,
            double_plus_pct DOUBLE PRECISION,
            -- driving detail
            sg_distance DOUBLE PRECISION,
            sg_accuracy DOUBLE PRECISION,
            sg_penalties DOUBLE PRECISION,
            -- approach detail
            gir_pct DOUBLE PRECISION,
            gir_miss_left_pct DOUBLE PRECISION,
            gir_miss_right_pct DOUBLE PRECISION,
            gir_miss_short_pct DOUBLE PRECISION,
            gir_miss_long_pct DOUBLE PRECISION,
            avg_proximity_gir_ft DOUBLE PRECISION,
            avg_proximity_all_ft DOUBLE PRECISION,
            -- putting detail
            one_putt_pct DOUBLE PRECISION,
            two_putt_pct DOUBLE PRECISION,
            three_putt_pct DOUBLE PRECISION,
            putts_per_hole DOUBLE PRECISION,
            putts_per_gir DOUBLE PRECISION,
            -- raw json for anything else
            raw_json JSONB,
            PRIMARY KEY (round_id, goal_hcp)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sga_by_distance (
            round_id INTEGER,
            goal_hcp INTEGER,
            category TEXT,
            slab_id INTEGER,
            slab_label TEXT,
            sga DOUBLE PRECISION,
            shots_count INTEGER,
            PRIMARY KEY (round_id, goal_hcp, category, slab_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sga_by_terrain (
            round_id INTEGER,
            goal_hcp INTEGER,
            category TEXT,
            terrain TEXT,
            sga DOUBLE PRECISION,
            shots_count INTEGER,
            PRIMARY KEY (round_id, goal_hcp, category, terrain)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sga_by_hole_shape (
            round_id INTEGER,
            goal_hcp INTEGER,
            hole_shape TEXT,
            sga DOUBLE PRECISION,
            shots_count INTEGER,
            PRIMARY KEY (round_id, goal_hcp, hole_shape)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sga_putting_by_hole (
            round_id INTEGER,
            goal_hcp INTEGER,
            hole_id INTEGER,
            sga DOUBLE PRECISION,
            PRIMARY KEY (round_id, goal_hcp, hole_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS clubs (
            user_id TEXT,
            bag_id INTEGER,
            club_id INTEGER,
            club_type INTEGER,
            sensor_uuid TEXT,
            sensor_type_id INTEGER,
            name TEXT,
            perceived_distance DOUBLE PRECISION,
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            is_deleted TEXT,
            make TEXT,
            model TEXT,
            loft TEXT,
            flex TEXT,
            raw_json JSONB,
            PRIMARY KEY (user_id, bag_id, club_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS courses (
            course_id INTEGER PRIMARY KEY,
            course_version INTEGER,
            course_name TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            city TEXT,
            state TEXT,
            country TEXT,
            num_holes INTEGER,
            mens_par INTEGER,
            womens_par INTEGER,
            raw_json JSONB
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS course_tees (
            course_id INTEGER,
            tee_id INTEGER,
            tee_name TEXT,
            distance INTEGER,
            slope INTEGER,
            rating DOUBLE PRECISION,
            PRIMARY KEY (course_id, tee_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_cache (
            round_id INTEGER PRIMARY KEY,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            date TEXT,
            start_hour INTEGER,
            end_hour INTEGER,
            raw_response JSONB
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS course_holes (
            course_id INTEGER,
            course_version INTEGER,
            hole_id INTEGER,
            mens_par INTEGER,
            womens_par INTEGER,
            mens_handicap INTEGER,
            womens_handicap INTEGER,
            is_dual_green TEXT,
            PRIMARY KEY (course_id, hole_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS course_hole_tees (
            course_id INTEGER,
            hole_id INTEGER,
            tee_id INTEGER,
            tee_name TEXT,
            distance DOUBLE PRECISION,
            PRIMARY KEY (course_id, hole_id, tee_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS player_profile (
            user_id TEXT PRIMARY KEY,
            handicap DOUBLE PRECISION,
            total_rounds INTEGER,
            holes_played INTEGER,
            shots_played INTEGER,
            goal_hcp DOUBLE PRECISION,
            num_rounds_setting INTEGER
        )
    """)

    # GHIN tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ghin_scores (
            id INTEGER PRIMARY KEY,
            golfer_id INTEGER,
            played_at DATE,
            course_name TEXT,
            course_id TEXT,
            tee_name TEXT,
            tee_set_id TEXT,
            tee_set_side TEXT,
            adjusted_gross_score INTEGER,
            front9_adjusted INTEGER,
            back9_adjusted INTEGER,
            net_score INTEGER,
            net_score_differential DOUBLE PRECISION,
            course_rating DOUBLE PRECISION,
            slope_rating INTEGER,
            differential DOUBLE PRECISION,
            unadjusted_differential DOUBLE PRECISION,
            pcc INTEGER,
            course_handicap TEXT,
            score_type TEXT,
            number_of_holes INTEGER,
            number_of_played_holes INTEGER,
            posted_at TIMESTAMP,
            posted_on_home_course BOOLEAN,
            is_manual BOOLEAN,
            edited BOOLEAN,
            exceptional BOOLEAN,
            used BOOLEAN,
            revision BOOLEAN,
            penalty TEXT,
            penalty_type TEXT,
            raw_json JSONB
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ghin_handicap_history (
            id TEXT,
            rev_date DATE,
            handicap_index DOUBLE PRECISION,
            low_hi DOUBLE PRECISION,
            hard_cap TEXT,
            soft_cap TEXT,
            PRIMARY KEY (id)
        )
    """)

    cur.execute("""
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

    # SGA categories reference table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sga_categories (
            category_id INTEGER PRIMARY KEY,
            category_name TEXT NOT NULL,
            description TEXT
        )
    """)
    # Seed sga_categories if empty
    cur.execute("SELECT COUNT(*) FROM sga_categories")
    if cur.fetchone()[0] == 0:
        cur.execute("""
            INSERT INTO sga_categories (category_id, category_name, description) VALUES
            (1, 'driving', 'Tee shots on par 4s and par 5s'),
            (2, 'approach', 'Approach shots to the green'),
            (3, 'chip', 'Chipping around the green'),
            (4, 'sand', 'Sand/bunker shots'),
            (5, 'putting', 'Putting on the green')
        """)

    con.commit()
    return con, cur


def _safe_ts(val):
    """Return None for zero/invalid timestamps that PostgreSQL cannot parse."""
    if val and isinstance(val, str) and val.startswith("0000-00-00"):
        return None
    return val


def store_round(con, cur, rd: dict,
                weather: dict = None, sun: dict = None):
    """Upsert a round and its holes/shots."""
    w = weather or {}
    s = sun or {}

    cur.execute("""
        INSERT INTO rounds (
            round_id, round_uuid, user_id, course_id, course_version, course_name,
            start_time, end_time, last_modified, tee_id, num_holes, num_shots,
            par, over_under, is_ended, ball_make_id, ball_model_id,
            drive_hcp, approach_hcp, chip_hcp, sand_hcp, putt_hcp, notes,
            avg_temp_f, avg_feels_like_f, temp_max_f, temp_min_f,
            avg_humidity_pct, avg_dew_point_f, avg_wind_mph, avg_wind_dir,
            max_wind_gust_mph, total_precip_in, precip_prob_pct, precip_type,
            snow_in, avg_cloud_cover_pct, avg_pressure_hpa, avg_visibility_mi,
            avg_uv_index, avg_solar_radiation, solar_energy, severe_risk,
            moon_phase, feels_like_max_f, feels_like_min_f, precip_cover_pct,
            snow_depth_in, weather_conditions, weather_description, weather_icon,
            sunrise, sunset, dawn, dusk, day_length_hrs, solar_noon, golden_hour,
            first_light, last_light, sun_altitude, sunrise_azimuth, sunset_azimuth,
            moon_illumination, moon_phase_name, moonrise, moonset
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON CONFLICT (round_id) DO UPDATE SET
            round_uuid=EXCLUDED.round_uuid, user_id=EXCLUDED.user_id,
            course_id=EXCLUDED.course_id, course_version=EXCLUDED.course_version,
            course_name=EXCLUDED.course_name, start_time=EXCLUDED.start_time,
            end_time=EXCLUDED.end_time, last_modified=EXCLUDED.last_modified,
            tee_id=EXCLUDED.tee_id, num_holes=EXCLUDED.num_holes,
            num_shots=EXCLUDED.num_shots, par=EXCLUDED.par,
            over_under=EXCLUDED.over_under, is_ended=EXCLUDED.is_ended,
            ball_make_id=EXCLUDED.ball_make_id, ball_model_id=EXCLUDED.ball_model_id,
            drive_hcp=EXCLUDED.drive_hcp, approach_hcp=EXCLUDED.approach_hcp,
            chip_hcp=EXCLUDED.chip_hcp, sand_hcp=EXCLUDED.sand_hcp,
            putt_hcp=EXCLUDED.putt_hcp, notes=EXCLUDED.notes,
            avg_temp_f=EXCLUDED.avg_temp_f, avg_feels_like_f=EXCLUDED.avg_feels_like_f,
            temp_max_f=EXCLUDED.temp_max_f, temp_min_f=EXCLUDED.temp_min_f,
            avg_humidity_pct=EXCLUDED.avg_humidity_pct, avg_dew_point_f=EXCLUDED.avg_dew_point_f,
            avg_wind_mph=EXCLUDED.avg_wind_mph, avg_wind_dir=EXCLUDED.avg_wind_dir,
            max_wind_gust_mph=EXCLUDED.max_wind_gust_mph, total_precip_in=EXCLUDED.total_precip_in,
            precip_prob_pct=EXCLUDED.precip_prob_pct, precip_type=EXCLUDED.precip_type,
            snow_in=EXCLUDED.snow_in, avg_cloud_cover_pct=EXCLUDED.avg_cloud_cover_pct,
            avg_pressure_hpa=EXCLUDED.avg_pressure_hpa, avg_visibility_mi=EXCLUDED.avg_visibility_mi,
            avg_uv_index=EXCLUDED.avg_uv_index, avg_solar_radiation=EXCLUDED.avg_solar_radiation,
            solar_energy=EXCLUDED.solar_energy, severe_risk=EXCLUDED.severe_risk,
            moon_phase=EXCLUDED.moon_phase, feels_like_max_f=EXCLUDED.feels_like_max_f,
            feels_like_min_f=EXCLUDED.feels_like_min_f, precip_cover_pct=EXCLUDED.precip_cover_pct,
            snow_depth_in=EXCLUDED.snow_depth_in, weather_conditions=EXCLUDED.weather_conditions,
            weather_description=EXCLUDED.weather_description, weather_icon=EXCLUDED.weather_icon,
            sunrise=EXCLUDED.sunrise, sunset=EXCLUDED.sunset, dawn=EXCLUDED.dawn,
            dusk=EXCLUDED.dusk, day_length_hrs=EXCLUDED.day_length_hrs,
            solar_noon=EXCLUDED.solar_noon, golden_hour=EXCLUDED.golden_hour,
            first_light=EXCLUDED.first_light, last_light=EXCLUDED.last_light,
            sun_altitude=EXCLUDED.sun_altitude, sunrise_azimuth=EXCLUDED.sunrise_azimuth,
            sunset_azimuth=EXCLUDED.sunset_azimuth, moon_illumination=EXCLUDED.moon_illumination,
            moon_phase_name=EXCLUDED.moon_phase_name, moonrise=EXCLUDED.moonrise,
            moonset=EXCLUDED.moonset
    """, [
        rd["roundId"], rd.get("roundUUID"), rd.get("userId"),
        rd["courseId"], rd.get("courseVersion"), rd.get("courseName"),
        _safe_ts(rd.get("startTime")), _safe_ts(rd.get("endTime")), _safe_ts(rd.get("lastModifiedTime")),
        rd.get("teeId"), rd.get("noOfHoles"), rd.get("noOfShots"),
        rd.get("par"),
        rd.get("overUnder"),
        rd.get("isEnded"), rd.get("ballMakeId"), rd.get("ballModelId"),
        rd.get("driveHcp"), rd.get("approachHcp"), rd.get("chipHcp"),
        rd.get("sandHcp"), rd.get("puttHcp"), rd.get("notes"),
        # weather
        w.get("avg_temp_f"), w.get("avg_feels_like_f"),
        w.get("temp_max_f"), w.get("temp_min_f"),
        w.get("avg_humidity_pct"), w.get("avg_dew_point_f"),
        w.get("avg_wind_mph"), w.get("avg_wind_dir"), w.get("max_wind_gust_mph"),
        w.get("total_precip_in"), w.get("precip_prob_pct"),
        w.get("precip_type"), w.get("snow_in"),
        w.get("avg_cloud_cover_pct"), w.get("avg_pressure_hpa"),
        w.get("avg_visibility_mi"), w.get("avg_uv_index"),
        w.get("avg_solar_radiation"), w.get("solar_energy"),
        w.get("severe_risk"), w.get("moon_phase"),
        w.get("feels_like_max_f"), w.get("feels_like_min_f"),
        w.get("precip_cover_pct"), w.get("snow_depth_in"),
        w.get("weather_conditions"), w.get("weather_description"), w.get("weather_icon"),
        # sunrise/sunset
        s.get("sunrise"), s.get("sunset"), s.get("dawn"), s.get("dusk"),
        s.get("day_length_hrs"), s.get("solar_noon"), s.get("golden_hour"),
        s.get("first_light"), s.get("last_light"),
        s.get("sun_altitude"), s.get("sunrise_azimuth"), s.get("sunset_azimuth"),
        s.get("moon_illumination"), s.get("moon_phase_name"),
        s.get("moonrise"), s.get("moonset"),
    ])

    for hole in rd.get("holes") or []:
        if hole is None:
            continue
        cur.execute("""
            INSERT INTO holes (
                round_id, hole_id, num_shots, is_gir, putts,
                is_fairway, is_fairway_right, is_fairway_left,
                is_sand_save_chance, is_sand_save,
                is_up_down_chance, is_up_down,
                approach_shot_id, pin_lat, pin_long,
                start_time, end_time, score_override
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (round_id, hole_id) DO UPDATE SET
                num_shots=EXCLUDED.num_shots, is_gir=EXCLUDED.is_gir,
                putts=EXCLUDED.putts, is_fairway=EXCLUDED.is_fairway,
                is_fairway_right=EXCLUDED.is_fairway_right,
                is_fairway_left=EXCLUDED.is_fairway_left,
                is_sand_save_chance=EXCLUDED.is_sand_save_chance,
                is_sand_save=EXCLUDED.is_sand_save,
                is_up_down_chance=EXCLUDED.is_up_down_chance,
                is_up_down=EXCLUDED.is_up_down,
                approach_shot_id=EXCLUDED.approach_shot_id,
                pin_lat=EXCLUDED.pin_lat, pin_long=EXCLUDED.pin_long,
                start_time=EXCLUDED.start_time, end_time=EXCLUDED.end_time,
                score_override=EXCLUDED.score_override
        """, [
            rd["roundId"], hole["holeId"], hole.get("noOfShots"),
            hole.get("isGir"), hole.get("putts"),
            hole.get("isFairWay"), hole.get("isFairWayRight"), hole.get("isFairWayLeft"),
            hole.get("isSandSaveChance"), hole.get("isSandSave"),
            hole.get("isUpDownChance"), hole.get("isUpDown"),
            hole.get("approachShotId"),
            hole.get("pinLat"), hole.get("pinLong"),
            _safe_ts(hole.get("startTime")), _safe_ts(hole.get("endTime")),
            hole.get("scoreOverride"),
        ])

        for shot in hole.get("shots", []):
            cur.execute("""
                INSERT INTO shots (
                    round_id, hole_id, shot_id, shot_uuid, club_type, club_id,
                    start_lat, start_long, end_lat, end_long,
                    distance, is_half_swing, start_altitude, end_altitude,
                    shot_time, should_ignore, num_penalties,
                    user_start_terrain_override, should_consider_putt_as_chip,
                    tour_quality
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (round_id, hole_id, shot_id) DO UPDATE SET
                    shot_uuid=EXCLUDED.shot_uuid, club_type=EXCLUDED.club_type,
                    club_id=EXCLUDED.club_id, start_lat=EXCLUDED.start_lat,
                    start_long=EXCLUDED.start_long, end_lat=EXCLUDED.end_lat,
                    end_long=EXCLUDED.end_long, distance=EXCLUDED.distance,
                    is_half_swing=EXCLUDED.is_half_swing,
                    start_altitude=EXCLUDED.start_altitude,
                    end_altitude=EXCLUDED.end_altitude,
                    shot_time=EXCLUDED.shot_time,
                    should_ignore=EXCLUDED.should_ignore,
                    num_penalties=EXCLUDED.num_penalties,
                    user_start_terrain_override=EXCLUDED.user_start_terrain_override,
                    should_consider_putt_as_chip=EXCLUDED.should_consider_putt_as_chip,
                    tour_quality=EXCLUDED.tour_quality
            """, [
                rd["roundId"], hole["holeId"], shot["shotId"],
                shot.get("shotUUID"), shot.get("clubType"), shot.get("clubId"),
                shot.get("startLat"), shot.get("startLong"),
                shot.get("endLat"), shot.get("endLong"),
                shot.get("distance"), shot.get("isHalfSwing"),
                shot.get("startAltitude"), shot.get("endAltitude"),
                _safe_ts(shot.get("shotTime")), shot.get("shouldIgnore"),
                shot.get("noOfPenalties"), shot.get("userStartTerrainOverride"),
                shot.get("shouldConsiderPuttAsChip"), shot.get("tourQuality"),
            ])

    con.commit()


def store_sga(con, cur, round_id: int, goal_hcp: int, sga: dict):
    """Store SGA analysis with full detail extraction."""
    if not sga:
        return
    ov = sga.get("overall", {})
    sec = ov.get("overallSection", {})
    ts = ov.get("traditionalStats", {})
    sa = ov.get("scoreAnalysis", {})
    dr = sga.get("driving", {})
    ap = sga.get("approach", {})
    sh = sga.get("short", {})
    pu = sga.get("putting", {})
    dva = dr.get("distanceVsAccuracy", {})
    gir = ap.get("gir", {})
    apr = pu.get("avgPuttsPerRound", {})

    cur.execute("""
        INSERT INTO sga_analysis (
            round_id, goal_hcp,
            overall_sga, drive_sga, approach_sga, short_game_sga, putting_sga,
            historic_drive_sga, historic_approach_sga, historic_short_sga, historic_putting_sga,
            pace_of_play, avg_drive_distance, longest_drive, avg_approach_distance,
            total_putts, zero_putts, one_putts, two_putts, three_putts,
            gir_hit, gir_total, fairways_hit, fairways_total,
            up_and_down_success, up_and_down_total, total_distance,
            par3_avg_score, par3_sga, par4_avg_score, par4_sga,
            par5_avg_score, par5_sga,
            birdie_pct, par_pct, bogey_pct, double_plus_pct,
            sg_distance, sg_accuracy, sg_penalties,
            gir_pct, gir_miss_left_pct, gir_miss_right_pct,
            gir_miss_short_pct, gir_miss_long_pct,
            avg_proximity_gir_ft, avg_proximity_all_ft,
            one_putt_pct, two_putt_pct, three_putt_pct,
            putts_per_hole, putts_per_gir,
            raw_json
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON CONFLICT (round_id, goal_hcp) DO UPDATE SET
            overall_sga=EXCLUDED.overall_sga, drive_sga=EXCLUDED.drive_sga,
            approach_sga=EXCLUDED.approach_sga, short_game_sga=EXCLUDED.short_game_sga,
            putting_sga=EXCLUDED.putting_sga,
            historic_drive_sga=EXCLUDED.historic_drive_sga,
            historic_approach_sga=EXCLUDED.historic_approach_sga,
            historic_short_sga=EXCLUDED.historic_short_sga,
            historic_putting_sga=EXCLUDED.historic_putting_sga,
            pace_of_play=EXCLUDED.pace_of_play,
            avg_drive_distance=EXCLUDED.avg_drive_distance,
            longest_drive=EXCLUDED.longest_drive,
            avg_approach_distance=EXCLUDED.avg_approach_distance,
            total_putts=EXCLUDED.total_putts, zero_putts=EXCLUDED.zero_putts,
            one_putts=EXCLUDED.one_putts, two_putts=EXCLUDED.two_putts,
            three_putts=EXCLUDED.three_putts,
            gir_hit=EXCLUDED.gir_hit, gir_total=EXCLUDED.gir_total,
            fairways_hit=EXCLUDED.fairways_hit, fairways_total=EXCLUDED.fairways_total,
            up_and_down_success=EXCLUDED.up_and_down_success,
            up_and_down_total=EXCLUDED.up_and_down_total,
            total_distance=EXCLUDED.total_distance,
            par3_avg_score=EXCLUDED.par3_avg_score, par3_sga=EXCLUDED.par3_sga,
            par4_avg_score=EXCLUDED.par4_avg_score, par4_sga=EXCLUDED.par4_sga,
            par5_avg_score=EXCLUDED.par5_avg_score, par5_sga=EXCLUDED.par5_sga,
            birdie_pct=EXCLUDED.birdie_pct, par_pct=EXCLUDED.par_pct,
            bogey_pct=EXCLUDED.bogey_pct, double_plus_pct=EXCLUDED.double_plus_pct,
            sg_distance=EXCLUDED.sg_distance, sg_accuracy=EXCLUDED.sg_accuracy,
            sg_penalties=EXCLUDED.sg_penalties,
            gir_pct=EXCLUDED.gir_pct,
            gir_miss_left_pct=EXCLUDED.gir_miss_left_pct,
            gir_miss_right_pct=EXCLUDED.gir_miss_right_pct,
            gir_miss_short_pct=EXCLUDED.gir_miss_short_pct,
            gir_miss_long_pct=EXCLUDED.gir_miss_long_pct,
            avg_proximity_gir_ft=EXCLUDED.avg_proximity_gir_ft,
            avg_proximity_all_ft=EXCLUDED.avg_proximity_all_ft,
            one_putt_pct=EXCLUDED.one_putt_pct, two_putt_pct=EXCLUDED.two_putt_pct,
            three_putt_pct=EXCLUDED.three_putt_pct,
            putts_per_hole=EXCLUDED.putts_per_hole, putts_per_gir=EXCLUDED.putts_per_gir,
            raw_json=EXCLUDED.raw_json
    """, [
        round_id, goal_hcp,
        sec.get("sga"), sec.get("drivingSga"), sec.get("approachSga"),
        sec.get("shortSga"), sec.get("puttingSga"),
        # historic
        dr.get("historicSga"), ap.get("historicSga"),
        sh.get("historicSga"), pu.get("historicSga"),
        # traditional
        ov.get("paceOfPlay"),
        ts.get("averageDriveDistance", {}).get("value"),
        ts.get("longestDrive", {}).get("value"),
        ts.get("averageApproachDistance", {}).get("value"),
        ts.get("totalPutts", {}).get("value"),
        ts.get("totalPutts", {}).get("zeroPutt"),
        ts.get("totalPutts", {}).get("onePutt"),
        ts.get("totalPutts", {}).get("twoPutt"),
        ts.get("totalPutts", {}).get("threePutt"),
        ts.get("gir", {}).get("noOfGirsHit"),
        ts.get("gir", {}).get("noOfHoles"),
        ts.get("hitFairway", {}).get("fairways"),
        ts.get("hitFairway", {}).get("totalFairways"),
        ts.get("upAndDown", {}).get("upAndDownSuccess"),
        ts.get("upAndDown", {}).get("totalChances"),
        ts.get("totalDistance", {}).get("value"),
        # score analysis
        sa.get("parsData", {}).get("par3", {}).get("score"),
        sa.get("parsData", {}).get("par3", {}).get("sga"),
        sa.get("parsData", {}).get("par4", {}).get("score"),
        sa.get("parsData", {}).get("par4", {}).get("sga"),
        sa.get("parsData", {}).get("par5", {}).get("score"),
        sa.get("parsData", {}).get("par5", {}).get("sga"),
        sa.get("birdies", {}).get("actual", {}).get("value"),
        sa.get("pars", {}).get("actual", {}).get("value"),
        sa.get("bogies", {}).get("actual", {}).get("value"),
        sa.get("doubleplus", {}).get("actual", {}).get("value"),
        # driving
        dva.get("sgDistance"), dva.get("sgAccuracy"), dva.get("sgPenalties"),
        # approach
        gir.get("gir", {}).get("value"),
        gir.get("left", {}).get("value"),
        gir.get("right", {}).get("value"),
        gir.get("short", {}).get("value"),
        gir.get("long", {}).get("value"),
        gir.get("girApproach", {}).get("actual", {}).get("value"),
        gir.get("allApproach", {}).get("actual", {}).get("value"),
        # putting
        apr.get("onePutt", {}).get("actual", {}).get("value"),
        apr.get("twoPutt", {}).get("actual", {}).get("value"),
        apr.get("threePutt", {}).get("actual", {}).get("value"),
        apr.get("perHole", {}).get("actual") if isinstance(apr.get("perHole"), dict) else apr.get("perHole", {}).get("actual"),
        apr.get("perGir", {}).get("actual") if isinstance(apr.get("perGir"), dict) else apr.get("perGir", {}).get("actual"),
        # raw
        json.dumps(sga),
    ])

    # SGA by distance slabs (driving, approach, chip, sand, putting)
    for cat, key, items_key in [
        ("driving", dr, "drivingByHoleLength"),
        ("approach", ap, "approachByPinDistance"),
        ("chip", sh, "chipByPinDistance"),
        ("sand", sh, "sandByPinDistance"),
        ("putting", pu, "puttingByLength"),
    ]:
        for item in key.get(items_key, []):
            cur.execute("""
                INSERT INTO sga_by_distance (round_id, goal_hcp, category, slab_id, slab_label, sga, shots_count)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (round_id, goal_hcp, category, slab_id) DO UPDATE SET
                    slab_label=EXCLUDED.slab_label, sga=EXCLUDED.sga,
                    shots_count=EXCLUDED.shots_count
            """, [
                round_id, goal_hcp, cat, item.get("slabId"),
                item.get("slab", {}).get("value", ""),
                item.get("sga"), item.get("shotsCount"),
            ])

    # SGA by terrain (approach)
    for item in ap.get("approachByTerrain", []):
        cur.execute("""
            INSERT INTO sga_by_terrain (round_id, goal_hcp, category, terrain, sga, shots_count)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (round_id, goal_hcp, category, terrain) DO UPDATE SET
                sga=EXCLUDED.sga, shots_count=EXCLUDED.shots_count
        """, [
            round_id, goal_hcp, "approach", item.get("terrain"),
            item.get("sga"), item.get("shotsCount"),
        ])

    # SGA by hole shape (driving)
    for item in dr.get("drivingByHoleShape", []):
        cur.execute("""
            INSERT INTO sga_by_hole_shape (round_id, goal_hcp, hole_shape, sga, shots_count)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (round_id, goal_hcp, hole_shape) DO UPDATE SET
                sga=EXCLUDED.sga, shots_count=EXCLUDED.shots_count
        """, [
            round_id, goal_hcp, item.get("holeShape"),
            item.get("sga"), item.get("shotsCount"),
        ])

    # Putting SGA by hole
    for item in pu.get("puttingByHole", {}).get("holeSga", []):
        cur.execute("""
            INSERT INTO sga_putting_by_hole (round_id, goal_hcp, hole_id, sga)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (round_id, goal_hcp, hole_id) DO UPDATE SET
                sga=EXCLUDED.sga
        """, [round_id, goal_hcp, item.get("holeId"), item.get("sga")])

    con.commit()


def store_course(con, cur, course: dict, course_id: int):
    """Store course info and tee data."""
    cur.execute("""
        INSERT INTO courses (
            course_id, course_version, course_name, latitude, longitude,
            city, state, country, num_holes, mens_par, womens_par, raw_json
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (course_id) DO UPDATE SET
            course_version=EXCLUDED.course_version, course_name=EXCLUDED.course_name,
            latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude,
            city=EXCLUDED.city, state=EXCLUDED.state, country=EXCLUDED.country,
            num_holes=EXCLUDED.num_holes, mens_par=EXCLUDED.mens_par,
            womens_par=EXCLUDED.womens_par, raw_json=EXCLUDED.raw_json
    """, [
        course_id,
        course.get("courseVersion"),
        course.get("courseName", course.get("name")),
        course.get("latitude"),
        course.get("longitude"),
        course.get("city"),
        course.get("state"),
        course.get("country"),
        course.get("noOfHoles"),
        course.get("mensPar"),
        course.get("womensPar"),
        json.dumps(course),
    ])
    # Store tee data
    for tee in course.get("courseTees", []):
        cur.execute("""
            INSERT INTO course_tees (course_id, tee_id, tee_name, distance, slope, rating)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (course_id, tee_id) DO UPDATE SET
                tee_name=EXCLUDED.tee_name, distance=EXCLUDED.distance,
                slope=EXCLUDED.slope, rating=EXCLUDED.rating
        """, [
            course_id, tee.get("teeId"), tee.get("name"),
            tee.get("distance"), tee.get("slope"), tee.get("rating"),
        ])

    con.commit()


def store_course_holes(con, cur, course_id: int, course_version: int, holes: list):
    """Store per-hole course data (par, handicap, tee distances)."""
    for h in holes:
        hole_id = h.get("holeId")
        cur.execute("""
            INSERT INTO course_holes (
                course_id, course_version, hole_id,
                mens_par, womens_par, mens_handicap, womens_handicap, is_dual_green
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (course_id, hole_id) DO UPDATE SET
                course_version=EXCLUDED.course_version,
                mens_par=EXCLUDED.mens_par, womens_par=EXCLUDED.womens_par,
                mens_handicap=EXCLUDED.mens_handicap, womens_handicap=EXCLUDED.womens_handicap,
                is_dual_green=EXCLUDED.is_dual_green
        """, [
            course_id, course_version, hole_id,
            h.get("mensPar"), h.get("womensPar"),
            h.get("mensHandicap"), h.get("womensHandicap"),
            h.get("isDualGreen"),
        ])
        for tee in h.get("holeTees", []):
            cur.execute("""
                INSERT INTO course_hole_tees (course_id, hole_id, tee_id, tee_name, distance)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (course_id, hole_id, tee_id) DO UPDATE SET
                    tee_name=EXCLUDED.tee_name, distance=EXCLUDED.distance
            """, [
                course_id, hole_id, tee.get("teeId"),
                tee.get("name"), tee.get("distance"),
            ])

    con.commit()


def store_player_profile(con, cur, user_id: str, profile: dict):
    """Store player profile/stats."""
    settings = profile.get("settings", {})
    cur.execute("""
        INSERT INTO player_profile (
            user_id, handicap, total_rounds, holes_played,
            shots_played, goal_hcp, num_rounds_setting
        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (user_id) DO UPDATE SET
            handicap=EXCLUDED.handicap, total_rounds=EXCLUDED.total_rounds,
            holes_played=EXCLUDED.holes_played, shots_played=EXCLUDED.shots_played,
            goal_hcp=EXCLUDED.goal_hcp, num_rounds_setting=EXCLUDED.num_rounds_setting
    """, [
        user_id, profile.get("handicap"),
        profile.get("totalRounds"), profile.get("holesPlayed"),
        profile.get("shotsPlayed"),
        settings.get("goalHcp"), settings.get("noOfRounds"),
    ])
    con.commit()


def store_clubs(con, cur, user_id: str, bag: dict):
    """Store bag/club data."""
    if not bag:
        return
    bag_id = bag.get("bagId")
    for club in bag.get("clubs", []):
        cur.execute("""
            INSERT INTO clubs (
                user_id, bag_id, club_id, club_type, sensor_uuid, sensor_type_id,
                name, perceived_distance, start_date, end_date, is_deleted,
                make, model, loft, flex, raw_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (user_id, bag_id, club_id) DO UPDATE SET
                club_type=EXCLUDED.club_type, sensor_uuid=EXCLUDED.sensor_uuid,
                sensor_type_id=EXCLUDED.sensor_type_id, name=EXCLUDED.name,
                perceived_distance=EXCLUDED.perceived_distance,
                start_date=EXCLUDED.start_date, end_date=EXCLUDED.end_date,
                is_deleted=EXCLUDED.is_deleted,
                make=EXCLUDED.make, model=EXCLUDED.model,
                loft=EXCLUDED.loft, flex=EXCLUDED.flex,
                raw_json=EXCLUDED.raw_json
        """, [
            user_id, bag_id, club.get("clubId"), club.get("clubType"),
            club.get("sensorUUID"), club.get("sensorTypeId"),
            club.get("name"), club.get("perceivedDistance"),
            club.get("startDate"), club.get("endDate"),
            club.get("isDeleted"),
            club.get("make"), club.get("model"),
            club.get("loft"), club.get("flex"),
            json.dumps(club),
        ])
    con.commit()


def store_ghin_score(con, cur, s: dict):
    """Store a GHIN score and its hole details."""
    cur.execute("""
        INSERT INTO ghin_scores (
            id, golfer_id, played_at, course_name, course_id,
            tee_name, tee_set_id, tee_set_side,
            adjusted_gross_score, front9_adjusted, back9_adjusted,
            net_score, net_score_differential,
            course_rating, slope_rating, differential, unadjusted_differential,
            pcc, course_handicap, score_type,
            number_of_holes, number_of_played_holes,
            posted_at, posted_on_home_course,
            is_manual, edited, exceptional, used, revision,
            penalty, penalty_type, raw_json
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON CONFLICT (id) DO UPDATE SET
            golfer_id=EXCLUDED.golfer_id, played_at=EXCLUDED.played_at,
            course_name=EXCLUDED.course_name, course_id=EXCLUDED.course_id,
            tee_name=EXCLUDED.tee_name, tee_set_id=EXCLUDED.tee_set_id,
            tee_set_side=EXCLUDED.tee_set_side,
            adjusted_gross_score=EXCLUDED.adjusted_gross_score,
            front9_adjusted=EXCLUDED.front9_adjusted,
            back9_adjusted=EXCLUDED.back9_adjusted,
            net_score=EXCLUDED.net_score,
            net_score_differential=EXCLUDED.net_score_differential,
            course_rating=EXCLUDED.course_rating, slope_rating=EXCLUDED.slope_rating,
            differential=EXCLUDED.differential,
            unadjusted_differential=EXCLUDED.unadjusted_differential,
            pcc=EXCLUDED.pcc, course_handicap=EXCLUDED.course_handicap,
            score_type=EXCLUDED.score_type,
            number_of_holes=EXCLUDED.number_of_holes,
            number_of_played_holes=EXCLUDED.number_of_played_holes,
            posted_at=EXCLUDED.posted_at,
            posted_on_home_course=EXCLUDED.posted_on_home_course,
            is_manual=EXCLUDED.is_manual, edited=EXCLUDED.edited,
            exceptional=EXCLUDED.exceptional, used=EXCLUDED.used,
            revision=EXCLUDED.revision,
            penalty=EXCLUDED.penalty, penalty_type=EXCLUDED.penalty_type,
            raw_json=EXCLUDED.raw_json
    """, [
        s["id"], s.get("golfer_id"), s.get("played_at"),
        s.get("course_name"), s.get("course_id"),
        s.get("tee_name"), s.get("tee_set_id"), s.get("tee_set_side"),
        s.get("adjusted_gross_score"),
        s.get("front9_adjusted"), s.get("back9_adjusted"),
        s.get("net_score"), s.get("net_score_differential"),
        s.get("course_rating"), s.get("slope_rating"),
        s.get("differential"), s.get("unadjusted_differential"),
        s.get("pcc"), s.get("course_handicap"),
        s.get("score_type"), s.get("number_of_holes"),
        s.get("number_of_played_holes"),
        s.get("posted_at"), s.get("posted_on_home_course"),
        s.get("is_manual"), s.get("edited"), s.get("exceptional"),
        s.get("used"), s.get("revision"),
        s.get("penalty"), s.get("penalty_type"),
        json.dumps(s),
    ])
    for h in s.get("hole_details", []):
        cur.execute("""
            INSERT INTO ghin_hole_scores (
                ghin_score_id, hole_number, par, raw_score,
                adjusted_gross_score, stroke_allocation, x_hole
            ) VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ghin_score_id, hole_number) DO UPDATE SET
                par=EXCLUDED.par, raw_score=EXCLUDED.raw_score,
                adjusted_gross_score=EXCLUDED.adjusted_gross_score,
                stroke_allocation=EXCLUDED.stroke_allocation,
                x_hole=EXCLUDED.x_hole
        """, [
            s["id"], h.get("hole_number"), h.get("par"),
            h.get("raw_score"), h.get("adjusted_gross_score"),
            h.get("stroke_allocation"), h.get("x_hole"),
        ])

    con.commit()


def store_ghin_handicap(con, cur, rev: dict):
    """Store a handicap revision."""
    cur.execute("""
        INSERT INTO ghin_handicap_history (id, rev_date, handicap_index, low_hi, hard_cap, soft_cap)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
            rev_date=EXCLUDED.rev_date, handicap_index=EXCLUDED.handicap_index,
            low_hi=EXCLUDED.low_hi, hard_cap=EXCLUDED.hard_cap,
            soft_cap=EXCLUDED.soft_cap
    """, [
        rev.get("ID"), rev.get("RevDate", "")[:10],
        float(rev["Value"]) if rev.get("Value") else None,
        float(rev["LowHI"]) if rev.get("LowHI") else None,
        rev.get("Hard_Cap"), rev.get("Soft_Cap"),
    ])
    con.commit()


# ─── Metabase Views ─────────────────────────────────────────────────────────

VIEWS = {
    "v_round_summary": """
        CREATE OR REPLACE VIEW v_round_summary AS
        SELECT
            r.round_id,
            r.course_name,
            r.start_time,
            r.start_time::DATE as round_date,
            EXTRACT(YEAR FROM r.start_time)::INT as year,
            EXTRACT(MONTH FROM r.start_time)::INT as month,
            TO_CHAR(r.start_time, 'Mon') as month_name,
            r.par,
            r.par + r.over_under as score,
            r.over_under,
            r.num_holes,
            r.avg_temp_f,
            r.avg_feels_like_f,
            r.avg_wind_mph,
            r.max_wind_gust_mph,
            r.avg_humidity_pct,
            r.total_precip_in,
            r.weather_conditions,
            r.weather_description,
            s.overall_sga,
            s.drive_sga,
            s.approach_sga,
            s.short_game_sga,
            s.putting_sga,
            s.avg_drive_distance,
            s.longest_drive,
            s.total_putts,
            s.one_putts,
            s.two_putts,
            s.three_putts,
            s.gir_hit,
            s.gir_total,
            CASE WHEN s.gir_total > 0
                 THEN ROUND(s.gir_hit * 100.0 / s.gir_total, 1) END as gir_pct,
            s.fairways_hit,
            s.fairways_total,
            CASE WHEN s.fairways_total > 0
                 THEN ROUND(s.fairways_hit * 100.0 / s.fairways_total, 1) END as fairway_pct,
            s.up_and_down_success,
            s.up_and_down_total,
            s.birdie_pct,
            s.par_pct,
            s.bogey_pct,
            s.double_plus_pct,
            s.par3_avg_score,
            s.par4_avg_score,
            s.par5_avg_score,
            s.putts_per_hole,
            s.putts_per_gir
        FROM rounds r
        LEFT JOIN sga_analysis s ON r.round_id = s.round_id AND s.goal_hcp = 0
        WHERE r.num_holes = 18
    """,

    "v_hole_detail": """
        CREATE OR REPLACE VIEW v_hole_detail AS
        SELECT
            h.round_id,
            r.course_name,
            r.start_time::DATE as round_date,
            h.hole_id,
            ch.mens_par as par,
            h.num_shots as score,
            h.num_shots - COALESCE(ch.mens_par, 0) as over_under,
            h.putts,
            h.is_gir,
            h.is_fairway,
            h.is_fairway_left,
            h.is_fairway_right,
            h.is_sand_save_chance,
            h.is_sand_save,
            h.is_up_down_chance,
            h.is_up_down,
            cht.distance as tee_distance,
            ch.mens_handicap as handicap_rating
        FROM holes h
        JOIN rounds r ON h.round_id = r.round_id
        LEFT JOIN course_holes ch ON r.course_id = ch.course_id AND h.hole_id = ch.hole_id
        LEFT JOIN course_hole_tees cht ON r.course_id = cht.course_id
                                       AND h.hole_id = cht.hole_id
                                       AND r.tee_id = cht.tee_id
        WHERE r.num_holes = 18
    """,

    "v_shot_detail": """
        CREATE OR REPLACE VIEW v_shot_detail AS
        SELECT
            s.round_id,
            r.course_name,
            r.start_time::DATE as round_date,
            s.hole_id,
            s.shot_id,
            c.club_type as real_club_type,
            CASE c.club_type
                WHEN 1 THEN 'Driver' WHEN 2 THEN '3-Wood' WHEN 3 THEN '5-Wood'
                WHEN 4 THEN '7-Wood' WHEN 5 THEN 'Hybrid'
                WHEN 6 THEN '2-Iron' WHEN 7 THEN '3-Iron' WHEN 8 THEN '4-Iron'
                WHEN 9 THEN '5-Iron' WHEN 10 THEN '6-Iron' WHEN 11 THEN '7-Iron'
                WHEN 12 THEN '8-Iron' WHEN 13 THEN '9-Iron' WHEN 14 THEN 'PW'
                WHEN 15 THEN 'GW' WHEN 16 THEN 'SW' WHEN 17 THEN 'LW'
                WHEN 42 THEN 'Putter'
                ELSE 'Club ' || c.club_type
            END as club_name,
            s.distance,
            s.start_lat,
            s.start_long,
            s.end_lat,
            s.end_long,
            s.start_altitude,
            s.end_altitude,
            s.num_penalties,
            s.shot_time
        FROM shots s
        JOIN rounds r ON s.round_id = r.round_id
        LEFT JOIN clubs c ON s.club_type = c.club_id
        WHERE s.should_ignore != 'T'
          AND r.num_holes = 18
    """,

    "v_handicap_history": """
        CREATE OR REPLACE VIEW v_handicap_history AS
        SELECT rev_date, handicap_index, low_hi
        FROM ghin_handicap_history
        WHERE handicap_index < 900
        ORDER BY rev_date
    """,
}


def create_views(con, cur):
    """Create Metabase-friendly views."""
    for view_name, view_sql in VIEWS.items():
        try:
            cur.execute(view_sql)
            con.commit()
            log.info(f"  View {view_name}: created")
        except Exception as e:
            con.rollback()
            log.error(f"  View {view_name}: FAILED - {e}")


def add_computed_columns(con, cur):
    """Add and populate computed columns for Metabase filters."""
    try:
        cur.execute("ALTER TABLE rounds ADD COLUMN IF NOT EXISTS course_and_tee TEXT")
        cur.execute("""
            UPDATE rounds
            SET course_and_tee = rounds.course_name || ' - ' || ct.tee_name
            FROM course_tees ct
            WHERE rounds.course_id = ct.course_id
              AND rounds.tee_id = ct.tee_id
        """)
        con.commit()
        log.info("  Computed column course_and_tee: populated")
    except Exception as e:
        con.rollback()
        log.error(f"  Computed column course_and_tee: FAILED - {e}")


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
        description="Pull Arccos golf data into PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python arccos_data_pull_pg.py                      # pull all rounds (or respect .env MAX_ROUNDS)
  python arccos_data_pull_pg.py --max 1              # pull just 1 round to test
  python arccos_data_pull_pg.py --year 2025          # pull only 2025 rounds
  python arccos_data_pull_pg.py --date 2026-04-04    # pull round(s) from a specific date
  python arccos_data_pull_pg.py --year 2025 --max 0  # combine filters
        """,
    )
    parser.add_argument("--max", type=int, default=None,
                        help="Max new rounds to pull (default: 0=all, or .env ARCCOS_MAX_ROUNDS)")
    parser.add_argument("--year", type=int, default=None,
                        help="Only pull rounds from this year (e.g. 2025)")
    parser.add_argument("--date", type=str, default=None,
                        help="Only pull round(s) from this date (YYYY-MM-DD)")
    parser.add_argument("--pg-host", type=str, default=None,
                        help="PostgreSQL host (default: env PG_HOST or localhost)")
    parser.add_argument("--pg-port", type=int, default=None,
                        help="PostgreSQL port (default: env PG_PORT or 5432)")
    parser.add_argument("--pg-user", type=str, default=None,
                        help="PostgreSQL user (default: env PG_USER or postgres)")
    parser.add_argument("--pg-password", type=str, default=None,
                        help="PostgreSQL password (default: env PG_PASSWORD or postgres)")
    parser.add_argument("--pg-database", type=str, default=None,
                        help="PostgreSQL database (default: env PG_DATABASE or arccos)")
    parser.add_argument("--fresh", action="store_true",
                        help="Ignore existing DB data and re-pull everything matched")
    return parser.parse_args()


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    # Resolve config: CLI args > .env > defaults
    max_rounds = args.max if args.max is not None else int(os.getenv("ARCCOS_MAX_ROUNDS", "0"))
    log.info("=" * 60)
    log.info("  Arccos Golf Data Pull (PostgreSQL)")
    log.info("=" * 60)
    log.info("")

    # Show active filters
    filters = []
    if args.year:
        filters.append(f"year={args.year}")
    if args.date:
        filters.append(f"date={args.date}")
    if max_rounds:
        filters.append(f"max={max_rounds}")
    filters.append("SGA: scratch through 9-hcp")
    log.info(f"  Config: {', '.join(filters)}")
    log.info("")

    # Auth
    email = os.getenv("ARCCOS_EMAIL") or input("Email: ").strip()
    password = os.getenv("ARCCOS_PASSWORD") or getpass("Password: ")
    if not password:
        log.error("ARCCOS_PASSWORD not set in .env and no password provided.")
        sys.exit(1)
    auth = login(email, password)
    user_id = auth["userId"]
    token = auth["token"]
    log.info("")

    # Init DB
    con, cur = init_db(
        pg_host=args.pg_host,
        pg_port=args.pg_port,
        pg_user=args.pg_user,
        pg_password=args.pg_password,
        pg_database=args.pg_database,
    )
    pg_host = args.pg_host or os.getenv("PG_HOST", "localhost")
    pg_database = args.pg_database or os.getenv("PG_DATABASE", "arccos")
    log.info(f"Database: {pg_host}/{pg_database}")
    log.info("")

    # Check what we already have
    existing = set()
    if not args.fresh:
        try:
            cur.execute("SELECT round_id FROM rounds")
            rows = cur.fetchall()
            existing = {r[0] for r in rows}
            if existing:
                log.info(f"Already have {len(existing)} rounds in DB (use --fresh to re-pull)")
        except Exception:
            con.rollback()

    # Pull round list
    log.info("Pulling round list...")
    rounds_list = pull_all_rounds(user_id, token)

    # Apply filters
    rounds_list = filter_rounds(rounds_list, year=args.year, date=args.date)
    if args.year or args.date:
        filter_desc = args.date or str(args.year)
        log.info(f"  After filtering for {filter_desc}: {len(rounds_list)} rounds")

    # Determine how many we'll actually pull
    to_pull = [r for r in rounds_list if r["roundId"] not in existing]
    if max_rounds:
        to_pull = to_pull[:max_rounds]
    pull_target = len(to_pull)
    log.info(f"  Will pull {pull_target} new round(s)" +
          (f" (of {len(rounds_list)} total, limited by --max {max_rounds})" if max_rounds else ""))
    log.info("")

    # Track courses we've already fetched
    fetched_courses = set()
    try:
        cur.execute("SELECT course_id FROM courses")
        rows = cur.fetchall()
        fetched_courses = {r[0] for r in rows}
    except Exception:
        con.rollback()

    new_count = 0
    for i, r in enumerate(rounds_list):
        round_id = r["roundId"]
        course_id = r["courseId"]

        if round_id in existing:
            log.info(f"  [{i+1}/{len(rounds_list)}] Round {round_id} — already in DB, skipping")
            continue

        if max_rounds and new_count >= max_rounds:
            log.info(f"  Reached max rounds limit ({max_rounds}), stopping.")
            break

        new_count += 1
        log.info(f"  [{new_count}/{pull_target}] Round {round_id} ({r.get('courseName', '?')}, {r.get('startTime', '?')[:10]})")

        # Pull full detail
        detail = pull_round_detail(user_id, round_id, token)
        time.sleep(0.15)

        # Pull SGA at multiple goal handicaps (scratch through 9)
        sga_goals = list(range(0, -10, -1))  # [0, -1, -2, ..., -9]
        log.info(f"    Fetching SGA at {len(sga_goals)} goal handicaps...")
        sga_results = []
        for ghcp in sga_goals:
            sga = pull_sga_analysis(user_id, round_id, token, goal_hcp=ghcp)
            sga_results.append((ghcp, sga))
            time.sleep(0.15)

        # Get GPS and time info for enrichment
        first_hole = (detail.get("holes") or [None])[0] or {}
        first_shot = (first_hole.get("shots") or [None])[0] or {}
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
            log.info(f"    Weather for {date_str} at ({lat:.2f}, {lon:.2f})...")
            weather = fetch_weather(lat, lon, date_str, start_hour, end_hour,
                                   round_id=round_id, con=con, cur=cur)
            if weather:
                log.info(f"    {weather['avg_temp_f']}F (feels {weather['avg_feels_like_f']}F), "
                      f"{weather['avg_wind_mph']}mph wind, UV {weather['avg_uv_index']}, "
                      f"{weather['weather_conditions']}")
            time.sleep(0.2)

        # 2. Elevation backfill on every shot
        log.info(f"    Fetching elevations for {detail.get('noOfShots', '?')} shots...")
        enrich_elevations(detail)
        time.sleep(0.2)

        # 3. Sunrise/sunset
        sun = None
        if lat and lon and date_str:
            log.info(f"    Fetching sunrise/sunset...")
            sun = fetch_sunrise_sunset(lat, lon, date_str)
            if sun:
                log.info(f"    Sunrise {sun['sunrise']}, Sunset {sun['sunset']}, "
                      f"Day {sun['day_length_hrs']}hrs")
            time.sleep(0.2)

        # Merge fields that only exist in the list response
        for key in ("par", "driveHcp", "approachHcp", "chipHcp", "sandHcp", "puttHcp"):
            if key not in detail or detail[key] is None:
                detail[key] = r.get(key)

        # Store
        store_round(con, cur, detail, weather, sun)
        for ghcp, sga in sga_results:
            if sga:
                store_sga(con, cur, round_id, ghcp, sga)

        # Course + hole details (once per course, cached across runs)
        if course_id not in fetched_courses:
            cv = r.get("courseVersion", 1)
            # Check if we already have hole details for this course+version
            cur.execute(
                "SELECT COUNT(*) FROM course_holes WHERE course_id=%s AND course_version=%s",
                [course_id, cv]
            )
            existing_holes = cur.fetchone()[0]
            if existing_holes > 0:
                log.info(f"    Course {course_id} already cached ({existing_holes} holes)")
                fetched_courses.add(course_id)
            else:
                try:
                    course = pull_course(course_id, cv, token)
                    store_course(con, cur, course, course_id)
                    time.sleep(0.3)
                    log.info(f"    Fetching course hole details...")
                    hole_details = []
                    for hole_num in range(1, course.get("noOfHoles", 18) + 1):
                        try:
                            hd = api_get(f"/courses/{course_id}/holes/{hole_num}",
                                         token, params={"courseVersion": cv})
                            hole_details.append(hd)
                        except Exception:
                            break
                        time.sleep(0.1)
                    if hole_details:
                        store_course_holes(con, cur, course_id, cv, hole_details)
                        log.info(f"    Stored {len(hole_details)} hole details for {course.get('name', course_id)}")
                    fetched_courses.add(course_id)
                except Exception as e:
                    log.error(f"Course fetch failed: {e}")

        # Pause between rounds to respect rate limits
        time.sleep(1.0)

    # Pull bag/club data and player profile (once)
    try:
        log.info("  Pulling bag/club data...")
        user_data = api_get(f"/users/{user_id}", token)
        for bag_info in user_data.get("bags", []):
            bag_id = bag_info.get("bagId")
            bag = api_get(f"/users/{user_id}/bags/{bag_id}", token)
            if bag:
                store_clubs(con, cur, user_id, bag)
                club_count = len(bag.get("clubs", []))
                active = sum(1 for c in bag.get("clubs", []) if c.get("isDeleted") != "T")
                log.info(f"    Stored {club_count} clubs ({active} active) from bag {bag_id}")
    except Exception as e:
        log.error(f"Bag pull failed: {e}")

    try:
        log.info("  Pulling player profile...")
        profile = api_get(f"/sga/playerProfile/{user_id}", token)
        store_player_profile(con, cur, user_id, profile)
        log.info(f"    HCP: {profile.get('handicap')}, Rounds: {profile.get('totalRounds')}, Shots: {profile.get('shotsPlayed')}")
    except Exception as e:
        log.error(f"Player profile pull failed: {e}")

    log.info("")
    log.info("=" * 60)
    log.info(f"  Done! Pulled {new_count} new Arccos rounds.")
    log.info("")

    # ─── GHIN Pull ───────────────────────────────────────────────────────
    ghin_email = os.getenv("GHIN_EMAIL")
    ghin_password = os.getenv("GHIN_PASSWORD")
    if ghin_email and ghin_password:
        log.info("=" * 60)
        log.info("  GHIN Data Pull")
        log.info("=" * 60)
        log.info("")
        try:
            ghin_auth = ghin_login(ghin_email, ghin_password)
            ghin_token = ghin_auth["token"]
            ghin_id = ghin_auth["golfer_id"]

            # Scores
            log.info("  Pulling GHIN scores...")
            ghin_scores = pull_ghin_scores(ghin_id, ghin_token)
            log.info(f"  Found {len(ghin_scores)} scores")
            for s in ghin_scores:
                store_ghin_score(con, cur, s)
            time.sleep(0.2)

            # Handicap history
            log.info("  Pulling handicap history...")
            revisions = pull_ghin_handicap_history(ghin_id, ghin_token)
            log.info(f"  Found {len(revisions)} revisions")
            for rev in revisions:
                store_ghin_handicap(con, cur, rev)

            log.info("")
            log.info(f"  GHIN: {len(ghin_scores)} scores, {len(revisions)} handicap revisions stored")
            log.info("=" * 60)
            log.info("")
        except Exception as e:
            log.error(f"GHIN pull failed: {e}")
            log.info("")
    else:
        log.info("  (Skipping GHIN — set GHIN_EMAIL and GHIN_PASSWORD in .env)")
        log.info("")

    # ─── Computed columns & views ────────────────────────────────────────
    log.info("Adding computed columns...")
    add_computed_columns(con, cur)

    log.info("Creating Metabase views...")
    create_views(con, cur)
    log.info("")

    # Summary
    log.info("=" * 60)
    log.info("  Database Summary")
    log.info("=" * 60)
    cur.execute("SELECT COUNT(*) FROM rounds")
    count = cur.fetchone()[0]
    if count > 0:
        cur.execute("""
            SELECT
                COUNT(*) as rounds,
                SUM(num_shots) as total_shots,
                MIN(start_time) as earliest,
                MAX(start_time) as latest,
                ROUND(AVG(over_under)::numeric, 1) as avg_over_under
            FROM rounds
        """)
        summary = cur.fetchone()
        log.info(f"  Arccos rounds:      {summary[0]}")
        log.info(f"  Total shots:        {summary[1]}")
        log.info(f"  Date range:         {str(summary[2])[:10]} to {str(summary[3])[:10]}")
        log.info(f"  Avg over/under:     {summary[4]:+.1f}")

    cur.execute("SELECT COUNT(*) FROM ghin_scores")
    ghin_count = cur.fetchone()[0]
    if ghin_count > 0:
        cur.execute("""
            SELECT COUNT(*), MIN(played_at), MAX(played_at),
                   ROUND(AVG(differential)::numeric, 1)
            FROM ghin_scores
        """)
        gs = cur.fetchone()
        cur.execute("""
            SELECT handicap_index FROM ghin_handicap_history
            ORDER BY rev_date DESC LIMIT 1
        """)
        hcp = cur.fetchone()
        log.info(f"  GHIN scores:        {gs[0]} ({gs[1]} to {gs[2]})")
        log.info(f"  Avg differential:   {gs[3]}")
        if hcp:
            log.info(f"  Current HCP index:  {hcp[0]}")

    cur.execute("SELECT COUNT(*) FROM sga_analysis")
    sga_count = cur.fetchone()[0]
    if sga_count:
        log.info(f"  SGA analyses:       {sga_count}")
    cur.execute("SELECT COUNT(*) FROM clubs")
    club_count = cur.fetchone()[0]
    if club_count:
        cur.execute("SELECT COUNT(*) FROM clubs WHERE is_deleted != 'T' OR is_deleted IS NULL")
        active_clubs = cur.fetchone()[0]
        log.info(f"  Clubs:              {club_count} ({active_clubs} active)")

    log.info("")
    log.info(f"  Database: {pg_host}/{pg_database}")
    log.info("=" * 60)

    cur.close()
    con.close()


if __name__ == "__main__":
    main()

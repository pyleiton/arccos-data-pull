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

SCHEMA_PATH = Path(__file__).parent / "sql" / "schema.sql"


def init_db(pg_host: str = None, pg_port: int = None, pg_user: str = None,
            pg_password: str = None, pg_database: str = None) -> tuple:
    """Connect to PostgreSQL and verify the schema is present.
    Returns (connection, cursor).

    Schema management is deliberately a separate step — apply sql/schema.sql
    manually before running this script. This avoids surprise schema changes
    on every data-pull run.
    """
    host = pg_host or os.getenv("PG_HOST", "localhost")
    port = pg_port or int(os.getenv("PG_PORT", "5432"))
    user = pg_user or os.getenv("PG_USER", "postgres")
    password = pg_password or os.getenv("PG_PASSWORD", "postgres")
    database = pg_database or os.getenv("PG_DATABASE", "arccos")

    con = psycopg2.connect(
        host=host, port=port,
        user=user, password=password,
        dbname=database,
    )
    cur = con.cursor()

    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'users'
    """)
    if cur.fetchone() is None:
        con.close()
        raise RuntimeError(
            f"Schema not found in database '{database}'. "
            f"Apply it first:\n"
            f"  docker exec -i arccos-postgres psql -U postgres -d {database} < sql/schema.sql"
        )
    return con, cur


def upsert_user(con, cur, handle: str, display_name: str = None,
                email: str = None, arccos_user_id: str = None,
                ghin_number: str = None) -> int:
    """Insert or update a row in users by handle, returning the BIGINT user_id.
    Late-binding fields (arccos_user_id, ghin_number) are filled in only when
    the existing row has them as NULL — never overwritten."""
    if not handle:
        raise ValueError("ARCCOS_USER_HANDLE is required (set in .env or pass --user)")
    cur.execute("""
        INSERT INTO users (handle, display_name, email, arccos_user_id, ghin_number)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (handle) DO UPDATE SET
            display_name   = COALESCE(EXCLUDED.display_name,   users.display_name),
            email          = COALESCE(EXCLUDED.email,          users.email),
            arccos_user_id = COALESCE(users.arccos_user_id,    EXCLUDED.arccos_user_id),
            ghin_number    = COALESCE(users.ghin_number,       EXCLUDED.ghin_number)
        RETURNING user_id
    """, [handle, display_name or handle, email, arccos_user_id, ghin_number])
    user_id = cur.fetchone()[0]
    con.commit()
    return user_id



def _safe_ts(val):
    """Return None for zero/invalid timestamps that PostgreSQL cannot parse."""
    if val and isinstance(val, str) and val.startswith("0000-00-00"):
        return None
    return val


def store_round(con, cur, user_id: int, rd: dict,
                weather: dict = None, sun: dict = None):
    """Upsert a round and its holes/shots for the given internal user_id."""
    w = weather or {}
    s = sun or {}

    cur.execute("""
        INSERT INTO rounds (
            round_id, round_uuid, user_id, arccos_user_id, course_id, course_version, course_name,
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
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON CONFLICT (round_id) DO UPDATE SET
            round_uuid=EXCLUDED.round_uuid, arccos_user_id=EXCLUDED.arccos_user_id,
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
        rd["roundId"], rd.get("roundUUID"), user_id, rd.get("userId"),
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
                round_id, hole_id, user_id, num_shots, is_gir, putts,
                is_fairway, is_fairway_right, is_fairway_left,
                is_sand_save_chance, is_sand_save,
                is_up_down_chance, is_up_down,
                approach_shot_id, pin_lat, pin_long,
                start_time, end_time, score_override
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
            rd["roundId"], hole["holeId"], user_id, hole.get("noOfShots"),
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
            # tour_quality is JSONB — Arccos returns either a scalar or
            # {percentile, method}; either way, json.dumps gives valid JSON.
            tq = shot.get("tourQuality")
            tour_quality_json = json.dumps(tq) if tq is not None else None
            shot_params = [
                rd["roundId"], hole["holeId"], shot["shotId"], user_id,
                shot.get("shotUUID"), shot.get("clubType"), shot.get("clubId"),
                shot.get("startLat"), shot.get("startLong"),
                shot.get("endLat"), shot.get("endLong"),
                shot.get("distance"), shot.get("isHalfSwing"),
                shot.get("startAltitude"), shot.get("endAltitude"),
                _safe_ts(shot.get("shotTime")), shot.get("shouldIgnore"),
                shot.get("noOfPenalties"), shot.get("userStartTerrainOverride"),
                shot.get("shouldConsiderPuttAsChip"), tour_quality_json,
            ]
            cur.execute("""
                INSERT INTO shots (
                    round_id, hole_id, shot_id, user_id, shot_uuid, club_type, club_id,
                    start_lat, start_long, end_lat, end_long,
                    distance, is_half_swing, start_altitude, end_altitude,
                    shot_time, should_ignore, num_penalties,
                    user_start_terrain_override, should_consider_putt_as_chip,
                    tour_quality
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
            """, shot_params)

    con.commit()


def store_sga(con, cur, user_id: int, round_id: int, goal_hcp: int, sga: dict):
    """Store SGA analysis with full detail extraction for the given internal user_id."""
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
            round_id, goal_hcp, user_id,
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
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
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
        round_id, goal_hcp, user_id,
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
                INSERT INTO sga_by_distance (round_id, goal_hcp, category, slab_id, user_id, slab_label, sga, shots_count)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (round_id, goal_hcp, category, slab_id) DO UPDATE SET
                    slab_label=EXCLUDED.slab_label, sga=EXCLUDED.sga,
                    shots_count=EXCLUDED.shots_count
            """, [
                round_id, goal_hcp, cat, item.get("slabId"), user_id,
                item.get("slab", {}).get("value", ""),
                item.get("sga"), item.get("shotsCount"),
            ])

    # SGA by terrain (approach)
    for item in ap.get("approachByTerrain", []):
        cur.execute("""
            INSERT INTO sga_by_terrain (round_id, goal_hcp, category, terrain, user_id, sga, shots_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (round_id, goal_hcp, category, terrain) DO UPDATE SET
                sga=EXCLUDED.sga, shots_count=EXCLUDED.shots_count
        """, [
            round_id, goal_hcp, "approach", item.get("terrain"), user_id,
            item.get("sga"), item.get("shotsCount"),
        ])

    # SGA by hole shape (driving)
    for item in dr.get("drivingByHoleShape", []):
        cur.execute("""
            INSERT INTO sga_by_hole_shape (round_id, goal_hcp, hole_shape, user_id, sga, shots_count)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (round_id, goal_hcp, hole_shape) DO UPDATE SET
                sga=EXCLUDED.sga, shots_count=EXCLUDED.shots_count
        """, [
            round_id, goal_hcp, item.get("holeShape"), user_id,
            item.get("sga"), item.get("shotsCount"),
        ])

    # Putting SGA by hole
    for item in pu.get("puttingByHole", {}).get("holeSga", []):
        cur.execute("""
            INSERT INTO sga_putting_by_hole (round_id, goal_hcp, hole_id, user_id, sga)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (round_id, goal_hcp, hole_id) DO UPDATE SET
                sga=EXCLUDED.sga
        """, [round_id, goal_hcp, item.get("holeId"), user_id, item.get("sga")])

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


def store_player_profile(con, cur, user_id: int, arccos_user_id: str, profile: dict):
    """Store player profile/stats keyed by internal user_id."""
    settings = profile.get("settings", {})
    cur.execute("""
        INSERT INTO player_profile (
            user_id, arccos_user_id, handicap, total_rounds, holes_played,
            shots_played, goal_hcp, num_rounds_setting
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (user_id) DO UPDATE SET
            arccos_user_id=EXCLUDED.arccos_user_id,
            handicap=EXCLUDED.handicap, total_rounds=EXCLUDED.total_rounds,
            holes_played=EXCLUDED.holes_played, shots_played=EXCLUDED.shots_played,
            goal_hcp=EXCLUDED.goal_hcp, num_rounds_setting=EXCLUDED.num_rounds_setting
    """, [
        user_id, arccos_user_id, profile.get("handicap"),
        profile.get("totalRounds"), profile.get("holesPlayed"),
        profile.get("shotsPlayed"),
        settings.get("goalHcp"), settings.get("noOfRounds"),
    ])
    con.commit()


def store_clubs(con, cur, user_id: int, arccos_user_id: str, bag: dict):
    """Store bag/club data keyed by internal user_id."""
    if not bag:
        return
    bag_id = bag.get("bagId")
    for club in bag.get("clubs", []):
        cur.execute("""
            INSERT INTO clubs (
                user_id, arccos_user_id, bag_id, club_id, club_type, sensor_uuid, sensor_type_id,
                name, perceived_distance, start_date, end_date, is_deleted,
                make, model, loft, flex, raw_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (user_id, bag_id, club_id) DO UPDATE SET
                arccos_user_id=EXCLUDED.arccos_user_id,
                club_type=EXCLUDED.club_type, sensor_uuid=EXCLUDED.sensor_uuid,
                sensor_type_id=EXCLUDED.sensor_type_id, name=EXCLUDED.name,
                perceived_distance=EXCLUDED.perceived_distance,
                start_date=EXCLUDED.start_date, end_date=EXCLUDED.end_date,
                is_deleted=EXCLUDED.is_deleted,
                make=EXCLUDED.make, model=EXCLUDED.model,
                loft=EXCLUDED.loft, flex=EXCLUDED.flex,
                raw_json=EXCLUDED.raw_json
        """, [
            user_id, arccos_user_id, bag_id, club.get("clubId"), club.get("clubType"),
            club.get("sensorUUID"), club.get("sensorTypeId"),
            club.get("name"), club.get("perceivedDistance"),
            club.get("startDate"), club.get("endDate"),
            club.get("isDeleted"),
            club.get("make"), club.get("model"),
            club.get("loft"), club.get("flex"),
            json.dumps(club),
        ])
    con.commit()


def store_ghin_score(con, cur, user_id: int, s: dict):
    """Store a GHIN score and its hole details for the given internal user_id."""
    cur.execute("""
        INSERT INTO ghin_scores (
            id, user_id, golfer_id, played_at, course_name, course_id,
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
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
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
        s["id"], user_id, s.get("golfer_id"), s.get("played_at"),
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
                ghin_score_id, hole_number, user_id, par, raw_score,
                adjusted_gross_score, stroke_allocation, x_hole
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ghin_score_id, hole_number) DO UPDATE SET
                par=EXCLUDED.par, raw_score=EXCLUDED.raw_score,
                adjusted_gross_score=EXCLUDED.adjusted_gross_score,
                stroke_allocation=EXCLUDED.stroke_allocation,
                x_hole=EXCLUDED.x_hole
        """, [
            s["id"], h.get("hole_number"), user_id, h.get("par"),
            h.get("raw_score"), h.get("adjusted_gross_score"),
            h.get("stroke_allocation"), h.get("x_hole"),
        ])

    con.commit()


def store_ghin_handicap(con, cur, user_id: int, rev: dict):
    """Store a handicap revision for the given internal user_id."""
    cur.execute("""
        INSERT INTO ghin_handicap_history (id, user_id, rev_date, handicap_index, low_hi, hard_cap, soft_cap)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
            rev_date=EXCLUDED.rev_date, handicap_index=EXCLUDED.handicap_index,
            low_hi=EXCLUDED.low_hi, hard_cap=EXCLUDED.hard_cap,
            soft_cap=EXCLUDED.soft_cap
    """, [
        rev.get("ID"), user_id, rev.get("RevDate", "")[:10],
        float(rev["Value"]) if rev.get("Value") else None,
        float(rev["LowHI"]) if rev.get("LowHI") else None,
        rev.get("Hard_Cap"), rev.get("Soft_Cap"),
    ])
    con.commit()




def populate_course_and_tee(con, cur, user_id: int):
    """Populate the rounds.course_and_tee computed column for this user."""
    try:
        cur.execute("""
            UPDATE rounds
            SET course_and_tee = rounds.course_name || ' - ' || ct.tee_name
            FROM course_tees ct
            WHERE rounds.user_id = %s
              AND rounds.course_id = ct.course_id
              AND rounds.tee_id = ct.tee_id
        """, [user_id])
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
    parser.add_argument("--user", type=str, default=None,
                        help="User handle for the row in the users table (default: env ARCCOS_USER_HANDLE)")
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

    # Resolve user identity (multi-tenant: every row will be tagged with this user)
    handle = args.user or os.getenv("ARCCOS_USER_HANDLE")
    if not handle:
        log.error("ARCCOS_USER_HANDLE not set in .env and --user not given.")
        log.error("This identifies which row in the users table the pulled data belongs to.")
        sys.exit(1)
    display_name = os.getenv("ARCCOS_DISPLAY_NAME") or handle

    # Show active filters
    filters = [f"user={handle}"]
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
    arccos_user_id = auth["userId"]
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

    # Resolve internal user_id (creates row if missing, fills in arccos_user_id if not yet known)
    user_id = upsert_user(
        con, cur,
        handle=handle,
        display_name=display_name,
        email=email,
        arccos_user_id=arccos_user_id,
    )
    log.info(f"User: {handle} (user_id={user_id}, arccos_user_id={arccos_user_id})")
    log.info("")

    # Check what we already have for THIS user only
    existing = set()
    if not args.fresh:
        try:
            cur.execute("SELECT round_id FROM rounds WHERE user_id = %s", [user_id])
            rows = cur.fetchall()
            existing = {r[0] for r in rows}
            if existing:
                log.info(f"Already have {len(existing)} rounds for {handle} (use --fresh to re-pull)")
        except Exception:
            con.rollback()

    # Pull round list
    log.info("Pulling round list...")
    rounds_list = pull_all_rounds(arccos_user_id, token)

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

    # Track courses we've already fetched (shared across users)
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
        detail = pull_round_detail(arccos_user_id, round_id, token)
        time.sleep(0.15)

        # Pull SGA at multiple goal handicaps (scratch through 9)
        sga_goals = list(range(0, -10, -1))  # [0, -1, -2, ..., -9]
        log.info(f"    Fetching SGA at {len(sga_goals)} goal handicaps...")
        sga_results = []
        for ghcp in sga_goals:
            sga = pull_sga_analysis(arccos_user_id, round_id, token, goal_hcp=ghcp)
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
        store_round(con, cur, user_id, detail, weather, sun)
        for ghcp, sga in sga_results:
            if sga:
                store_sga(con, cur, user_id, round_id, ghcp, sga)

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
        user_data = api_get(f"/users/{arccos_user_id}", token)
        for bag_info in user_data.get("bags", []):
            bag_id = bag_info.get("bagId")
            bag = api_get(f"/users/{arccos_user_id}/bags/{bag_id}", token)
            if bag:
                store_clubs(con, cur, user_id, arccos_user_id, bag)
                club_count = len(bag.get("clubs", []))
                active = sum(1 for c in bag.get("clubs", []) if c.get("isDeleted") != "T")
                log.info(f"    Stored {club_count} clubs ({active} active) from bag {bag_id}")
    except Exception as e:
        log.error(f"Bag pull failed: {e}")

    try:
        log.info("  Pulling player profile...")
        profile = api_get(f"/sga/playerProfile/{arccos_user_id}", token)
        store_player_profile(con, cur, user_id, arccos_user_id, profile)
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

            # Backfill ghin_number on the user row if not yet known
            if ghin_id:
                cur.execute(
                    "UPDATE users SET ghin_number = %s WHERE user_id = %s AND ghin_number IS NULL",
                    [str(ghin_id), user_id],
                )
                con.commit()

            # Scores
            log.info("  Pulling GHIN scores...")
            ghin_scores = pull_ghin_scores(ghin_id, ghin_token)
            log.info(f"  Found {len(ghin_scores)} scores")
            for s in ghin_scores:
                store_ghin_score(con, cur, user_id, s)
            time.sleep(0.2)

            # Handicap history
            log.info("  Pulling handicap history...")
            revisions = pull_ghin_handicap_history(ghin_id, ghin_token)
            log.info(f"  Found {len(revisions)} revisions")
            for rev in revisions:
                store_ghin_handicap(con, cur, user_id, rev)

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

    # ─── Computed columns ────────────────────────────────────────────────
    # Views are created in sql/schema.sql at init_db time, no need to recreate.
    log.info("Populating computed columns...")
    populate_course_and_tee(con, cur, user_id)
    log.info("")

    # Summary (scoped to this user)
    log.info("=" * 60)
    log.info(f"  Database Summary for {handle}")
    log.info("=" * 60)
    cur.execute("SELECT COUNT(*) FROM rounds WHERE user_id = %s", [user_id])
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
            WHERE user_id = %s
        """, [user_id])
        summary = cur.fetchone()
        log.info(f"  Arccos rounds:      {summary[0]}")
        log.info(f"  Total shots:        {summary[1]}")
        log.info(f"  Date range:         {str(summary[2])[:10]} to {str(summary[3])[:10]}")
        log.info(f"  Avg over/under:     {summary[4]:+.1f}")

    cur.execute("SELECT COUNT(*) FROM ghin_scores WHERE user_id = %s", [user_id])
    ghin_count = cur.fetchone()[0]
    if ghin_count > 0:
        cur.execute("""
            SELECT COUNT(*), MIN(played_at), MAX(played_at),
                   ROUND(AVG(differential)::numeric, 1)
            FROM ghin_scores
            WHERE user_id = %s
        """, [user_id])
        gs = cur.fetchone()
        cur.execute("""
            SELECT handicap_index FROM ghin_handicap_history
            WHERE user_id = %s
            ORDER BY rev_date DESC LIMIT 1
        """, [user_id])
        hcp = cur.fetchone()
        log.info(f"  GHIN scores:        {gs[0]} ({gs[1]} to {gs[2]})")
        log.info(f"  Avg differential:   {gs[3]}")
        if hcp:
            log.info(f"  Current HCP index:  {hcp[0]}")

    cur.execute("SELECT COUNT(*) FROM sga_analysis WHERE user_id = %s", [user_id])
    sga_count = cur.fetchone()[0]
    if sga_count:
        log.info(f"  SGA analyses:       {sga_count}")
    cur.execute("SELECT COUNT(*) FROM clubs WHERE user_id = %s", [user_id])
    club_count = cur.fetchone()[0]
    if club_count:
        cur.execute(
            "SELECT COUNT(*) FROM clubs WHERE user_id = %s AND (is_deleted != 'T' OR is_deleted IS NULL)",
            [user_id],
        )
        active_clubs = cur.fetchone()[0]
        log.info(f"  Clubs:              {club_count} ({active_clubs} active)")

    log.info("")
    log.info(f"  Database: {pg_host}/{pg_database}")
    log.info("=" * 60)

    cur.close()
    con.close()


if __name__ == "__main__":
    main()

"""
ARUVADAI — TxLINE AI Pundit Bot (World Cup)
============================================
Combines the TxLINE odds and scores SSE streams into one live Telegram feed:
sharp odds moves (>5% shift within 60s, logic ported from txline_scanner.py)
plus goals, red cards, kickoff and fulltime. Runs both streams in parallel
threads with auto-reconnect and prints every alert to the terminal too.

NOTE ON THE SCORES SCHEMA: /api/scores/snapshot returned 404 for every
variant tried (competitionId, fixtureId, path forms) — the route isn't
exposed in this dev environment. The live /api/scores/stream connects fine
(200, text/event-stream) but only emitted heartbeat events during testing,
even for a fixture whose scheduled kickoff had already passed — no real
score payload was observed. The parser below is written defensively:
it tries several plausible field-name conventions (mirroring the confirmed
/api/fixtures/snapshot schema, e.g. Participant1/Participant2/GameState)
and falls back to a keyword scan over the payload's string values. Validate
the *_KEYS tuples below against a real payload once one is observed, and
adjust them if TxLINE's actual naming differs.
"""

import os
import re
import json
import time
import threading
import traceback
from collections import defaultdict, deque
from datetime import datetime, timezone

import requests
from requests.exceptions import RequestException, ChunkedEncodingError
from sseclient import SSEClient
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.expanduser("~/aruvadai-txline/.env"))

from aruvadai_telegram import BOT_TOKEN, CHAT_ID

TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


def send(message, silent=False):
    """
    curl_cffi's chrome-impersonation session (aruvadai_telegram.send) fails
    TLS to api.telegram.org in some sandboxed/proxied environments; plain
    requests works there, so this bot uses it instead.
    """
    if not BOT_TOKEN or not CHAT_ID:
        print("[TELEGRAM] No token/chat_id configured")
        return False
    try:
        r = requests.post(
            TELEGRAM_API_URL,
            json={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML", "disable_notification": silent},
            timeout=8,
        )
        return r.json().get("ok", False)
    except Exception as e:
        print(f"[TELEGRAM ERROR] {e}")
        return False


# ── Config ────────────────────────────────────────────────────
MOVEMENT_THRESHOLD = 0.05   # >5% price shift
WINDOW_SECONDS = 60         # lookback window for movement detection

API_TOKEN  = os.environ.get("TXLINE_API_TOKEN", "")
BASE_URL   = os.environ.get("TXLINE_BASE_URL", "https://txline-dev.txodds.com")
AUTH_URL   = f"{BASE_URL}/auth/guest/start"
ODDS_STREAM_URL   = f"{BASE_URL}/api/odds/stream"
SCORES_STREAM_URL = f"{BASE_URL}/api/scores/stream"
FIXTURES_URL = f"{BASE_URL}/api/fixtures/snapshot?competitionId=72"

RECONNECT_DELAY = 5
MAX_RECONNECT_DELAY = 60
FIXTURES_REFRESH_SECS = 3600


# ── Auth (same guest-JWT + X-Api-Token pattern as txline_scanner.py) ──
class TokenManager:
    """Holds the current guest JWT and refreshes it on demand."""

    def __init__(self):
        self._jwt = None
        self._lock = threading.Lock()

    def get(self):
        with self._lock:
            if self._jwt is None:
                self._jwt = self._fetch_jwt()
            return self._jwt

    def refresh(self):
        with self._lock:
            self._jwt = self._fetch_jwt()
            return self._jwt

    def _fetch_jwt(self):
        backoff = 2
        while True:
            try:
                resp = requests.post(AUTH_URL, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                jwt = data.get("token") or data.get("jwt") or data.get("access_token")
                if not jwt:
                    raise ValueError(f"No JWT field in auth response: {data}")
                log(f"[AUTH] Obtained new guest JWT ({jwt[:12]}...)")
                return jwt
            except (RequestException, ValueError) as e:
                log(f"[AUTH] Failed to get JWT: {e} — retrying in {backoff}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)


token_mgr = TokenManager()


def auth_headers():
    return {
        "Authorization": f"Bearer {token_mgr.get()}",
        "X-Api-Token": API_TOKEN,
    }


# ── Fixture cache: fixture_id -> {"home", "away", "label"} ────
fixtures_lock = threading.Lock()
fixtures = {}


def fetch_fixtures():
    for attempt in range(2):
        try:
            resp = requests.get(FIXTURES_URL, headers=auth_headers(), timeout=15)
            if resp.status_code in (401, 403):
                token_mgr.refresh()
                resp = requests.get(FIXTURES_URL, headers=auth_headers(), timeout=15)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("fixtures", data) if isinstance(data, dict) else data

            mapping = {}
            for fx in items:
                fid = fx.get("FixtureId") or fx.get("fixtureId") or fx.get("id")
                if fid is None:
                    continue
                p1 = fx.get("Participant1") or fx.get("HomeTeam") or fx.get("home")
                p2 = fx.get("Participant2") or fx.get("AwayTeam") or fx.get("away")
                p1_is_home = fx.get("Participant1IsHome", True)
                if p1 and p2:
                    home, away = (p1, p2) if p1_is_home else (p2, p1)
                    label = f"{home} vs {away}"
                else:
                    home = away = None
                    label = fx.get("Name") or fx.get("name") or str(fid)
                mapping[str(fid)] = {"home": home, "away": away, "label": label}

            with fixtures_lock:
                fixtures.update(mapping)
            log(f"[FIXTURES] Loaded {len(mapping)} fixture(s)")
            return
        except RequestException as e:
            if attempt == 0:
                log(f"[FIXTURES] Fetch failed ({e}) — retrying in 3s")
                time.sleep(3)
            else:
                log(f"[FIXTURES] Failed after retry: {e} — continuing with raw fixture ids")


def fixture_label(fixture_id):
    with fixtures_lock:
        return fixtures.get(str(fixture_id), {}).get("label", str(fixture_id))


def fixture_teams(fixture_id):
    with fixtures_lock:
        fx = fixtures.get(str(fixture_id))
    if fx and fx.get("home") and fx.get("away"):
        return fx["home"], fx["away"]
    return str(fixture_id), str(fixture_id)


def fixtures_refresh_loop():
    while True:
        time.sleep(FIXTURES_REFRESH_SECS)
        fetch_fixtures()


# ── Shared SSE payload normalization ───────────────────────────
def normalize_items(payload):
    """
    Tolerant of a single update object, a list of them, a {"updates": [...]}
    wrapper, or raw JSON strings at either level (TxLINE sends event.data as
    a JSON string, and some batched payloads nest JSON-encoded strings).
    """
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            return []

    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict) and "updates" in payload:
        items = payload["updates"]
    else:
        items = [payload]

    out = []
    for item in items:
        if isinstance(item, str):
            try:
                item = json.loads(item)
            except (json.JSONDecodeError, TypeError):
                continue
        if isinstance(item, dict):
            out.append(item)
    return out


# ── ODDS: sharp move detection (ported from txline_scanner.py) ─
history = defaultdict(lambda: deque())
history_lock = threading.Lock()


def market_key(fixture_id, super_odds_type, market_parameters):
    params = json.dumps(market_parameters, sort_keys=True) if market_parameters else ""
    return (str(fixture_id), str(super_odds_type), params)


def line_label(market_parameters):
    if not market_parameters:
        return ""
    if isinstance(market_parameters, dict):
        for key in ("Line", "line", "Handicap", "handicap", "Total", "total"):
            if key in market_parameters:
                return str(market_parameters[key])
        return ",".join(f"{k}={v}" for k, v in market_parameters.items())
    return str(market_parameters)


def evaluate_movement(key, selection, old_price, new_price):
    if old_price is None or old_price == 0:
        return None
    pct_change = (new_price - old_price) / old_price
    if abs(pct_change) < MOVEMENT_THRESHOLD:
        return None

    fixture_id, super_odds_type, params_json = key
    market_parameters = json.loads(params_json) if params_json else {}
    direction = "SHARP DROP" if pct_change < 0 else "SHARP RISE"

    return {
        "fixture_id": fixture_id,
        "market_type": super_odds_type,
        "line": line_label(market_parameters),
        "selection": selection,
        "old_price": old_price,
        "new_price": new_price,
        "pct_change": round(pct_change * 100, 2),
        "direction": direction,
    }


def process_prices(fixture_id, super_odds_type, market_parameters, prices, price_names, now_ts):
    if not price_names:
        price_names = [f"price_{i}" for i in range(len(prices))]

    prices_dict = dict(zip(price_names, prices))
    key = market_key(fixture_id, super_odds_type, market_parameters)
    signals = []

    with history_lock:
        entries = history[key]
        entries.append((now_ts, prices_dict))

        cutoff = now_ts - WINDOW_SECONDS
        while entries and entries[0][0] < cutoff:
            entries.popleft()

        if len(entries) >= 2:
            _, oldest_prices = entries[0]
            for selection, new_price in prices_dict.items():
                old_price = oldest_prices.get(selection)
                sig = evaluate_movement(key, selection, old_price, new_price)
                if sig:
                    signals.append(sig)

    return signals


def extract_odds_updates(payload):
    updates = []
    for item in normalize_items(payload):
        try:
            fixture_id = item["FixtureId"]
            super_odds_type = item["SuperOddsType"]
            market_parameters = item.get("MarketParameters", "")
            prices = item["Prices"]
        except KeyError:
            continue
        price_names = item.get("PriceNames")
        updates.append((fixture_id, super_odds_type, market_parameters, prices, price_names))
    return updates


def format_sharp_move(sig):
    name = fixture_label(sig["fixture_id"])
    market = f"{sig['market_type']} {sig['line']}".strip()
    return (
        f"⚡ <b>SHARP MOVE</b>\n"
        f"{name} | {market}\n"
        f"{sig['selection']}: {sig['old_price']}→{sig['new_price']} | {sig['pct_change']:+.1f}% {sig['direction']}\n"
        f"📊 Market repricing fast"
    )


def handle_odds_message(raw_data):
    try:
        payload = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
    except (json.JSONDecodeError, TypeError):
        return

    now_ts = time.time()
    for fixture_id, super_odds_type, market_parameters, prices, price_names in extract_odds_updates(payload):
        for sig in process_prices(fixture_id, super_odds_type, market_parameters, prices, price_names, now_ts):
            announce(format_sharp_move(sig))


# ── SCORES: goal / red card / kickoff / fulltime detection ────
FIXTURE_ID_KEYS = ("FixtureId", "fixtureId", "Id")
HOME_SCORE_KEYS = ("Participant1Score", "HomeScore", "Score1")
AWAY_SCORE_KEYS = ("Participant2Score", "AwayScore", "Score2")
MINUTE_KEYS     = ("Minute", "Min", "Clock", "MatchMinute")
STATE_KEYS      = ("GameState", "Status", "State")
RED_HOME_KEYS   = ("Participant1RedCards", "HomeRedCards", "RedCards1")
RED_AWAY_KEYS   = ("Participant2RedCards", "AwayRedCards", "RedCards2")

# Confirmed via /api/fixtures/snapshot: pre-match fixtures report GameState 1.
NOT_STARTED_STATE = 1

FULLTIME_KEYWORDS = ("full time", "fulltime", "full-time", "finished", "match ended", "final result")

score_state = {}   # fixture_id -> {"home","away","state","red_home","red_away"}
score_lock = threading.Lock()
kickoff_sent = set()
fulltime_sent = set()


def find_first(item, keys):
    for k in keys:
        v = item.get(k)
        if v is not None:
            return v
    return None


def text_blob(item):
    return " ".join(str(v) for v in item.values() if isinstance(v, str)).lower()


def format_goal(home_team, away_team, home_score, away_score, minute):
    return (
        f"⚽ <b>GOAL!</b>\n"
        f"{home_team} {home_score} - {away_score} {away_team}\n"
        f"Minute: {minute}'\n"
        f"📉 Odds shifting now..."
    )


def format_kickoff(home_team, away_team):
    return (
        f"🟢 <b>KICKOFF</b>\n"
        f"{home_team} vs {away_team}\n"
        f"Match is LIVE — watching for sharp moves"
    )


def format_fulltime(home_team, away_team, home_score, away_score):
    return (
        f"🏁 <b>FULLTIME</b>\n"
        f"{home_team} {home_score} - {away_score} {away_team}\n"
        f"Match over. Final result confirmed."
    )


def format_red_card(team, minute):
    return (
        f"🟥 <b>RED CARD</b>\n"
        f"{team}\n"
        f"Minute: {minute}'\n"
        f"🔥 Down to 10 men"
    )


def handle_score_item(item):
    fixture_id = find_first(item, FIXTURE_ID_KEYS)
    if fixture_id is None:
        return  # heartbeat or a payload shape we don't recognize
    fixture_id = str(fixture_id)

    home_score = find_first(item, HOME_SCORE_KEYS)
    away_score = find_first(item, AWAY_SCORE_KEYS)
    minute = find_first(item, MINUTE_KEYS)
    state = find_first(item, STATE_KEYS)
    red_home = find_first(item, RED_HOME_KEYS)
    red_away = find_first(item, RED_AWAY_KEYS)
    blob = text_blob(item)
    minute_label = minute if minute is not None else "?"

    events = []  # decided while holding the lock, announced after releasing it

    with score_lock:
        prev = score_state.get(fixture_id)
        merged = {
            "home": home_score if home_score is not None else (prev or {}).get("home"),
            "away": away_score if away_score is not None else (prev or {}).get("away"),
            "state": state if state is not None else (prev or {}).get("state"),
            "red_home": red_home if red_home is not None else (prev or {}).get("red_home"),
            "red_away": red_away if red_away is not None else (prev or {}).get("red_away"),
        }
        score_state[fixture_id] = merged

        if prev is not None:
            home_team, away_team = fixture_teams(fixture_id)
            h_disp = merged["home"] if merged["home"] is not None else "?"
            a_disp = merged["away"] if merged["away"] is not None else "?"

            if home_score is not None and prev.get("home") is not None and home_score > prev["home"]:
                events.append(format_goal(home_team, away_team, h_disp, a_disp, minute_label))
            if away_score is not None and prev.get("away") is not None and away_score > prev["away"]:
                events.append(format_goal(home_team, away_team, h_disp, a_disp, minute_label))
            if red_home is not None and prev.get("red_home") is not None and red_home > prev["red_home"]:
                events.append(format_red_card(home_team, minute_label))
            if red_away is not None and prev.get("red_away") is not None and red_away > prev["red_away"]:
                events.append(format_red_card(away_team, minute_label))

            if fixture_id not in kickoff_sent and (
                (state is not None and prev.get("state") == NOT_STARTED_STATE and state != NOT_STARTED_STATE)
                or "kickoff" in blob
            ):
                events.append(format_kickoff(home_team, away_team))
                kickoff_sent.add(fixture_id)

            if fixture_id not in fulltime_sent and any(kw in blob for kw in FULLTIME_KEYWORDS):
                events.append(format_fulltime(home_team, away_team, h_disp, a_disp))
                fulltime_sent.add(fixture_id)

    for msg in events:
        announce(msg)


def handle_score_message(raw_data):
    try:
        payload = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
    except (json.JSONDecodeError, TypeError):
        return
    for item in normalize_items(payload):
        handle_score_item(item)


# ── Telegram + terminal output ─────────────────────────────────
_TAG_RE = re.compile(r"<[^>]+>")


def announce(msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    plain = _TAG_RE.sub("", msg).replace("\n", " | ")
    print(f"[{ts}] {plain}")
    if not send(msg):
        print(f"[{ts}] [TELEGRAM] send failed")


# ── Stream connection ────────────────────────────────────────
def connect_stream(url):
    resp = requests.get(
        url,
        headers={**auth_headers(), "Accept": "text/event-stream"},
        stream=True,
        timeout=(15, None),
    )
    if resp.status_code in (401, 403):
        log(f"[STREAM] Auth rejected ({resp.status_code}) — refreshing JWT")
        token_mgr.refresh()
        resp.close()
        resp = requests.get(
            url,
            headers={**auth_headers(), "Accept": "text/event-stream"},
            stream=True,
            timeout=(15, None),
        )
    resp.raise_for_status()
    return resp


def run_stream(name, url, handler):
    backoff = RECONNECT_DELAY
    while True:
        try:
            log(f"[{name}] Connecting to TxLINE {name.lower()} stream...")
            resp = connect_stream(url)
            client = SSEClient(resp)
            log(f"[{name}] Connected.")
            backoff = RECONNECT_DELAY

            for event in client.events():
                if not event.data:
                    continue
                handler(event.data)

            log(f"[{name}] Stream closed by server, reconnecting...")

        except (RequestException, ChunkedEncodingError) as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status in (401, 403):
                log(f"[{name}] Auth error ({status}) — refreshing JWT")
                token_mgr.refresh()
            else:
                log(f"[{name}] Connection error: {e}")
        except Exception as e:
            log(f"[{name}] Unexpected error: {e}")
            traceback.print_exc()

        log(f"[{name}] Reconnecting in {backoff}s...")
        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_RECONNECT_DELAY)


# ── Utility ───────────────────────────────────────────────────
def log(msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def main():
    if not API_TOKEN:
        raise SystemExit("TXLINE_API_TOKEN not set — add it to .env")
    if not BOT_TOKEN or not CHAT_ID:
        raise SystemExit("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set — add them to .env")

    print("=" * 64)
    print("  ARUVADAI — TxLINE AI Pundit Bot v1.0")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  Odds threshold : {MOVEMENT_THRESHOLD*100:.0f}% within {WINDOW_SECONDS}s")
    print("  Streams        : odds + scores (parallel, auto-reconnect)")
    print("=" * 64 + "\n")

    token_mgr.get()
    fetch_fixtures()
    threading.Thread(target=fixtures_refresh_loop, daemon=True).start()

    threads = [
        threading.Thread(target=run_stream, args=("ODDS", ODDS_STREAM_URL, handle_odds_message), daemon=True),
        threading.Thread(target=run_stream, args=("SCORES", SCORES_STREAM_URL, handle_score_message), daemon=True),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Stopped.")

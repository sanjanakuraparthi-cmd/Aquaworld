#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import secrets
import sqlite3
import uuid
from datetime import date, datetime, timedelta, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "aquaworld.db"
HOST = os.environ.get("AQUAWORLD_HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT") or os.environ.get("AQUAWORLD_PORT") or "8000")

ROOM_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
ROOM_MEMBER_TTL = timedelta(seconds=45)
PUBLIC_ROOM_CODE = "OCEAN"
PUBLIC_ROOM_OWNER_ID = "public_ocean"
BLOCKED_WORDS = (
    "asshole",
    "bastard",
    "bitch",
    "bullshit",
    "dick",
    "fck",
    "fuck",
    "motherfucker",
    "penis",
    "pussy",
    "rape",
    "shit",
    "slut",
    "vagina",
    "whore",
)

CONTEST_THEMES = [
    ("Cutest Fish", "Show off the fish everyone wants to keep forever."),
    ("Best Blue Fish", "Ocean blues, night glows, and dreamy sea tones."),
    ("Wildest Design", "Chaotic, weird, bold, and impossible to ignore."),
    ("Most Cozy Fish", "Soft colors, calm energy, and gentle vibes."),
    ("Best Plant Friend", "Plants, seaweed, and the prettiest tank greens."),
    ("Most Legendary", "Big personality, rare look, unforgettable release."),
]


class ApiError(Exception):
    def __init__(self, status: int, error: str) -> None:
        super().__init__(error)
        self.status = status
        self.error = error


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_now(value: datetime | None = None) -> str:
    stamp = (value or now_utc()).replace(microsecond=0)
    return stamp.isoformat().replace("+00:00", "Z")


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS fish_submissions (
                submission_id TEXT PRIMARY KEY,
                contest_period TEXT NOT NULL,
                contest_title TEXT NOT NULL,
                room_code TEXT,
                owner_client_id TEXT NOT NULL,
                owner_id TEXT,
                owner_name TEXT NOT NULL,
                owner_color TEXT,
                fish_id TEXT,
                fish_name TEXT NOT NULL,
                fish_kind TEXT,
                personality TEXT,
                rarity TEXT,
                rarity_color TEXT,
                speed TEXT,
                scale REAL,
                image TEXT NOT NULL,
                likes INTEGER NOT NULL DEFAULT 0,
                frenzy_score INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS fish_votes (
                submission_id TEXT NOT NULL,
                voter_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (submission_id, voter_id),
                FOREIGN KEY (submission_id) REFERENCES fish_submissions(submission_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS rooms (
                room_code TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                state_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS room_members (
                room_code TEXT NOT NULL,
                client_id TEXT NOT NULL,
                name TEXT NOT NULL,
                color TEXT,
                joined_at TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                PRIMARY KEY (room_code, client_id),
                FOREIGN KEY (room_code) REFERENCES rooms(room_code) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS room_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_code TEXT NOT NULL,
                sender_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (room_code) REFERENCES rooms(room_code) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_room_members_room_last_seen
            ON room_members(room_code, last_seen);

            CREATE INDEX IF NOT EXISTS idx_room_events_room_event
            ON room_events(room_code, event_id);
            """
        )


def contest_for_today(today: date | None = None) -> dict:
    today = today or date.today()
    idx = today.toordinal() % len(CONTEST_THEMES)
    title, description = CONTEST_THEMES[idx]
    start = today
    end = start + timedelta(days=1)
    start_at = datetime.combine(start, datetime.min.time())
    end_at = datetime.combine(end, datetime.min.time())
    return {
        "period": start.isoformat(),
        "title": title,
        "description": description,
        "starts_on": start.isoformat(),
        "ends_on": (end - timedelta(days=1)).isoformat(),
        "starts_at": start_at.isoformat(),
        "ends_at": end_at.isoformat(),
    }


def row_to_entry(row: sqlite3.Row, voter_id: str | None = None, conn: sqlite3.Connection | None = None) -> dict:
    liked = False
    if voter_id and conn is not None:
        liked = conn.execute(
            "SELECT 1 FROM fish_votes WHERE submission_id = ? AND voter_id = ?",
            (row["submission_id"], voter_id),
        ).fetchone() is not None
    return {
        "submission_id": row["submission_id"],
        "contest_period": row["contest_period"],
        "contest_title": row["contest_title"],
        "room_code": row["room_code"],
        "owner_client_id": row["owner_client_id"],
        "owner_id": row["owner_id"],
        "owner_name": row["owner_name"],
        "owner_color": row["owner_color"],
        "fish_id": row["fish_id"],
        "fish_name": row["fish_name"],
        "fish_kind": row["fish_kind"],
        "personality": row["personality"],
        "rarity": row["rarity"],
        "rarity_color": row["rarity_color"],
        "speed": row["speed"],
        "scale": row["scale"],
        "image": row["image"],
        "likes": row["likes"],
        "frenzy_score": row["frenzy_score"],
        "created_at": row["created_at"],
        "liked_by_me": liked,
    }


def get_global_state(voter_id: str | None = None) -> dict:
    contest = contest_for_today()
    with db() as conn:
        leaderboard_rows = conn.execute(
            """
            SELECT * FROM fish_submissions
            WHERE contest_period = ?
            ORDER BY likes DESC, created_at ASC
            """,
            (contest["period"],),
        ).fetchall()
        frenzy_rows = conn.execute(
            """
            SELECT * FROM fish_submissions
            WHERE frenzy_score > 0
            ORDER BY frenzy_score DESC, likes DESC, created_at ASC
            LIMIT 12
            """
        ).fetchall()
        gallery_rows = conn.execute(
            """
            SELECT * FROM fish_submissions
            ORDER BY created_at DESC
            LIMIT 20
            """
        ).fetchall()
        hall_periods = conn.execute(
            """
            SELECT contest_period, MAX(likes) AS top_likes
            FROM fish_submissions
            WHERE contest_period <> ?
            GROUP BY contest_period
            ORDER BY contest_period DESC
            LIMIT 8
            """,
            (contest["period"],),
        ).fetchall()
        hall_of_fame = []
        for period_row in hall_periods:
            winner = conn.execute(
                """
                SELECT * FROM fish_submissions
                WHERE contest_period = ? AND likes = ?
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (period_row["contest_period"], period_row["top_likes"]),
            ).fetchone()
            if winner:
                hall_of_fame.append(row_to_entry(winner, voter_id, conn))
        return {
            "ok": True,
            "contest": contest,
            "leaderboard": [row_to_entry(r, voter_id, conn) for r in leaderboard_rows],
            "frenzy": [row_to_entry(r, voter_id, conn) for r in frenzy_rows],
            "gallery": [row_to_entry(r, voter_id, conn) for r in gallery_rows],
            "hall_of_fame": hall_of_fame,
        }


def normalize_room_code(raw: str | None) -> str:
    text = "".join(ch for ch in str(raw or "").upper() if ch.isalnum())
    return text[:12]


def generate_room_code(conn: sqlite3.Connection) -> str:
    for _ in range(64):
        code = "".join(secrets.choice(ROOM_CODE_ALPHABET) for _ in range(6))
        exists = conn.execute("SELECT 1 FROM rooms WHERE room_code = ?", (code,)).fetchone()
        if not exists:
            return code
    raise ApiError(500, "Could not create a unique room code")


def default_room_state() -> dict:
    return {
        "fishes": [],
        "plants": [],
        "theme": "Ocean",
        "night": False,
        "draft": None,
        "foodPellets": [],
    }


def coerce_room_state(raw_state: dict | None) -> dict:
    base = default_room_state()
    if isinstance(raw_state, dict):
        base.update(raw_state)
    base["fishes"] = list(base.get("fishes") or [])
    base["plants"] = list(base.get("plants") or [])
    base["foodPellets"] = list(base.get("foodPellets") or [])
    base["night"] = bool(base.get("night"))
    base["draft"] = base.get("draft") or None
    base["theme"] = str(base.get("theme") or "Ocean")[:32]
    return base


def load_room_row(conn: sqlite3.Connection, room_code: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM rooms WHERE room_code = ?", (normalize_room_code(room_code),)).fetchone()


def load_room_state(row: sqlite3.Row | None) -> dict:
    if not row:
        return default_room_state()
    try:
        parsed = json.loads(row["state_json"] or "{}")
    except json.JSONDecodeError:
        parsed = {}
    return coerce_room_state(parsed)


def save_room_state(conn: sqlite3.Connection, room_code: str, state: dict) -> None:
    code = normalize_room_code(room_code)
    clean_state = coerce_room_state(state)
    conn.execute(
        "UPDATE rooms SET state_json = ?, updated_at = ? WHERE room_code = ?",
        (json.dumps(clean_state, separators=(",", ":"), ensure_ascii=False), iso_now(), code),
    )


def member_to_dict(row: sqlite3.Row) -> dict:
    return {
        "client_id": row["client_id"],
        "name": row["name"],
        "color": row["color"],
        "joined_at": row["joined_at"],
    }


def active_room_member_rows(conn: sqlite3.Connection, room_code: str) -> list[sqlite3.Row]:
    cutoff = iso_now(now_utc() - ROOM_MEMBER_TTL)
    return conn.execute(
        """
        SELECT * FROM room_members
        WHERE room_code = ? AND last_seen >= ?
        ORDER BY joined_at ASC, client_id ASC
        """,
        (normalize_room_code(room_code), cutoff),
    ).fetchall()


def room_acting_owner_id(conn: sqlite3.Connection, room_row: sqlite3.Row) -> str:
    members = active_room_member_rows(conn, room_row["room_code"])
    active_ids = {row["client_id"] for row in members}
    if room_row["owner_id"] in active_ids:
        return room_row["owner_id"]
    if members:
        return members[0]["client_id"]
    return room_row["owner_id"]


def room_descriptor(conn: sqlite3.Connection, room_row: sqlite3.Row) -> dict:
    members = active_room_member_rows(conn, room_row["room_code"])
    return {
        "room_code": room_row["room_code"],
        "owner_id": room_row["owner_id"],
        "acting_owner_id": room_acting_owner_id(conn, room_row),
        "member_count": len(members),
        "created_at": room_row["created_at"],
        "updated_at": room_row["updated_at"],
    }


def latest_room_event_id(conn: sqlite3.Connection, room_code: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(event_id), 0) AS last_event_id FROM room_events WHERE room_code = ?",
        (normalize_room_code(room_code),),
    ).fetchone()
    return int(row["last_event_id"] or 0)


def append_room_event(conn: sqlite3.Connection, room_code: str, sender_id: str, message: dict) -> int:
    cur = conn.execute(
        """
        INSERT INTO room_events (room_code, sender_id, payload_json, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            normalize_room_code(room_code),
            str(sender_id or ""),
            json.dumps(message, separators=(",", ":"), ensure_ascii=False),
            iso_now(),
        ),
    )
    return int(cur.lastrowid)


def room_event_dict(row: sqlite3.Row) -> dict:
    try:
        message = json.loads(row["payload_json"] or "{}")
    except json.JSONDecodeError:
        message = {}
    return {
        "event_id": row["event_id"],
        "sender_id": row["sender_id"],
        "message": message,
        "created_at": row["created_at"],
    }


def upsert_room_member(conn: sqlite3.Connection, room_code: str, client_id: str, name: str, color: str | None) -> sqlite3.Row:
    code = normalize_room_code(room_code)
    now = iso_now()
    existing = conn.execute(
        "SELECT * FROM room_members WHERE room_code = ? AND client_id = ?",
        (code, client_id),
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE room_members
            SET name = ?, color = ?, last_seen = ?
            WHERE room_code = ? AND client_id = ?
            """,
            (name, color, now, code, client_id),
        )
    else:
        conn.execute(
            """
            INSERT INTO room_members (room_code, client_id, name, color, joined_at, last_seen)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (code, client_id, name, color, now, now),
        )
    return conn.execute(
        "SELECT * FROM room_members WHERE room_code = ? AND client_id = ?",
        (code, client_id),
    ).fetchone()


def cleanup_stale_members(conn: sqlite3.Connection, room_code: str, skip_client_id: str | None = None) -> None:
    code = normalize_room_code(room_code)
    cutoff = iso_now(now_utc() - ROOM_MEMBER_TTL)
    if skip_client_id:
        stale_rows = conn.execute(
            """
            SELECT * FROM room_members
            WHERE room_code = ? AND last_seen < ? AND client_id <> ?
            ORDER BY joined_at ASC
            """,
            (code, cutoff, skip_client_id),
        ).fetchall()
    else:
        stale_rows = conn.execute(
            """
            SELECT * FROM room_members
            WHERE room_code = ? AND last_seen < ?
            ORDER BY joined_at ASC
            """,
            (code, cutoff),
        ).fetchall()
    for row in stale_rows:
        conn.execute(
            "DELETE FROM room_members WHERE room_code = ? AND client_id = ?",
            (code, row["client_id"]),
        )
        append_room_event(
            conn,
            code,
            row["client_id"],
            {
                "type": "USER_LEFT",
                "id": row["client_id"],
                "name": row["name"],
                "color": row["color"],
            },
        )


def ensure_room_member(conn: sqlite3.Connection, room_code: str, client_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM room_members WHERE room_code = ? AND client_id = ?",
        (normalize_room_code(room_code), client_id),
    ).fetchone()
    if not row:
        raise ApiError(403, "Join the room first")
    return row


def clamp_float(value, default: float = 0.0, minimum: float | None = None, maximum: float | None = None) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = default
    if minimum is not None:
        number = max(minimum, number)
    if maximum is not None:
        number = min(maximum, number)
    return number


def clamp_int(value, default: int = 0, minimum: int | None = None, maximum: int | None = None) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = default
    if minimum is not None:
        number = max(minimum, number)
    if maximum is not None:
        number = min(maximum, number)
    return number


def clean_text(value: object, default: str = "", limit: int = 120) -> str:
    text = str(value or default).strip()
    if not text:
        text = default
    return text[:limit]


def moderation_key(value: object) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def contains_blocked_word(value: object) -> bool:
    key = moderation_key(value)
    return bool(key) and any(word in key for word in BLOCKED_WORDS)


def safe_public_name(value: object, default: str = "Aquarist", limit: int = 40) -> str:
    text = clean_text(value, default, limit)
    return default if contains_blocked_word(text) else text


def moderate_chat_text(value: object, limit: int = 100) -> str:
    text = clean_text(value, "", limit)
    if not text:
        return ""
    lowered = text.lower()
    updated = text
    replaced = False
    for word in BLOCKED_WORDS:
        if word in lowered:
            replacement = "•" * len(word)
            updated = updated.replace(word, replacement).replace(word.title(), replacement).replace(word.upper(), replacement)
            replaced = True
    if replaced:
        return updated[:limit]
    if contains_blocked_word(text):
        return "[message moderated]"
    return text


def ensure_room_exists(conn: sqlite3.Connection, room_code: str, owner_id: str | None = None) -> sqlite3.Row:
    code = normalize_room_code(room_code)
    row = load_room_row(conn, code)
    if row:
        return row
    now = iso_now()
    conn.execute(
        """
        INSERT INTO rooms (room_code, owner_id, state_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            code,
            clean_text(owner_id, PUBLIC_ROOM_OWNER_ID if code == PUBLIC_ROOM_CODE else uuid.uuid4().hex[:12], 64),
            json.dumps(default_room_state(), separators=(",", ":")),
            now,
            now,
        ),
    )
    return load_room_row(conn, code)


def find_room_fish(state: dict, fish_id: str) -> tuple[dict | None, str | None]:
    for key in ("fishes", "plants"):
        for item in state.get(key, []):
            if str(item.get("id") or "") == fish_id:
                return item, key
    return None, None


def replace_room_fish(state: dict, fish: dict, kind: str) -> None:
    fish_id = str(fish.get("id") or "")
    state["fishes"] = [item for item in state.get("fishes", []) if str(item.get("id") or "") != fish_id]
    state["plants"] = [item for item in state.get("plants", []) if str(item.get("id") or "") != fish_id]
    key = "plants" if kind == "Plant" else "fishes"
    state[key].append(fish)


def clean_fish_payload(raw_fish: dict, owner_id: str, current: dict | None = None, forced_kind: str | None = None) -> dict:
    fish = json.loads(json.dumps(current or {}))
    if isinstance(raw_fish, dict):
        fish.update(raw_fish)
    current_image = current.get("img") if current else ""
    current_born_at = current.get("bornAt") if current else int(now_utc().timestamp() * 1000)
    current_food_score = current.get("foodScore") if current else 0
    current_global_likes = current.get("globalLikes") if current else 0
    current_speech_expires = current.get("speechExpires") if current else 0
    fish["id"] = clean_text(fish.get("id") or uuid.uuid4().hex[:8], "fish", 64)
    fish["name"] = safe_public_name(fish.get("name"), clean_text(current.get("name"), "Fish", 40) if current else "Fish", 40)
    fish["kind"] = forced_kind or ("Plant" if clean_text(fish.get("kind"), "Fish", 10).lower() == "plant" else "Fish")
    fish["img"] = str(fish.get("img") or current_image or "")
    fish["personality"] = clean_text(fish.get("personality"), "Friendly", 24)
    fish["rarity"] = clean_text(fish.get("rarity"), "Common", 24)
    fish["rarityColor"] = clean_text(fish.get("rarityColor"), "#9fb4d1", 24)
    fish["speed"] = clean_text(fish.get("speed"), "Calm", 24)
    fish["scale"] = clamp_float(fish.get("scale"), current.get("scale") if current else 1.0, 0.15, 6.0)
    fish["x"] = clamp_float(fish.get("x"), current.get("x") if current else 120.0)
    fish["y"] = clamp_float(fish.get("y"), current.get("y") if current else 120.0)
    fish["vx"] = clamp_float(fish.get("vx"), current.get("vx") if current else 0.0, -8.0, 8.0)
    fish["vy"] = clamp_float(fish.get("vy"), current.get("vy") if current else 0.0, -8.0, 8.0)
    fish["angle"] = clamp_float(fish.get("angle"), current.get("angle") if current else 0.0, -12.0, 12.0)
    fish["wobble"] = clamp_float(fish.get("wobble"), current.get("wobble") if current else 0.0, -12.0, 12.0)
    fish["ownerId"] = owner_id
    fish["bornAt"] = clamp_int(fish.get("bornAt"), clamp_int(current_born_at, int(now_utc().timestamp() * 1000)))
    fish["foodScore"] = clamp_int(fish.get("foodScore"), clamp_int(current_food_score, 0), 0)
    fish["inFeedRace"] = bool(fish.get("inFeedRace") if "inFeedRace" in fish else current.get("inFeedRace") if current else False)
    fish["inContest"] = bool(fish.get("inContest") if "inContest" in fish else current.get("inContest") if current else False)
    fish["contestEntryId"] = fish.get("contestEntryId") if fish.get("contestEntryId") else current.get("contestEntryId") if current else None
    fish["globalLikes"] = clamp_int(fish.get("globalLikes"), clamp_int(current_global_likes, 0), 0)
    fish["speech"] = clean_text(fish.get("speech"), "", 100) if fish.get("speech") else None
    fish["speechExpires"] = clamp_int(fish.get("speechExpires"), clamp_int(current_speech_expires, 0), 0)
    return fish


def apply_room_message(
    conn: sqlite3.Connection,
    room_row: sqlite3.Row,
    sender_id: str,
    sender_name: str | None,
    sender_color: str | None,
    raw_message: dict,
) -> tuple[int, dict, dict]:
    room_code = room_row["room_code"]
    message = raw_message if isinstance(raw_message, dict) else {}
    msg_type = clean_text(message.get("type"), "", 32)
    if not msg_type:
        raise ApiError(400, "Missing room event type")

    member_row = ensure_room_member(conn, room_code, sender_id)
    acting_owner_id = room_acting_owner_id(conn, room_row)
    state = load_room_state(room_row)
    state_changed = False

    def require_owner() -> None:
        if sender_id != acting_owner_id:
            raise ApiError(403, "Only the active room leader can do that")

    if msg_type == "CHAT":
        clean_message = {
            "type": "CHAT",
            "name": safe_public_name(sender_name or member_row["name"], member_row["name"], 40),
            "color": clean_text(sender_color or member_row["color"], member_row["color"] or "#9fb4d1", 24),
            "text": moderate_chat_text(message.get("text"), 100),
        }
    elif msg_type == "DRAW_SEG":
        clean_message = {
            "type": "DRAW_SEG",
            "tool": "eraser" if clean_text(message.get("tool"), "pen", 16) == "eraser" else "pen",
            "x1": clamp_float(message.get("x1")),
            "y1": clamp_float(message.get("y1")),
            "x2": clamp_float(message.get("x2")),
            "y2": clamp_float(message.get("y2")),
            "color": clean_text(message.get("color"), "#000000", 24),
            "size": clamp_float(message.get("size"), 3.0, 1.0, 40.0),
            "opacity": clamp_float(message.get("opacity"), 1.0, 0.05, 1.0),
            "mirror": bool(message.get("mirror")),
        }
    elif msg_type == "DRAW_FILL":
        clean_message = {
            "type": "DRAW_FILL",
            "x": clamp_float(message.get("x")),
            "y": clamp_float(message.get("y")),
            "color": clean_text(message.get("color"), "#000000", 24),
        }
    elif msg_type == "DRAW_SPRAY":
        clean_message = {
            "type": "DRAW_SPRAY",
            "x": clamp_float(message.get("x")),
            "y": clamp_float(message.get("y")),
            "color": clean_text(message.get("color"), "#000000", 24),
            "size": clamp_float(message.get("size"), 3.0, 1.0, 40.0),
            "opacity": clamp_float(message.get("opacity"), 1.0, 0.05, 1.0),
            "mirror": bool(message.get("mirror")),
        }
    elif msg_type == "DRAW_SNAPSHOT":
        clean_message = {
            "type": "DRAW_SNAPSHOT",
            "image": str(message.get("image") or "") or None,
        }
        state["draft"] = clean_message["image"]
        state_changed = True
    elif msg_type == "DRAW_CLEAR":
        clean_message = {"type": "DRAW_CLEAR"}
        state["draft"] = None
        state_changed = True
    elif msg_type == "INIT":
        require_owner()
        fishes = [clean_fish_payload(item, str(item.get("ownerId") or sender_id), forced_kind="Fish") for item in list(message.get("fishes") or []) if isinstance(item, dict)]
        plants = [clean_fish_payload(item, str(item.get("ownerId") or sender_id), forced_kind="Plant") for item in list(message.get("plants") or []) if isinstance(item, dict)]
        clean_message = {
            "type": "INIT",
            "fishes": fishes,
            "plants": plants,
            "theme": clean_text(message.get("theme"), state.get("theme") or "Ocean", 32),
            "night": bool(message.get("night")),
            "draft": str(message.get("draft") or "") or None,
            "foodPellets": list(message.get("foodPellets") or []),
        }
        state.update(
            {
                "fishes": fishes,
                "plants": plants,
                "theme": clean_message["theme"],
                "night": clean_message["night"],
                "draft": clean_message["draft"],
                "foodPellets": list(clean_message["foodPellets"]),
            }
        )
        state_changed = True
    elif msg_type == "FISH_ADD":
        fish = clean_fish_payload(message.get("fish") or {}, sender_id, forced_kind="Plant" if clean_text(message.get("kind"), "", 12) == "Plant" else "Fish")
        if not fish.get("img"):
            raise ApiError(400, "Fish image missing")
        replace_room_fish(state, fish, fish["kind"])
        clean_message = {"type": "FISH_ADD", "kind": fish["kind"], "fish": fish}
        state_changed = True
    elif msg_type == "FISH_EDIT":
        fish_id = clean_text(message.get("id") or (message.get("fish") or {}).get("id"), "", 64)
        current, _ = find_room_fish(state, fish_id)
        if not current:
            raise ApiError(404, "Fish not found")
        if clean_text(current.get("ownerId"), "", 64) != sender_id:
            raise ApiError(403, "Only the owner can edit this fish")
        fish = clean_fish_payload(message.get("fish") or {}, sender_id, current=current)
        if not fish.get("img"):
            raise ApiError(400, "Fish image missing")
        replace_room_fish(state, fish, fish["kind"])
        clean_message = {"type": "FISH_EDIT", "id": fish_id, "fish": fish}
        state_changed = True
    elif msg_type == "FISH_DEL":
        fish_id = clean_text(message.get("id"), "", 64)
        current, _ = find_room_fish(state, fish_id)
        if not current:
            raise ApiError(404, "Fish not found")
        if clean_text(current.get("ownerId"), "", 64) != sender_id:
            raise ApiError(403, "Only the owner can remove this fish")
        state["fishes"] = [item for item in state.get("fishes", []) if clean_text(item.get("id"), "", 64) != fish_id]
        state["plants"] = [item for item in state.get("plants", []) if clean_text(item.get("id"), "", 64) != fish_id]
        clean_message = {"type": "FISH_DEL", "id": fish_id}
        state_changed = True
    elif msg_type == "FOOD_DROP":
        clean_message = {
            "type": "FOOD_DROP",
            "x": clamp_float(message.get("x")),
            "y": clamp_float(message.get("y")),
            "pelletId": clean_text(message.get("pelletId"), f"drop_{uuid.uuid4().hex[:8]}", 64),
        }
    elif msg_type == "FOOD_EAT":
        fish_id = clean_text(message.get("fishId"), "", 64)
        current, key = find_room_fish(state, fish_id)
        if not current:
            raise ApiError(404, "Fish not found")
        if clean_text(current.get("ownerId"), "", 64) != sender_id:
            raise ApiError(403, "Only the owner can feed-sync this fish")
        current["foodScore"] = max(clamp_int(current.get("foodScore"), 0, 0), clamp_int(message.get("foodScore"), 0, 0))
        if key:
            replace_room_fish(state, current, "Plant" if key == "plants" else "Fish")
        clean_message = {
            "type": "FOOD_EAT",
            "pelletId": clean_text(message.get("pelletId"), "", 64),
            "fishId": fish_id,
            "foodScore": current["foodScore"],
        }
        state_changed = True
    elif msg_type == "FISH_FEED_STATE":
        fish_id = clean_text(message.get("id"), "", 64)
        current, key = find_room_fish(state, fish_id)
        if not current:
            raise ApiError(404, "Fish not found")
        if clean_text(current.get("ownerId"), "", 64) != sender_id:
            raise ApiError(403, "Only the owner can sync this fish")
        current["foodScore"] = max(clamp_int(current.get("foodScore"), 0, 0), clamp_int((message.get("fish") or {}).get("foodScore"), 0, 0))
        if key:
            replace_room_fish(state, current, "Plant" if key == "plants" else "Fish")
        clean_message = {"type": "FISH_FEED_STATE", "id": fish_id, "fish": {"id": fish_id, "ownerId": sender_id, "foodScore": current["foodScore"]}}
        state_changed = True
    elif msg_type == "THEME":
        require_owner()
        clean_message = {"type": "THEME", "theme": clean_text(message.get("theme"), state.get("theme") or "Ocean", 32)}
        state["theme"] = clean_message["theme"]
        state_changed = True
    elif msg_type == "NIGHT":
        require_owner()
        clean_message = {"type": "NIGHT", "night": bool(message.get("night"))}
        state["night"] = clean_message["night"]
        state_changed = True
    else:
        raise ApiError(400, "Unknown room event")

    if state_changed:
        save_room_state(conn, room_code, state)
        room_row = load_room_row(conn, room_code)

    event_id = append_room_event(conn, room_code, sender_id, clean_message)
    return event_id, clean_message, room_descriptor(conn, room_row)


class AquaHandler(SimpleHTTPRequestHandler):
    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/global/state":
            voter_id = parse_qs(parsed.query).get("voter_id", [None])[0]
            return self.write_json(get_global_state(voter_id))
        if parsed.path == "/api/global/status":
            return self.write_json({"ok": True, "contest": contest_for_today()})
        if parsed.path == "/api/rooms/status":
            return self.handle_room_status(parsed)
        if parsed.path == "/api/rooms/sync":
            return self.handle_room_sync(parsed)
        return super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return self.write_json({"ok": False, "error": "Invalid JSON"}, 400)

        if parsed.path == "/api/global/submit":
            return self.handle_submit(payload)
        if parsed.path == "/api/global/like":
            return self.handle_like(payload)
        if parsed.path == "/api/global/remove":
            return self.handle_remove(payload)
        if parsed.path == "/api/global/frenzy":
            return self.handle_frenzy(payload)
        if parsed.path == "/api/rooms/create":
            return self.handle_room_create(payload)
        if parsed.path == "/api/rooms/join":
            return self.handle_room_join(payload)
        if parsed.path == "/api/rooms/event":
            return self.handle_room_event(payload)
        if parsed.path == "/api/rooms/leave":
            return self.handle_room_leave(payload)

        return self.write_json({"ok": False, "error": "Not found"}, 404)

    def handle_submit(self, payload: dict) -> None:
        fish = payload.get("fish") or {}
        owner_client_id = (payload.get("client_id") or "").strip()
        owner_name = safe_public_name(payload.get("owner_name"), "Aquarist", 40)
        fish_name = safe_public_name(fish.get("name"), "Fish", 40)
        image = fish.get("img") or ""
        if not owner_client_id or not image:
            return self.write_json({"ok": False, "error": "Missing fish data"}, 400)

        contest = contest_for_today()
        submission_id = payload.get("submission_id") or f"sub_{uuid.uuid4().hex[:16]}"
        now = iso_now()
        with db() as conn:
            existing = conn.execute(
                "SELECT owner_client_id, likes, frenzy_score FROM fish_submissions WHERE submission_id = ?",
                (submission_id,),
            ).fetchone()
            if existing and existing["owner_client_id"] != owner_client_id:
                return self.write_json({"ok": False, "error": "Submission belongs to another owner"}, 403)
            if existing:
                conn.execute(
                    """
                    UPDATE fish_submissions
                    SET contest_period = ?, contest_title = ?, room_code = ?, owner_id = ?, owner_name = ?, owner_color = ?,
                        fish_id = ?, fish_name = ?, fish_kind = ?, personality = ?, rarity = ?, rarity_color = ?, speed = ?,
                        scale = ?, image = ?, updated_at = ?
                    WHERE submission_id = ?
                    """,
                    (
                        contest["period"], contest["title"], payload.get("room_code"), payload.get("owner_id"),
                        owner_name, payload.get("owner_color"), fish.get("id"), fish_name, fish.get("kind"),
                        fish.get("personality"), fish.get("rarity"), fish.get("rarityColor"), fish.get("speed"),
                        fish.get("scale"), image, now, submission_id,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO fish_submissions (
                        submission_id, contest_period, contest_title, room_code, owner_client_id, owner_id, owner_name, owner_color,
                        fish_id, fish_name, fish_kind, personality, rarity, rarity_color, speed, scale, image, likes, frenzy_score, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
                    """,
                    (
                        submission_id, contest["period"], contest["title"], payload.get("room_code"), owner_client_id,
                        payload.get("owner_id"), owner_name, payload.get("owner_color"), fish.get("id"), fish_name,
                        fish.get("kind"), fish.get("personality"), fish.get("rarity"), fish.get("rarityColor"),
                        fish.get("speed"), fish.get("scale"), image, int(fish.get("foodScore") or 0), now, now,
                    ),
                )
            row = conn.execute("SELECT * FROM fish_submissions WHERE submission_id = ?", (submission_id,)).fetchone()
        self.write_json({"ok": True, "entry": row_to_entry(row)})

    def handle_like(self, payload: dict) -> None:
        submission_id = payload.get("submission_id")
        voter_id = (payload.get("voter_id") or "").strip()
        if not submission_id or not voter_id:
            return self.write_json({"ok": False, "error": "Missing vote data"}, 400)
        with db() as conn:
            row = conn.execute("SELECT * FROM fish_submissions WHERE submission_id = ?", (submission_id,)).fetchone()
            if not row:
                return self.write_json({"ok": False, "error": "Submission not found"}, 404)
            if row["owner_client_id"] == voter_id:
                return self.write_json({"ok": False, "error": "No self-voting"}, 403)
            existing = conn.execute(
                "SELECT 1 FROM fish_votes WHERE submission_id = ? AND voter_id = ?",
                (submission_id, voter_id),
            ).fetchone()
            if existing:
                conn.execute("DELETE FROM fish_votes WHERE submission_id = ? AND voter_id = ?", (submission_id, voter_id))
                conn.execute("UPDATE fish_submissions SET likes = MAX(likes - 1, 0), updated_at = ? WHERE submission_id = ?", (iso_now(), submission_id))
                liked = False
            else:
                conn.execute(
                    "INSERT INTO fish_votes (submission_id, voter_id, created_at) VALUES (?, ?, ?)",
                    (submission_id, voter_id, iso_now()),
                )
                conn.execute("UPDATE fish_submissions SET likes = likes + 1, updated_at = ? WHERE submission_id = ?", (iso_now(), submission_id))
                liked = True
            fresh = conn.execute("SELECT * FROM fish_submissions WHERE submission_id = ?", (submission_id,)).fetchone()
        self.write_json({"ok": True, "liked": liked, "entry": row_to_entry(fresh)})

    def handle_remove(self, payload: dict) -> None:
        submission_id = payload.get("submission_id")
        client_id = (payload.get("client_id") or "").strip()
        if not submission_id or not client_id:
            return self.write_json({"ok": False, "error": "Missing removal data"}, 400)
        with db() as conn:
            row = conn.execute("SELECT * FROM fish_submissions WHERE submission_id = ?", (submission_id,)).fetchone()
            if not row:
                return self.write_json({"ok": True, "removed": False})
            if row["owner_client_id"] != client_id:
                return self.write_json({"ok": False, "error": "Only the owner can remove this submission"}, 403)
            conn.execute("DELETE FROM fish_votes WHERE submission_id = ?", (submission_id,))
            conn.execute("DELETE FROM fish_submissions WHERE submission_id = ?", (submission_id,))
        self.write_json({"ok": True, "removed": True, "submission_id": submission_id})

    def handle_frenzy(self, payload: dict) -> None:
        submission_id = payload.get("submission_id")
        client_id = (payload.get("client_id") or "").strip()
        score = int(payload.get("score") or 0)
        if not submission_id or not client_id:
            return self.write_json({"ok": False, "error": "Missing frenzy data"}, 400)
        with db() as conn:
            row = conn.execute("SELECT * FROM fish_submissions WHERE submission_id = ?", (submission_id,)).fetchone()
            if not row:
                return self.write_json({"ok": False, "error": "Submission not found"}, 404)
            if row["owner_client_id"] != client_id:
                return self.write_json({"ok": False, "error": "Only the owner can update frenzy score"}, 403)
            best = max(row["frenzy_score"], score)
            conn.execute(
                "UPDATE fish_submissions SET frenzy_score = ?, updated_at = ? WHERE submission_id = ?",
                (best, iso_now(), submission_id),
            )
            fresh = conn.execute("SELECT * FROM fish_submissions WHERE submission_id = ?", (submission_id,)).fetchone()
        self.write_json({"ok": True, "entry": row_to_entry(fresh)})

    def handle_room_create(self, payload: dict) -> None:
        client_id = clean_text(payload.get("client_id"), uuid.uuid4().hex[:6], 64)
        name = safe_public_name(payload.get("name"), "Aquarist", 40)
        color = clean_text(payload.get("color"), "#9fb4d1", 24)
        with db() as conn:
            room_code = generate_room_code(conn)
            now = iso_now()
            conn.execute(
                """
                INSERT INTO rooms (room_code, owner_id, state_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (room_code, client_id, json.dumps(default_room_state(), separators=(",", ":")), now, now),
            )
            upsert_room_member(conn, room_code, client_id, name, color)
            room_row = load_room_row(conn, room_code)
            response = {
                "ok": True,
                "room": room_descriptor(conn, room_row),
                "state": load_room_state(room_row),
                "members": [member_to_dict(row) for row in active_room_member_rows(conn, room_code)],
                "last_event_id": latest_room_event_id(conn, room_code),
            }
        self.write_json(response)

    def handle_room_join(self, payload: dict) -> None:
        room_code = normalize_room_code(payload.get("room_code"))
        client_id = clean_text(payload.get("client_id"), uuid.uuid4().hex[:6], 64)
        name = safe_public_name(payload.get("name"), "Aquarist", 40)
        color = clean_text(payload.get("color"), "#9fb4d1", 24)
        if not room_code:
            return self.write_json({"ok": False, "error": "Missing room code"}, 400)
        try:
            with db() as conn:
                room_row = load_room_row(conn, room_code)
                if not room_row and room_code == PUBLIC_ROOM_CODE:
                    room_row = ensure_room_exists(conn, PUBLIC_ROOM_CODE, PUBLIC_ROOM_OWNER_ID)
                if not room_row:
                    raise ApiError(404, "Room not found")
                cleanup_stale_members(conn, room_code, skip_client_id=client_id)
                existing = conn.execute(
                    "SELECT 1 FROM room_members WHERE room_code = ? AND client_id = ?",
                    (room_code, client_id),
                ).fetchone()
                upsert_room_member(conn, room_code, client_id, name, color)
                if not existing:
                    append_room_event(
                        conn,
                        room_code,
                        client_id,
                        {"type": "USER_JOIN", "id": client_id, "name": name, "color": color},
                    )
                room_row = load_room_row(conn, room_code)
                response = {
                    "ok": True,
                    "room": room_descriptor(conn, room_row),
                    "state": load_room_state(room_row),
                    "members": [member_to_dict(row) for row in active_room_member_rows(conn, room_code)],
                    "last_event_id": latest_room_event_id(conn, room_code),
                }
        except ApiError as err:
            return self.write_json({"ok": False, "error": err.error}, err.status)
        self.write_json(response)

    def handle_room_sync(self, parsed) -> None:
        qs = parse_qs(parsed.query)
        room_code = normalize_room_code(qs.get("room_code", [""])[0])
        client_id = clean_text(qs.get("client_id", [""])[0], "", 64)
        last_event_id = clamp_int(qs.get("last_event_id", ["0"])[0], 0, 0)
        if not room_code or not client_id:
            return self.write_json({"ok": False, "error": "Missing room sync data"}, 400)
        try:
            with db() as conn:
                room_row = load_room_row(conn, room_code)
                if not room_row:
                    raise ApiError(404, "Room not found")
                member = ensure_room_member(conn, room_code, client_id)
                upsert_room_member(conn, room_code, client_id, member["name"], member["color"])
                cleanup_stale_members(conn, room_code, skip_client_id=client_id)
                room_row = load_room_row(conn, room_code)
                event_rows = conn.execute(
                    """
                    SELECT * FROM room_events
                    WHERE room_code = ? AND event_id > ?
                    ORDER BY event_id ASC
                    """,
                    (room_code, last_event_id),
                ).fetchall()
                response = {
                    "ok": True,
                    "room": room_descriptor(conn, room_row),
                    "members": [member_to_dict(row) for row in active_room_member_rows(conn, room_code)],
                    "events": [room_event_dict(row) for row in event_rows],
                    "last_event_id": latest_room_event_id(conn, room_code),
                    "state": load_room_state(room_row) if last_event_id == 0 else None,
                }
        except ApiError as err:
            return self.write_json({"ok": False, "error": err.error}, err.status)
        self.write_json(response)

    def handle_room_status(self, parsed) -> None:
        qs = parse_qs(parsed.query)
        room_code = normalize_room_code(qs.get("room_code", [""])[0])
        if not room_code:
            return self.write_json({"ok": False, "error": "Missing room code"}, 400)
        with db() as conn:
            room_row = load_room_row(conn, room_code)
            if not room_row and room_code == PUBLIC_ROOM_CODE:
                room_row = ensure_room_exists(conn, PUBLIC_ROOM_CODE, PUBLIC_ROOM_OWNER_ID)
            if not room_row:
                return self.write_json({"ok": False, "exists": False, "error": "Room not found"}, 404)
            cleanup_stale_members(conn, room_code)
            room_row = load_room_row(conn, room_code)
            self.write_json(
                {
                    "ok": True,
                    "exists": True,
                    "room": room_descriptor(conn, room_row),
                    "members": [member_to_dict(row) for row in active_room_member_rows(conn, room_code)],
                }
            )

    def handle_room_event(self, payload: dict) -> None:
        room_code = normalize_room_code(payload.get("room_code"))
        client_id = clean_text(payload.get("client_id"), "", 64)
        name = safe_public_name(payload.get("name"), "Aquarist", 40)
        color = clean_text(payload.get("color"), "#9fb4d1", 24)
        message = payload.get("message")
        if not room_code or not client_id or not isinstance(message, dict):
            return self.write_json({"ok": False, "error": "Missing room event data"}, 400)
        try:
            with db() as conn:
                room_row = load_room_row(conn, room_code)
                if not room_row:
                    raise ApiError(404, "Room not found")
                ensure_room_member(conn, room_code, client_id)
                upsert_room_member(conn, room_code, client_id, name, color)
                cleanup_stale_members(conn, room_code, skip_client_id=client_id)
                room_row = load_room_row(conn, room_code)
                event_id, _, descriptor = apply_room_message(conn, room_row, client_id, name, color, message)
                response = {"ok": True, "event_id": event_id, "room": descriptor}
        except ApiError as err:
            return self.write_json({"ok": False, "error": err.error}, err.status)
        self.write_json(response)

    def handle_room_leave(self, payload: dict) -> None:
        room_code = normalize_room_code(payload.get("room_code"))
        client_id = clean_text(payload.get("client_id"), "", 64)
        if not room_code or not client_id:
            return self.write_json({"ok": False, "error": "Missing leave data"}, 400)
        with db() as conn:
            member = conn.execute(
                "SELECT * FROM room_members WHERE room_code = ? AND client_id = ?",
                (room_code, client_id),
            ).fetchone()
            if member:
                conn.execute(
                    "DELETE FROM room_members WHERE room_code = ? AND client_id = ?",
                    (room_code, client_id),
                )
                append_room_event(
                    conn,
                    room_code,
                    client_id,
                    {"type": "USER_LEFT", "id": client_id, "name": member["name"], "color": member["color"]},
                )
        self.write_json({"ok": True, "left": bool(member)})

    def write_json(self, data: dict, status: int = 200) -> None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def translate_path(self, path: str) -> str:
        parsed = urlparse(path)
        path = parsed.path
        if path == "/":
            path = "/aquarium-collab-done.html"
        return str((ROOT / path.lstrip("/")).resolve())


def main() -> None:
    os.chdir(ROOT)
    init_db()
    server = ThreadingHTTPServer((HOST, PORT), AquaHandler)
    print(f"AquaWorld server running on http://{HOST}:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()

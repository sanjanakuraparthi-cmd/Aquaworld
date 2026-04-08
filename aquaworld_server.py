#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import date, datetime, timedelta
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse


ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "aquaworld.db"
HOST = os.environ.get("AQUAWORLD_HOST", "127.0.0.1")
PORT = int(os.environ.get("AQUAWORLD_PORT", "8000"))

CONTEST_THEMES = [
    ("Cutest Fish", "Show off the fish everyone wants to keep forever."),
    ("Best Blue Fish", "Ocean blues, night glows, and dreamy sea tones."),
    ("Wildest Design", "Chaotic, weird, bold, and impossible to ignore."),
    ("Most Cozy Fish", "Soft colors, calm energy, and gentle vibes."),
    ("Best Plant Friend", "Plants, seaweed, and the prettiest tank greens."),
    ("Most Legendary", "Big personality, rare look, unforgettable release."),
]


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
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
            """
        )


def iso_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def contest_for_today(today: date | None = None) -> dict:
    today = today or date.today()
    year, week, _ = today.isocalendar()
    idx = (year * 53 + week) % len(CONTEST_THEMES)
    title, description = CONTEST_THEMES[idx]
    start = date.fromisocalendar(year, week, 1)
    end = start + timedelta(days=6)
    return {
        "period": f"{year}-W{week:02d}",
        "title": title,
        "description": description,
        "starts_on": start.isoformat(),
        "ends_on": end.isoformat(),
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
            LIMIT 12
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


class AquaHandler(SimpleHTTPRequestHandler):
    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
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
        if parsed.path == "/api/global/frenzy":
            return self.handle_frenzy(payload)

        return self.write_json({"ok": False, "error": "Not found"}, 404)

    def handle_submit(self, payload: dict) -> None:
        fish = payload.get("fish") or {}
        owner_client_id = (payload.get("client_id") or "").strip()
        owner_name = (payload.get("owner_name") or "Aquarist").strip() or "Aquarist"
        fish_name = (fish.get("name") or "Fish").strip() or "Fish"
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

    def write_json(self, data: dict, status: int = 200) -> None:
        body = json.dumps(data).encode("utf-8")
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

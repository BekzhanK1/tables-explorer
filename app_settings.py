from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

DB_PATH = Path(__file__).resolve().parent / "data" / "app_settings.sqlite3"


@dataclass(frozen=True)
class DbProfile:
    id: int
    name: str
    host: str
    port: int
    dbname: str
    user: str
    password: str
    sslmode: str | None = None
    connect_timeout: int = 5
    include_in_timeline: bool = True
    is_prod: bool = False

    def connection_kwargs(self) -> dict[str, Any]:
        settings: dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "connect_timeout": self.connect_timeout,
        }
        if self.sslmode:
            settings["sslmode"] = self.sslmode
        return settings


def _connect_sqlite() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _connect_sqlite() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS db_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                host TEXT NOT NULL,
                port INTEGER NOT NULL DEFAULT 5432,
                dbname TEXT NOT NULL,
                pg_user TEXT NOT NULL,
                password TEXT NOT NULL DEFAULT '',
                sslmode TEXT,
                connect_timeout INTEGER NOT NULL DEFAULT 5,
                is_active INTEGER NOT NULL DEFAULT 0,
                is_prod INTEGER NOT NULL DEFAULT 0,
                include_in_timeline INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS app_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(db_profiles)").fetchall()
        }
        if "is_prod" not in columns:
            conn.execute(
                "ALTER TABLE db_profiles ADD COLUMN is_prod INTEGER NOT NULL DEFAULT 0"
            )
        conn.commit()


def _row_to_profile(row: sqlite3.Row) -> DbProfile:
    return DbProfile(
        id=row["id"],
        name=row["name"],
        host=row["host"],
        port=row["port"],
        dbname=row["dbname"],
        user=row["pg_user"],
        password=row["password"] or "",
        sslmode=row["sslmode"],
        connect_timeout=row["connect_timeout"],
        include_in_timeline=bool(row["include_in_timeline"]),
        is_prod=bool(row["is_prod"]),
    )


def list_profiles() -> list[DbProfile]:
    init_db()
    with _connect_sqlite() as conn:
        rows = conn.execute(
            "SELECT * FROM db_profiles ORDER BY is_active DESC, name COLLATE NOCASE"
        ).fetchall()
    return [_row_to_profile(row) for row in rows]


def get_profile(profile_id: int) -> DbProfile | None:
    init_db()
    with _connect_sqlite() as conn:
        row = conn.execute(
            "SELECT * FROM db_profiles WHERE id = ?", (profile_id,)
        ).fetchone()
    return _row_to_profile(row) if row else None


def get_active_profile() -> DbProfile | None:
    init_db()
    with _connect_sqlite() as conn:
        row = conn.execute(
            "SELECT * FROM db_profiles WHERE is_active = 1 ORDER BY id LIMIT 1"
        ).fetchone()
    return _row_to_profile(row) if row else None


def get_prod_profile() -> DbProfile | None:
    init_db()
    with _connect_sqlite() as conn:
        row = conn.execute(
            "SELECT * FROM db_profiles WHERE is_prod = 1 ORDER BY id LIMIT 1"
        ).fetchone()
    return _row_to_profile(row) if row else None


def set_prod_profile(profile_id: int | None) -> None:
    init_db()
    with _connect_sqlite() as conn:
        conn.execute("UPDATE db_profiles SET is_prod = 0")
        if profile_id is not None:
            conn.execute(
                "UPDATE db_profiles SET is_prod = 1, updated_at = datetime('now') WHERE id = ?",
                (profile_id,),
            )
        conn.commit()


def format_profile_label(profile: DbProfile, *, active: DbProfile | None, prod: DbProfile | None) -> str:
    tags: list[str] = []
    if active and profile.id == active.id:
        tags.append("источник")
    if prod and profile.id == prod.id:
        tags.append("PROD")
    suffix = f" ({', '.join(tags)})" if tags else ""
    return f"{profile.name}{suffix}"


def get_prod_db_label() -> str:
    profile = get_prod_profile()
    if profile is None:
        return "PROD: не задан"
    return f"PROD: {profile.name} ({profile.host}/{profile.dbname})"


def set_active_profile(profile_id: int) -> None:
    init_db()
    with _connect_sqlite() as conn:
        conn.execute("UPDATE db_profiles SET is_active = 0")
        conn.execute(
            "UPDATE db_profiles SET is_active = 1, updated_at = datetime('now') WHERE id = ?",
            (profile_id,),
        )
        conn.commit()


def save_profile(
    *,
    profile_id: int | None,
    name: str,
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
    sslmode: str | None,
    connect_timeout: int,
    include_in_timeline: bool,
    make_active: bool = False,
) -> DbProfile:
    init_db()
    clean_name = name.strip()
    if not clean_name:
        raise ValueError("Укажите название подключения")

    with _connect_sqlite() as conn:
        existing_count = conn.execute("SELECT COUNT(*) AS c FROM db_profiles").fetchone()["c"]
        if profile_id is None:
            row = conn.execute(
                """
                INSERT INTO db_profiles (
                    name, host, port, dbname, pg_user, password,
                    sslmode, connect_timeout, is_active, include_in_timeline
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
                """,
                (
                    clean_name,
                    host.strip(),
                    port,
                    dbname.strip(),
                    user.strip(),
                    password,
                    (sslmode or "").strip() or None,
                    connect_timeout,
                    1 if make_active or existing_count == 0 else 0,
                    1 if include_in_timeline else 0,
                ),
            ).fetchone()
            new_id = row["id"]
        else:
            existing = get_profile(profile_id)
            if existing is None:
                raise ValueError("Подключение не найдено")
            stored_password = password if password else existing.password
            conn.execute(
                """
                UPDATE db_profiles
                SET name = ?, host = ?, port = ?, dbname = ?, pg_user = ?, password = ?,
                    sslmode = ?, connect_timeout = ?, include_in_timeline = ?,
                    updated_at = datetime('now')
                WHERE id = ?
                """,
                (
                    clean_name,
                    host.strip(),
                    port,
                    dbname.strip(),
                    user.strip(),
                    stored_password,
                    (sslmode or "").strip() or None,
                    connect_timeout,
                    1 if include_in_timeline else 0,
                    profile_id,
                ),
            )
            new_id = profile_id

        if make_active:
            conn.execute("UPDATE db_profiles SET is_active = 0")
            conn.execute(
                "UPDATE db_profiles SET is_active = 1 WHERE id = ?", (new_id,)
            )
        conn.commit()

    profile = get_profile(new_id)
    if profile is None:
        raise RuntimeError("Failed to load saved profile")
    return profile


def delete_profile(profile_id: int) -> None:
    init_db()
    with _connect_sqlite() as conn:
        was_active = conn.execute(
            "SELECT is_active FROM db_profiles WHERE id = ?", (profile_id,)
        ).fetchone()
        conn.execute("DELETE FROM db_profiles WHERE id = ?", (profile_id,))
        if was_active and was_active["is_active"]:
            fallback = conn.execute(
                "SELECT id FROM db_profiles ORDER BY id LIMIT 1"
            ).fetchone()
            if fallback:
                conn.execute(
                    "UPDATE db_profiles SET is_active = 1 WHERE id = ?",
                    (fallback["id"],),
                )
        conn.commit()


def test_connection(profile: DbProfile) -> tuple[bool, str]:
    try:
        with psycopg.connect(**profile.connection_kwargs()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True, "Подключение успешно"
    except Exception as exc:
        return False, str(exc)


def import_from_env_if_empty() -> DbProfile | None:
    """Создаёт профиль из .env, если SQLite ещё пуст."""
    init_db()
    if list_profiles():
        return get_active_profile()

    load_dotenv()
    host = os.getenv("PGHOST")
    dbname = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    if not all([host, dbname, user, password]):
        return None

    return save_profile(
        profile_id=None,
        name=os.getenv("PGPROFILE_NAME", dbname) or dbname,
        host=host,
        port=int(os.getenv("PGPORT", "5432")),
        dbname=dbname,
        user=user,
        password=password,
        sslmode=os.getenv("PGSSLMODE"),
        connect_timeout=int(os.getenv("PGCONNECT_TIMEOUT", "5")),
        include_in_timeline=True,
        make_active=True,
    )


def profiles_for_timeline() -> list[dict[str, Any]]:
    """Конфиги для fetch_function_timeline (поле name + psycopg kwargs)."""
    profiles = [p for p in list_profiles() if p.include_in_timeline]
    if not profiles:
        active = get_active_profile()
        if active:
            profiles = [active]
    result: list[dict[str, Any]] = []
    for profile in profiles:
        cfg = profile.connection_kwargs()
        cfg["name"] = profile.name
        result.append(cfg)
    return result

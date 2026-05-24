import sqlite3
import os
from werkzeug.security import generate_password_hash, check_password_hash


def get_db_path():
    return os.getenv("DATABASE_PATH", "./data/agente.db")


def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    os.makedirs(os.path.dirname(os.path.abspath(get_db_path())), exist_ok=True)
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                security_key_hash TEXT NOT NULL,
                name TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'aberto',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS ticket_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id INTEGER NOT NULL,
                sender TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (ticket_id) REFERENCES tickets(id)
            );

            CREATE TABLE IF NOT EXISTS sessions (
                phone TEXT PRIMARY KEY,
                user_id INTEGER,
                state TEXT NOT NULL DEFAULT 'AWAIT_IS_CLIENT',
                data TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT DEFAULT (datetime('now'))
            );
        """)


# --- User ---

def create_user(phone, email, security_key, name):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO users (phone, email, security_key_hash, name) VALUES (?, ?, ?, ?)",
            (phone, email.lower().strip(), generate_password_hash(security_key), name),
        )
        return conn.execute("SELECT * FROM users WHERE phone = ?", (phone,)).fetchone()


def get_user_by_phone(phone):
    with get_conn() as conn:
        return conn.execute("SELECT * FROM users WHERE phone = ?", (phone,)).fetchone()


def get_user_by_email(email):
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM users WHERE email = ?", (email.lower().strip(),)
        ).fetchone()


def verify_security_key(user_row, key):
    return check_password_hash(user_row["security_key_hash"], key)


def email_exists(email):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM users WHERE email = ?", (email.lower().strip(),)
        ).fetchone()
        return row is not None


# --- Session ---

def get_session(phone):
    import json
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM sessions WHERE phone = ?", (phone,)).fetchone()
        if row is None:
            # First contact — ask if they're already a client
            return {"state": "AWAIT_IS_CLIENT", "user_id": None, "data": {}}
        return {
            "state": row["state"],
            "user_id": row["user_id"],
            "data": json.loads(row["data"]),
        }


def save_session(phone, state, user_id=None, data=None):
    import json
    data_str = json.dumps(data or {})
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO sessions (phone, user_id, state, data, updated_at)
               VALUES (?, ?, ?, ?, datetime('now'))
               ON CONFLICT(phone) DO UPDATE SET
                   user_id=excluded.user_id,
                   state=excluded.state,
                   data=excluded.data,
                   updated_at=excluded.updated_at""",
            (phone, user_id, state, data_str),
        )


# --- Tickets ---

def create_ticket(user_id, title, description):
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO tickets (user_id, title, description) VALUES (?, ?, ?)",
            (user_id, title, description),
        )
        ticket_id = cur.lastrowid
        conn.execute(
            "INSERT INTO ticket_messages (ticket_id, sender, message) VALUES (?, 'cliente', ?)",
            (ticket_id, description),
        )
        return conn.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id,)).fetchone()


def get_tickets_by_user(user_id):
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM tickets WHERE user_id = ? ORDER BY updated_at DESC",
            (user_id,),
        ).fetchall()


def get_ticket(ticket_id):
    with get_conn() as conn:
        return conn.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id,)).fetchone()


def get_ticket_messages(ticket_id):
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM ticket_messages WHERE ticket_id = ? ORDER BY created_at ASC",
            (ticket_id,),
        ).fetchall()


def add_ticket_message(ticket_id, sender, message):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO ticket_messages (ticket_id, sender, message) VALUES (?, ?, ?)",
            (ticket_id, sender, message),
        )
        conn.execute(
            "UPDATE tickets SET updated_at = datetime('now') WHERE id = ?",
            (ticket_id,),
        )


def update_ticket_status(ticket_id, status):
    with get_conn() as conn:
        conn.execute(
            "UPDATE tickets SET status = ?, updated_at = datetime('now') WHERE id = ?",
            (status, ticket_id),
        )

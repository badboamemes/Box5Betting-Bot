import os
import time
import random
import sqlite3
import re
import asyncio
import threading
import math
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Optional, Tuple, List, Set, Literal
from collections import Counter
from dataclasses import dataclass

import discord
from discord.ext import commands

# ------
# Global Constants
# -------
# Chance to reply at all when someone replies to the bot:
# 1 = always, 2 = 50%, 5 = 20%, etc.
TRIGGER_REPLY_CHANCE_DENOM = 1
BOT_REPLY_COOLDOWN_SECONDS = 3
Mode = Literal["text", "image_only", "text_then_image"]

@dataclass(frozen=True)
class ReplyVariant:
    mode: Mode
    text: str = ""
    weight: int = 1
    image_url: Optional[str] = None
    image_chance_denom: int = 0  # 0=never, N=1 in N, 1=always

REPLY_VARIANTS: List[ReplyVariant] = [
    ReplyVariant(
        mode="image_only",
        weight=10,
        image_url="https://cdn.discordapp.com/attachments/609223233857912862/1452791659921412390/5252345923456.jpg?ex=694b192b&is=6949c7ab&hm=2a3cad1cb7647306b3f7a7d761b2fa75a4c1ab212801bc2c4775ea8ef63a695f&",
        image_chance_denom=1,  # 1 in 19 for this variant
    ),
    ReplyVariant(
        mode="text_then_image",
        weight=10,
        text="**Glory to Benjamin Netanyahu! I LOVE BOOM!**",
        image_url="https://cdn.discordapp.com/attachments/926219336014762015/1460511755481645253/israe-flag-960_720.jpg?ex=69672f11&is=6965dd91&hm=324cfae9622e293846ed908e976c3c30122c0b104d9247e298a09b401ba6a078&",
        image_chance_denom=1,  # 1 in 19 for this variant
    ),
    ReplyVariant(
        mode="text",
        text="**Finger Cav? Never heard of her.**", weight=10),
    ReplyVariant(mode="text", text="**#FTD.**", weight=10),
    ReplyVariant(mode="text", text="**Crown 2022 > BD 14**", weight=10, image_url=None, image_chance_denom=0),
    ReplyVariant(mode="text", text="**J. Webb is worse than Hitler.**", weight=10),
    ReplyVariant(mode="text", text="**I'm 5'1 414lb (muscle).**", weight=10),
    ReplyVariant(mode="image_only",
                weight=10,
                image_url="https://cdn.discordapp.com/attachments/767551545666961418/1459445420412637215/fade.gif?ex=696699b7&is=69654837&hm=fd0e7c91feceaec466034eec21fb5ade4ed87c5ebe4d77e5e70b0d0fd8969e21&",
    ),
    ReplyVariant(
        mode="image_only",
        weight=10,
        image_url="https://cdn.discordapp.com/attachments/1259029551820963844/1393483665199071412/caption.gif?ex=6966930b&is=6965418b&hm=2f9a2698505aa31667d4127b31c2fc48587b09192e3c5b46058236f8d6360814&",
        image_chance_denom=1,  
    ),
    ReplyVariant(mode="text", text="**You are a gorilla.**", weight=10),

]

_last_reply_to_bot = {}  


def pick_variant() -> ReplyVariant:
    # Weighted selection among variants
    weights = [max(0, v.weight) for v in REPLY_VARIANTS]
    # Fallback if someone accidentally set all weights to 0
    if sum(weights) <= 0:
        return REPLY_VARIANTS[0]
    return random.choices(REPLY_VARIANTS, weights=weights, k=1)[0]    

_last_reply_to_bot: Dict[int, float] = {}

# ------------------
# Configuration
# ------------------
TOKEN = "TOKEN_HERE"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "Box5Betting2026SEASON.db")
print(f"[DB] Using database file: {DB_PATH}")

PREFIX = "!"
START_BALANCE = 1000

DAILY_CREDITS = 750
DAILY_SECONDS = 24 * 60 * 60

# -----------------------------
# Parole Configuration
# -----------------------------
PAROLE_TIME_SECONDS = 60 * 60         # 1 hour
PAROLE_PAY_INTERVAL_SECONDS = 10 * 60 # every 10 minutes
PAROLE_RATE = 0.06                    # 6%
PAROLE_CHECK_SECONDS = 60             # check every minute



# -----------------------------
# Powerball Lottery Configuration
# -----------------------------
LOTTERY_TICKET_COST = 5000
LOTTERY_MAIN_MIN = 1
LOTTERY_MAIN_MAX = 6
LOTTERY_PB_MIN = 1
LOTTERY_PB_MAX = 4

LOTTERY_ANIM_TICKS_PER_BALL = 4
LOTTERY_ANIM_DELAY = 0.2

WIN_MULTIPLIERS: Dict[int, int] = {
    67: 23,
    41: 14,
    69: 9,
    21: 10,
    99: 3,
    0: 2,
    6: 6,
    7: 7,
    55: 3,
    88: 2,
    11: 2,
    33: 3,
    77: 2,
    66: 3,
    44: 3,
    22: 4,
}

REEL_MIN = 0
REEL_MAX = 99

TOP_N = 15

# Jackpot tax configuration
JACKPOT_TAX_RATE = 0.40            # 40% tax on jackpot payouts only
JACKPOT_TAX_REBATE_RATE = 0.30     # 30% of that tax goes back into lottery pool


# Owner-only command access
OWNER_ID = discord_id_here  # Replace with your Discord user ID

# -----------------------------
# Plinko Configuration
# -----------------------------
PLINKO_ROWS = 5
PLINKO_MULTIPLIERS: List[float] = [8.0, 2.2, 0.7, 0.5, 0.1, 0.5, 0.7, 2.2, 8.0]
PLINKO_ANIM_DELAY = 0.2
MAX_PLINKO_BALLS = 25

# -----------------------------
# Tax Configuration
# -----------------------------
TAX_TIMEZONE = "America/Chicago"
TAX_CHECK_SECONDS = 300  # check every 5 minutes
TAX_WEEKDAYS = {1, 2, 3, 4, 6}  # Wed=2, Thu=3, Fri=4, Sun=6

# -----------------------------
# Crypto Market Configuration (V2)
# -----------------------------
CRYPTO_V2_FEE = 0.007          # 0.7% swap fee
CRYPTO_V2_TICK_SECONDS = 20   # background tick cadence

CRYPTO_V2_DEFAULTS = [
    ("BLOO", "Bloo Coin",        1000.0,   10_000_000.0),
    ("BDC",  "Devil Coin",       1000.0,   20_000_000.0),
    ("BAC",  "Crusader Coin",    1000.0,   30_000_000.0),
    ("CAR",  "Crown Coin",       1000.0,   40_000_000.0),
    ("SCVC", "SCV Coin",         1000.0,   50_000_000.0),
    ("SUTA", "Phantom Coin",     1000.0,   32_000_000.0),
    ("HOP",  "Cadet Coin",       1000.0,   70_000_000.0),
]

# ============================================================
# Formatting helpers
# ============================================================

def fmt_money(n: int) -> str:
    """
    Compact formatting ONLY starts at 100,000.

      99,999  -> 99,999
      100,000 -> 100 K
      345,678 -> 345.68 K
      1,234,567 -> 1.23 M
      1,345,678,546 -> 1.35 B
    """
    try:
        n_int = int(n)
    except Exception:
        return str(n)

    sign = "-" if n_int < 0 else ""
    x = abs(n_int)

    if x < 100_000:
        return f"{n_int:,}"

    units = [
        ("K", 1_000),
        ("M", 1_000_000),
        ("B", 1_000_000_000),
        ("T", 1_000_000_000_000),
        ("Q", 1_000_000_000_000_000),
    ]

    suffix = "Q"
    factor = 1_000_000_000_000_000
    for s, f in units:
        if x < f * 1000:
            suffix = s
            factor = f
            break

    val = x / factor
    s = f"{val:.2f}".rstrip("0").rstrip(".")
    return f"{sign}{s} {suffix}"

def fmt_coin(x: float, decimals: int = 3) -> str:
    try:
        v = float(x)
    except Exception:
        return str(x)
    d = max(0, min(int(decimals), 12))
    s = f"{v:,.{d}f}".rstrip("0").rstrip(".")
    return s

def fmt_crypto_money(x: float, decimals: int = 3) -> str:
    try:
        v = float(x)
    except Exception:
        return str(x)

    if not math.isfinite(v):
        return "N/A"

    sign = "-" if v < 0 else ""
    v = abs(v)

    if v < 100_000:
        d = max(0, min(int(decimals), 12))
        s = f"{v:,.{d}f}".rstrip("0").rstrip(".")
        return f"{sign}{s}"

    thresholds = [
        ("K", 1_000.0,                 1_000_000.0),
        ("M", 1_000_000.0,             1_000_000_000.0),
        ("B", 1_000_000_000.0,         1_000_000_000_000.0),
        ("T", 1_000_000_000_000.0,     1_000_000_000_000_000.0),
        ("Q", 1_000_000_000_000_000.0, float("inf")),
    ]

    for suffix, factor, upper in thresholds:
        if v < upper:
            val = v / factor
            d = max(0, min(int(decimals), 6))
            s = f"{val:.{d}f}".rstrip("0").rstrip(".")
            return f"{sign}{s} {suffix}"

    return f"{sign}{v}"

_AMOUNT_RE = re.compile(r"^\s*([+-]?\d+(?:\.\d+)?)\s*([a-zA-Z]{0,4})\s*$")
_SUFFIX_MULT = {
    "": 1,
    "K": 1_000,
    "M": 1_000_000,
    "B": 1_000_000_000,
    "T": 1_000_000_000_000,
    "Q": 1_000_000_000_000_000,
    "THOU": 1_000,
    "MIL": 1_000_000,
    "MILL": 1_000_000,
    "BIL": 1_000_000_000,
    "TRIL": 1_000_000_000_000,
    "QUAD": 1_000_000_000_000_000,
    "MM": 1_000_000,
}

def parse_amount_int(s: str, *, min_value: int = 1, max_value: Optional[int] = None) -> int:
    if s is None:
        raise ValueError("missing amount")
    raw = str(s).strip().replace(",", "").replace("_", "")
    m = _AMOUNT_RE.match(raw)
    if not m:
        raise ValueError("invalid amount format")
    num_s, suf = m.group(1), (m.group(2) or "").upper()
    if suf.endswith("S"):
        suf = suf[:-1]
    if suf not in _SUFFIX_MULT:
        raise ValueError(f"unknown suffix '{suf}'")
    n = float(num_s) * float(_SUFFIX_MULT[suf])
    val = int(n)  # trunc toward zero
    if val < min_value:
        raise ValueError("too small")
    if max_value is not None and val > int(max_value):
        raise ValueError("too large")
    return val

def parse_time_window(window: str) -> int:
    """
    Parses: "30m", "2h", "24h", "7d", "1w"
    Returns seconds. Defaults to 24h if unknown.
    """
    w = (window or "").strip().lower()
    if not w:
        return 24 * 60 * 60

    if w.endswith("w"):
        try:
            n = float(w[:-1].strip())
            return int(n * 7 * 24 * 60 * 60)
        except Exception:
            return 7 * 24 * 60 * 60

    if w.endswith("m"):
        try:
            n = float(w[:-1].strip())
            return int(n * 60)
        except Exception:
            return 30 * 60

    if w.endswith("h"):
        try:
            n = float(w[:-1].strip())
            return int(n * 60 * 60)
        except Exception:
            return 24 * 60 * 60

    if w.endswith("d"):
        try:
            n = float(w[:-1].strip())
            return int(n * 24 * 60 * 60)
        except Exception:
            return 7 * 24 * 60 * 60

    try:
        n = float(w)
        return int(n * 60 * 60)
    except Exception:
        return 24 * 60 * 60

# ============================================================
# Database (SQLite)
# ============================================================

db_lock = threading.Lock()
_db_printed = False

def db_connect():
    global _db_printed
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    if not _db_printed:
        _db_printed = True
        try:
            actual = conn.execute("PRAGMA database_list;").fetchall()
            print(f"[DB] CWD={os.getcwd()}")
            print(f"[DB] DB_PATH={DB_PATH}")
            print(f"[DB] SQLite database_list={actual}")
        except Exception as e:
            print(f"[DB] PRAGMA database_list failed: {e}")
    return conn

def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    return column in cols

def init_crypto_v2_schema_and_seed(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto_v2_markets (
            symbol TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            reserve_money REAL NOT NULL,
            reserve_coin REAL NOT NULL,
            fee REAL NOT NULL,
            created_ts INTEGER NOT NULL,
            last_price REAL NOT NULL,
            last_tick_ts INTEGER NOT NULL,
            day_open_price REAL NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto_v2_holdings (
            user_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            coins REAL NOT NULL,
            PRIMARY KEY (user_id, symbol),
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
            FOREIGN KEY (symbol) REFERENCES crypto_v2_markets(symbol) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto_v2_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            kind TEXT NOT NULL,
            pct REAL NOT NULL,
            note TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto_v2_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            price REAL NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crypto_v2_prices_symbol_ts ON crypto_v2_prices(symbol, ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crypto_v2_events_ts ON crypto_v2_events(ts)")

    now = int(time.time())
    for sym, name, start_price, liquidity_money in CRYPTO_V2_DEFAULTS:
        sym_u = str(sym).strip().upper()
        sp = float(start_price) if float(start_price) > 0 else 1.0
        lm = float(liquidity_money) if float(liquidity_money) > 0 else 1000.0

        reserve_money = lm
        reserve_coin = lm / sp
        last_price = sp

        conn.execute(
            """
            INSERT INTO crypto_v2_markets
              (symbol, name, reserve_money, reserve_coin, fee, created_ts, last_price, last_tick_ts, day_open_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO NOTHING
            """,
            (sym_u, str(name), float(reserve_money), float(reserve_coin), float(CRYPTO_V2_FEE), now, float(last_price), now, float(last_price)),
        )

    conn.commit()

def init_db():
    with db_lock:
        conn = db_connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT NOT NULL,
                    balance INTEGER NOT NULL,
                    last_daily INTEGER NOT NULL,
                    jailed INTEGER NOT NULL DEFAULT 0,
                    jail_ts INTEGER NOT NULL DEFAULT 0,
                    paroled INTEGER NOT NULL DEFAULT 0,
                    parole_ts INTEGER NOT NULL DEFAULT 0,
                    parole_last_pay_ts INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            # Backfill if older schema
            if not _column_exists(conn, "users", "jailed"):
                conn.execute("ALTER TABLE users ADD COLUMN jailed INTEGER NOT NULL DEFAULT 0")
            if not _column_exists(conn, "users", "jail_ts"):
                conn.execute("ALTER TABLE users ADD COLUMN jail_ts INTEGER NOT NULL DEFAULT 0")
            if not _column_exists(conn, "users", "paroled"):
                conn.execute("ALTER TABLE users ADD COLUMN paroled INTEGER NOT NULL DEFAULT 0")
            if not _column_exists(conn, "users", "parole_ts"):
                conn.execute("ALTER TABLE users ADD COLUMN parole_ts INTEGER NOT NULL DEFAULT 0")
            if not _column_exists(conn, "users", "parole_last_pay_ts"):
                conn.execute("ALTER TABLE users ADD COLUMN parole_last_pay_ts INTEGER NOT NULL DEFAULT 0")


            # Betting tables
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bets (
                    bet_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER,
                    channel_id INTEGER,
                    creator_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    status TEXT NOT NULL,              -- open|closed|resolved|canceled
                    created_ts INTEGER NOT NULL,
                    closed_ts INTEGER,
                    resolved_ts INTEGER,
                    winning_option INTEGER,
                    bonus_pool INTEGER NOT NULL DEFAULT 0,
                    note TEXT
                )
                """
            )
            if not _column_exists(conn, "bets", "bonus_pool"):
                conn.execute("ALTER TABLE bets ADD COLUMN bonus_pool INTEGER NOT NULL DEFAULT 0")

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bet_options (
                    bet_id INTEGER NOT NULL,
                    option_num INTEGER NOT NULL,
                    label TEXT NOT NULL,
                    PRIMARY KEY (bet_id, option_num),
                    FOREIGN KEY (bet_id) REFERENCES bets(bet_id) ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bet_wagers (
                    bet_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    option_num INTEGER NOT NULL,
                    amount INTEGER NOT NULL,
                    placed_ts INTEGER NOT NULL,
                    PRIMARY KEY (bet_id, user_id, option_num),
                    FOREIGN KEY (bet_id, option_num) REFERENCES bet_options(bet_id, option_num) ON DELETE CASCADE
                )
                """
            )
            try:
                conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_bet_one_option_per_user ON bet_wagers (bet_id, user_id)")
            except sqlite3.IntegrityError:
                pass

            # System state + tax events
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS system_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tax_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER NOT NULL,
                    date_key TEXT NOT NULL,
                    users_taxed INTEGER NOT NULL,
                    total_tax INTEGER NOT NULL
                )
                """
            )

            # Lottery tickets
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lottery_tickets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    n1 INTEGER NOT NULL,
                    n2 INTEGER NOT NULL,
                    n3 INTEGER NOT NULL,
                    n4 INTEGER NOT NULL,
                    n5 INTEGER NOT NULL,
                    pb INTEGER NOT NULL,
                    bought_ts INTEGER NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_lottery_tickets_user_id ON lottery_tickets (user_id)")

            # Crypto V2 schema and seed
            init_crypto_v2_schema_and_seed(conn)

            conn.commit()
        finally:
            conn.close()

# ====================================---------
# User functions
# =====================================---

def get_user(user_id: int) -> Optional[Tuple[int, str, int, int]]:
    with db_lock:
        conn = db_connect()
        try:
            cur = conn.execute(
                "SELECT user_id, username, balance, last_daily FROM users WHERE user_id = ?",
                (int(user_id),),
            )
            return cur.fetchone()
        finally:
            conn.close()

def insert_user(user_id: int, username: str):
    now = int(time.time())
    with db_lock:
        conn = db_connect()
        try:
            conn.execute(
                "INSERT INTO users (user_id, username, balance, last_daily) VALUES (?, ?, ?, ?)",
                (int(user_id), str(username), int(START_BALANCE), now),
            )
            conn.commit()
        finally:
            conn.close()

def update_username(user_id: int, username: str):
    with db_lock:
        conn = db_connect()
        try:
            conn.execute("UPDATE users SET username = ? WHERE user_id = ?", (str(username), int(user_id)))
            conn.commit()
        finally:
            conn.close()

def set_balance(user_id: int, balance: int):
    with db_lock:
        conn = db_connect()
        try:
            conn.execute("UPDATE users SET balance = ? WHERE user_id = ?", (int(balance), int(user_id)))
            conn.commit()
        finally:
            conn.close()

def add_balance(user_id: int, amount: int) -> None:
    with db_lock:
        conn = db_connect()
        try:
            conn.execute(
                "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                (int(amount), int(user_id)),
            )
            conn.commit()
        finally:
            conn.close()

def apply_daily_if_due(user_id: int) -> int:
    row = get_user(user_id)
    if row is None:
        return 0
    _, _, balance, last_daily = row
    now = int(time.time())
    if now - int(last_daily) >= DAILY_SECONDS:
        new_balance = int(balance) + int(DAILY_CREDITS)
        with db_lock:
            conn = db_connect()
            try:
                conn.execute(
                    "UPDATE users SET balance = ?, last_daily = ? WHERE user_id = ?",
                    (int(new_balance), now, int(user_id)),
                )
                conn.commit()
            finally:
                conn.close()
        return int(DAILY_CREDITS)
    return 0

def get_top_users(limit: int = TOP_N) -> List[Tuple[str, int]]:
    with db_lock:
        conn = db_connect()
        try:
            cur = conn.execute(
                "SELECT username, balance FROM users ORDER BY balance DESC, user_id ASC LIMIT ?",
                (int(limit),),
            )
            return cur.fetchall()
        finally:
            conn.close()

def get_all_activated_user_ids() -> Set[int]:
    with db_lock:
        conn = db_connect()
        try:
            cur = conn.execute("SELECT user_id FROM users")
            return {int(r[0]) for r in cur.fetchall()}
        finally:
            conn.close()

def chunk_mentions(user_ids: List[int], max_len: int = 1900) -> List[str]:
    chunks: List[str] = []
    current = ""
    for uid in user_ids:
        mention = f"<@{uid}> "
        if len(current) + len(mention) > max_len:
            chunks.append(current.strip())
            current = mention
        else:
            current += mention
    if current.strip():
        chunks.append(current.strip())
    return chunks

# =======================
# Betting helpers/definitions
# ========================

def create_bet(guild_id: Optional[int], channel_id: Optional[int], creator_id: int, title: str, options: List[str]) -> int:
    now = int(time.time())
    with db_lock:
        conn = db_connect()
        try:
            cur = conn.execute(
                """
                INSERT INTO bets (guild_id, channel_id, creator_id, title, status, created_ts, bonus_pool)
                VALUES (?, ?, ?, ?, 'open', ?, 0)
                """,
                (guild_id, channel_id, int(creator_id), str(title), now),
            )
            bet_id = int(cur.lastrowid)
            for i, label in enumerate(options, start=1):
                conn.execute(
                    "INSERT INTO bet_options (bet_id, option_num, label) VALUES (?, ?, ?)",
                    (bet_id, int(i), str(label)),
                )
            conn.commit()
            return bet_id
        finally:
            conn.close()

def get_bet(bet_id: int) -> Optional[Tuple]:
    with db_lock:
        conn = db_connect()
        try:
            cur = conn.execute(
                """
                SELECT bet_id, title, status, creator_id, created_ts, closed_ts, resolved_ts,
                       winning_option, bonus_pool, note
                FROM bets
                WHERE bet_id = ?
                """,
                (int(bet_id),),
            )
            return cur.fetchone()
        finally:
            conn.close()

def get_bet_options(bet_id: int) -> List[Tuple[int, str]]:
    with db_lock:
        conn = db_connect()
        try:
            cur = conn.execute(
                "SELECT option_num, label FROM bet_options WHERE bet_id = ? ORDER BY option_num ASC",
                (int(bet_id),),
            )
            return [(int(r[0]), str(r[1])) for r in cur.fetchall()]
        finally:
            conn.close()

def get_bet_totals(bet_id: int) -> Tuple[int, Dict[int, int]]:
    with db_lock:
        conn = db_connect()
        try:
            cur = conn.execute(
                "SELECT option_num, COALESCE(SUM(amount), 0) FROM bet_wagers WHERE bet_id = ? GROUP BY option_num",
                (int(bet_id),),
            )
            rows = cur.fetchall()
        finally:
            conn.close()
    totals = {int(opt): int(s) for opt, s in rows}
    total_pool = sum(totals.values())
    return total_pool, totals

def list_open_bets(limit: int = 10) -> List[Tuple[int, str, int]]:
    with db_lock:
        conn = db_connect()
        try:
            cur = conn.execute(
                "SELECT bet_id, title, created_ts FROM bets WHERE status = 'open' ORDER BY created_ts DESC LIMIT ?",
                (int(limit),),
            )
            return [(int(r[0]), str(r[1]), int(r[2])) for r in cur.fetchall()]
        finally:
            conn.close()

def american_odds_str(total_pool: int, opt_pool: int) -> str:
    if total_pool <= 0 or opt_pool <= 0:
        return "N/A"
    profit = (total_pool / opt_pool) - 1.0
    if profit <= 0:
        return "N/A"
    if profit >= 1.0:
        odds = int(round(100 * profit))
        odds = min(odds, 9999)
        return f"+{odds}"
    odds = int(round(100 / profit))
    odds = min(odds, 9999)
    return f"-{odds}"

def get_user_wager_for_bet(bet_id: int, user_id: int) -> Optional[Tuple[int, int]]:
    with db_lock:
        conn = db_connect()
        try:
            row = conn.execute(
                "SELECT option_num, amount FROM bet_wagers WHERE bet_id = ? AND user_id = ? LIMIT 1",
                (int(bet_id), int(user_id)),
            ).fetchone()
            if row is None:
                return None
            return int(row[0]), int(row[1])
        finally:
            conn.close()

def place_wager(bet_id: int, user_id: int, option_num: int, amount: int) -> Tuple[bool, str]:
    if amount <= 0:
        return False, "Amount must be a positive whole number."

    bet = get_bet(bet_id)
    if bet is None:
        return False, "That bet_id does not exist."
    _, _title, status, _creator, *_rest = bet
    if status != "open":
        return False, f"That bet is not open (status: {status})."

    options = dict(get_bet_options(bet_id))
    if int(option_num) not in options:
        return False, "Invalid option number for this bet."

    row = get_user(user_id)
    if row is None:
        return False, f"You are not activated yet. Run `{PREFIX}activate` first."

    bal = int(row[2])
    if amount > bal:
        return False, f"You only have **{fmt_money(bal)}** Marcus Money. Your wager (**{fmt_money(amount)}**) is too large."

    now = int(time.time())

    with db_lock:
        conn = db_connect()
        try:
            existing_choice = conn.execute(
                "SELECT option_num FROM bet_wagers WHERE bet_id = ? AND user_id = ? LIMIT 1",
                (int(bet_id), int(user_id)),
            ).fetchone()

            if existing_choice is not None:
                chosen_opt = int(existing_choice[0])
                if chosen_opt != int(option_num):
                    return (
                        False,
                        f"You already placed a wager on **Option {chosen_opt}** for this bet. "
                        f"You can only add more to that same option (not switch).",
                    )

            conn.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (int(amount), int(user_id)))

            existing = conn.execute(
                "SELECT amount FROM bet_wagers WHERE bet_id = ? AND user_id = ? AND option_num = ?",
                (int(bet_id), int(user_id), int(option_num)),
            ).fetchone()

            if existing is None:
                conn.execute(
                    "INSERT INTO bet_wagers (bet_id, user_id, option_num, amount, placed_ts) VALUES (?, ?, ?, ?, ?)",
                    (int(bet_id), int(user_id), int(option_num), int(amount), now),
                )
            else:
                conn.execute(
                    "UPDATE bet_wagers SET amount = amount + ?, placed_ts = ? WHERE bet_id = ? AND user_id = ? AND option_num = ?",
                    (int(amount), now, int(bet_id), int(user_id), int(option_num)),
                )

            conn.commit()
        finally:
            conn.close()

    return True, f"Wager placed: **{fmt_money(amount)}** on **Option {int(option_num)}** ({options[int(option_num)]})."

def close_bet(bet_id: int) -> Tuple[bool, str]:
    bet = get_bet(bet_id)
    if bet is None:
        return False, "That bet_id does not exist."
    _, _, status, *_rest = bet
    if status != "open":
        return False, f"Bet is not open (status: {status})."
    now = int(time.time())
    with db_lock:
        conn = db_connect()
        try:
            conn.execute("UPDATE bets SET status = 'closed', closed_ts = ? WHERE bet_id = ?", (now, int(bet_id)))
            conn.commit()
        finally:
            conn.close()
    return True, "Bet closed."

def add_bet_bonus_pool(bet_id: int, amount: int) -> Tuple[bool, str]:
    if amount <= 0:
        return False, "Amount must be a positive whole number."
    bet = get_bet(bet_id)
    if bet is None:
        return False, "That bet_id does not exist."
    _id, title, status, *_rest = bet
    if status != "open":
        return False, f"Bonus pool can only be added while the bet is **open** (status: {status})."
    with db_lock:
        conn = db_connect()
        try:
            conn.execute("UPDATE bets SET bonus_pool = bonus_pool + ? WHERE bet_id = ?", (int(amount), int(bet_id)))
            conn.commit()
        finally:
            conn.close()
    return True, f"Added **{fmt_money(amount)}** to the house bonus pool for bet **#{bet_id}** ({title})."

def cancel_bet_and_refund(bet_id: int, note: str = "Canceled and refunded.") -> Tuple[bool, str]:
    bet = get_bet(bet_id)
    if bet is None:
        return False, "That bet_id does not exist."
    _, _, status, *_rest = bet
    if status in ("resolved", "canceled"):
        return False, f"Bet already {status}."
    with db_lock:
        conn = db_connect()
        try:
            cur = conn.execute(
                "SELECT user_id, COALESCE(SUM(amount), 0) FROM bet_wagers WHERE bet_id = ? GROUP BY user_id",
                (int(bet_id),),
            )
            refunds = [(int(uid), int(a)) for uid, a in cur.fetchall()]
            for uid, amt in refunds:
                conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (int(amt), int(uid)))

            now = int(time.time())
            conn.execute(
                "UPDATE bets SET status = 'canceled', resolved_ts = ?, note = ? WHERE bet_id = ?",
                (now, str(note), int(bet_id)),
            )
            conn.commit()
        finally:
            conn.close()
    return True, note

def resolve_bet_and_payout(bet_id: int, winning_option: int) -> Tuple[bool, str]:
    bet = get_bet(bet_id)
    if bet is None:
        return False, "That bet_id does not exist."
    _id, title, status, *_rest = bet
    bonus_pool = int(bet[8])
    if status not in ("open", "closed"):
        return False, f"Bet cannot be resolved (status: {status})."

    options = dict(get_bet_options(bet_id))
    if int(winning_option) not in options:
        return False, "Invalid winning option for this bet."

    with db_lock:
        conn = db_connect()
        try:
            cur = conn.execute("SELECT user_id, option_num, amount FROM bet_wagers WHERE bet_id = ?", (int(bet_id),))
            wagers = [(int(uid), int(opt), int(amt)) for uid, opt, amt in cur.fetchall()]
            if not wagers:
                now = int(time.time())
                conn.execute(
                    "UPDATE bets SET status = 'resolved', resolved_ts = ?, winning_option = ?, note = ? WHERE bet_id = ?",
                    (now, int(winning_option), "No wagers were placed.", int(bet_id)),
                )
                conn.commit()
                return True, "Resolved: no wagers were placed."

            total_pool = sum(amt for _, _, amt in wagers)
            winners = [(uid, amt) for uid, opt, amt in wagers if opt == int(winning_option)]
            win_pool = sum(amt for _, amt in winners)
            losing_pool = total_pool - win_pool
            now = int(time.time())

            if win_pool == 0:
                cur2 = conn.execute(
                    "SELECT user_id, COALESCE(SUM(amount), 0) FROM bet_wagers WHERE bet_id = ? GROUP BY user_id",
                    (int(bet_id),),
                )
                refunds = [(int(uid), int(a)) for uid, a in cur2.fetchall()]
                for uid, amt in refunds:
                    conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (int(amt), int(uid)))

                conn.execute(
                    "UPDATE bets SET status = 'resolved', resolved_ts = ?, winning_option = ?, note = ? WHERE bet_id = ?",
                    (now, int(winning_option), "No winners. All wagers refunded. Bonus pool not paid out.", int(bet_id)),
                )
                conn.commit()
                return True, "Resolved: no winners. All wagers refunded. Bonus pool not paid out."

            extra_pool = int(losing_pool) + int(bonus_pool)

            winner_rows = []
            for uid, stake in winners:
                raw = (extra_pool * stake) / win_pool
                base = int(raw)
                frac = raw - base
                winner_rows.append([uid, stake, base, frac])

            base_total = sum(r[2] for r in winner_rows)
            remainder = extra_pool - base_total

            winner_rows.sort(key=lambda r: (-r[3], r[0]))
            for i in range(int(remainder)):
                winner_rows[i % len(winner_rows)][2] += 1

            for uid, stake, share, _frac in winner_rows:
                payout = int(stake) + int(share)
                conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (int(payout), int(uid)))

            note = f"Resolved: {title}. Bonus pool used: {bonus_pool}."
            conn.execute(
                "UPDATE bets SET status = 'resolved', resolved_ts = ?, winning_option = ?, note = ? WHERE bet_id = ?",
                (now, int(winning_option), str(note), int(bet_id)),
            )
            if status == "open":
                conn.execute("UPDATE bets SET closed_ts = ? WHERE bet_id = ?", (now, int(bet_id)))

            conn.commit()
        finally:
            conn.close()

    return True, (
        f"Resolved bet **#{bet_id}**: winner is **Option {winning_option}** ({options[int(winning_option)]}).\n"
        f"Bonus pool: **{fmt_money(bonus_pool)}**. Payouts distributed proportionally."
    )

# ========================
# Crypto V2 System
# ========================

def _v2_price(reserve_money: float, reserve_coin: float) -> float:
    if reserve_coin <= 0:
        return 0.0
    return float(reserve_money) / float(reserve_coin)

def v2_list_markets() -> List[Tuple[str, str, float, float, float, float]]:
    with db_lock:
        conn = db_connect()
        try:
            rows = conn.execute(
                "SELECT symbol, name, reserve_money, reserve_coin, day_open_price, fee FROM crypto_v2_markets ORDER BY symbol ASC"
            ).fetchall()
        finally:
            conn.close()

    out = []
    for sym, name, rm, rc, day_open, fee in rows:
        price = _v2_price(float(rm), float(rc))
        out.append((str(sym), str(name), float(price), float(day_open), float(rm), float(fee)))
    return out

def v2_get_market(sym: str) -> Optional[Tuple[str, str, float, float, float, int, float, float]]:
    sym = (sym or "").strip().upper()
    if not sym:
        return None
    with db_lock:
        conn = db_connect()
        try:
            row = conn.execute(
                """
                SELECT symbol, name, reserve_money, reserve_coin, last_price, last_tick_ts, day_open_price, fee
                FROM crypto_v2_markets
                WHERE symbol = ?
                """,
                (sym,),
            ).fetchone()
            return row
        finally:
            conn.close()

def _v2_set_holding_conn(conn: sqlite3.Connection, user_id: int, sym: str, coins: float) -> None:
    if coins < 0:
        coins = 0.0
    existing = conn.execute(
        "SELECT coins FROM crypto_v2_holdings WHERE user_id = ? AND symbol = ?",
        (int(user_id), str(sym)),
    ).fetchone()
    if existing is None:
        conn.execute(
            "INSERT INTO crypto_v2_holdings (user_id, symbol, coins) VALUES (?, ?, ?)",
            (int(user_id), str(sym), float(coins)),
        )
    else:
        conn.execute(
            "UPDATE crypto_v2_holdings SET coins = ? WHERE user_id = ? AND symbol = ?",
            (float(coins), int(user_id), str(sym)),
        )

def v2_get_holding(user_id: int, sym: str) -> float:
    sym = (sym or "").strip().upper()
    if not sym:
        return 0.0
    with db_lock:
        conn = db_connect()
        try:
            row = conn.execute(
                "SELECT coins FROM crypto_v2_holdings WHERE user_id = ? AND symbol = ?",
                (int(user_id), sym),
            ).fetchone()
            return float(row[0]) if row else 0.0
        finally:
            conn.close()

def v2_get_portfolio(user_id: int) -> List[Tuple[str, float]]:
    with db_lock:
        conn = db_connect()
        try:
            cur = conn.execute(
                "SELECT symbol, coins FROM crypto_v2_holdings WHERE user_id = ? ORDER BY symbol ASC",
                (int(user_id),),
            )
            return [(str(r[0]), float(r[1])) for r in cur.fetchall()]
        finally:
            conn.close()

def _v2_record_price_conn(conn: sqlite3.Connection, sym: str, ts: int, price: float) -> None:
    sym = (sym or "").strip().upper()
    if not sym:
        return
    try:
        ts_i = int(ts)
        price_f = float(price)
    except Exception:
        return
    if price_f <= 0:
        return
    conn.execute("INSERT INTO crypto_v2_prices (ts, symbol, price) VALUES (?, ?, ?)", (ts_i, sym, price_f))

def v2_get_price_series_since(sym: str, since_ts: int, limit: int = 5000) -> List[Tuple[int, float]]:
    sym = (sym or "").strip().upper()
    if not sym:
        return []
    limit = max(50, min(int(limit), 5000))
    with db_lock:
        conn = db_connect()
        try:
            cur = conn.execute(
                """
                SELECT ts, price
                FROM crypto_v2_prices
                WHERE symbol = ? AND ts >= ?
                ORDER BY ts ASC
                LIMIT ?
                """,
                (sym, int(since_ts), int(limit)),
            )
            return [(int(r[0]), float(r[1])) for r in cur.fetchall()]
        finally:
            conn.close()

def v2_buy(user_id: int, sym: str, money_in: int) -> Tuple[bool, str]:
    sym = (sym or "").strip().upper()
    if money_in <= 0:
        return False, "Buy amount must be a positive whole number."

    row = get_user(user_id)
    if row is None:
        return False, f"You are not activated yet. Run `{PREFIX}activate` first."
    bal = int(row[2])
    if money_in > bal:
        return False, f"You only have **{fmt_money(bal)}** Marcus Money. You can’t spend **{fmt_money(money_in)}**."

    with db_lock:
        conn = db_connect()
        try:
            m = conn.execute(
                "SELECT reserve_money, reserve_coin, fee FROM crypto_v2_markets WHERE symbol = ?",
                (sym,),
            ).fetchone()
            if not m:
                return False, f"Unknown crypto symbol `{sym}`. Use `{PREFIX}crypto`."
            reserve_money = float(m[0])
            reserve_coin = float(m[1])
            fee = float(m[2])

            if reserve_money <= 0 or reserve_coin <= 0:
                return False, "Market is illiquid right now. Try later."

            k = reserve_money * reserve_coin
            effective_in = float(money_in) * (1.0 - fee)

            new_curve_money = reserve_money + effective_in
            new_curve_coin = k / new_curve_money
            coins_out = reserve_coin - new_curve_coin
            if coins_out <= 0:
                return False, "Trade too small to execute."

            reserve_money_new = reserve_money + float(money_in)
            reserve_coin_new = reserve_coin - coins_out

            if reserve_coin_new <= 0:
                return False, "Market error: insufficient pool coin reserve."

            price_before = _v2_price(reserve_money, reserve_coin)
            price_after = _v2_price(reserve_money_new, reserve_coin_new)

            conn.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (int(money_in), int(user_id)))

            h = conn.execute(
                "SELECT coins FROM crypto_v2_holdings WHERE user_id = ? AND symbol = ?",
                (int(user_id), sym),
            ).fetchone()
            prev = float(h[0]) if h else 0.0
            _v2_set_holding_conn(conn, int(user_id), sym, prev + float(coins_out))

            now = int(time.time())
            conn.execute(
                """
                UPDATE crypto_v2_markets
                SET reserve_money = ?, reserve_coin = ?, last_price = ?, last_tick_ts = ?
                WHERE symbol = ?
                """,
                (float(reserve_money_new), float(reserve_coin_new), float(price_after), now, sym),
            )
            _v2_record_price_conn(conn, sym, now, float(price_after))

            conn.commit()
        finally:
            conn.close()

    return True, (
        f"Bought **{fmt_coin(coins_out, 3)} {sym}** for **{fmt_money(money_in)}** Marcus Money.\n"
        f"Price: {fmt_coin(price_before, 3)} → {fmt_coin(price_after, 3)} (slippage included)"
    )

def v2_sell(user_id: int, sym: str, coins_in: float) -> Tuple[bool, str]:
    sym = (sym or "").strip().upper()
    try:
        coins_in = float(coins_in)
    except Exception:
        return False, "Sell amount must be a number."
    if coins_in <= 0:
        return False, "Sell amount must be positive."

    row = get_user(user_id)
    if row is None:
        return False, f"You are not activated yet. Run `{PREFIX}activate` first."

    owned = v2_get_holding(user_id, sym)
    if coins_in > owned + 1e-12:
        return False, f"You only have **{fmt_coin(owned)} {sym}**."

    with db_lock:
        conn = db_connect()
        try:
            m = conn.execute(
                "SELECT reserve_money, reserve_coin, fee FROM crypto_v2_markets WHERE symbol = ?",
                (sym,),
            ).fetchone()
            if not m:
                return False, f"Unknown crypto symbol `{sym}`. Use `{PREFIX}crypto`."
            reserve_money = float(m[0])
            reserve_coin = float(m[1])
            fee = float(m[2])

            if reserve_money <= 0 or reserve_coin <= 0:
                return False, "Market is illiquid right now. Try later."

            k = reserve_money * reserve_coin
            effective_in = float(coins_in) * (1.0 - fee)

            new_curve_coin = reserve_coin + effective_in
            new_curve_money = k / new_curve_coin
            money_out = reserve_money - new_curve_money
            if money_out <= 0:
                return False, "Trade too small to execute."

            reserve_coin_new = reserve_coin + float(coins_in)
            reserve_money_new = reserve_money - money_out

            price_before = _v2_price(reserve_money, reserve_coin)
            price_after = _v2_price(reserve_money_new, reserve_coin_new)

            payout = int(math.floor(money_out))
            if payout <= 0:
                return False, "Trade too small after fees."

            h = conn.execute(
                "SELECT coins FROM crypto_v2_holdings WHERE user_id = ? AND symbol = ?",
                (int(user_id), sym),
            ).fetchone()
            prev = float(h[0]) if h else 0.0
            new_hold = max(0.0, prev - float(coins_in))
            _v2_set_holding_conn(conn, int(user_id), sym, new_hold)

            conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (int(payout), int(user_id)))

            now = int(time.time())
            conn.execute(
                """
                UPDATE crypto_v2_markets
                SET reserve_money = ?, reserve_coin = ?, last_price = ?, last_tick_ts = ?
                WHERE symbol = ?
                """,
                (float(reserve_money_new), float(reserve_coin_new), float(price_after), now, sym),
            )
            _v2_record_price_conn(conn, sym, now, float(price_after))

            conn.commit()
        finally:
            conn.close()

    return True, (
        f"Sold **{fmt_coin(coins_in, 3)} {sym}** for **{fmt_money(payout)}** Marcus Money.\n"
        f"Price: {fmt_coin(price_before, 3)} → {fmt_coin(price_after, 3)} (slippage included)"
    )

def v2_market_tick_once() -> None:
    now = int(time.time())
    with db_lock:
        conn = db_connect()
        try:
            rows = conn.execute(
                "SELECT symbol, reserve_money, reserve_coin, day_open_price, last_tick_ts FROM crypto_v2_markets"
            ).fetchall()

            # Day open reset at midnight (UTC day boundary based on epoch days)
            for sym, rm, rc, _day_open, last_tick in rows:
                price = _v2_price(float(rm), float(rc))
                if int(now // 86400) != int(int(last_tick) // 86400):
                    conn.execute(
                        "UPDATE crypto_v2_markets SET day_open_price = ? WHERE symbol = ?",
                        (float(price), str(sym)),
                    )

            for sym, rm, rc, _day_open, _last_tick in rows:
                sym = str(sym)
                reserve_money = float(rm)
                reserve_coin = float(rc)
                if reserve_money <= 0 or reserve_coin <= 0:
                    continue

                price_before = _v2_price(reserve_money, reserve_coin)
                k = reserve_money * reserve_coin

                base_sigma = 0.0015
                drift_bias = 3.5e-7
                noise = (sum(random.uniform(-1, 1) for _ in range(6)) / 6.0) * base_sigma - drift_bias

                kind = None
                shock = 0.0
                r = random.random()
                if r < 0.003:
                    kind = "MOON"
                    shock = random.uniform(0.01, 0.08)
                elif r < 0.005:
                    kind = "CRASH"
                    shock = -random.uniform(0.01, 0.1)

                drift = -3.5e-8
                total_move = max(noise + shock - drift, -0.0070)
                mult = max(0.05, 1.0 + total_move)
                target_price = price_before * mult

                new_rm = math.sqrt(k * target_price)
                new_rc = math.sqrt(k / target_price)

                if new_rm < 1000.0:
                    new_rm = 1000.0
                    new_rc = k / new_rm
                if new_rc < 0.0001:
                    new_rc = 0.0001
                    new_rm = k / new_rc

                price_after = _v2_price(new_rm, new_rc)

                conn.execute(
                    """
                    UPDATE crypto_v2_markets
                    SET reserve_money = ?, reserve_coin = ?, last_price = ?, last_tick_ts = ?
                    WHERE symbol = ?
                    """,
                    (float(new_rm), float(new_rc), float(price_after), int(now), sym),
                )
                _v2_record_price_conn(conn, sym, int(now), float(price_after))

                if kind is not None:
                    pct = (price_after / price_before - 1.0) * 100.0 if price_before > 0 else 0.0
                    note = "Viral hype wave hit the market." if kind == "MOON" else "Liquidity panic cascaded through the pool."
                    conn.execute(
                        "INSERT INTO crypto_v2_events (ts, symbol, kind, pct, note) VALUES (?, ?, ?, ?, ?)",
                        (int(now), sym, kind, float(pct), note),
                    )

            conn.commit()
        finally:
            conn.close()

def v2_recent_events(limit: int = 10) -> List[Tuple[int, str, str, float, str]]:
    with db_lock:
        conn = db_connect()
        try:
            cur = conn.execute(
                "SELECT ts, symbol, kind, pct, note FROM crypto_v2_events ORDER BY ts DESC, id DESC LIMIT ?",
                (int(limit),),
            )
            return [(int(r[0]), str(r[1]), str(r[2]), float(r[3]), str(r[4])) for r in cur.fetchall()]
        finally:
            conn.close()

def v2_set_market(sym: str, start_price: float, liquidity_money: float) -> Tuple[bool, str]:
    sym = (sym or "").strip().upper()
    try:
        sp = float(start_price)
        lm = float(liquidity_money)
    except Exception:
        return False, "start_price and liquidity_money must be numbers."
    if sp <= 0:
        return False, "start_price must be > 0."
    if lm <= 0:
        return False, "liquidity_money must be > 0."

    with db_lock:
        conn = db_connect()
        try:
            row = conn.execute("SELECT symbol FROM crypto_v2_markets WHERE symbol = ?", (sym,)).fetchone()
            if not row:
                return False, f"Unknown symbol `{sym}`."

            reserve_money = lm
            reserve_coin = lm / sp
            now = int(time.time())

            conn.execute(
                """
                UPDATE crypto_v2_markets
                SET reserve_money = ?, reserve_coin = ?, last_price = ?, day_open_price = ?, last_tick_ts = ?
                WHERE symbol = ?
                """,
                (float(reserve_money), float(reserve_coin), float(sp), float(sp), now, sym),
            )
            conn.execute("DELETE FROM crypto_v2_prices WHERE symbol = ?", (sym,))
            conn.execute("DELETE FROM crypto_v2_events WHERE symbol = ?", (sym,))
            _v2_record_price_conn(conn, sym, now, float(sp))

            conn.commit()
        finally:
            conn.close()

    return True, f"Set **{sym}** base price to **{fmt_coin(sp, 6)}** and liquidity to **{fmt_crypto_money(lm, 3)}** MM."

# ============================================================
# TAX SYSTEM
# ============================================================

def _get_state(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM system_state WHERE key = ?", (str(key),)).fetchone()
    return str(row[0]) if row else str(default)

def _set_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO system_state (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (str(key), str(value)),
    )

def tax_rate_for_balance(balance: int) -> float:
    b = int(balance)
    if b < 100_000:
        return 0.03
    if b < 1_000_000:
        return 0.05
    if b < 10_000_000:
        return 0.08
    if b < 50_000_000:
        return 0.10
    if b < 150_000_000:
        return 0.15
    if b < 500_000_000:
        return 0.18
    if b < 5_000_000_000:
        return 0.20
    if b < 20_000_000_000:
        return 0.23
    return 0.25

def run_tax_if_due() -> Optional[Tuple[str, int, int]]:
    tz = ZoneInfo(TAX_TIMEZONE)
    now_dt = datetime.now(tz)
    weekday = now_dt.weekday()
    if weekday not in TAX_WEEKDAYS:
        return None

    date_key = now_dt.strftime("%Y-%m-%d")
    now_ts = int(time.time())

    with db_lock:
        conn = db_connect()
        try:
            last_tax_date = _get_state(conn, "last_tax_date", "")
            if last_tax_date == date_key:
                return None

            rows = conn.execute("SELECT user_id, balance FROM users").fetchall()
            users_taxed = 0
            total_tax = 0

            for uid, bal in rows:
                bal_i = int(bal)
                rate = tax_rate_for_balance(bal_i)
                if rate <= 0.0:
                    continue
                tax_amt = int(math.floor(bal_i * rate))
                if tax_amt <= 0:
                    continue

                new_bal = bal_i - tax_amt
                if new_bal < 0:
                    tax_amt = bal_i
                    new_bal = 0

                conn.execute("UPDATE users SET balance = ? WHERE user_id = ?", (int(new_bal), int(uid)))
                users_taxed += 1
                total_tax += tax_amt

            # Record last run date + event record
            _set_state(conn, "last_tax_date", date_key)
            conn.execute(
                "INSERT INTO tax_events (ts, date_key, users_taxed, total_tax) VALUES (?, ?, ?, ?)",
                (now_ts, date_key, int(users_taxed), int(total_tax)),
            )

            # Send ALL collected tax into the lottery pool
            if total_tax > 0:
                pool = _get_lottery_pool_conn(conn)
                pool += int(total_tax)
                _set_lottery_pool_conn(conn, int(pool))

            conn.commit()
            return (date_key, users_taxed, total_tax)

        finally:
            conn.close()

# ============================================================
# LOTTERY SYSTEM
# ============================================================

def _get_lottery_pool_conn(conn: sqlite3.Connection) -> int:
    try:
        return int(_get_state(conn, "lottery_pool", "0"))
    except Exception:
        return 0

def _set_lottery_pool_conn(conn: sqlite3.Connection, value: int) -> None:
    _set_state(conn, "lottery_pool", str(int(max(0, value))))

def _validate_lottery_numbers(main_nums: List[int], pb: int) -> Tuple[bool, str]:
    if len(main_nums) != 5:
        return False, "You must provide exactly 5 main numbers."
    for x in main_nums:
        if not (LOTTERY_MAIN_MIN <= int(x) <= LOTTERY_MAIN_MAX):
            return False, f"Main numbers must be {LOTTERY_MAIN_MIN}-{LOTTERY_MAIN_MAX}."
    if not (LOTTERY_PB_MIN <= int(pb) <= LOTTERY_PB_MAX):
        return False, f"Powerball must be {LOTTERY_PB_MIN}-{LOTTERY_PB_MAX}."
    return True, ""

def _quickpick_ticket() -> Tuple[List[int], int]:
    main_nums = [random.randint(LOTTERY_MAIN_MIN, LOTTERY_MAIN_MAX) for _ in range(5)]
    pb = random.randint(LOTTERY_PB_MIN, LOTTERY_PB_MAX)
    return main_nums, pb

def _count_main_matches(ticket_main: List[int], win_main: List[int]) -> int:
    # Order matters: count position-by-position matches
    return sum(1 for a, b in zip(ticket_main, win_main) if int(a) == int(b))


def _prize_for_match(main_matches: int, pb_match: bool) -> Tuple[int, bool]:
    mm = int(main_matches)
    if mm >= 5 and pb_match:
        return (0, True)
    if mm >= 5:
        return (100000, False)
    if mm == 4:
        return (25000, False)
    if mm == 3:
        return (5000, False)
    if mm == 2:
        return (500, False)
    if mm == 1:
        return (0, False)
    return (0, False)

def _fmt_ticket(main_nums: List[int], pb: int) -> str:
    return f"{' '.join(str(int(x)) for x in main_nums)}  | PB {int(pb)}"

# ============================================================
# Bot setup
# ============================================================

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents)
bot.remove_command("help")

def display_name(member: discord.abc.User) -> str:
    return member.display_name if isinstance(member, discord.Member) else member.name

def spin() -> int:
    return random.randint(REEL_MIN, REEL_MAX)

def format_two_digits(n: int) -> str:
    s = f"{n:02d}"
    return f"{s[0]} {s[1]}"

def win_table_text() -> str:
    parts = []
    for k in sorted(WIN_MULTIPLIERS.keys()):
        parts.append(f"{format_two_digits(k)} ({WIN_MULTIPLIERS[k]}x)")
    return ", ".join(parts)

async def send_reply(ctx: commands.Context, content: str, **kwargs):
    return await ctx.reply(content, mention_author=False, **kwargs)

async def require_activated(ctx: commands.Context) -> bool:
    row = get_user(ctx.author.id)
    if row is None:
        await send_reply(ctx, f"You are not activated yet. Run `{PREFIX}activate` to create your slot profile.")
        return False
    update_username(ctx.author.id, display_name(ctx.author))
    return True

def is_owner(ctx: commands.Context) -> bool:
    return ctx.author.id == OWNER_ID

def format_ts(ts: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ts)))

_market_task_started = False
_tax_task_started = False

async def market_daemon():
    while True:
        try:
            v2_market_tick_once()
        except Exception as e:
            print(f"[market_daemon] error: {e}")
        await asyncio.sleep(CRYPTO_V2_TICK_SECONDS)

async def tax_daemon():
    while True:
        try:
            res = run_tax_if_due()
            if res:
                date_key, users_taxed, total_tax = res
                print(f"[TAX] {date_key}: taxed {users_taxed} users, collected {total_tax}")
        except Exception as e:
            print(f"[tax_daemon] error: {e}")
        await asyncio.sleep(TAX_CHECK_SECONDS)

_parole_task_started = False

async def parole_daemon():
    while True:
        try:
            parole_tick_once()
        except Exception as e:
            print(f"[parole_daemon] error: {e}")
        await asyncio.sleep(PAROLE_CHECK_SECONDS)


@bot.event
async def on_ready():
    global _market_task_started, _tax_task_started
    init_db()
    if not _market_task_started:
        _market_task_started = True
        asyncio.create_task(market_daemon())
    if not _tax_task_started:
        _tax_task_started = True
        asyncio.create_task(tax_daemon())
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")

@bot.event
async def on_message(message):
    if message.author.bot:
        return

    if message.reference and message.reference.message_id:
        try:
            ref_msg = message.reference.resolved
            if ref_msg is None:
                ref_msg = await message.channel.fetch_message(message.reference.message_id)
        except Exception:
            ref_msg = None

        if ref_msg is not None and ref_msg.author and ref_msg.author.id == bot.user.id:
            # Global trigger odds
            if TRIGGER_REPLY_CHANCE_DENOM > 1:
                if random.randint(1, int(TRIGGER_REPLY_CHANCE_DENOM)) != 1:
                    await bot.process_commands(message)
                    return

            now = time.time()
            last = _last_reply_to_bot.get(message.author.id, 0.0)
            if now - last >= float(BOT_REPLY_COOLDOWN_SECONDS):
                _last_reply_to_bot[message.author.id] = now

                v = pick_variant()

                # --- send behavior by mode ---
                if v.mode == "image_only":
                    if v.image_url:
                        # For image_only, make it "always" by using denom=1,
                        # but we still honor denom if you want "rare image-only"
                        denom = int(v.image_chance_denom or 1)
                        if random.randint(1, denom) == 1:
                            await message.reply(v.image_url, mention_author=False)
                    else:
                        # Fallback if misconfigured
                        await message.reply(" ", mention_author=False)

                elif v.mode == "text":
                    await message.reply(v.text or " ", mention_author=False)

                else:  # "text_then_image"
                    await message.reply(v.text or " ", mention_author=False)
                    if v.image_url and int(v.image_chance_denom) > 0:
                        if random.randint(1, int(v.image_chance_denom)) == 1:
                            await message.reply(v.image_url, mention_author=False)

    await bot.process_commands(message)

@bot.event
async def on_ready():
    global _market_task_started, _tax_task_started, _parole_task_started
    init_db()
    if not _market_task_started:
        _market_task_started = True
        asyncio.create_task(market_daemon())
    if not _tax_task_started:
        _tax_task_started = True
        asyncio.create_task(tax_daemon())
    if not _parole_task_started:
        _parole_task_started = True
        asyncio.create_task(parole_daemon())

    print(f"Logged in as {bot.user} (ID: {bot.user.id})")



# =======================
# Blackjack
# =======================

BLACKJACK_GAMES: Dict[int, dict] = {}
BJ_RANKS = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]

def bj_draw_card() -> str:
    return random.choice(BJ_RANKS)

def bj_hand_value(cards: List[str]) -> int:
    total = 0
    aces = 0
    for c in cards:
        if c == "A":
            total += 11
            aces += 1
        elif c in ("J", "Q", "K"):
            total += 10
        else:
            total += int(c)
    while total > 21 and aces > 0:
        total -= 10
        aces -= 1
    return total

def bj_is_blackjack(cards: List[str]) -> bool:
    return len(cards) == 2 and bj_hand_value(cards) == 21

def bj_fmt(cards: List[str]) -> str:
    return " ".join(cards)

# ============================================================
# Roulette
# ============================================================

ROULETTE_RED = {1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36}

def roulette_color(n: int) -> str:
    if n == 0:
        return "green"
    return "red" if n in ROULETTE_RED else "black"

def roulette_parse_choice(choice: str) -> Tuple[bool, str, dict]:
    c = choice.strip().lower()

    if c.isdigit():
        n = int(c)
        if 0 <= n <= 36:
            return True, f"Number {n}", {"type": "number", "n": n, "payout": 50}
        return False, "", {}

    if c.startswith("n") and c[1:].isdigit():
        n = int(c[1:])
        if 0 <= n <= 36:
            return True, f"Number {n}", {"type": "number", "n": n, "payout": 50}
        return False, "", {}

    if c in ("red", "black"):
        return True, c.title(), {"type": "color", "color": c, "payout": 1}

    if c in ("even", "odd"):
        return True, c.title(), {"type": "parity", "parity": c, "payout": 1}

    if c in ("high", "low"):
        return True, c.title(), {"type": "highlow", "hl": c, "payout": 1}

    if c in ("dozen1", "d1", "1st12", "first12"):
        return True, "Dozen 1 (1-12)", {"type": "dozen", "dozen": 1, "payout": 3}
    if c in ("dozen2", "d2", "2nd12", "second12"):
        return True, "Dozen 2 (13-24)", {"type": "dozen", "dozen": 2, "payout": 3}
    if c in ("dozen3", "d3", "3rd12", "third12"):
        return True, "Dozen 3 (25-36)", {"type": "dozen", "dozen": 3, "payout": 3}

    if c in ("col1", "column1", "c1"):
        return True, "Column 1", {"type": "column", "col": 1, "payout": 3}
    if c in ("col2", "column2", "c2"):
        return True, "Column 2", {"type": "column", "col": 2, "payout": 3}
    if c in ("col3", "column3", "c3"):
        return True, "Column 3", {"type": "column", "col": 3, "payout": 3}

    return False, "", {}

def roulette_is_win(spin_n: int, rule: dict) -> bool:
    t = rule["type"]
    if t == "number":
        return spin_n == rule["n"]
    if spin_n == 0:
        return False
    if t == "color":
        return roulette_color(spin_n) == rule["color"]
    if t == "parity":
        return (spin_n % 2 == 0) if rule["parity"] == "even" else (spin_n % 2 == 1)
    if t == "highlow":
        return (19 <= spin_n <= 36) if rule["hl"] == "high" else (1 <= spin_n <= 18)
    if t == "dozen":
        d = rule["dozen"]
        if d == 1:
            return 1 <= spin_n <= 12
        if d == 2:
            return 13 <= spin_n <= 24
        return 25 <= spin_n <= 36
    if t == "column":
        col = rule["col"]
        mod = spin_n % 3
        if col == 1:
            return mod == 1
        if col == 2:
            return mod == 2
        return mod == 0
    return False

# ============================================================
# Jail system
# ============================================================

def get_jail_status(user_id: int) -> Tuple[bool, int]:
    with db_lock:
        conn = db_connect()
        try:
            row = conn.execute(
                "SELECT jailed, jail_ts FROM users WHERE user_id = ?",
                (int(user_id),),
            ).fetchone()
            if not row:
                return (False, 0)
            return (int(row[0]) == 1, int(row[1]))
        finally:
            conn.close()

def set_jailed(user_id: int, jailed: bool) -> None:
    with db_lock:
        conn = db_connect()
        try:
            if jailed:
                now = int(time.time())
                conn.execute(
                    "UPDATE users SET jailed = 1, jail_ts = ? WHERE user_id = ?",
                    (now, int(user_id)),
                )
            else:
                conn.execute(
                    "UPDATE users SET jailed = 0, jail_ts = 0 WHERE user_id = ?",
                    (int(user_id),),
                )
            conn.commit()
        finally:
            conn.close()

def get_parole_status(user_id: int) -> Tuple[bool, int, int]:
    """
    Returns (paroled, parole_ts, parole_last_pay_ts)
    """
    with db_lock:
        conn = db_connect()
        try:
            row = conn.execute(
                "SELECT paroled, parole_ts, parole_last_pay_ts FROM users WHERE user_id = ?",
                (int(user_id),),
            ).fetchone()
            if not row:
                return (False, 0, 0)
            return (int(row[0]) == 1, int(row[1]), int(row[2]))
        finally:
            conn.close()

def set_parole(user_id: int, paroled: bool) -> None:
    with db_lock:
        conn = db_connect()
        try:
            if paroled:
                now = int(time.time())
                conn.execute(
                    "UPDATE users SET paroled = 1, parole_ts = ?, parole_last_pay_ts = ? WHERE user_id = ?",
                    (now, now, int(user_id)),
                )
            else:
                conn.execute(
                    "UPDATE users SET paroled = 0, parole_ts = 0, parole_last_pay_ts = 0 WHERE user_id = ?",
                    (int(user_id),),
                )
            conn.commit()
        finally:
            conn.close()

def parole_tick_once() -> None:
    """
    For each paroled user:
      - If parole expired: clear parole
      - Otherwise, for each missed 10-min interval since last pay:
          take 5% of current balance (floor), move to lottery pool
    """
    now = int(time.time())

    with db_lock:
        conn = db_connect()
        try:
            rows = conn.execute(
                "SELECT user_id, balance, parole_ts, parole_last_pay_ts FROM users WHERE paroled = 1"
            ).fetchall()

            if not rows:
                return

            pool = _get_lottery_pool_conn(conn)

            for uid, bal, parole_ts, last_pay_ts in rows:
                uid = int(uid)
                bal_i = int(bal)
                parole_ts = int(parole_ts)
                last_pay_ts = int(last_pay_ts)

                # Expire parole after 1 hour
                if parole_ts <= 0 or (now - parole_ts) >= PAROLE_TIME_SECONDS:
                    conn.execute(
                        "UPDATE users SET paroled = 0, parole_ts = 0, parole_last_pay_ts = 0 WHERE user_id = ?",
                        (uid,),
                    )
                    continue

                if last_pay_ts <= 0:
                    last_pay_ts = parole_ts

                elapsed = now - last_pay_ts
                steps = elapsed // PAROLE_PAY_INTERVAL_SECONDS
                if steps <= 0:
                    continue

                # Apply each step sequentially so it's truly "5% of your money" each time
                current_balance = int(
                    conn.execute("SELECT balance FROM users WHERE user_id = ?", (uid,)).fetchone()[0]
                )

                paid_total = 0
                for _ in range(int(steps)):
                    if current_balance <= 0:
                        break
                    take = int(math.floor(current_balance * PAROLE_RATE))
                    if take <= 0 and current_balance > 0:
                        take = 1
                    take = min(take, current_balance)
                    if take <= 0:
                        break

                    current_balance -= take
                    paid_total += take

                if paid_total > 0:
                    conn.execute("UPDATE users SET balance = ? WHERE user_id = ?", (int(current_balance), uid))
                    pool += int(paid_total)

                # Advance last pay time by the number of processed steps
                new_last = last_pay_ts + int(steps) * PAROLE_PAY_INTERVAL_SECONDS
                conn.execute("UPDATE users SET parole_last_pay_ts = ? WHERE user_id = ?", (int(new_last), uid))

            _set_lottery_pool_conn(conn, int(pool))
            conn.commit()
        finally:
            conn.close()




# ============================================================
# Plinko renderer
# ============================================================

def plinko_render_multi(
    paths: List[List[int]],
    step_index: int,
    multipliers: List[float],
    bet_per_ball: int,
    balls: int,
) -> str:
    slots = len(multipliers)
    rows = (len(paths[0]) - 1) if paths else PLINKO_ROWS
    CELL_W = 4

    def cell_text(s: str) -> str:
        return f"{s:^{CELL_W}}"

    def label_in_cell(text: str) -> str:
        t = str(text)
        if len(t) > CELL_W:
            t = t[:CELL_W]
        return f"{t:^{CELL_W}}"

    step_index = max(0, min(int(step_index), rows))

    trail_marks = set()
    for r in range(0, step_index):
        for p in paths:
            if 0 <= r < len(p):
                c = int(p[r])
                if 0 <= c < slots:
                    trail_marks.add((r, c))

    current_counts: Dict[int, int] = {}
    for p in paths:
        if 0 <= step_index < len(p):
            c = int(p[step_index])
            if 0 <= c < slots:
                current_counts[c] = current_counts.get(c, 0) + 1

    lines = []
    total_cost = int(bet_per_ball) * int(balls)
    lines.append(
        f"Plinko — Balls: {balls} — Bet per ball: {fmt_money(bet_per_ball)} MM — Total: {fmt_money(total_cost)} MM"
    )
    lines.append("")

    for r in range(rows + 1):
        row_cells = []
        for c in range(slots):
            ch = " "
            if r == step_index:
                n = current_counts.get(c, 0)
                if n == 1:
                    ch = "o"
                elif n >= 2:
                    ch = str(min(n, 9))
            elif (r, c) in trail_marks:
                ch = "."
            row_cells.append(ch)

        inner = "".join(cell_text(ch) for ch in row_cells)
        lines.append("|" + inner + "|")

    inner_w = slots * CELL_W
    lines.append("+" + ("-" * inner_w) + "+")
    lines.append(" " + "".join(label_in_cell(f"{m:g}") for m in multipliers))
    lines.append(" " + "".join(label_in_cell("^") for _ in multipliers))
    lines.append(" " + "".join(label_in_cell(str(i)) for i in range(1, slots + 1)))

    return "```text\n" + "\n".join(lines) + "\n```"

# ==============
# Commands
# ==============

@bot.command(name="ticketcount", aliases=["ticketscount", "totaltickets", "pooltickets"])
@commands.guild_only()
async def ticketcount_cmd(ctx: commands.Context):
    with db_lock:
        conn = db_connect()
        try:
            total = int(conn.execute("SELECT COUNT(*) FROM lottery_tickets").fetchone()[0])
            pool = int(_get_lottery_pool_conn(conn))
            users = int(conn.execute("SELECT COUNT(DISTINCT user_id) FROM lottery_tickets").fetchone()[0])
        finally:
            conn.close()

    await send_reply(
        ctx,
        f"**Powerball Ticket Count**\n"
        f"Active tickets: **{total}**\n"
        f"Players with tickets: **{users}**\n"
        f"Current pool: **{fmt_money(pool)}** MM"
    )

@bot.command(name="blowjob")
@commands.guild_only()
async def blow_cmd(ctx: commands.Context, *, _words: str = ""):
    """
    !blowjob (anything...)
    Takes 7% of your current cash balance and adds it to the lottery pool.
    Extra words are ignored so users can type: !blow yes please
    """
    if not await require_activated(ctx):
        return

    row = get_user(ctx.author.id)
    if row is None:
        await send_reply(ctx, f"You are not activated yet. Run `{PREFIX}activate` first.")
        return

    bal = int(row[2])
    donate = int(math.floor(bal * 0.07))

    if donate <= 0:
        await send_reply(ctx, "Not right now.")
        return

    with db_lock:
        conn = db_connect()
        try:
            # Re-read balance inside the lock for accuracy
            cur_bal_row = conn.execute(
                "SELECT balance FROM users WHERE user_id = ?",
                (int(ctx.author.id),),
            ).fetchone()
            if not cur_bal_row:
                await send_reply(ctx, "Database error: missing your user row.")
                return

            cur_bal = int(cur_bal_row[0])
            donate = int(math.floor(cur_bal * 0.10))
            if donate <= 0:
                await send_reply(ctx, "Not right now.")
                return

            # Subtract from user
            conn.execute(
                "UPDATE users SET balance = balance - ? WHERE user_id = ?",
                (int(donate), int(ctx.author.id)),
            )

            # Add to lottery pool
            pool = _get_lottery_pool_conn(conn)
            pool += int(donate)
            _set_lottery_pool_conn(conn, pool)

            conn.commit()

            new_bal = int(
                conn.execute(
                    "SELECT balance FROM users WHERE user_id = ?",
                    (int(ctx.author.id),),
                ).fetchone()[0]
            )
        finally:
            conn.close()

    await send_reply(
        ctx,
        f"**💋Blowjob Complete💋**\n"
    )



@bot.command(name="crypto")
async def crypto_cmd(ctx: commands.Context):
    coins = v2_list_markets()
    if not coins:
        await send_reply(ctx, "No cryptos available.")
        return
    lines = ["**Crypto Market (V2)** (AMM pricing; buys push up, sells push down)"]
    for sym, name, price, day_open, rm, _fee in coins:
        pct = (price / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
        lines.append(
            f"**{sym}** — {name}\n"
            f"Price: **{fmt_coin(price, 3)}**  ̷M̷ | 24h: **{pct:+.2f}%** | Liquidity: **{fmt_crypto_money(rm, 3)}**  ̷M̷"
        )
    await send_reply(ctx, "\n".join(lines))

@bot.command(name="price")
async def price_cmd(ctx: commands.Context, symbol: str):
    c = v2_get_market(symbol)
    if c is None:
        await send_reply(ctx, f"Unknown crypto symbol `{symbol}`. Use `{PREFIX}crypto`.")
        return
    sym, name, rm, rc, _last_price, _ts, day_open, fee = c
    price = _v2_price(float(rm), float(rc))
    pct = (price / float(day_open) - 1.0) * 100.0 if float(day_open) > 0 else 0.0
    await send_reply(
        ctx,
        f"**{sym}** — {name}\n"
        f"Price: **{fmt_coin(price, 3)}** Marcus Money\n"
        f"24h: **{pct:+.2f}%** | Pool: {fmt_crypto_money(float(rm), 3)} MM / {fmt_coin(float(rc), 3)} {sym}\n"
        f"Fee: {float(fee)*100:.2f}%"
    )

@bot.command(name="buy")
async def buy_cmd(ctx: commands.Context, symbol: str, money: str):
    if not await require_activated(ctx):
        return
    try:
        money_i = parse_amount_int(money, min_value=1)
    except ValueError:
        await send_reply(ctx, f"Usage: `{PREFIX}buy <symbol> <money>` (supports 2K/3M/1B/...)")
        return

    ok, msg = v2_buy(ctx.author.id, symbol, money_i)
    if not ok:
        await send_reply(ctx, msg)
        return

    row = get_user(ctx.author.id)
    bal = int(row[2]) if row else 0
    sym = symbol.strip().upper()
    owned = v2_get_holding(ctx.author.id, sym)
    await send_reply(ctx, f"{msg}\nBalance: **{fmt_money(bal)}** | Holding: **{fmt_coin(owned, 3)} {sym}**")

@bot.command(name="sell")
async def sell_cmd(ctx: commands.Context, symbol: str, coins: str):
    if not await require_activated(ctx):
        return

    sym = symbol.strip().upper()
    c_raw = (coins or "").strip().lower()

    if c_raw in ("all", "max"):
        owned = v2_get_holding(ctx.author.id, sym)
        if owned <= 0:
            await send_reply(ctx, f"You have no **{sym}** to sell.")
            return
        coins_f = float(owned)
    else:
        try:
            coins_f = float(coins)
        except Exception:
            await send_reply(ctx, f"Usage: `{PREFIX}sell <symbol> <coins|all>`")
            return

    ok, msg = v2_sell(ctx.author.id, sym, coins_f)
    if not ok:
        await send_reply(ctx, msg)
        return

    row = get_user(ctx.author.id)
    bal = int(row[2]) if row else 0
    owned_after = v2_get_holding(ctx.author.id, sym)
    await send_reply(ctx, f"{msg}\nBalance: **{fmt_money(bal)}** | Holding: **{fmt_coin(owned_after, 3)} {sym}**")

@bot.command(name="portfolio")
async def portfolio_cmd(ctx: commands.Context):
    if not await require_activated(ctx):
        return
    holdings = v2_get_portfolio(ctx.author.id)
    if not holdings:
        await send_reply(ctx, "You have no crypto holdings yet.")
        return

    coin_map = {sym: (price, name) for sym, name, price, _day_open, _rm, _fee in v2_list_markets()}

    total_value = 0.0
    lines = ["**Your Portfolio (V2)**"]
    for sym, amt in holdings:
        if amt <= 0:
            continue
        price = coin_map.get(sym, (0.0, ""))[0]
        value = amt * float(price)
        total_value += value
        lines.append(f"**{sym}**: {fmt_coin(amt, 3)}  (≈ **{fmt_crypto_money(value, 3)}**  ̷M̷)")

    row = get_user(ctx.author.id)
    cash = float(int(row[2])) if row else 0.0

    lines.append("")
    lines.append(f"Cash: **{fmt_money(int(cash))}**  ̷M̷")
    lines.append(f"Crypto value: **{fmt_crypto_money(total_value, 3)}**  ̷M̷")
    lines.append(f"Net worth: **{fmt_crypto_money(cash + total_value, 3)}**  ̷M̷")
    await send_reply(ctx, "\n".join(lines))

@bot.command(name="crypto_news")
async def crypto_news_cmd(ctx: commands.Context):
    events = v2_recent_events(limit=8)
    if not events:
        await send_reply(ctx, "No crypto events recorded yet.")
        return
    lines = ["**Market News (V2)** (shock events)"]
    for ts, sym, kind, pct, note in events:
        when = format_ts(ts)
        lines.append(f"{when} — **{sym} {kind}** ({pct:+.2f}%) — {note}")
    await send_reply(ctx, "\n".join(lines))

def render_ascii_price_chart(points: List[Tuple[int, float]], width: int = 60, height: int = 15) -> str:
    if not points:
        return "(no data)"

    pts = sorted(points, key=lambda x: x[0])
    prices = [float(p) for _, p in pts if p is not None]
    if not prices:
        return "(no data)"

    pmin = min(prices)
    pmax = max(prices)

    if abs(pmax - pmin) < 1e-12:
        grid = [[" " for _ in range(width)] for _ in range(height)]
        y = height // 2
        for x in range(width):
            grid[y][x] = "─"
        header = f"max={pmax:.6f}"
        footer = f"min={pmin:.6f}"
        out = [header]
        out += ["".join(row) for row in grid]
        out.append(footer)
        return "\n".join(out)

    n = len(pts)

    def idx_for_x(x: int) -> int:
        if width <= 1:
            return 0
        return int(round((x / (width - 1)) * (n - 1)))

    col_prices = []
    for x in range(width):
        i = idx_for_x(x)
        col_prices.append(float(pts[i][1]))

    grid = [[" " for _ in range(width)] for _ in range(height)]

    def y_for_price(price: float) -> int:
        t = (price - pmin) / (pmax - pmin)
        y = int(round((1.0 - t) * (height - 1)))
        return max(0, min(height - 1, y))

    prev_y = None
    for x, pr in enumerate(col_prices):
        y = y_for_price(pr)
        grid[y][x] = "●"
        if prev_y is not None and abs(prev_y - y) > 1:
            y0, y1 = sorted([prev_y, y])
            for yy in range(y0 + 1, y1):
                if grid[yy][x] == " ":
                    grid[yy][x] = "│"
        prev_y = y

    header = f"max={pmax:.6f}"
    footer = f"min={pmin:.6f}"
    out = [header]
    out += ["".join(row) for row in grid]
    out.append(footer)
    return "\n".join(out)

@bot.command(name="crypto_graph", aliases=["graph", "chart"])
async def crypto_graph_cmd(ctx: commands.Context, symbol: str, window: str = "24h"):
    c = v2_get_market(symbol)
    if c is None:
        await send_reply(ctx, f"Unknown crypto symbol `{symbol}`. Use `{PREFIX}crypto`.")
        return

    sym = symbol.strip().upper()
    seconds = parse_time_window(window)
    now = int(time.time())
    since_ts = now - seconds

    pts = v2_get_price_series_since(sym, since_ts, limit=5000)
    if len(pts) < 5:
        _sym, _name, rm, rc, _last_price, _ts, _day_open, _fee = c
        price_now = _v2_price(float(rm), float(rc))
        pts = [(now, float(price_now))]

    chart = render_ascii_price_chart(pts, width=60, height=15)
    await send_reply(ctx, f"**{sym}** price chart (V2) — window: **{window}** (points: {len(pts)})\n```{chart}```")

@bot.command(name="crypto_set")
@commands.guild_only()
async def crypto_set_cmd(ctx: commands.Context, symbol: str, start_price: str, liquidity_money: str):
    if not is_owner(ctx):
        await send_reply(ctx, "You are not allowed to use this command.")
        return
    try:
        sp = float(start_price)
        lm = float(liquidity_money)
    except Exception:
        await send_reply(ctx, f"Usage: `{PREFIX}crypto_set <symbol> <start_price> <liquidity_money>`")
        return

    ok, msg = v2_set_market(symbol, sp, lm)
    await send_reply(ctx, msg if ok else f"Error: {msg}")

@bot.command(name="helpme")
async def help_cmd(ctx: commands.Context):
    msg = (
        "**Slot Bot Commands**\n"
        "\n"
        f"`{PREFIX}helpme` — Show this help message.\n"
        f"`{PREFIX}activate` — Create profile (starts at **{fmt_money(START_BALANCE)}** credits).\n"
        f"`{PREFIX}balance` — Show balance (auto-applies daily bonus if due).\n"
        f"`{PREFIX}daily` — Claim daily bonus (**+{fmt_money(DAILY_CREDITS)}** every 24 hours).\n"
        f"`{PREFIX}slot <bet>` — Spin 2-digit number and bet credits. Wins: {win_table_text()}.\n"
        f"`{PREFIX}leaders` — Show the top **{TOP_N}** balances.\n"
        f"`{PREFIX}gift @User <amount>` — Gift credits to someone (taken from you).\n"
        f"`{PREFIX}giftall <amount> @User1 @User2 ...` — Gift multiple users (taken from YOUR balance).\n"
        f"`{PREFIX}gifteveryone <amount> [ping|noping]` — Gift to every activated user in this server.\n"
        f"`{PREFIX}give @User <amount>` — Owner-only: give credits.\n"
        f"`{PREFIX}take @User <amount>` — Owner-only: take credits.\n"
        f"`{PREFIX}tax_status` — Show whether tax is due / last tax date.\n"
        f"`{PREFIX}tax_now` — Owner-only (only runs Wed/Fri/Sun).\n"
        "\n"
        "**Crypto (AMM Market)**\n"
        f"`{PREFIX}crypto` — List markets.\n"
        f"`{PREFIX}price <SYM>` — Show market price.\n"
        f"`{PREFIX}buy <SYM> <money>` — Buy coin (e.g., `2K`, `3M`).\n"
        f"`{PREFIX}sell <SYM> <coins|all>` — Sell coin.\n"
        f"`{PREFIX}portfolio` — Show holdings and net worth.\n"
        f"`{PREFIX}crypto_news` — Recent shock events.\n"
        f"`{PREFIX}crypto_graph <SYM> [24h|7d|...]` — ASCII chart.\n"
        f"`{PREFIX}crypto_set <SYM> <start_price> <liquidity_money>` — Owner-only.\n"
        "\n"
        "**Parimutuel Betting**\n"
        f"`{PREFIX}bet_create Title | Option 1 | Option 2 | ...` — Owner-only.\n"
        f"`{PREFIX}bet_list` — List open bets.\n"
        f"`{PREFIX}bet_info <bet_id>` — Show options and lines.\n"
        f"`{PREFIX}bet_my <bet_id>` — Show your wager.\n"
        f"`{PREFIX}bet <bet_id> <option#> <amount>` — Place a wager.\n"
        f"`{PREFIX}bet_bonus <bet_id> <amount>` — Owner-only.\n"
        f"`{PREFIX}bet_close <bet_id>` — Owner-only.\n"
        f"`{PREFIX}bet_resolve <bet_id> <option#>` — Owner-only.\n"
        f"`{PREFIX}bet_cancel <bet_id>` — Owner-only.\n"
        "\n"
        "**Help Page 2**\n"
        f"Use `{PREFIX}helpme2` for more commands."
    )
    await send_reply(ctx, msg)

@bot.command(name="helpme2")
async def help_cmd(ctx: commands.Context):
    msg = (
        "**Slot Bot Commands Pg.2**\n"
        "\n"
        "**Blackjack**\n"
        f"`{PREFIX}blackjack <bet>` / `{PREFIX}bj <bet>` — Start a hand.\n"
        f"`{PREFIX}hit` — Draw a card.\n"
        f"`{PREFIX}stand` — Finish your hand.\n"
        "\n"
        "**Roulette**\n"
        f"`{PREFIX}roulette <bet> <choice>` — Example: `{PREFIX}roulette 50 red`\n"
        "\n"
        "**Plinko**\n"
        f"`{PREFIX}plinko <bet_per_ball> <balls>` — Drop balls.\n"
        "\n"
        "**Lottery**\n"
        f"`{PREFIX}ticket` / `{PREFIX}pb` / `{PREFIX}lottery` — Buy a random ticket.\n"
        f"`{PREFIX}draw` — Owner-only draw.\n"
        "\n"
        "**Crime**\n"
        f"`{PREFIX}steal @User [ping|noping]` — Attempt to steal.\n"
        f"`{PREFIX}getoutofjail` — Pay 20% to get released.\n"
    )
    await send_reply(ctx, msg)


def steal_success_prob(target_balance: int) -> float:
    try:
        b = max(0, int(target_balance))
    except Exception:
        b = 0

    base = 0.25
    floor = 0.06
    scale = 400_000
    exponent = 0.55

    ratio = (scale / (scale + b)) ** exponent
    p = base * ratio
    return max(floor, min(base, p))

@bot.command(name="steal")
@commands.guild_only()
async def steal_cmd(ctx: commands.Context, target: discord.Member, ping: str = "noping"):
    if not await require_activated(ctx):
        return

    if target.bot:
        await send_reply(ctx, "You can't steal from bots.")
        return
    if target.id == ctx.author.id:
        await send_reply(ctx, "You can't steal from yourself.")
        return
    if get_user(target.id) is None:
        await send_reply(ctx, f"That user is not activated yet. They must run `{PREFIX}activate` first.")
        return

    p = (ping or "noping").strip().lower()
    do_ping = p in ("ping", "pings", "yes", "y", "true", "t", "1")
    if p not in ("ping", "pings", "yes", "y", "true", "t", "1", "noping", "nopings", "no", "n", "false", "f", "0"):
        await send_reply(ctx, f"Usage: `{PREFIX}steal @User [ping|noping]`")
        return

    allowed = discord.AllowedMentions(users=[target]) if do_ping else discord.AllowedMentions.none()
    target_ref = target.mention if do_ping else target.display_name

    thief_row = get_user(ctx.author.id)
    target_row = get_user(target.id)
    if thief_row is None or target_row is None:
        await send_reply(ctx, "Market error: missing user row(s).")
        return

    thief_bal = int(thief_row[2])
    target_bal = int(target_row[2])

    # --- Minimum balance requirement ---
    MIN_STEAL_BALANCE = 500
    if thief_bal < MIN_STEAL_BALANCE:
        await send_reply(
            ctx,
            f"You need at least **{fmt_money(MIN_STEAL_BALANCE)}** Marcus Money to use `{PREFIX}steal`.\n"
            f"Your balance: **{fmt_money(thief_bal)}**",
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return

    p_success = steal_success_prob(target_bal)

    if random.random() < p_success:
        with db_lock:
            conn = db_connect()
            try:
                trow = conn.execute("SELECT balance FROM users WHERE user_id = ?", (int(target.id),)).fetchone()
                arow = conn.execute("SELECT balance FROM users WHERE user_id = ?", (int(ctx.author.id),)).fetchone()
                if not trow or not arow:
                    await send_reply(ctx, "Market error: missing balances.")
                    return

                tbal = int(trow[0])

                steal_amt = int(math.floor(tbal * 0.05))
                if tbal > 0:
                    steal_amt = max(1, steal_amt)
                steal_amt = min(steal_amt, tbal)

                if steal_amt <= 0:
                    await send_reply(ctx, f"Steal attempt succeeded, but **{target.display_name}** has no money to take.")
                    return

                conn.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (int(steal_amt), int(target.id)))
                conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (int(steal_amt), int(ctx.author.id)))
                conn.commit()

                new_thief_bal = int(conn.execute("SELECT balance FROM users WHERE user_id = ?", (int(ctx.author.id),)).fetchone()[0])
                new_target_bal = int(conn.execute("SELECT balance FROM users WHERE user_id = ?", (int(target.id),)).fetchone()[0])
            finally:
                conn.close()

        await send_reply(
            ctx,
            f"Steal attempt: **SUCCESS** (chance was {p_success*100:.1f}%).\n"
            f"You stole **{fmt_money(steal_amt)}** Marcus Money from **{target_ref}**.\n"
            f"Your balance: **{fmt_money(new_thief_bal)}** | {target_ref}'s balance: **{fmt_money(new_target_bal)}**",
            allowed_mentions=allowed,
        )
        return

    roll = random.random()
    if roll < 0.95:
        set_jailed(ctx.author.id, True)
        await send_reply(
            ctx,
            "Steal attempt: **FAILED**.\n"
            "You got caught and went to **jail**.\n"
            f"You can only use `{PREFIX}getoutofjail` now (cost: **12%** of your money).",
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return

    loss = int(math.floor(thief_bal * 0.75))
    if loss <= 0 and thief_bal > 0:
        loss = 1

    if loss > 0:
        add_balance(ctx.author.id, -loss)

    row2 = get_user(ctx.author.id)
    new_bal = int(row2[2]) if row2 else 0

    await send_reply(
        ctx,
        "Steal attempt: **FAILED**.\n"
        f"Catastrophic outcome: you lost **{fmt_money(loss)}** Marcus Money (**75%**).\n"
        f"New balance: **{fmt_money(new_bal)}**",
        allowed_mentions=discord.AllowedMentions.none(),
    )

@bot.command(name="getoutofjail")
async def getoutofjail_cmd(ctx: commands.Context):
    if not await require_activated(ctx):
        return

    jailed, _ts = get_jail_status(ctx.author.id)
    if not jailed:
        await send_reply(ctx, "You are not in jail.")
        return

    row = get_user(ctx.author.id)
    bal = int(row[2]) if row else 0

    cost = int(math.floor(bal * 0.12))
    if cost < 0:
        cost = 0

    if cost > 0:
        add_balance(ctx.author.id, -cost)

    set_jailed(ctx.author.id, False)
    set_parole(ctx.author.id, True)

    row2 = get_user(ctx.author.id)
    new_bal = int(row2[2]) if row2 else 0

    await send_reply(
        ctx,
        f"You paid **{fmt_money(cost)}** Marcus Money (**12%**) and got out of jail.\n"
        f"New balance: **{fmt_money(new_bal)}**\n"
        f"Parole: **1 hour** — every **10 minutes** you pay **6%** into the Powerball pool."
    )


@bot.check
async def block_commands_when_jailed(ctx: commands.Context) -> bool:
    if ctx.command is None:
        return True
    allowed = {"getoutofjail"}
    if ctx.command.name in allowed:
        return True

    jailed, _jail_ts = get_jail_status(ctx.author.id)
    if jailed:
        raise commands.CheckFailure(
            f"You are in **jail**. You cannot use commands right now.\n"
            f"Use `{PREFIX}getoutofjail` to pay **20%** of your money and get released."
        )
    return True

@bot.event
async def on_command_error(ctx: commands.Context, error: Exception):
    if isinstance(error, commands.CheckFailure):
        await send_reply(ctx, str(error))
        return
    raise error

# ============================================================
# Lottery commands
# ============================================================

@bot.command(name="ticket", aliases=["lottery", "pb"])
@commands.guild_only()
async def powerball_cmd(ctx: commands.Context):
    if not await require_activated(ctx):
        return

    main_nums, pb = _quickpick_ticket()

    ok, why = _validate_lottery_numbers(main_nums, pb)
    if not ok:
        await send_reply(ctx, why)
        return

    row = get_user(ctx.author.id)
    bal = int(row[2]) if row else 0
    if LOTTERY_TICKET_COST > bal:
        await send_reply(ctx, f"Ticket costs **{fmt_money(LOTTERY_TICKET_COST)}** MM. You only have **{fmt_money(bal)}**.")
        return

    now = int(time.time())

    with db_lock:
        conn = db_connect()
        try:
            conn.execute(
                "UPDATE users SET balance = balance - ? WHERE user_id = ?",
                (int(LOTTERY_TICKET_COST), int(ctx.author.id)),
            )

            pool = _get_lottery_pool_conn(conn)
            pool += int(LOTTERY_TICKET_COST)
            _set_lottery_pool_conn(conn, pool)

            conn.execute(
                """
                INSERT INTO lottery_tickets (user_id, n1, n2, n3, n4, n5, pb, bought_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(ctx.author.id),
                    int(main_nums[0]),
                    int(main_nums[1]),
                    int(main_nums[2]),
                    int(main_nums[3]),
                    int(main_nums[4]),
                    int(pb),
                    int(now),
                ),
            )

            conn.commit()
            new_bal = int(
                conn.execute("SELECT balance FROM users WHERE user_id = ?", (int(ctx.author.id),)).fetchone()[0]
            )
        finally:
            conn.close()

    await send_reply(
        ctx,
        f"Powerball ticket purchased for **{fmt_money(LOTTERY_TICKET_COST)}** MM.\n"
        f"Your numbers: **{_fmt_ticket(main_nums, pb)}**\n"
        f"Pool is now: **{fmt_money(pool)}** MM\n"
        f"Your balance: **{fmt_money(new_bal)}** MM"
    )

@bot.command(name="draw", aliases=["lotterydraw", "pbdraw", "drawlottery"])
@commands.guild_only()
async def powerball_draw_cmd(ctx: commands.Context):
    if not is_owner(ctx):
        await send_reply(ctx, "You are not allowed to use this command.")
        return

    with db_lock:
        conn = db_connect()
        try:
            pool = _get_lottery_pool_conn(conn)
            tickets = conn.execute(
                "SELECT id, user_id, n1, n2, n3, n4, n5, pb FROM lottery_tickets ORDER BY id ASC"
            ).fetchall()
        finally:
            conn.close()

    if not tickets:
        await send_reply(ctx, "No lottery tickets have been purchased yet.")
        return

    win_main = [random.randint(LOTTERY_MAIN_MIN, LOTTERY_MAIN_MAX) for _ in range(5)]
    win_pb = random.randint(LOTTERY_PB_MIN, LOTTERY_PB_MAX)

    def render_draw(revealed: int, locked_main: List[Optional[int]], locked_pb: Optional[int]) -> str:
        main_disp = []
        for i in range(5):
            v = locked_main[i]
            main_disp.append(str(v) if v is not None else "?")
        pb_disp = str(locked_pb) if locked_pb is not None else "?"
        return (
            "**POWERBALL DRAW**\n"
            f"Pool: **{fmt_money(pool)}** MM\n\n"
            f"Main: **{' '.join(main_disp)}**\n"
            f"Powerball: **{pb_disp}**"
        )

    locked_main: List[Optional[int]] = [None, None, None, None, None]
    locked_pb: Optional[int] = None

    msg = await send_reply(ctx, render_draw(0, locked_main, locked_pb))

    for i in range(5):
        for _ in range(LOTTERY_ANIM_TICKS_PER_BALL):
            locked_main[i] = random.randint(LOTTERY_MAIN_MIN, LOTTERY_MAIN_MAX)
            try:
                await msg.edit(content=render_draw(i, locked_main, locked_pb))
            except discord.HTTPException:
                break
            await asyncio.sleep(LOTTERY_ANIM_DELAY)
        locked_main[i] = win_main[i]
        try:
            await msg.edit(content=render_draw(i + 1, locked_main, locked_pb))
        except discord.HTTPException:
            pass
        await asyncio.sleep(LOTTERY_ANIM_DELAY)

    for _ in range(LOTTERY_ANIM_TICKS_PER_BALL):
        locked_pb = random.randint(LOTTERY_PB_MIN, LOTTERY_PB_MAX)
        try:
            await msg.edit(content=render_draw(5, locked_main, locked_pb))
        except discord.HTTPException:
            break
        await asyncio.sleep(LOTTERY_ANIM_DELAY)
    locked_pb = win_pb
    try:
        await msg.edit(content=render_draw(6, locked_main, locked_pb))
    except discord.HTTPException:
        pass

    tier_buckets = {5: [], 4: [], 3: [], 2: []}
    jackpot_users: List[int] = []

    for _tid, uid, n1, n2, n3, n4, n5, pb in tickets:
        t_main = [n1, n2, n3, n4, n5]
        mm = _count_main_matches(t_main, win_main)
        pb_match = (int(pb) == int(win_pb))

        prize, is_jackpot = _prize_for_match(mm, pb_match)
        if prize <= 0 and not is_jackpot:
            continue

        if is_jackpot:
            jackpot_users.append(int(uid))
        else:
            if mm in tier_buckets:
                tier_buckets[int(mm)].append((int(uid), int(prize)))

    pool_remaining = int(pool)
    small_paid: Dict[int, int] = {}

    for tier in [5, 4, 3, 2]:
        if pool_remaining <= 0:
            break
        bucket = tier_buckets[tier]
        if not bucket:
            continue

        tier_total = sum(p for _uid, p in bucket)
        if tier_total <= pool_remaining:
            for uid, p in bucket:
                small_paid[uid] = small_paid.get(uid, 0) + int(p)
            pool_remaining -= int(tier_total)
        else:
            if tier_total <= 0:
                break
            scaled_rows = []
            for uid, p in bucket:
                scaled = int(math.floor(pool_remaining * (p / tier_total)))
                scaled_rows.append([uid, p, scaled])

            base_sum = sum(r[2] for r in scaled_rows)
            rem = pool_remaining - base_sum
            scaled_rows.sort(key=lambda r: (r[0],))
            for i in range(int(rem)):
                scaled_rows[i % len(scaled_rows)][2] += 1

            for uid, _p, scaled in scaled_rows:
                if scaled > 0:
                    small_paid[uid] = small_paid.get(uid, 0) + int(scaled)

            pool_remaining = 0
            break

        # --- Jackpot payout (TAXED) ---
    jackpot_paid_gross: Dict[int, int] = {}
    jackpot_paid_net: Dict[int, int] = {}
    jackpot_tax_paid: Dict[int, int] = {}

    jackpot_tax_total = 0
    jackpot_rebate_total = 0

    jackpot_total = int(pool_remaining) if jackpot_users else 0

    if jackpot_users and jackpot_total > 0:
        uniq = sorted(set(jackpot_users))
        share = jackpot_total // len(uniq)
        rem = jackpot_total - (share * len(uniq))

        for i, uid in enumerate(uniq):
            gross = int(share + (1 if i < rem else 0))
            if gross <= 0:
                continue

            tax = int(math.floor(gross * float(JACKPOT_TAX_RATE)))
            if tax < 0:
                tax = 0
            if tax > gross:
                tax = gross

            rebate = int(math.floor(tax * float(JACKPOT_TAX_REBATE_RATE)))
            if rebate < 0:
                rebate = 0
            if rebate > tax:
                rebate = tax

            net = gross - tax

            jackpot_paid_gross[int(uid)] = gross
            jackpot_tax_paid[int(uid)] = tax
            if net > 0:
                jackpot_paid_net[int(uid)] = net

            jackpot_tax_total += int(tax)
            jackpot_rebate_total += int(rebate)

        # Pool is consumed by jackpot payout, but rebate creates a new pool seed
        pool_remaining = int(jackpot_rebate_total)


    with db_lock:
        conn = db_connect()
        try:
            _set_lottery_pool_conn(conn, int(pool_remaining))

            for uid, amt in small_paid.items():
                conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (int(amt), int(uid)))

            # Pay NET jackpot after tax
            for uid, amt in jackpot_paid_net.items():
                conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (int(amt), int(uid)))


            conn.execute("DELETE FROM lottery_tickets")
            conn.commit()
        finally:
            conn.close()

    final_lines = [
        "**POWERBALL RESULTS**",
        f"Winning main: **{' '.join(str(x) for x in win_main)}**",
        f"Winning powerball: **{win_pb}**",
        "",
        f"Small-winner payouts paid: **{fmt_money(sum(small_paid.values()))}** MM",
    ]
    if jackpot_users:
        final_lines.append(f"Jackpot gross paid: **{fmt_money(sum(jackpot_paid_gross.values()))}** MM")
        final_lines.append(f"Jackpot tax (40%): **{fmt_money(jackpot_tax_total)}** MM")
        final_lines.append(f"Rebate to pool (30% of tax): **{fmt_money(jackpot_rebate_total)}** MM")
        final_lines.append(f"Jackpot net paid: **{fmt_money(sum(jackpot_paid_net.values()))}** MM")

    final_lines.append(f"Pool now: **{fmt_money(pool_remaining)}** MM")


    await send_reply(ctx, "\n".join(final_lines), allowed_mentions=discord.AllowedMentions.none())

    all_winner_ids = sorted(set([uid for uid, amt in small_paid.items() if amt > 0] + [uid for uid, amt in jackpot_paid_gross.items() if amt > 0]))
    if all_winner_ids:
        mention_list = " ".join(f"<@{uid}>" for uid in all_winner_ids)
        details = []
        for uid in all_winner_ids:
            parts = []
            if small_paid.get(uid, 0) > 0:
                parts.append(f"small **{fmt_money(small_paid[uid])}**")
            if jackpot_paid_gross.get(uid, 0) > 0:
                gross = jackpot_paid_gross.get(uid, 0)
                tax = jackpot_tax_paid.get(uid, 0)
                net = jackpot_paid_net.get(uid, 0)
                parts.append(f"JACKPOT **{fmt_money(net)}** (gross {fmt_money(gross)}, tax {fmt_money(tax)})")

            details.append(f"<@{uid}>: " + ", ".join(parts))

        await send_reply(
            ctx,
            f"**WINNERS** {mention_list}\n" + "\n".join(details),
            allowed_mentions=discord.AllowedMentions(users=[discord.Object(id=uid) for uid in all_winner_ids]),
        )
    else:
        await send_reply(ctx, "No winners this draw.", allowed_mentions=discord.AllowedMentions.none())

def _chunk_lines(lines: List[str], max_len: int = 1900) -> List[str]:
    """
    Discord messages should stay under 2000 chars.
    We use 1900 to leave room for formatting.
    """
    chunks: List[str] = []
    cur = ""
    for line in lines:
        add = (line + "\n")
        if len(cur) + len(add) > max_len:
            if cur.strip():
                chunks.append(cur.rstrip())
            cur = add
        else:
            cur += add
    if cur.strip():
        chunks.append(cur.rstrip())
    return chunks


@bot.command(name="mytickets", aliases=["tickets", "myticket"])
@commands.guild_only()
async def mytickets_cmd(ctx: commands.Context):
    if not await require_activated(ctx):
        return

    with db_lock:
        conn = db_connect()
        try:
            rows = conn.execute(
                """
                SELECT id, n1, n2, n3, n4, n5, pb, bought_ts
                FROM lottery_tickets
                WHERE user_id = ?
                ORDER BY id ASC
                """,
                (int(ctx.author.id),),
            ).fetchall()

            pool = _get_lottery_pool_conn(conn)
        finally:
            conn.close()

    if not rows:
        await send_reply(ctx, "You have no active Powerball tickets right now.")
        return

    lines: List[str] = []
    lines.append(f"**Your Powerball Tickets** — Count: **{len(rows)}**")
    lines.append(f"Current pool: **{fmt_money(int(pool))}** MM")
    lines.append("")

    # List each ticket
    for tid, n1, n2, n3, n4, n5, pb, bought_ts in rows:
        ticket_str = _fmt_ticket([n1, n2, n3, n4, n5], pb)
        when = format_ts(int(bought_ts))
        lines.append(f"**#{int(tid)}** — **{ticket_str}** — {when}")

    # Send in chunks if long
    for chunk in _chunk_lines(lines, max_len=1900):
        await send_reply(ctx, chunk)



# ============================================================
# Core economy commands
# ============================================================

@bot.command(name="activate")
async def activate_cmd(ctx: commands.Context):
    existing = get_user(ctx.author.id)
    if existing is not None:
        update_username(ctx.author.id, display_name(ctx.author))
        await send_reply(ctx, "You are already activated. Your stored name has been refreshed.")
        return
    insert_user(ctx.author.id, display_name(ctx.author))
    await send_reply(
        ctx,
        f"Activated. Starting balance: **{fmt_money(START_BALANCE)}** Marcus Money.\n"
        f"Daily bonus: **+{fmt_money(DAILY_CREDITS)}** Marcus Money every 24 hours (`{PREFIX}daily`)."
    )

@bot.command(name="balance")
async def balance_cmd(ctx: commands.Context):
    if not await require_activated(ctx):
        return
    added = apply_daily_if_due(ctx.author.id)
    row = get_user(ctx.author.id)
    bal = int(row[2])
    msg = f"Balance: **{fmt_money(bal)}** Marcus Money."
    if added:
        msg += f" Daily bonus applied: **+{fmt_money(added)}**."
    await send_reply(ctx, msg)

@bot.command(name="daily")
async def daily_cmd(ctx: commands.Context):
    if not await require_activated(ctx):
        return
    row = get_user(ctx.author.id)
    user_id, _, balance, last_daily = row
    now = int(time.time())
    if now - int(last_daily) >= DAILY_SECONDS:
        new_balance = int(balance) + DAILY_CREDITS
        with db_lock:
            conn = db_connect()
            try:
                conn.execute(
                    "UPDATE users SET balance = ?, last_daily = ? WHERE user_id = ?",
                    (int(new_balance), now, int(user_id)),
                )
                conn.commit()
            finally:
                conn.close()
        await send_reply(ctx, f"Daily claimed: **+{fmt_money(DAILY_CREDITS)}**. New balance: **{fmt_money(new_balance)}** Marcus Money.")
    else:
        remaining = DAILY_SECONDS - (now - int(last_daily))
        hrs = remaining // 3600
        mins = (remaining % 3600) // 60
        secs = remaining % 60
        await send_reply(ctx, f"Daily not ready. Try again in **{hrs}h {mins}m {secs}s**.")

@bot.command(name="slot")
async def slot_cmd(ctx: commands.Context, bet: str):
    try:
        bet_i = parse_amount_int(bet, min_value=1)
    except ValueError:
        await send_reply(ctx, f"Bet must be a positive number. Examples: `{PREFIX}slot 10`, `{PREFIX}slot 2K`, `{PREFIX}slot 1.5M`")
        return

    if not await require_activated(ctx):
        return

    added = apply_daily_if_due(ctx.author.id)
    row = get_user(ctx.author.id)
    bal = int(row[2])
    if bet_i > bal:
        await send_reply(ctx, f"You only have **{fmt_money(bal)}** Marcus Money. Your bet (**{fmt_money(bet_i)}**) is too large.")
        return

    n = spin()
    mult = WIN_MULTIPLIERS.get(n)
    if mult is not None:
        win_amount = bet_i * mult
        new_bal = bal + win_amount
        set_balance(ctx.author.id, new_bal)
        msg = (
            f"🎰 **{format_two_digits(n)}**\n"
            f"WIN! Payout: **+{fmt_money(win_amount)}** (x{mult})\n"
            f"New balance: **{fmt_money(new_bal)}** Marcus Money."
        )
    else:
        new_bal = bal - bet_i
        set_balance(ctx.author.id, new_bal)
        msg = (
            f"🎰 **{format_two_digits(n)}**\n"
            f"Loss: **-{fmt_money(bet_i)}** Marcus Money.\n"
            f"New balance: **{fmt_money(new_bal)}** Marcus Money."
        )
    if added:
        msg += f"\nDaily bonus applied: **+{fmt_money(added)}**."
    await send_reply(ctx, msg)

@bot.command(name="leaders")
async def leaderboard_cmd(ctx: commands.Context):
    rows = get_top_users(TOP_N)
    if not rows:
        await send_reply(ctx, f"No activated players yet. Run `{PREFIX}activate` to join.")
        return
    lines = []
    for i, (username, balance) in enumerate(rows, start=1):
        lines.append(f"**{i}.** {username} — **{fmt_money(int(balance))}**  ̷M̷")
    await send_reply(ctx, "\n".join(lines))

# -----------------------------
# Tax Commands
# -----------------------------
@bot.command(name="tax_status")
async def tax_status_cmd(ctx: commands.Context):
    tz = ZoneInfo(TAX_TIMEZONE)
    now_dt = datetime.now(tz)
    date_key = now_dt.strftime("%Y-%m-%d")
    wd = now_dt.weekday()
    with db_lock:
        conn = db_connect()
        try:
            last_tax_date = _get_state(conn, "last_tax_date", "")
        finally:
            conn.close()

    eligible = (wd in TAX_WEEKDAYS)
    await send_reply(
        ctx,
        f"Tax timezone: **{TAX_TIMEZONE}**\n"
        f"Today: **{date_key}** | Eligible day: **{'YES' if eligible else 'NO'}**\n"
        f"Last tax date: **{last_tax_date or 'never'}**"
    )

@bot.command(name="tax_now")
@commands.guild_only()
async def tax_now_cmd(ctx: commands.Context):
    if not is_owner(ctx):
        await send_reply(ctx, "You are not allowed to use this command.")
        return
    res = run_tax_if_due()
    if not res:
        await send_reply(ctx, "Tax not run (either not an eligible day, or it already ran today).")
        return
    date_key, users_taxed, total_tax = res
    await send_reply(ctx, f"Tax run for **{date_key}**: taxed **{users_taxed}** users, collected **{fmt_money(total_tax)}** MM.")

# -----------------------------
# Gift commands
# -----------------------------
@bot.command(name="gift")
@commands.guild_only()
async def gift_cmd(ctx: commands.Context, member: discord.Member, amount: str):
    try:
        amount_i = parse_amount_int(amount, min_value=1)
    except ValueError:
        await send_reply(ctx, f"Usage: `{PREFIX}gift @User 500` (supports 2K/3M/1B/...)")
        return

    if not await require_activated(ctx):
        return
    if member.bot:
        await send_reply(ctx, "You can't gift money to bots.")
        return
    if member.id == ctx.author.id:
        await send_reply(ctx, "You can't gift money to yourself.")
        return
    if get_user(member.id) is None:
        await send_reply(ctx, f"That user is not activated yet. They must run `{PREFIX}activate` first.")
        return

    update_username(ctx.author.id, display_name(ctx.author))
    update_username(member.id, member.display_name)

    sender_id = int(ctx.author.id)
    recipient_id = int(member.id)

    with db_lock:
        conn = db_connect()
        try:
            srow = conn.execute("SELECT balance FROM users WHERE user_id = ?", (sender_id,)).fetchone()
            rrow = conn.execute("SELECT balance FROM users WHERE user_id = ?", (recipient_id,)).fetchone()
            if not srow:
                await send_reply(ctx, f"You are not activated yet. Run `{PREFIX}activate` first.")
                return
            if not rrow:
                await send_reply(ctx, f"That user is not activated yet. They must run `{PREFIX}activate` first.")
                return

            sender_bal = int(srow[0])
            if amount_i > sender_bal:
                await send_reply(ctx, f"You only have **{fmt_money(sender_bal)}** Marcus Money. You can’t gift **{fmt_money(amount_i)}**.")
                return

            conn.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (int(amount_i), sender_id))
            conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (int(amount_i), recipient_id))
            conn.commit()

            sender_new_bal = int(conn.execute("SELECT balance FROM users WHERE user_id = ?", (sender_id,)).fetchone()[0])
            recip_new_bal = int(conn.execute("SELECT balance FROM users WHERE user_id = ?", (recipient_id,)).fetchone()[0])
        finally:
            conn.close()

    await send_reply(
        ctx,
        f"Gift sent: **{fmt_money(amount_i)}** Marcus Money from **{display_name(ctx.author)}** to **{member.display_name}**.\n"
        f"Your balance: **{fmt_money(sender_new_bal)}** | {member.display_name}'s balance: **{fmt_money(recip_new_bal)}**"
    )

@gift_cmd.error
async def gift_cmd_error(ctx: commands.Context, error: Exception):
    if isinstance(error, commands.BadArgument):
        await send_reply(ctx, f"Usage: `{PREFIX}gift @User 500`")
    else:
        raise error

@bot.command(name="giftall")
@commands.guild_only()
async def giftall_cmd(ctx: commands.Context, amount: str, members: commands.Greedy[discord.Member]):
    try:
        amount_i = parse_amount_int(amount, min_value=1)
    except ValueError:
        await send_reply(ctx, f"Usage: `{PREFIX}giftall 100 @User1 @User2` (supports 2K/3M/1B/...)")
        return

    if not await require_activated(ctx):
        return
    if not members:
        await send_reply(ctx, f"Usage: `{PREFIX}giftall <amount> @User1 @User2 ...`")
        return

    seen: Set[int] = set()
    recipients: List[discord.Member] = []
    for m in members:
        if m.bot or m.id == ctx.author.id or m.id in seen:
            continue
        seen.add(m.id)
        recipients.append(m)

    if not recipients:
        await send_reply(ctx, "No valid recipients. (You can’t gift bots or yourself.)")
        return

    not_activated = [m.display_name for m in recipients if get_user(m.id) is None]
    if not_activated:
        await send_reply(ctx, f"These users are not activated yet (they must run `{PREFIX}activate`): " + ", ".join(not_activated))
        return

    total_cost = int(amount_i) * len(recipients)

    update_username(ctx.author.id, display_name(ctx.author))
    for m in recipients:
        update_username(m.id, m.display_name)

    sender_id = int(ctx.author.id)
    recipient_ids = [int(m.id) for m in recipients]

    with db_lock:
        conn = db_connect()
        try:
            srow = conn.execute("SELECT balance FROM users WHERE user_id = ?", (sender_id,)).fetchone()
            if not srow:
                await send_reply(ctx, f"You are not activated yet. Run `{PREFIX}activate` first.")
                return

            sender_bal = int(srow[0])
            if total_cost > sender_bal:
                await send_reply(ctx, f"You only have **{fmt_money(sender_bal)}** Marcus Money. `giftall` would cost **{fmt_money(total_cost)}**.")
                return

            conn.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (int(total_cost), sender_id))
            for rid in recipient_ids:
                conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (int(amount_i), rid))
            conn.commit()

            sender_new_bal = int(conn.execute("SELECT balance FROM users WHERE user_id = ?", (sender_id,)).fetchone()[0])
        finally:
            conn.close()

    names = ", ".join(m.display_name for m in recipients)
    await send_reply(
        ctx,
        f"Giftall sent: **{fmt_money(amount_i)}** each to **{len(recipients)}** users (total **{fmt_money(total_cost)}**).\n"
        f"Recipients: {names}\n"
        f"Your balance: **{fmt_money(sender_new_bal)}**"
    )

@bot.command(name="gifteveryone")
@commands.guild_only()
async def gifteveryone_cmd(ctx: commands.Context, amount: str, ping: str = "noping"):
    try:
        amount_i = parse_amount_int(amount, min_value=1)
    except ValueError:
        await send_reply(ctx, f"Usage: `{PREFIX}gifteveryone 100 ping` (supports 2K/3M/1B/...)")
        return

    if not await require_activated(ctx):
        return

    p = (ping or "noping").strip().lower()
    do_ping = p in ("ping", "pings", "yes", "y", "true", "t", "1")
    if p not in ("ping", "pings", "yes", "y", "true", "t", "1", "noping", "nopings", "no", "n", "false", "f", "0"):
        await send_reply(ctx, f"Usage: `{PREFIX}gifteveryone <amount> [ping|noping]`")
        return

    guild = ctx.guild
    try:
        members = [m async for m in guild.fetch_members(limit=None)]
    except Exception:
        members = list(getattr(guild, "members", []))

    if not members:
        await send_reply(ctx, "I couldn't read the server member list. Enable Server Members Intent in Developer Portal and keep intents.members=True.")
        return

    activated_ids = get_all_activated_user_ids()
    recipients: List[discord.Member] = []
    for m in members:
        if m.bot or m.id == ctx.author.id:
            continue
        if int(m.id) in activated_ids:
            recipients.append(m)

    if not recipients:
        await send_reply(ctx, "No activated users found in this server (besides you).")
        return

    sender_id = int(ctx.author.id)
    recipient_ids = [int(m.id) for m in recipients]
    total_cost = int(amount_i) * len(recipient_ids)

    update_pairs = [(display_name(ctx.author), sender_id)] + [(m.display_name, int(m.id)) for m in recipients]

    with db_lock:
        conn = db_connect()
        try:
            srow = conn.execute("SELECT balance FROM users WHERE user_id = ?", (sender_id,)).fetchone()
            if not srow:
                await send_reply(ctx, f"You are not activated yet. Run `{PREFIX}activate` first.")
                return

            sender_bal = int(srow[0])
            if total_cost > sender_bal:
                await send_reply(ctx, f"You only have **{fmt_money(sender_bal)}** Marcus Money. `gifteveryone` would cost **{fmt_money(total_cost)}**.")
                return

            conn.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (int(total_cost), sender_id))
            conn.executemany(
                "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                [(int(amount_i), rid) for rid in recipient_ids],
            )
            conn.executemany("UPDATE users SET username = ? WHERE user_id = ?", update_pairs)
            conn.commit()

            sender_new_bal = int(conn.execute("SELECT balance FROM users WHERE user_id = ?", (sender_id,)).fetchone()[0])
        finally:
            conn.close()

    await send_reply(
        ctx,
        f"GiftEveryone sent: **{fmt_money(amount_i)}** Marcus Money each to **{len(recipient_ids)}** users "
        f"(total **{fmt_money(total_cost)}**).\n"
        f"Your new balance: **{fmt_money(sender_new_bal)}**\n"
        f"Pinging: **{'ON' if do_ping else 'OFF'}**"
    )

    if do_ping:
        for chunk in chunk_mentions(recipient_ids):
            await send_reply(ctx, chunk)

# -----------------------------
# Blackjack Commands
# -----------------------------
@bot.command(name="blackjack", aliases=["bj"])
async def blackjack_cmd(ctx: commands.Context, bet: str):
    try:
        bet_i = parse_amount_int(bet, min_value=1)
    except ValueError:
        await send_reply(ctx, f"Bet must be a positive number. Example: `{PREFIX}blackjack 25`")
        return

    if not await require_activated(ctx):
        return

    if ctx.author.id in BLACKJACK_GAMES:
        await send_reply(ctx, f"You already have an active blackjack hand. Use `{PREFIX}hit`, or `{PREFIX}stand`")
        return

    row = get_user(ctx.author.id)
    bal = int(row[2])
    if bet_i > bal:
        await send_reply(ctx, f"You only have **{fmt_money(bal)}** Marcus Money. Your bet (**{fmt_money(bet_i)}**) is too large.")
        return

    add_balance(ctx.author.id, -bet_i)

    player = [bj_draw_card(), bj_draw_card()]
    dealer = [bj_draw_card(), bj_draw_card()]
    game = {"bet": bet_i, "player": player, "dealer": dealer, "ts": int(time.time())}
    BLACKJACK_GAMES[ctx.author.id] = game

    pval = bj_hand_value(player)
    d_up = dealer[0]

    if bj_is_blackjack(player):
        dval = bj_hand_value(dealer)
        if bj_is_blackjack(dealer):
            add_balance(ctx.author.id, bet_i)
            del BLACKJACK_GAMES[ctx.author.id]
            row2 = get_user(ctx.author.id)
            await send_reply(ctx, f"**Blackjack**\nYour hand: {bj_fmt(player)} (**21**)\nDealer hand: {bj_fmt(dealer)} (**{dval}**)\nResult: **PUSH**. Bet refunded.\nBalance: **{fmt_money(int(row2[2]))}**")
            return
        else:
            profit = int(bet_i * 6)
            add_balance(ctx.author.id, bet_i + profit)
            del BLACKJACK_GAMES[ctx.author.id]
            row2 = get_user(ctx.author.id)
            await send_reply(ctx, f"**Blackjack**\nYour hand: {bj_fmt(player)} (**21**)\nDealer shows: {d_up} [?]\nResult: **BLACKJACK WIN**. Profit: **+{fmt_money(profit)}**\nBalance: **{fmt_money(int(row2[2]))}**")
            return

    row2 = get_user(ctx.author.id)
    await send_reply(
        ctx,
        f"**Blackjack** (Bet: **{fmt_money(bet_i)}**)\n"
        f"Your hand: {bj_fmt(player)} (**{pval}**)\n"
        f"Dealer shows: {d_up} [?]\n"
        f"Use `{PREFIX}hit` or `{PREFIX}stand`.\n"
        f"Balance: **{fmt_money(int(row2[2]))}**"
    )

@bot.command(name="hit")
async def hit_cmd(ctx: commands.Context):
    if not await require_activated(ctx):
        return
    game = BLACKJACK_GAMES.get(ctx.author.id)
    if not game:
        await send_reply(ctx, f"You have no active blackjack hand. Start one with `{PREFIX}blackjack <bet>`.")
        return

    game["player"].append(bj_draw_card())
    pval = bj_hand_value(game["player"])
    d_up = game["dealer"][0]

    if pval > 21:
        bet_i = int(game["bet"])
        del BLACKJACK_GAMES[ctx.author.id]
        row2 = get_user(ctx.author.id)
        await send_reply(ctx, f"**Blackjack**\nYour hand: {bj_fmt(game['player'])} (**{pval}**)\nDealer shows: {d_up} [?]\nResult: **BUST**. You lose **{fmt_money(bet_i)}**.\nBalance: **{fmt_money(int(row2[2]))}**")
        return

    await send_reply(ctx, f"**Blackjack**\nYour hand: {bj_fmt(game['player'])} (**{pval}**)\nDealer shows: {d_up} [?]\nUse `{PREFIX}hit` or `{PREFIX}stand`.")

@bot.command(name="stand")
async def stand_cmd(ctx: commands.Context):
    if not await require_activated(ctx):
        return
    game = BLACKJACK_GAMES.get(ctx.author.id)
    if not game:
        await send_reply(ctx, f"You have no active blackjack hand. Start one with `{PREFIX}blackjack <bet>`.")
        return

    player = game["player"]
    dealer = game["dealer"]
    bet_i = int(game["bet"])
    pval = bj_hand_value(player)

    while bj_hand_value(dealer) < 17:
        dealer.append(bj_draw_card())
    dval = bj_hand_value(dealer)

    payout = 0
    if dval > 21 or pval > dval:
        payout = int(round(bet_i * 2.6))
        result = "YOU WIN"
    elif pval < dval:
        payout = 0
        result = "YOU LOSE"
    else:
        payout = bet_i
        result = "PUSH"

    if payout:
        add_balance(ctx.author.id, payout)

    del BLACKJACK_GAMES[ctx.author.id]
    row2 = get_user(ctx.author.id)

    if payout and payout != bet_i:
        profit = payout - bet_i
        payout_line = f"Payout: **+{fmt_money(profit)}** profit (stake returned)."
    elif payout == bet_i:
        payout_line = "Bet refunded."
    else:
        payout_line = f"Lost: **-{fmt_money(bet_i)}**."

    await send_reply(
        ctx,
        f"**Blackjack**\nYour hand: {bj_fmt(player)} (**{pval}**)\nDealer hand: {bj_fmt(dealer)} (**{dval}**)\n"
        f"Result: **{result}**\n{payout_line}\nBalance: **{fmt_money(int(row2[2]))}**"
    )

# -----------------------------
# Roulette Commands
# -----------------------------
@bot.command(name="roulette", aliases=["roul"])
async def roulette_cmd(ctx: commands.Context, bet: str, *, choice: str):
    try:
        bet_i = parse_amount_int(bet, min_value=1)
    except ValueError:
        await send_reply(ctx, f"Bet must be a positive number. Example: `{PREFIX}roulette 25 red`")
        return

    if not await require_activated(ctx):
        return

    ok, label, rule = roulette_parse_choice(choice)
    if not ok:
        await send_reply(ctx, f"Invalid roulette choice. Examples: `{PREFIX}roulette 50 red`, `{PREFIX}roulette 50 17`, `{PREFIX}roulette 50 even`, `{PREFIX}roulette 50 dozen2`, `{PREFIX}roulette 50 col3`")
        return

    row = get_user(ctx.author.id)
    bal = int(row[2])
    if bet_i > bal:
        await send_reply(ctx, f"You only have **{fmt_money(bal)}** Marcus Money. Your bet (**{fmt_money(bet_i)}**) is too large.")
        return

    add_balance(ctx.author.id, -bet_i)

    spin_n = random.randint(0, 36)
    color = roulette_color(spin_n)
    won = roulette_is_win(spin_n, rule)
    payout_mult = int(rule["payout"])

    if won:
        profit = bet_i * payout_mult
        add_balance(ctx.author.id, bet_i + profit)
        row2 = get_user(ctx.author.id)
        await send_reply(ctx, f"**Roulette** (Bet: **{fmt_money(bet_i)}** on **{label}**)\nSpin: **{spin_n}** ({color})\nResult: **WIN** — Profit: **+{fmt_money(profit)}**\nBalance: **{fmt_money(int(row2[2]))}**")
    else:
        row2 = get_user(ctx.author.id)
        await send_reply(ctx, f"**Roulette** (Bet: **{fmt_money(bet_i)}** on **{label}**)\nSpin: **{spin_n}** ({color})\nResult: **LOSS** — Lost: **-{fmt_money(bet_i)}**\nBalance: **{fmt_money(int(row2[2]))}**")

# -----------------------------
# Plinko Commands
# -----------------------------
@bot.command(name="plinko")
async def plinko_cmd(ctx: commands.Context, bet_per_ball: str, balls: int):
    try:
        bet_per_ball_i = parse_amount_int(bet_per_ball, min_value=1)
    except ValueError:
        await send_reply(ctx, f"Bet must be a positive number. Example: `{PREFIX}plinko 10K 5`")
        return

    if balls <= 0:
        await send_reply(ctx, f"Balls must be a positive whole number. Example: `{PREFIX}plinko 10000 5`")
        return
    if balls > MAX_PLINKO_BALLS:
        await send_reply(ctx, f"Too many balls. Max is **{MAX_PLINKO_BALLS}** to avoid rate limits.")
        return
    if not await require_activated(ctx):
        return

    total_cost = int(bet_per_ball_i) * int(balls)

    row = get_user(ctx.author.id)
    bal = int(row[2])
    if total_cost > bal:
        await send_reply(
            ctx,
            f"You only have **{fmt_money(bal)}** Marcus Money.\n"
            f"Total cost is **{fmt_money(total_cost)}** ({fmt_money(bet_per_ball_i)} x {balls})."
        )
        return

    add_balance(ctx.author.id, -total_cost)

    SLOTS = 13
    ROWS = 6
    multipliers = [10, 5, 2.1, 1.5, 0.8, 0.6, 0.4, 0.6, 0.8, 1.5, 2.1, 5, 10]

    paths: List[List[int]] = []
    landings: List[int] = []

    for _ in range(balls):
        pos = SLOTS // 2
        path = [pos]
        for _r in range(ROWS):
            step = 0 if random.random() < 0.20 else random.choice([-1, 1])
            pos = max(0, min(SLOTS - 1, pos + step))
            path.append(pos)
        paths.append(path)
        landings.append(path[-1])

    delay = PLINKO_ANIM_DELAY
    if balls >= 5:
        delay = max(0.20, PLINKO_ANIM_DELAY * 0.30)

    msg = await send_reply(ctx, plinko_render_multi(paths, 0, multipliers, bet_per_ball_i, balls))

    for step in range(1, ROWS + 1):
        await asyncio.sleep(delay)
        try:
            await msg.edit(content=plinko_render_multi(paths, step, multipliers, bet_per_ball_i, balls))
        except discord.HTTPException:
            break

    total_return = 0
    slot_counts: Dict[int, int] = {}
    for landed in landings:
        slot_counts[landed] = slot_counts.get(landed, 0) + 1
        mult = float(multipliers[landed])
        ball_return = int(math.floor(int(bet_per_ball_i) * mult))
        if ball_return > 0:
            total_return += ball_return

    if total_return > 0:
        add_balance(ctx.author.id, total_return)

    profit = total_return - total_cost
    parts = []
    for slot_idx in sorted(slot_counts.keys()):
        count = slot_counts[slot_idx]
        m = float(multipliers[slot_idx])
        parts.append(f"Slot {slot_idx + 1} (x{m:g}): {count}")

    row2 = get_user(ctx.author.id)
    new_bal = int(row2[2]) if row2 else 0

    await send_reply(
        ctx,
        f"Plinko complete: **{balls}** balls at **{fmt_money(bet_per_ball_i)}** each (total **{fmt_money(total_cost)}**).\n"
        f"Returned: **{fmt_money(total_return)}** MM | {'Profit' if profit >= 0 else 'Loss'}: **{fmt_money(abs(profit))}** MM\n"
        f"Distribution: " + ("; ".join(parts) if parts else "N/A") + "\n"
        f"Balance: **{fmt_money(new_bal)}** MM"
    )

# -----------------------------
# Betting commands
# -----------------------------
@bot.command(name="bet_create")
@commands.guild_only()
async def bet_create_cmd(ctx: commands.Context, *, spec: str):
    if not is_owner(ctx):
        await send_reply(ctx, "You are not allowed to use this command.")
        return

    parts = [p.strip() for p in spec.split("|") if p.strip()]
    if len(parts) < 3:
        await send_reply(ctx, f"Usage: `{PREFIX}bet_create Title | Option 1 | Option 2 | ...` (need 2+ options)")
        return

    title = parts[0]
    options = parts[1:]

    bet_id = create_bet(
        guild_id=ctx.guild.id if ctx.guild else None,
        channel_id=ctx.channel.id if ctx.guild else None,
        creator_id=ctx.author.id,
        title=title,
        options=options,
    )

    lines = [f"Created bet **#{bet_id}**: **{title}**", "Options:"]
    for i, opt in enumerate(options, start=1):
        lines.append(f"  **Option {i}.** {opt}")
    lines.append(f"\nUsers wager with: `{PREFIX}bet {bet_id} <option#> <amount>`")
    lines.append(f"Add bonus pool: `{PREFIX}bet_bonus {bet_id} <amount>` (owner only)")
    lines.append(f"View lines with: `{PREFIX}bet_info {bet_id}`")
    await send_reply(ctx, "\n".join(lines))

@bot.command(name="bet_list")
@commands.guild_only()
async def bet_list_cmd(ctx: commands.Context):
    bets = list_open_bets(limit=10)
    if not bets:
        await send_reply(ctx, "No open bets right now.")
        return
    lines = ["**Open Bets**"]
    for bet_id, title, created_ts in bets:
        lines.append(f"**#{bet_id}** — {title} (created {format_ts(int(created_ts))})")
    lines.append(f"\nUse `{PREFIX}bet_info <bet_id>` for options/lines.")
    await send_reply(ctx, "\n".join(lines))

@bot.command(name="bet_info")
@commands.guild_only()
async def bet_info_cmd(ctx: commands.Context, bet_id: int):
    bet = get_bet(bet_id)
    if bet is None:
        await send_reply(ctx, "That bet_id does not exist.")
        return

    _id, title, status, _creator_id, _created_ts, _closed_ts, _resolved_ts, winning_option, bonus_pool, note = bet
    options = get_bet_options(bet_id)
    total_pool, totals = get_bet_totals(bet_id)
    effective_total = int(total_pool) + int(bonus_pool)

    header = [
        f"**Bet #{bet_id}** — **{title}**",
        f"Status: **{status}**",
        f"User pool: **{fmt_money(total_pool)}** | Bonus pool: **{fmt_money(bonus_pool)}** | Effective pool: **{fmt_money(effective_total)}**",
    ]
    if winning_option:
        header.append(f"Winner: **Option {winning_option}**")
    if note:
        header.append(f"Note: {note}")

    lines = ["\n**Lines** (odds include bonus pool)"]
    for opt_num, label in options:
        opt_pool = int(totals.get(int(opt_num), 0))
        odds = american_odds_str(effective_total, opt_pool)
        lines.append(f"**Option {opt_num}** {odds} — {label}")

    footer = [
        "",
        f"Wager: `{PREFIX}bet {bet_id} <option#> <amount>`",
        f"Your wager: `{PREFIX}bet_my {bet_id}`",
    ]
    if is_owner(ctx):
        footer.append(
            f"Owner: `{PREFIX}bet_bonus {bet_id} <amount>` / `{PREFIX}bet_close {bet_id}` / "
            f"`{PREFIX}bet_resolve {bet_id} <option#>` / `{PREFIX}bet_cancel {bet_id}`"
        )

    await send_reply(ctx, "\n".join(header + lines + footer))

@bot.command(name="bet_my")
@commands.guild_only()
async def bet_my_cmd(ctx: commands.Context, bet_id: int):
    if not await require_activated(ctx):
        return

    bet = get_bet(bet_id)
    if bet is None:
        await send_reply(ctx, "That bet_id does not exist.")
        return

    _id, title, status, _creator_id, _created_ts, _closed_ts, _resolved_ts, _winning_option, bonus_pool, _note = bet
    options = dict(get_bet_options(bet_id))

    uw = get_user_wager_for_bet(bet_id, ctx.author.id)
    if uw is None:
        await send_reply(ctx, f"You have no wager on bet **#{bet_id}** yet. Use `{PREFIX}bet {bet_id} <option#> <amount>`.")
        return

    opt_num, amt = uw
    label = options.get(opt_num, "Unknown option")

    total_pool, totals = get_bet_totals(bet_id)
    opt_pool = int(totals.get(opt_num, 0))
    effective_total = int(total_pool) + int(bonus_pool)
    odds = american_odds_str(effective_total, opt_pool)

    await send_reply(
        ctx,
        f"**Bet #{bet_id}** — **{title}**\n"
        f"Status: **{status}**\n"
        f"User pool: **{fmt_money(total_pool)}** | Bonus pool: **{fmt_money(bonus_pool)}** | Effective pool: **{fmt_money(effective_total)}**\n"
        f"Your wager: **{fmt_money(amt)}** on **Option {opt_num}** ({label})\n"
        f"Current line: **{odds}**"
    )

@bot.command(name="bet")
@commands.guild_only()
async def bet_cmd(ctx: commands.Context, bet_id: int, option_num: int, amount: str):
    if not await require_activated(ctx):
        return
    try:
        amount_i = parse_amount_int(amount, min_value=1)
    except ValueError:
        await send_reply(ctx, f"Usage: `{PREFIX}bet {bet_id} <option#> <amount>` (supports 2K/3M/1B/...)")
        return

    ok, msg = place_wager(bet_id, ctx.author.id, option_num, amount_i)
    if not ok:
        await send_reply(ctx, msg)
        return

    bet = get_bet(bet_id)
    bonus_pool = int(bet[8]) if bet else 0
    total_pool, totals = get_bet_totals(bet_id)
    opt_pool = int(totals.get(option_num, 0))
    effective_total = int(total_pool) + int(bonus_pool)
    odds = american_odds_str(effective_total, opt_pool)

    row = get_user(ctx.author.id)
    bal = int(row[2]) if row else 0

    await send_reply(
        ctx,
        f"{msg}\n"
        f"Bonus pool: **{fmt_money(bonus_pool)}** | Effective pool: **{fmt_money(effective_total)}**\n"
        f"Option {option_num} line is now **{odds}**. Your balance: **{fmt_money(bal)}**."
    )

@bot.command(name="bet_bonus")
@commands.guild_only()
async def bet_bonus_cmd(ctx: commands.Context, bet_id: int, amount: str):
    if not is_owner(ctx):
        await send_reply(ctx, "You are not allowed to use this command.")
        return
    try:
        amount_i = parse_amount_int(amount, min_value=1)
    except ValueError:
        await send_reply(ctx, f"Usage: `{PREFIX}bet_bonus {bet_id} <amount>` (supports 2K/3M/1B/...)")
        return

    ok, msg = add_bet_bonus_pool(bet_id, amount_i)
    if not ok:
        await send_reply(ctx, msg)
        return

    bet = get_bet(bet_id)
    bonus_pool = int(bet[8]) if bet else 0
    total_pool, _totals = get_bet_totals(bet_id)
    effective_total = int(total_pool) + int(bonus_pool)

    await send_reply(ctx, f"{msg}\nCurrent bonus pool: **{fmt_money(bonus_pool)}** | Effective pool: **{fmt_money(effective_total)}**")

@bot.command(name="bet_close")
@commands.guild_only()
async def bet_close_cmd(ctx: commands.Context, bet_id: int):
    if not is_owner(ctx):
        await send_reply(ctx, "You are not allowed to use this command.")
        return
    _ok, msg = close_bet(bet_id)
    await send_reply(ctx, msg)

@bot.command(name="bet_cancel")
@commands.guild_only()
async def bet_cancel_cmd(ctx: commands.Context, bet_id: int):
    if not is_owner(ctx):
        await send_reply(ctx, "You are not allowed to use this command.")
        return
    _ok, msg = cancel_bet_and_refund(bet_id)
    await send_reply(ctx, msg)

@bot.command(name="bet_resolve")
@commands.guild_only()
async def bet_resolve_cmd(ctx: commands.Context, bet_id: int, winning_option: int):
    if not is_owner(ctx):
        await send_reply(ctx, "You are not allowed to use this command.")
        return
    _ok, msg = resolve_bet_and_payout(bet_id, winning_option)
    await send_reply(ctx, msg)

# -----------------------------
# Owner give / take
# -----------------------------
@bot.command(name="give")
@commands.guild_only()
async def give_cmd(ctx: commands.Context, member: discord.Member, amount: str):
    if ctx.author.id != OWNER_ID:
        await send_reply(ctx, "You are not allowed to use this command.")
        return
    try:
        amount_i = parse_amount_int(amount, min_value=1)
    except ValueError:
        await send_reply(ctx, f"Usage: `{PREFIX}give @User 500` (supports 2K/3M/1B/...)")
        return

    target = get_user(member.id)
    if target is None:
        await send_reply(ctx, f"That user is not activated yet. They must run `{PREFIX}activate` first.")
        return

    update_username(member.id, member.display_name)
    add_balance(member.id, amount_i)
    updated = get_user(member.id)
    new_bal = int(updated[2])
    await send_reply(ctx, f"Gave **+{fmt_money(amount_i)}** Marcus Money to **{member.display_name}**. New balance: **{fmt_money(new_bal)}**.")

@bot.command(name="take", aliases=["remove"])
@commands.guild_only()
async def take_cmd(ctx: commands.Context, member: discord.Member, amount: str):
    if ctx.author.id != OWNER_ID:
        await send_reply(ctx, "You are not allowed to use this command.")
        return
    try:
        amount_i = parse_amount_int(amount, min_value=1)
    except ValueError:
        await send_reply(ctx, f"Usage: `{PREFIX}take @User 500` (supports 2K/3M/1B/...)")
        return

    target = get_user(member.id)
    if target is None:
        await send_reply(ctx, f"That user is not activated yet. They must run `{PREFIX}activate` first.")
        return

    update_username(member.id, member.display_name)

    current_bal = int(target[2])
    take_amt = min(int(amount_i), current_bal)
    if take_amt <= 0:
        await send_reply(ctx, f"**{member.display_name}** has **0** Marcus Money to take.")
        return

    add_balance(member.id, -take_amt)
    updated = get_user(member.id)
    new_bal = int(updated[2]) if updated else 0
    await send_reply(
        ctx,
        f"Took **-{fmt_money(take_amt)}** Marcus Money from **{member.display_name}**.\n"
        f"{member.display_name}'s new balance: **{fmt_money(new_bal)}**."
    )

# ----------
# Run
# ----------
if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN is not set. Set it in your environment before running.")
    bot.run(TOKEN)

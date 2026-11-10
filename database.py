import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "laundry.db")

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    # 1. Machines Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS machines (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        type TEXT NOT NULL,
        location TEXT NOT NULL,
        dorm TEXT NOT NULL DEFAULT 'male',
        current_user_name TEXT,
        status TEXT NOT NULL DEFAULT 'AVAILABLE',
        current_amp REAL DEFAULT 0.0,
        vibration_level REAL DEFAULT 0.0,
        cycle_start_time TEXT,
        cycle_end_time TEXT,
        reserved_user_name TEXT,
        reserved_contact TEXT,
        reserved_expire_time TEXT,
        updated_at TEXT
    )
    """)

    # Check and add columns if upgrading from old schema
    cursor.execute("PRAGMA table_info(machines)")
    columns = [row["name"] for row in cursor.fetchall()]
    if "dorm" not in columns:
        cursor.execute("ALTER TABLE machines ADD COLUMN dorm TEXT DEFAULT 'male'")
    if "current_user_name" not in columns:
        cursor.execute("ALTER TABLE machines ADD COLUMN current_user_name TEXT")
    if "fault_reports_count" not in columns:
        cursor.execute("ALTER TABLE machines ADD COLUMN fault_reports_count INTEGER DEFAULT 0")
    if "fault_reason" not in columns:
        cursor.execute("ALTER TABLE machines ADD COLUMN fault_reason TEXT")

    # 2. Reservations Queue Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS reservations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        machine_id TEXT NOT NULL,
        user_name TEXT NOT NULL,
        user_contact TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'WAITING',
        created_at TEXT NOT NULL,
        notified_at TEXT,
        FOREIGN KEY (machine_id) REFERENCES machines(id)
    )
    """)

    # 3. Telemetry Logs Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS telemetry_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        machine_id TEXT NOT NULL,
        current_amp REAL NOT NULL,
        vibration_level REAL NOT NULL,
        timestamp TEXT NOT NULL
    )
    """)

    # 4. Event-Driven Logs Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS event_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        machine_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        source TEXT NOT NULL,
        message TEXT NOT NULL,
        timestamp TEXT NOT NULL
    )
    """)

    # Seed 4 Male Dorm + 4 Female Dorm machines
    now = datetime.now().isoformat()
    all_initial_machines = [
        # 男生宿舍 (A棟 1F)
        ("washer-1", "A棟 洗衣機 1號", "washer", "男宿 A棟 1F 洗衣間", "male", None, "AVAILABLE", 0.0, 0.0, None, None, None, None, None, now),
        ("washer-2", "A棟 洗衣機 2號", "washer", "男宿 A棟 1F 洗衣間", "male", None, "AVAILABLE", 0.0, 0.0, None, None, None, None, None, now),
        ("dryer-1",  "A棟 烘衣機 1號", "dryer",  "男宿 A棟 1F 洗衣間", "male", None, "AVAILABLE", 0.0, 0.0, None, None, None, None, None, now),
        ("dryer-2",  "A棟 烘衣機 2號", "dryer",  "男宿 A棟 1F 洗衣間", "male", None, "AVAILABLE", 0.0, 0.0, None, None, None, None, None, now),
        # 女生宿舍 (B棟 1F)
        ("f-washer-1", "B棟 洗衣機 1號", "washer", "女宿 B棟 1F 洗衣間", "female", None, "AVAILABLE", 0.0, 0.0, None, None, None, None, None, now),
        ("f-washer-2", "B棟 洗衣機 2號", "washer", "女宿 B棟 1F 洗衣間", "female", None, "AVAILABLE", 0.0, 0.0, None, None, None, None, None, now),
        ("f-dryer-1",  "B棟 烘衣機 1號", "dryer",  "女宿 B棟 1F 洗衣間", "female", None, "AVAILABLE", 0.0, 0.0, None, None, None, None, None, now),
        ("f-dryer-2",  "B棟 烘衣機 2號", "dryer",  "女宿 B棟 1F 洗衣間", "female", None, "AVAILABLE", 0.0, 0.0, None, None, None, None, None, now),
    ]

    for m in all_initial_machines:
        cursor.execute("SELECT COUNT(*) FROM machines WHERE id = ?", (m[0],))
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
            INSERT INTO machines (
                id, name, type, location, dorm, current_user_name, status, current_amp, vibration_level,
                cycle_start_time, cycle_end_time, reserved_user_name, reserved_contact,
                reserved_expire_time, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, m)
        else:
            # Update dorm attribute
            cursor.execute("UPDATE machines SET dorm = ? WHERE id = ?", (m[4], m[0]))

    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_db()
    print("Database updated and initialized successfully at", DB_PATH)

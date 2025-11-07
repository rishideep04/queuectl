import psycopg2
import psycopg2.extras
import time
from datetime import datetime

DB_CONFIG = {
    "dbname": "queuectl",
    "user": "postgres",
    "password": "Rishi@282004",
    "host": "localhost",
    "port": 5432
}

def make_connection():
    dbconn = psycopg2.connect(**DB_CONFIG)
    dbconn.autocommit = False
    return dbconn

def safe_execute(db_func, max_retries=5, delay=0.5):
    for attempt in range(max_retries):
        try:
            return db_func()
        except psycopg2.OperationalError as e:
            print(f"database busy. Retrying... ({attempt+1}/{max_retries})")
            time.sleep(delay)
        except psycopg2.InterfaceError as e:
            print(f"connection dropped. Retrying... ({attempt+1}/{max_retries})")
            time.sleep(delay)
    raise psycopg2.OperationalError("database connection failed after multiple retries.")

def migrate():
    dbconn = make_connection()
    cur = dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('pending','processing','completed','failed','dead')),
        attempts INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 3,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL,
        next_attempt_at TIMESTAMP DEFAULT NULL,
        last_error TEXT DEFAULT NULL,
        job_timeout_seconds INTEGER NOT NULL DEFAULT 30,
        backoff_base INTEGER NOT NULL DEFAULT 2,
        max_backoff_seconds INTEGER NOT NULL DEFAULT 300
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS workers (
        id TEXT PRIMARY KEY,
        pid INTEGER NOT NULL,
        started_at TIMESTAMP NOT NULL,
        heartbeat_at TIMESTAMP NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('running','stopping','stopped'))
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    """)
    dbconn.commit()
    dbconn.close()
    print("postgreSQL database initialized successfully.")

def insert_job(job):
    def _insert():
        dbconn = make_connection()
        cur = dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at,job_timeout_seconds,backoff_base,max_backoff_seconds)
            VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s)
            ON CONFLICT (id) DO NOTHING;
        """, (
            job["id"],
            job["command"],
            job.get("state", "pending"),
            job.get("attempts", 0),
            job.get("max_retries", 3),
            job.get("created_at"),
            job.get("updated_at"),
            job.get("job_timeout_seconds",30),
            job.get("backoff_base",2),
            job.get("max_backoff_seconds",60),
        ))
        dbconn.commit()
        dbconn.close()
        print(f"Job '{job['id']}' inserted successfully.")
    safe_execute(_insert)

def init_db():
    print("initializing PostgreSQL database...")
    migrate()

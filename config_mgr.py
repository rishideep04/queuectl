from db import make_connection
import psycopg2
import psycopg2.extras

DEFAULT_CONFIG = {
    "max_retries":"3",
    "backoff_base":"2",
    "max_backoff_seconds":"300",
    "job_timeout_seconds":"30"
}

def ensure_defaults():
    dbconn=make_connection()
    cur=dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    for key,val in DEFAULT_CONFIG.items():
        cur.execute("INSERT INTO config (key,value) VALUES (%s,%s) ON CONFLICT (key) DO NOTHING;", (key,val))
    dbconn.commit()
    dbconn.close()
    
def set_config(key,value):
    dbconn=make_connection()
    cur=dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""INSERT INTO config (key, value) VALUES (%s, %s) ON CONFLICT(key) DO UPDATE SET value=excluded.value;""",(key,value))
    dbconn.commit()
    dbconn.close()
    print(f"Config set: {key} = {value}")

def get_config(key,default=None):
    dbconn=make_connection()
    cur=dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""SELECT value FROM config WHERE key=%s;""",(key,))
    row=cur.fetchone()
    dbconn.close()
    return int(row["value"]) if row and row["value"].isdigit() else default


def list_config():
    dbconn=make_connection()
    cur=dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""SELECT key, value FROM config ORDER BY key ASC;""")
    rows=cur.fetchall()
    dbconn.close()
    if not rows:
        print("No config entries found.")
        return {}
    for r in rows:
        print(f"{r['key']} = {r['value']}")
    return {r["key"]: r["value"] for r in rows}
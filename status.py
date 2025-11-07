from db import make_connection
from datetime import datetime 
import psycopg2
import psycopg2.extras

def get_jobcount():
    dbconn=make_connection()
    cur=dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
                SELECT state,COUNT(*) as count FROM jobs GROUP BY state;
            """)
    rows=cur.fetchall()
    dbconn.close()
    counts={row["state"]: row["count"] for row in rows}
    for state in ['pending','processing','completed','failed','dead']:
        counts.setdefault(state,0)
    return counts

def get_workercounts():
    dbconn=make_connection()
    cur=dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT status, COUNT(*) as count FROM workers GROUP BY status;
            """)
    rows=cur.fetchall()
    dbconn.close()
    counts={row["status"]: row["count"] for row in rows}
    for status in ['running', 'stopping', 'stopped']:
        counts.setdefault(status, 0)
    return counts 

def get_basicmetrics():
    dbconn=make_connection()
    cur=dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT COUNT(*) AS count FROM jobs;")
    total=cur.fetchone()["count"]
    cur.execute("SELECT COUNT(*) AS count FROM jobs WHERE state='completed';")
    success=cur.fetchone()["count"]
    cur.execute("SELECT COUNT(*) AS count FROM jobs WHERE state='failed' OR state='dead';")
    failed=cur.fetchone()["count"]
    cur.execute("SELECT SUM(attempts) AS total_attempts FROM jobs;")
    total_attempts=cur.fetchone()["total_attempts"] or 0
    dbconn.close()
    success_rate=round(((success/ total)*100),2) if total > 0 else 0
    fail_rate=round(((failed/total)*100),2) if total > 0 else 0
    return {
        "total_jobs": total,
        "successful_jobs": success,
        "failed_jobs": failed,
        "total_attempts": total_attempts,
        "success_rate": success_rate,
        "fail_rate": fail_rate
    }
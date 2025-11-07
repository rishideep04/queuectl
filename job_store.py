import json 
from datetime import datetime,timezone
from db import make_connection,insert_job
import psycopg2
import psycopg2.extras
from config_mgr import get_config 

def enqueue_job(job_json:str):
    try:
        job=json.loads(job_json)
    except json.JSONDecodeError:
        print("invalid format of json")
        return 
    req=["id","command"]
    for field in req:
        if field not in job:
            print(f"missing field: {field}")
            return 
    now = datetime.now(timezone.utc).isoformat()
    max_retries = get_config("max_retries", default=3)
    job["state"]="pending"
    job["attempts"]=0
    job["max_retries"]=int(max_retries)
    job["created_at"]=now
    job["updated_at"]=now
    job["job_timeout_seconds"] = get_config("job_timeout_seconds", default=30)
    job["backoff_base"] = get_config("backoff_base", default=2)
    job["max_backoff_seconds"] = get_config("max_backoff_seconds", default=60)
    insert_job(job)

def list_jobs(state:str=None):
    dbconn=make_connection()
    cur=dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if state:
        cur.execute("SELECT * FROM jobs WHERE state = %s ORDER BY created_at ASC;",(state,))
    else:
        cur.execute("SELECT * FROM jobs ORDER BY created_at ASC;")
    rows=cur.fetchall()
    dbconn.close()
    return [dict(row) for row in rows]
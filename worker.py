import os 
import time 
import subprocess 
import multiprocessing 
import signal 
import uuid
import psycopg2
import psycopg2.extras
from datetime import datetime,timezone,timedelta 
from db import make_connection,safe_execute
from scheduler import next_attempt_time
from config_mgr import get_config
def register_worker(pid):
    dbconn=make_connection()
    cur=dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    wid=str(uuid.uuid4())
    now=datetime.now(timezone.utc).isoformat()
    cur.execute("""
                INSERT INTO workers (id, pid, started_at, heartbeat_at, status) VALUES (%s, %s, %s, %s, 'running') ON CONFLICT (id) DO UPDATE SET pid = EXCLUDED.pid,heartbeat_at = EXCLUDED.heartbeat_at,status = 'running';
            """,(wid,pid,now,now))
    dbconn.commit()
    dbconn.close()
    return wid

def update_heartbeat(wid):
    dbconn=make_connection()
    cur=dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    now = datetime.now(timezone.utc).isoformat()
    cur.execute("""
                UPDATE workers SET heartbeat_at=%s,status=COALESCE(status,'running') WHERE id=%s;
            """,(now,wid))
    dbconn.commit()
    dbconn.close()

def get_workerstatus(wid):
    dbconn=make_connection()
    cur=dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""SELECT status FROM workers WHERE id=%s;""",(wid,))
    row=cur.fetchone()
    dbconn.close()
    return row["status"] if row else "unknown"

def mark_workerstopped(wid):
    dbconn=make_connection()
    cur=dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
                UPDATE workers SET status='stopped',heartbeat_at=%s WHERE id=%s;
                """,(datetime.now(timezone.utc).isoformat(),wid))
    dbconn.commit()
    dbconn.close()
    
def mark_jobstate(job_id,state,attempts=None,next_attempt_at=None,last_error=None):
    dbconn=make_connection()
    cur=dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    query="""UPDATE jobs SET state=%s,updated_at=%s,attempts=COALESCE(%s,attempts),next_attempt_at=COALESCE(%s,next_attempt_at),last_error=COALESCE(%s,last_error) where id=%s"""
    now=datetime.now(timezone.utc).isoformat()
    cur.execute(query,(state,now,attempts,next_attempt_at,last_error,job_id))
    dbconn.commit()
    dbconn.close()
    
def process_job(job,worker_id):
    mark_jobstate(job["id"],"processing")
    print(f"[Worker {worker_id} running job {job['id']} -> {job['command']}]")
    timeout = job.get("job_timeout_seconds",get_config("job_timeout_seconds",default=30))
    try:
        res=subprocess.run(job["command"],shell=True,capture_output=True,text=True,timeout=timeout)
        exit_code=res.returncode
        if exit_code==0:
            mark_jobstate(job["id"],"completed")
            print(f"[Worker {worker_id} Job {job['id']} completed successfully")
        else:
            handle_failure(job,worker_id,f"exit code {exit_code}")
    except subprocess.TimeoutExpired:
        handle_failure(job,worker_id,"job timed out")
    except Exception as e:
        handle_failure(job,worker_id,str(e))
        
def handle_failure(job,worker_id,error_message):
    attempts=job["attempts"]+1
    max_retries=job["max_retries"]
    if attempts>=max_retries:
        mark_jobstate(job["id"],"dead",attempts=attempts,last_error=error_message)
        print(f"[Worker {worker_id}] Job {job['id']} moved to DLQ after {attempts} attempts")
    else:
        next_time=next_attempt_time(attempts)
        mark_jobstate(job["id"],"failed",attempts=attempts,next_attempt_at=next_time,last_error=error_message)
        print(f"[Worker {worker_id}] Job {job['id']} failed (attempt {attempts}) retrying at {next_time}")
                
def worker_loop(worker_id,stop_event):
    wid=register_worker(os.getpid())
    print(f"[Worker {worker_id}] Registered as {wid}")
    while not stop_event.is_set():
        if get_workerstatus(wid)=="stopping":
            print(f"[Worker {worker_id}]stopping signal received.finishing current job")
            break
        dbconn = make_connection() 
        update_heartbeat(wid)
        job=claim_nextjob(dbconn)
        dbconn.close()
        if job:
            idle_cycles = 0
            process_job(dict(job),worker_id)
        else:
            time.sleep(1)
    mark_workerstopped(wid)
    print(f"[Worker {worker_id}] stopped gracefully")

def claim_nextjob(dbconn):
    now = datetime.now(timezone.utc).isoformat()
    job = None
    dbconn.autocommit = False
    cur = dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("""
            WITH next_job AS (SELECT id FROM jobs WHERE (state='pending' OR (state='failed' AND (next_attempt_at IS NULL OR next_attempt_at <= %s))) ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED) UPDATE jobs SET state='processing', updated_at=%s WHERE id IN (SELECT id FROM next_job) RETURNING *;
        """, (now, now))
        job = cur.fetchone()
        dbconn.commit() 
    except Exception as e:
        dbconn.rollback()
        print("error claiming job:", e)
    finally:
        cur.close()
    return job

def start_workers(count): 
    stop_event=multiprocessing.Event() 
    workers=[] 
    print(f"Starting {count} worker") 
    for i in range(count): 
        p=multiprocessing.Process(target=worker_loop,args=(i+1,stop_event)) 
        p.start() 
        workers.append(p) 
    print("workers running.") 
    try: 
        while True: 
            time.sleep(0.5) 
    except KeyboardInterrupt: 
        print("stopping all workers") 
        stop_event.set() 
        for p in workers: 
            p.join() 
        print("all workers stopped")
        
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run a single QueueCTL worker")
    parser.add_argument("--worker-id", type=int, default=1)
    args = parser.parse_args()
    
    stop_event = multiprocessing.Event()
    worker_loop(args.worker_id, stop_event)
import typer
from rich.table import Table
from rich.console import Console 
import json
import psycopg2
import psycopg2.extras
from datetime import datetime,timezone,timedelta  
from db import init_db,make_connection,insert_job
from job_store import enqueue_job,list_jobs
from worker import start_workers
from config_mgr import set_config, get_config, list_config,ensure_defaults
from status import get_jobcount, get_workercounts, get_basicmetrics
import subprocess
import sys
import os

app=typer.Typer(help="QueueCTL - Background Job Queue System CLI")
console=Console()

@app.command()
def init():
    init_db()
    
@app.command()
def enqueue(job_json:str):
    enqueue_job(job_json)

@app.command()
def list(state:str=typer.Option(None,help="filtering jobs by state[pending,processing,completed,failed,dead]")):
    jobs=list_jobs(state)
    if not jobs:
        console.print("no jobs found",style="yellow")
        return 
    table=Table(title="jobs list",header_style="bold cyan")
    table.add_column("ID")
    table.add_column("Command")
    table.add_column("State")
    table.add_column("Attempts")
    table.add_column("Max Retries")
    table.add_column("Created At")
    table.add_column("Updated At")

    for job in jobs:
        table.add_row(
            job["id"],
            job["command"],
            job["state"],
            str(job["attempts"]),
            str(job["max_retries"]),
            job["created_at"].isoformat() if job["created_at"] else "",
            job["updated_at"].isoformat() if job["updated_at"] else ""
        )
    console.print(table)

@app.command()
def worker_start(count:int=typer.Option(1,help="number of worker processes(or nodes) to start")):
    console.print(f"Starting {count} worker(s) in background...", style="bold green")
    for i in range(count):
        # Run each worker as a detached process
        subprocess.Popen(
            [sys.executable,"worker.py","--worker-id",str(i + 1)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            creationflags=subprocess.DETACHED_PROCESS if os.name == "nt" else 0x80000,
        )
    console.print(f"{count} worker(s) launched successfully.\n", style="bold cyan")

@app.command()
def dlq_list():
    dbconn=make_connection()
    cur=dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM jobs WHERE state='dead';")
    jobs=cur.fetchall()
    dbconn.close()
    if not jobs:
        console.print("no job for dlq to operate",style="yellow")
        return
    table=Table(title="Dead Letter Queue",header_style="bold red")
    table.add_column("ID")
    table.add_column("Command")
    table.add_column("Attempts")
    table.add_column("Last Error")
    table.add_column("Created At")
    table.add_column("Updated At")
    
    for job in jobs:
        table.add_row(job["id"],job["command"],str(job["attempts"]),job.get("last_error", "None"),str(job["created_at"]),str(job["updated_at"]))
    console.print(table)

@app.command()
def dlq_retry(job_id: str):
    dbconn=make_connection()
    cur=dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    max_retries=get_config("max_retries",default=3)
    cur.execute("UPDATE jobs SET state='pending',attempts=0,next_attempt_at=NULL,last_error=NULL,max_retries=%s,updated_at = NOW() WHERE id=%s",(max_retries,job_id,))
    dbconn.commit()
    dbconn.close()
    console.print(f"Job {job_id} moved to pending queue",style="green")
    
@app.command()
def worker_stop():
    dbconn=make_connection()
    cur=dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""UPDATE workers SET status='stopping' WHERE status='running';""")
    dbconn.commit()
    dbconn.close()
    console.print("stop signal sent to all workers",style="bold yellow")
    
@app.command()
def config(action:str=typer.Argument(...,help="get|set|list"),
           key:str=typer.Argument(None,help="configuration key"),
           value:str=typer.Argument(None,help="configuration value for 'set'")):
    ensure_defaults()
    if action == "list":
        configs = list_config()
        table = Table(title="QueueCTL Configuration", header_style="bold cyan")
        table.add_column("Key")
        table.add_column("Value")
        for k, v in configs.items():
            table.add_row(k, v)
        console.print(table)
    elif action == "get":
        if not key:
            console.print("please specify a key. Example: queuectl config get max_retries", style="red")
            return
        value = get_config(key)
        if value is None:
            console.print(f"no config found for key '{key}'.", style="yellow")
        else:
            console.print(f"{key} = {value}", style="green")
    elif action == "set":
        if not key or not value:
            console.print("please provide key and value. Example: queuectl config set max_retries 5", style="red")
            return
        set_config(key, value)
    else:
        console.print("invalid action. Use one of: list | get | set", style="red")

@app.command()
def status():
    job_counts=get_jobcount()
    worker_counts = get_workercounts()
    metrics = get_basicmetrics()

    console.print("\n[bold cyan]QueueCTL System Status[/bold cyan]")
    console.print(f"Last updated: {datetime.now(timezone.utc).isoformat()}Z\n")

    job_table=Table(title="Job States",header_style="bold green")
    job_table.add_column("State")
    job_table.add_column("Count")
    for state, count in job_counts.items():
        job_table.add_row(state,str(count))
    console.print(job_table)

    worker_table=Table(title="Worker States",header_style="bold magenta")
    worker_table.add_column("State")
    worker_table.add_column("Count")
    for status,count in worker_counts.items():
        worker_table.add_row(status,str(count))
    console.print(worker_table)

    console.print("[bold yellow]System Metrics[/bold yellow]")
    console.print(f"Total Jobs: {metrics['total_jobs']}")
    console.print(f"Success Rate: {metrics['success_rate']}%")
    console.print(f"Failure Rate: {metrics['fail_rate']}%")
    console.print(f"Total Attempts: {metrics['total_attempts']}")
if __name__=="__main__":
    app()
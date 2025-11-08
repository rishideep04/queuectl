import worker
from unittest.mock import patch
from datetime import datetime, timezone
import psycopg2

def make_dummy_job(state="pending", attempts=0, max_retries=2):
    return {
        "id": "dummy1",
        "command": "echo test",
        "state": state,
        "attempts": attempts,
        "max_retries": max_retries,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "job_timeout_seconds": 5,
        "backoff_base": 2,
        "max_backoff_seconds": 300
    }

@patch("subprocess.run")
def test_process_job_success(mock_run):
    mock_run.return_value.returncode = 0
    job = make_dummy_job()
    worker.mark_jobstate = lambda *args, **kwargs: None
    worker.process_job(job, worker_id=1)

@patch("subprocess.run", side_effect=Exception("fail"))
def test_process_job_failure(mock_run):
    job = make_dummy_job()
    worker.mark_jobstate = lambda *args, **kwargs: None
    worker.handle_failure = lambda job, wid, err: print("Handled failure")
    worker.process_job(job, worker_id=1)

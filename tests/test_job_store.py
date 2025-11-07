# tests/test_job_store.py
from job_store import enqueue_job, list_jobs
from db import make_connection
import json

def test_enqueue_job_inserts_to_db():
    job_data = json.dumps({"id": "job_test_1", "command": "echo test"})
    enqueue_job(job_data)

    jobs = list_jobs()
    ids = [j["id"] for j in jobs]
    assert "job_test_1" in ids

def test_enqueue_invalid_json():
    enqueue_job("invalid_json")  # should not raise exception

def test_enqueue_missing_field():
    job_data = json.dumps({"id": "job_test_2"})
    enqueue_job(job_data)  # missing command

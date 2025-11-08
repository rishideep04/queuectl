import psycopg2
from db import make_connection
from cli import dlq_retry 

def test_dlq_retry_moves_to_pending(monkeypatch):
    conn = make_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at)
        VALUES ('dlq_job1', 'echo fail', 'dead', 3, 3, NOW(), NOW());
    """)
    conn.commit()
    conn.close()

    dlq_retry("dlq_job1")
    conn = make_connection()
    cur = conn.cursor()
    cur.execute("SELECT state, attempts FROM jobs WHERE id='dlq_job1';")
    state, attempts = cur.fetchone()
    conn.close()

    assert state == "pending"
    assert attempts == 0

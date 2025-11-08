import pytest
import psycopg2
import sys
import os

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT_DIR not in sys.path: 
    sys.path.insert(0, ROOT_DIR)

from db import make_connection,migrate
@pytest.fixture(scope="session", autouse=True)
def setup_database():
    migrate()
    yield
    dbconn = make_connection()
    cur = dbconn.cursor()
    cur.execute("TRUNCATE TABLE jobs, workers, config RESTART IDENTITY CASCADE;")
    dbconn.commit()
    dbconn.close()

@pytest.fixture()
def dbconn():
    conn = make_connection()
    yield conn
    conn.close()

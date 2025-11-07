from flask import Flask, render_template
import psycopg2
import psycopg2.extras
from db import make_connection
from datetime import datetime

app = Flask(__name__)

def fetch_data():
    dbconn = make_connection()
    cur = dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM jobs ORDER BY created_at DESC;")
    jobs = cur.fetchall()
    cur.execute("SELECT * FROM workers ORDER BY started_at DESC;")
    workers = cur.fetchall()
    dbconn.close()
    return jobs, workers

@app.route("/")
def dashboard():
    jobs, workers = fetch_data()
    return render_template("dashboard.html", jobs=jobs, workers=workers, now=datetime.utcnow())

if __name__ == "__main__":
    print("queuectl Dashboard running at http://127.0.0.1:5000")
    app.run(debug=True)

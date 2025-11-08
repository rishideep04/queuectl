from scheduler import calc_backoff_delay, next_attempt_time

def test_calc_backoff_delay_basic(monkeypatch):
    monkeypatch.setattr("scheduler.get_config", lambda key, default=None: 2 if key == "backoff_base" else 300)
    delay = calc_backoff_delay(3)
    assert 5 <= delay <= 9 

def test_next_attempt_time_returns_future(monkeypatch):
    monkeypatch.setattr("scheduler.calc_backoff_delay", lambda a: 5)
    t = next_attempt_time(1)
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    assert "T" in t
    assert t > now.isoformat()

import math 
import random 
from datetime import datetime,timedelta,timezone
from config_mgr import get_config
def calc_backoff_delay(attempts:int):
    base = get_config("backoff_base",default=2)
    max_delay=get_config("max_backoff_seconds",default=300)
    delay=min(base**attempts,max_delay)
    jitter=random.uniform(0.8,1.2)
    return int(delay*jitter)

def next_attempt_time(attempts:int):
    delay=calc_backoff_delay(attempts)
    next_time=datetime.now(timezone.utc)+timedelta(seconds=delay)
    return next_time.isoformat()
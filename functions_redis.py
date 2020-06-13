import redis
import datetime
from rq import Queue
import time

redis = redis.Redis()
# utworzenie kolejki dla danej sesji Redis'a
q = Queue(connection=redis)

"""
Pobranie TTL dla danego użytkownika
"""
def get_user_ttl(email):
    time_left = redis.ttl(email)
    time_left = str(datetime.timedelta(seconds=time_left))
    
    if time_left[2] == '0':
        time_left = time_left[3:]
    else:
        time_left = time_left[2:]
        
    return time_left

"""
Aktualizacja TTL dla danego użytkownika
"""    
def update_ttl(email):
    redis.get(email)
    redis.expire(email, 300)

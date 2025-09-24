import redis
import sys

def initialize_redis(REDIS_HOST, REDIS_PORT):
    """
    Initializes the connection to Redis.
    This function is placed here to keep the main script clean.
    """
    try:
        redis_instance = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        redis_instance.ping()
        print("Successfully connected to Redis.")
        return redis_instance
    except redis.exceptions.ConnectionError as e:
        print(f"Could not connect to Redis: {e}")
        sys.exit(1)

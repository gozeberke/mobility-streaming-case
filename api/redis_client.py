import redis
import os

redis_host = os.getenv("REDIS_HOST", "kafka")
redis_port = int(os.getenv("REDIS_PORT", 6379))

r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)


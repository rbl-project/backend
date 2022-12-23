import redis
import os

redis = redis.Redis(
    host=os.getenv("RBL_REDIS_HOST"),
    port=os.getenv("RBL_REDIS_PORT"), 
    password=os.getenv("RBL_REDIS_PASSWORD")
)
# from flask import current_app as app
from celery import Celery
celery_instance = Celery(
    __name__, 
    broker = "redis://localhost:6379"
)

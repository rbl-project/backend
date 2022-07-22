from flask import Flask
from flask_cors import CORS
from logging.config import dictConfig
from manage.db_setup import db
from manage.db_setup import migrate
from api import (
    user_api
)

from dotenv import load_dotenv
import os
load_dotenv()

def set_logger():
    dictConfig({
        'version': 1,
        'formatters': {'default': {
            'format': '[%(asctime)s] [%(levelname)s] in [%(module)s]: %(message)s',
        }},
        'handlers': {'wsgi': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://flask.logging.wsgi_errors_stream',
            'formatter': 'default'
        }},
        'root': {
            'level': 'INFO',
            'handlers': ['wsgi']
        }
    })

def add_end_points(app):
    app.register_blueprint(user_api.userAPI, url_prefix = "/api")

def create_app():
    '''
    Function to create flask app
    '''
    app = Flask(__name__)
    CORS(app)

    # app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv("POSTGRESS_DATABASE_URL")
    # app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://khkqprkwhxtrka:88a1f1ff3f36ff4f807695af99b10567da845e70ade7f29f6991413b74f0b0db@ec2-54-152-28-9.compute-1.amazonaws.com:5432/d2829fk0fdcjc6'

    db.init_app(app)
    migrate.init_app(app, db)
    add_end_points(app)
    set_logger()

    return app

app = create_app()
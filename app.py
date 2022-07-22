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

    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv("POSTGRESS_DATABASE_URL")

    db.init_app(app)
    migrate.init_app(app, db)

    with app.app_context():
        db.create_all()

    add_end_points(app)
    set_logger()

    return app

app = create_app()
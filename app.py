from datetime import timedelta
from flask import Flask
from flask_cors import CORS
import logging
from logging.config import dictConfig
from dotenv import load_dotenv
import os
from utilities.constants import ONE_GB

# Models
from models.user_model import Users
from models.jwt_blocklist_model import TokenBlocklist

# Extensions
from manage.db_setup import db
from manage.db_setup import migrate
from manage.celery_setup import celery_instance
from manage.jwt_extension import jwt

# APIs
from api.DataVisulization import data_visualization_api
from api.User import user_api
from api.DatasetUtilities import dataset_api
from api.DataOverview import data_overview_api
from api.DataCleaning import data_cleaning_api

load_dotenv()

def set_jwt_token():
    @jwt.token_in_blocklist_loader
    def check_if_token_revoked(jwt_header, jwt_payload):
        jti = jwt_payload["jti"]
        token = TokenBlocklist.query.filter_by(jti = jti).first()
        return token is not None

def set_logger():
    '''
    Function to configure the logger
    '''
    dictConfig({
        'version': 1,
        'formatters': {'default': {
            'format': '[%(asctime)s] [%(levelname)s] in [%(module)s]: %(message)s',
            # 'format': '[%(asctime)s] [%(levelname)s] in [%(module)s]:[%(user_email)s] %(message)s',
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

    # =========================   LOGGER CODE FOR USER EMAIL   =========================
    # old_factory = logging.getLogRecordFactory()

    # def record_factory(*args, **kwargs):
    #     record = old_factory(*args, **kwargs)
    #     if current_user and current_user.is_authenticated:
    #         user = Users.query.get(int(current_user.id))
    #         if user:
    #             record.user_email = user.email
    #         else:
    #             record.user_email = "No User"
    #     else:
    #         record.user_email = "No User"
    #     return record

    # logging.setLogRecordFactory(record_factory)

def set_celery(app):
    '''
    Function to setup the celery
    '''
    celery_instance.conf.update(app.config)

def add_end_points(app):
    '''
    Function to register the api end points
    '''
    app.register_blueprint(user_api.userAPI, url_prefix = "/api")
    app.register_blueprint(dataset_api.datasetAPI, url_prefix = "/api")
    app.register_blueprint(data_visualization_api.dataVisulizationAPI, url_prefix = "/api")
    app.register_blueprint(data_overview_api.dataOverviewAPI, url_prefix = "/api")
    app.register_blueprint(data_cleaning_api.dataCleaningAPI, url_prefix = "/api")

def configure_app(app):
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv("POSTGRESS_DATABASE_URL")
    app.config["CELERY_BROKER_URL"] = os.getenv("CELERY_BROKER_URL")
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SECRET_KEY'] = os.getenv("SECRET_KEY")
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=3)
    app.config["UPLOAD_FOLDER"] = f"{os.getcwd()}/assets/user_datasets"
    app.config['MAX_CONTENT_LENGTH'] = ONE_GB 
    app.config["JWT_SECRET_KEY"] = os.getenv("JWT_SECRET_KEY")
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=2)

def create_app():
    '''
    Function to create flask app
    '''
    app = Flask(__name__)
    
    CORS(app)
    configure_app(app)

    # Set the extensions
    db.init_app(app)
    migrate.init_app(app, db)
    jwt.init_app(app)

    # with app.app_context():
    #     db.create_all()

    set_celery(app)
    add_end_points(app)
    set_logger()
    set_jwt_token()

    return app

app = create_app()
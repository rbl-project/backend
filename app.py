from datetime import timedelta
from flask import Flask
from flask_cors import CORS
import logging
from logging.config import dictConfig
from dotenv import load_dotenv
import os
from utilities.constants import ONE_GB
from flask_jwt_extended import get_current_user

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
from api.EDA import (
    dataset_overview_api, 
    data_correlation_api, 
    tabular_representation_api
)

# Utility
from utilities.respond import respond

load_dotenv()

def set_jwt_token():

    # To set a token as expired
    @jwt.token_in_blocklist_loader
    def check_if_token_revoked(jwt_header, jwt_payload):
        jti = jwt_payload["jti"]
        token = TokenBlocklist.query.filter_by(jti = jti).first()
        return token is not None

    # To return the user on get_current_user()
    @jwt.user_lookup_loader
    def user_lookup_callback(_jwt_header, jwt_data):
        # jwt_data => {'fresh': False, 'iat': 1672299841, 'jti': '9fbd2e41-545e-48e7-91d2-f3307bc24c5a', 'type': 'access', 'sub': {'id': 1, 'name': 'Prasahnt', 'email': 'prashant@gmail.com', 'db_count': 2, 'date_added': 'Mon, 15 Aug 2022 14:22:40 GMT', 'user_id': 1}, 'nbf': 1672299841, 'exp': 1672307041}
        identity = jwt_data["sub"]["id"]
        return Users.query.filter_by(id=identity).first()
    
    # To return cutom rensponse on expired token
    @jwt.expired_token_loader
    def expired_token_loader_callback(_jwt_header, jwt_data):
        return respond(error="Session expired. Please login again", code=401)

    # To return cutom rensponse on revoked token by user
    @jwt.revoked_token_loader
    def revoked_token_loader_callback(_jwt_header, jwt_data):
        return respond(error="No user found. Please login again", code=401)

    # To return cutom rensponse on invalid token
    @jwt.invalid_token_loader
    def invalid_token_loader_callback(reason_of_invalid_token):
        return respond(error="Invalid token. Please login again", code=401)

    # To return cutom rensponse when no token is set
    @jwt.unauthorized_loader
    def unauthorized_loader_callback(reason_of_unauthorized):
        return respond(error="No token found. Please login again", code=401)


def set_logger():
    '''
    Function to configure the logger
    '''
    dictConfig({
        'version': 1,
        'formatters': {'default': {
            'format': '[%(asctime)s] %(log_color_start)s[%(levelname)s] in [%(module)s]:[%(user_email)s]%(log_color_end)s %(message)s ',
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
    old_factory = logging.getLogRecordFactory()

    # below method need @jwt_required for get_current_user() to work thus we used try catch
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.log_color_start = ""
        record.log_color_end = ""
        record.user_email = "None"

        try:
            # set color for error
            if record.levelname == "ERROR":
                record.log_color_start = "\u001b[31m"
                record.log_color_end = "\u001b[0m"
            
            # set color for info
            if record.levelname == "INFO":
                record.log_color_start = "\u001b[34m"
                record.log_color_end = "\u001b[0m"
            
            # set user email
            current_user = get_current_user()
            if current_user and current_user.email:
                if current_user:
                    record.user_email = current_user.email
                else:
                    record.user_email = "None"
            else:
                record.user_email = "None"
        except Exception as e:
            record.log_color_start = ""
            record.log_color_end = ""
            record.user_email = "None"
        return record

    logging.setLogRecordFactory(record_factory)

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
    app.register_blueprint(dataset_overview_api.datasetOverviewAPI, url_prefix = "/api")
    app.register_blueprint(tabular_representation_api.tabularRepresentationAPI, url_prefix = "/api")
    app.register_blueprint(data_correlation_api.dataCorrelationAPI, url_prefix = "/api")

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
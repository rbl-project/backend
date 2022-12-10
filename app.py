from datetime import timedelta
from flask import Flask
from flask_cors import CORS
import logging
from logging.config import dictConfig
from manage.db_setup import db
from manage.db_setup import migrate
from flask_login import LoginManager, current_user
from dotenv import load_dotenv
import os
from models.user_model import Users
from utilities.respond import respond
# APIs
from api.DataVisulization import data_visualization_api
from api.User import user_api
from api.DatasetUtilities import dataset_api
from api.DataOverview import data_overview_api
from api.DataCleaning import data_cleaning_api

load_dotenv()

def set_login_manager(app):
    '''
    Function to setup the Authentication and Authorization configuration
    '''
    # login manager 
    login_manager = LoginManager()
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        return Users.query.get(int(user_id))

    @login_manager.unauthorized_handler
    def unauthorized():
        res = {
            "msg":"Unauthorized"
        }
        return respond(error=res, code=401)

def set_logger():
    '''
    Function to configure the logger
    '''
    dictConfig({
        'version': 1,
        'formatters': {'default': {
            'format': '[%(asctime)s] [%(levelname)s] in [%(module)s]:[%(user_email)s] %(message)s',
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

    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        if current_user and current_user.is_authenticated:
            user = Users.query.get(int(current_user.id))
            if user:
                record.user_email = user.email
            else:
                record.user_email = "No User"
        else:
            record.user_email = "No User"
        return record

    logging.setLogRecordFactory(record_factory)

def add_end_points(app):
    '''
    Function to register the api end points
    '''
    app.register_blueprint(user_api.userAPI, url_prefix = "/api")
    app.register_blueprint(dataset_api.datasetAPI, url_prefix = "/api")
    app.register_blueprint(data_visualization_api.dataVisulizationAPI, url_prefix = "/api")
    app.register_blueprint(data_overview_api.dataOverviewAPI, url_prefix = "/api")
    app.register_blueprint(data_cleaning_api.dataCleaningAPI, url_prefix = "/api")

def create_app():
    '''
    Function to create flask app
    '''
    app = Flask(__name__)
    
    CORS(app)
    
    if os.getenv("ENVIRONMENT") == "development":
        app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv("LOCAL_POSTGRES_URL")
    elif os.getenv("ENVIRONMENT") == "production":
        app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv("POSTGRESS_DATABASE_URL")

    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SECRET_KEY'] = os.getenv("SECRET_KEY")
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=3)

    db.init_app(app)
    migrate.init_app(app, db)


    # with app.app_context():
    #     db.create_all()

    set_login_manager(app)
    add_end_points(app)
    set_logger()

    return app

app = create_app()
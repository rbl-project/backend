from flask import Blueprint
from flask import current_app as app
from models.user_model import Users
from utilities.respond import respond
from flask_restful import  Api
from flask import request
from flask_login import current_user, login_required
import pandas as pd
from manage.db_setup import db
from sqlalchemy import text
from utilities.constants import ALLOWED_DB_PER_USER

datasetAPI = Blueprint("datasetAPI", __name__)
datasetAPI_restful = Api(datasetAPI)

# Api to upload dataset
@datasetAPI.route("/upload-dataset", methods=['POST'])
@login_required
def upload_dataset():
    err = None
    try:
        user = Users.query.filter_by(id=current_user.id).first()
        if not user:
            err = "No such user exits"
            raise

        if user.db_count > ALLOWED_DB_PER_USER:
            err = f"You cannot add more than {ALLOWED_DB_PER_USER} databases. Please delete few databases to add a new one."
            raise

        dataset = request.files['dataset']
        if not dataset.filename:
            err = "Dataset file is required"
            raise

        dataset_name = f'{dataset.filename.split(".")[0]}_{user.id}'
        if dataset_name in db.engine.table_names():
            err = "This database already exists"
            raise

        df = pd.read_csv(dataset)
        df.columns = [c.lower() for c in df.columns] # PostgreSQL doesn't like capitals or spaces

        df.to_sql(dataset_name, db.engine)
        
        user.db_count = user.db_count + 1
        user.save()

        app.logger.info("Dataset uploaded successfully %s",str(dataset.filename))

        res = {
            "msg":"Dataset Uploaded Successfully"
        }
        return respond(data=res)

    except Exception as e:
        app.logger.error("Error in uploading the dataset. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = 'Error in uploading the dataset'
        return respond(error=err)


# Api to delete a dataset
@datasetAPI.route("/delete-dataset", methods=["POST"])
@login_required
def delete_dataset():
    err = None
    try:
        user = Users.query.filter_by(id=current_user.id).first()
        if not user:
            err = "No such user exits"
            raise

        dataset_name = request.json.get("dataset_name")
        if not dataset_name:
            err = "Dataset name is required that is to be deleted"

        tables = db.engine.table_names()
        if f'{dataset_name.split(".")[0]}_{user.id}' not in tables:
            err = "No such database exists"
            raise
        # not the right way to do this. But due to time issue, this is done. Don't use raw query
        delete_sql_query = text(f'DROP TABLE "{dataset_name.split(".")[0]}_{user.id}";')

        try:
            result = db.engine.execute(delete_sql_query)
        except Exception as e:
            err = "Some error in deleting the dataset. Please try again later"
            raise e
        
        user.db_count = user.db_count - 1
        user.save()

        res = {
            "msg":"Dataset Deleted Successfully"
        }
        return respond(data=res)

    except Exception as e:
        app.logger.error("Error in deleting the dataset. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = 'Error in deleting the dataset'
        return respond(error=err)


# Api to fetch all the tables in the database
@datasetAPI.route("/get-datasets", methods=["GET"])
@login_required
def get_datasets():
    err = None
    try:
        user = Users.query.filter_by(id=current_user.id).first()
        if not user:
            err = "No such user exits"
            raise

        tables = db.engine.table_names()
        tables = [t for t in tables if t.endswith(f'_{user.id}')]
        res = {
            "tables":tables
        }
        return respond(data=res)

    except Exception as e:
        app.logger.error("Error in getting the datasets. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = 'Error in getting the datasets'
        return respond(error=err)


# Api to fetch all the columns in the table
@datasetAPI.route("/get-columns", methods=["POST"])
@login_required
def get_columns():
    err = None
    try:
        user = Users.query.filter_by(id=current_user.id).first()
        if not user:
            err = "No such user exits"
            raise

        table_name = request.json.get("dataset_name")
        if not table_name:
            err = "Dataset name is required that is to be deleted"

        tables = db.engine.table_names()
        if f'{table_name.split(".")[0]}_{user.id}' not in tables:
            err = "No such database exists"
            raise
        # not the right way to do this. But due to time issue, this is done. Don't use raw query
        columns_sql_query = text(f'SELECT * FROM "{table_name.split(".")[0]}_{user.id}" LIMIT 1;')

        try:
            result = db.engine.execute(columns_sql_query)
        except Exception as e:
            err = "Some error in getting the columns. Please try again later"
            raise e
        
        columns = [c[0] for c in result.cursor.description]
        res = {
            "columns":columns
        }
        return respond(data=res)

    except Exception as e:
        app.logger.error("Error in getting the columns. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = 'Error in getting the columns'
        return respond(error=err)
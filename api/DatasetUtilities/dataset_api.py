import os
from flask import (
    Blueprint,
    request,
    current_app as app,
    send_file,
    Response
)
from models.user_model import Users
from utilities.methods import ( 
    get_dataset, 
    get_dataset_name, 
    get_parquet_dataset_file_name, 
    get_user_directory
)
from utilities.respond import respond
from flask_restful import  Api
from flask_login import current_user, login_required
import pandas as pd
from manage.db_setup import db
from sqlalchemy import text
from utilities.constants import ALLOWED_DB_PER_USER
from pathlib import Path
from manage.celery_setup import celery_instance

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

        # dataset_name = f'{dataset.filename.split(".")[0]}_{user.id}'
        dataset_name = get_dataset_name(user.id, dataset.filename)

        # Check if you have the directory for the user
        directory = get_user_directory(user.email)
        Path(directory).mkdir(parents=True, exist_ok=True) # creates the directory if not present
        
        # Check if the dataset already exists
        dataset_file = get_parquet_dataset_file_name(dataset_name, user.email)
        if Path(dataset_file).is_file():
            err = "This dataset already exists"
            raise
        
        # Read the csv and convert it into parquet
        df = pd.read_csv(dataset)
        df.to_parquet(dataset_file, compression="snappy", index=False)

        user.db_count = user.db_count + 1
        user.save()

        app.logger.info("Dataset '%s' uploaded successfully",str(dataset.filename))

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

        # dataset_name = f'{dataset_name.split(".")[0]}_{user.id}'
        dataset_name = get_dataset_name(user.id, dataset_name)


        # Check if you have the directory for the user
        directory = get_user_directory(user.email)
        Path(directory).mkdir(parents=True, exist_ok=True) # creates the directory if not present

        # Check if the dataset already exists
        dataset_file = get_parquet_dataset_file_name(dataset_name, user.email)
        if not Path(dataset_file).is_file():
            err = "This dataset does not exists"
            raise
        
        # Delete the dataset
        Path(dataset_file).unlink()

        # =============== Older Approach ===============
        # tables = db.engine.table_names()
        # if f'{dataset_name.split(".")[0]}_{user.id}' not in tables:
        #     err = "No such database exists"
        #     raise
        # # not the right way to do this. But due to time issue, this is done. Don't use raw query
        # delete_sql_query = text(f'DROP TABLE "{dataset_name.split(".")[0]}_{user.id}";')

        # try:
        #     result = db.engine.execute(delete_sql_query)
        # except Exception as e:
        #     err = "Some error in deleting the dataset. Please try again later"
        #     raise e
        # ===============================================

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
    finally:
        # If directory is empty, delete the directory
        if not os.listdir(directory):
            os.rmdir(directory)


# Api to export a dataset
@datasetAPI.route("/export-dataset", methods=["POST"])
@login_required
def export_dataset():
    err = None
    try:
        user = Users.query.filter_by(id=current_user.id).first()
        if not user:
            err = "No such user exits"
            raise

        dataset_name = request.json.get("dataset_name")
        if not dataset_name:
            err = "Dataset name is required"

        dataset_name = get_dataset_name(user.id, dataset_name)
        dataset_file = get_parquet_dataset_file_name(dataset_name, user.email)

        if not Path(dataset_file).is_file():
            err = "This dataset does not exists"
            raise

        df = pd.read_parquet(dataset_file)
        df = df.to_csv(index=False)

        return Response(
            df,
            mimetype="text/csv",
            headers={"Content-disposition":
            "attachment; filename=filename.csv"}
        )

    except Exception as e:
        app.logger.error("Error in exporting the dataset. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = 'Error in exporting the dataset'
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
        
        # =============== Older Approach ===============
        # tables = db.engine.table_names()
        # tables = [t for t in tables if t.endswith(f'_{user.id}')]
        # ===============================================

        user_directory = get_user_directory(user.email)
        Path(user_directory).mkdir(parents=True, exist_ok=True) # creates the directory if not present

        all_datesets = os.listdir(user_directory)

        res = {
            "email": user.email,
            "datasets":all_datesets
        }
        
        keep_alive.delay()

        return respond(data=res)

    except Exception as e:
        app.logger.error("Error in getting the datasets. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = 'Error in getting the datasets'
        return respond(error=err)
    finally:
        # If directory is empty, delete the directory
        if not os.listdir(user_directory):
            os.rmdir(user_directory)

# Api to test redis functionality
@celery_instance.task
def keep_alive():
    import time
    for i in range(5):
        time.sleep(1)
        print("Celery task running")
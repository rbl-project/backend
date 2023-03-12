from datetime import datetime
import os
from flask import (
    Blueprint,
    request,
    current_app as app,
    Response
)
from models.user_model import Users
from utilities.methods import ( 
    check_dataset_copy_exists,
    get_dataset_name, 
    get_parquet_dataset_file_name, 
    get_user_directory,
    load_dataset,
    log_error
)
from utilities.respond import respond
from flask_restful import  Api
import pandas as pd
from utilities.constants import ALLOWED_DB_PER_USER
from pathlib import Path
from manage.celery_setup import celery_instance
from flask_jwt_extended import jwt_required, get_jwt_identity

datasetAPI = Blueprint("datasetAPI", __name__)
datasetAPI_restful = Api(datasetAPI)

# Api to upload dataset
@datasetAPI.route("/upload-dataset", methods=['POST'])
@jwt_required()
def upload_dataset():
    """
        TAKES dataset file as input
        PERFORMS the upload dataset operation
        RETURNS the success/failure as response
    """
    err = None
    try:
        current_user = get_jwt_identity()
        user = Users.query.filter_by(id=current_user["id"]).first()
        if not user:
            err = "No such user exits"
            raise

        if user.db_count >= ALLOWED_DB_PER_USER:
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
        df.to_parquet(dataset_file, compression="snappy", index=True)

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
@jwt_required()
def delete_dataset():
    """
        TAKES dataset name as input
        PERFORMS the delete dataset operation
        RETURNS the success/failure as response
    """
    err = None
    try:
        current_user = get_jwt_identity()
        user = Users.query.filter_by(id=current_user["id"]).first()
        if not user:
            err = "No such user exits"
            raise

        directory = get_user_directory(user.email)

        dataset_name = request.json.get("dataset_name")
        if not dataset_name:
            err = "Dataset name is required that is to be deleted"
            raise

        # dataset_name = f'{dataset_name.split(".")[0]}_{user.id}'
        dataset_name = get_dataset_name(user.id, dataset_name)

        # Check if you have the directory for the user
        Path(directory).mkdir(parents=True, exist_ok=True) # creates the directory if not present

        # Check if the dataset already exists
        dataset_file = get_parquet_dataset_file_name(dataset_name, user.email)
        if not Path(dataset_file).is_file():
            err = "This dataset does not exists"
            raise
        
        # Delete the dataset
        Path(dataset_file).unlink()

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
        if directory:
            if not os.listdir(directory):
                os.rmdir(directory)


# Api to export a dataset
@datasetAPI.route("/export-dataset", methods=["POST"])
@jwt_required()
def export_dataset():
    """
        TAKES dataset name as input
        PERFORMS export the dataset operations
        RETURNS the csv dataset as response
    """
    err = None
    try:
        current_user = get_jwt_identity()
        user = Users.query.filter_by(id=current_user["id"]).first()
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


# Api to rename a dataset
@datasetAPI.route("/rename-dataset", methods=["POST"])
@jwt_required()
def rename_dataset():
    """
        TAKES dataset name and new dataset name as input
        PERFORMS the rename operation of old dataset
        RETURNS the success/failure as response
    """
    err = None
    try:
        current_user = get_jwt_identity()
        user = Users.query.filter_by(id=current_user["id"]).first()
        if not user:
            err = "No such user exits"
            raise
        
        dataset_name = request.json.get("dataset_name")
        if not dataset_name:
            err = "Dataset name is required"
            raise

        new_dataset_name = request.json.get("new_dataset_name")
        if not new_dataset_name:
            err = "New dataset name is required"
            raise

        dataset_name = get_dataset_name(user.id, dataset_name)
        new_dataset_name = get_dataset_name(user.id, new_dataset_name)

        dataset_file = get_parquet_dataset_file_name(dataset_name, user.email)
        if not Path(dataset_file).is_file():
            err = f"The dataset does not exists"
            raise

        new_dataset_file = get_parquet_dataset_file_name(new_dataset_name, user.email)

        os.rename(dataset_file, new_dataset_file)

        res = {
            "msg":"Dataset Renamed Successfully"
        }

        return respond(data=res)

    except Exception as e:
        app.logger.error("Error in renaming the dataset. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = 'Error in renaming the dataset'
        return respond(error=err)


# Api to fetch all the tables in the database
@datasetAPI.route("/get-datasets", methods=["GET"])
@jwt_required()
def get_datasets():
    """
        TAKES nothing as input
        PERFORMS fetch all the datasets of the user
        RETURNS the list of datasets as response
    """
    err = None
    try:
        current_user = get_jwt_identity()
        user = Users.query.filter_by(id=current_user["id"]).first()
        if not user:
            err = "No such user exits"
            raise
        
        user_directory = get_user_directory(user.email)
        Path(user_directory).mkdir(parents=True, exist_ok=True) # creates the directory if not present

        all_datasets_list = os.listdir(user_directory)
        all_datasets_dict = []
        for dataset in all_datasets_list:
            name = dataset.split(".")[0] # remove the .parquet extension
            name = "_".join(name.split("_")[:-1]) # remove the user id
            temp = {
                "name":name,
                "modified": datetime.fromtimestamp(os.path.getmtime(os.path.join(user_directory, dataset))).strftime('%d %b, %Y %H:%M:%S'),
                "size": round(os.path.getsize(os.path.join(user_directory, dataset)) / (1024), 2) # (KB) THIS IS WRONG WE NEED CSV SIZE NOT PARQUET SIZE
            }
            all_datasets_dict.append(temp)

        # sort the list by modified date
        all_datasets_dict = sorted(all_datasets_dict, key=lambda k: datetime.strptime(k['modified'], '%d %b, %Y %H:%M:%S'), reverse=True)

        res = {
            "email": user.email,
            "datasets":all_datasets_dict,
            "db_count":user.db_count
        }

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
            

# Api to get the categorical columns of the dataset
@datasetAPI.route("/get-columns-info", methods=["POST"])
@jwt_required()
def get_all_columns_info():
    """
        TAKES dataset name as input
        PERFORMS fetch the categorical columns of the dataset
        RETURNS the list of categorical columns as response
    """
    err = None
    try:
        current_user = get_jwt_identity()
        user = Users.query.filter_by(id=current_user["id"]).first()
        if not user:
            err = "No such user exits"
            raise

        if not request.is_json:
            err="Missing JSON in request"
            raise
        
        dataset_name = request.json.get("dataset_name")
        if not dataset_name:
            err = "Dataset name is required"
            raise

        df, err = load_dataset(dataset_name, user.id, user.email)
        if err:
            raise
        
        # all the columns
        all_columns = df.columns.tolist()

        # get the categorical columns
        categorical_columns = df.select_dtypes(include=['object', 'bool']).columns.tolist()

        # get the unique values of categorical column which have less than 100 unique values
        values = {}
        final_categorical_columns = []
        for column in categorical_columns:
            if len(df[column].unique()) <= 100:
                final_categorical_columns.append(column)
                values[column] = df[column].unique().tolist()

        # get the numerical columns
        numerical_columns = df.select_dtypes(exclude=['object', 'bool']).columns.tolist()

        # numbert of rows and columns
        rows = df.shape[0]
        columns = df.shape[1]

        # datatypes of all the columns
        temp_dtypes = df.dtypes.to_dict()
        dtypes = {}
        for key, value in temp_dtypes.items():
            dtypes[key]=str(value)

        res = {
            "categorical_columns":final_categorical_columns,
            "numerical_columns":numerical_columns,
            "categorical_values": values,
            "all_columns":all_columns,
            "n_rows":rows,
            "n_columns":columns,
            "dtypes":dtypes
        }

        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in fetching the all columns info", error=err, exception=e)
        if not err:
            err = "Error in fetching the all columns info"
        return respond(error=err)


# Api to test redis functionality
@celery_instance.task
def keep_alive():
    """
    Celery task to keep the redis connection alive
    """
    import time
    for i in range(5):
        time.sleep(1)
        print("Celery task running")
        
        
# Api to get the categorical columns of the dataset
@datasetAPI.route("/get-categorical-columns-info", methods=["POST"])
@jwt_required()
def get_categorical_columns_info():
    """
        TAKES dataset name as input
        PERFORMS fetch the categorical columns of the dataset
        RETURNS the list of categorical columns as response
    """
    err = None
    try:
        current_user = get_jwt_identity()
        user = Users.query.filter_by(id=current_user["id"]).first()
        if not user:
            err = "No such user exits"
            raise

        if not request.is_json:
            err="Missing JSON in request"
            raise
        
        dataset_name = request.json.get("dataset_name")
        if not dataset_name:
            err = "Dataset name is required"
            raise

        df, err = load_dataset(dataset_name, user.id, user.email)
        if err:
            raise

        # get the categorical columns
        categorical_columns = df.select_dtypes(include=['object', 'bool']).columns.tolist()

        res = {
            "categorical_columns":categorical_columns,
            "n_categorical_columns":len(categorical_columns),
        }

        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in fetching the Categorical Columns List", error=err, exception=e)
        if not err:
            err = "Error in fetching the Categorical Columns List"
        return respond(error=err)
    

# Api to get the numerical columns of the dataset
@datasetAPI.route("/get-numerical-columns-info", methods=["POST"])
@jwt_required()
def get_numerical_columns_info():
    """
        TAKES dataset name as input
        PERFORMS fetch the numerical columns of the dataset
        RETURNS the list of numerical columns as response
    """
    err = None
    try:
        current_user = get_jwt_identity()
        user = Users.query.filter_by(id=current_user["id"]).first()
        if not user:
            err = "No such user exits"
            raise

        if not request.is_json:
            err="Missing JSON in request"
            raise
        
        dataset_name = request.json.get("dataset_name")
        if not dataset_name:
            err = "Dataset name is required"
            raise

        df, err = load_dataset(dataset_name, user.id, user.email)
        if err:
            raise

        # get the numerical columns
        numerical_columns = df.select_dtypes(exclude=['object', 'bool']).columns.tolist()

        res = {
            "numerical_columns":numerical_columns,
            "n_numerical_columns":len(numerical_columns),
        }

        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in fetching the Numerical Columns List", error=err, exception=e)
        if not err:
            err = "Error in fetching the Numerical Columns List"
        return respond(error=err)
    

# Api to save the current dataset copy as the new dataset
@datasetAPI.route("/save-changes", methods=["POST"])
@jwt_required()
def save_changes():
    """
        TAKES dataset name as input
        PERFORMS save the functionality of deleting the current dataset and renaming the copy dataset as new dataset
        RETURNS the success message as response
    """
    err = None
    try:
        current_user = get_jwt_identity()
        user = Users.query.filter_by(id=current_user["id"]).first()
        if not user:
            err = "No such user exits"
            raise

        if not request.is_json:
            err="Missing JSON in request"
            raise
        
        dataset_name = request.json.get("dataset_name")
        if not dataset_name:
            err = "Dataset name is required"
            raise
        
        # check if copy exits
        if not check_dataset_copy_exists(dataset_name, user.id, user.email):
            err = "Copy of the dataset does not exists"
            raise
        else:

            directory = get_user_directory(user.email)

            # delete the current dataset
            dataset_name = get_dataset_name(user.id, dataset_name)

            # Check if you have the directory for the user
            Path(directory).mkdir(parents=True, exist_ok=True) # creates the directory if not present

            # Check if the dataset already exists
            dataset_file = get_parquet_dataset_file_name(dataset_name, user.email)
            if not Path(dataset_file).is_file():
                err = "This original dataset does not exists"
                raise
            
            # Delete the dataset
            Path(dataset_file).unlink()
            
            # rename the copy dataset as new dataset
            copy_dataset_name = dataset_name + "_copy"

            copy_dataset_file = get_parquet_dataset_file_name(copy_dataset_name, user.email)

            os.rename(copy_dataset_file, dataset_file) # dataset_file is the og dataset file

            res = {
                "msg":"Dataset saved successfully"
            }

            app.logger.info("Saved the changes in %s dataset", dataset_name)

            return respond(data=res)

    except Exception as e:
        log_error(err_msg="Error in saving the dataset as new dataset", error=err, exception=e)
        if not err:
            err = "Error in saving the dataset as new dataset"
        return respond(error=err)


@datasetAPI.route("/revert-changes", methods=["POST"])
@jwt_required()
def revert_changes():
    """
    """
    err=None
    try:
        current_user = get_jwt_identity()
        user = Users.query.filter_by(id=current_user["id"]).first()
        if not user:
            err = "No such user exits"
            raise

        if not request.is_json:
            err="Missing JSON in request"
            raise
        
        dataset_name = request.json.get("dataset_name")
        if not dataset_name:
            err = "Dataset name is required"
            raise

        dataset_name = get_dataset_name(user.id, dataset_name) # dataset_name = iris_1
        dataset_name = dataset_name + "_copy" # dataset_name = iris_1_copy
        dataset_file_copy = get_parquet_dataset_file_name(dataset_name, user.email)

        if Path(dataset_file_copy).is_file():
            Path(dataset_file_copy).unlink()
        else:
            err = "Dataset copy does not exists"
            raise
        
        res = {
            "msg":"Dataset copy deleted successfully"
        }

        app.logger.info("Reverted the changes in %s dataset", dataset_name)
        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in reverting the changes", error=err, exception=e)
        if not err:
            err = "Error in reverting the changes"
        return respond(error=err)

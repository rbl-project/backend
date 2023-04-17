from datetime import datetime
import os
from flask import (
    Blueprint,
    request,
    current_app as app,
    Response
)
from models.user_model import Users
from models.dataset_metadata_model import MetaData
from utilities.methods import ( 
    check_dataset_copy_exists,
    delete_dataset_copy,
    make_dataset_copy,
    get_dataset_name, 
    get_parquet_dataset_file_name, 
    get_user_directory,
    load_dataset,
    load_dataset_copy,
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


        # ======== Dataset MetaData ==========
        dataset_file_name = dataset_name  # Name of parquet file saved in the directory 
        dataset_size = dataset.content_length / 1000000 # Size of the dataset in MB
        _dataset_name, dataset_extension = dataset.filename.split(".") # Name of the dataset file and extension (Original)
        n_rows,n_columns = df.shape # Number of rows in the dataset
        n_values = n_rows * n_columns # Number of values in the dataset
        column_list = list(df.columns) # List of columns in the dataset
        column_datatypes = df.dtypes.astype(str).to_dict() # Dictionary of column name and its type
        numerical_column_list = df.select_dtypes(exclude=['object', 'bool']).columns.tolist() # List of numerical columns
        categorical_column_list = df.select_dtypes(include=['object', 'bool']).columns.tolist() # List of categorical columns
        deleted_column_list =  [] # List of deleted columns

        
        # Create the metadata object
        metadata_obj = MetaData(
            user_id = user.id,
            user_email = user.email,
            is_copy = False,
            is_copy_modified = False,
            date_created = datetime.now(),
            last_modified = datetime.now(),
            dataset_name = _dataset_name,
            dataset_extension = dataset_extension,
            dataset_file_name = dataset_file_name,
            dataset_size = dataset_size,
            n_rows = n_rows,
            n_columns = n_columns,
            n_values = n_values,
            column_list = column_list,
            column_datatypes = column_datatypes,
            numerical_column_list = numerical_column_list,
            categorical_column_list = categorical_column_list,
            deleted_column_list =  deleted_column_list
        )
        
        # If Metadata already exists for the dataset then delete it and create a new one
        existing_metadata = MetaData.objects(dataset_file_name=dataset_file_name).first()
        if existing_metadata:
            existing_metadata.delete()

        # Save the metadata object
        metadata_obj.save()
        app.logger.info("Dataset '%s' metadata saved successfully",str(dataset.filename))

        # Saving the dataset as parquet file
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

        dataset_name_to_check_copy = dataset_name # iris
        dataset_name = get_dataset_name(user.id, dataset_name) # iris_1

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

        app.logger.info("Dataset '%s' deleted successfully",str(dataset_name))

        # check if copy of the dataset exists 
        if check_dataset_copy_exists(dataset_name_to_check_copy, user.id, user.email):
            # Delete the copy
            delete_dataset_copy(dataset_name_to_check_copy, user.id, user.email)
            app.logger.info("Dataset '%s' copy deleted successfully",str(dataset_name))

        # Delete the metadata
        dataset_name_copy = dataset_name + "_copy" # iris_1_copy

        metadata_obj = MetaData.objects(dataset_file_name=dataset_name).first()
        # if Metadata does not exists then log it and continue
        if metadata_obj:
            metadata_obj.delete()
            app.logger.info("Dataset '%s' metadata deleted successfully",str(dataset_name))
        else:
            app.logger.info(f"Metadata for '{dataset_name}' does not exists")
        
        metadata_obj_copy = MetaData.objects(dataset_file_name=dataset_name_copy).first()
        # if Metadata does not exists then log it and continue
        if metadata_obj_copy:
            metadata_obj_copy.delete()
            app.logger.info("Dataset '%s' metadata deleted successfully",str(dataset_name_copy))
        else:
            app.logger.info(f"Metadata for '{dataset_name_copy}' does not exists")

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
        
        dataset_name_inp = request.json.get("dataset_name")
        if not dataset_name_inp:
            err = "Dataset name is required"
            raise

        new_dataset_name_inp = request.json.get("new_dataset_name")
        if not new_dataset_name_inp:
            err = "New dataset name is required"
            raise

        dataset_name = get_dataset_name(user.id, dataset_name_inp) # irs_1
        new_dataset_name = get_dataset_name(user.id, new_dataset_name_inp) # temp_1

        dataset_file = get_parquet_dataset_file_name(dataset_name, user.email)
        if not Path(dataset_file).is_file():
            err = f"The dataset does not exists"
            raise

        new_dataset_file = get_parquet_dataset_file_name(new_dataset_name, user.email)
        os.rename(dataset_file, new_dataset_file)
        
        # Update the metadata
        metadata_obj = MetaData.objects(dataset_file_name=dataset_name).first_or_404(message=f"Metadata for '{dataset_name}' does not exists")
        metadata_obj.update(dataset_name=new_dataset_name_inp, dataset_file_name=new_dataset_name)
        
        app.logger.info("Dataset '%s' renamed to '%s' successfully",str(dataset_name_inp), str(new_dataset_name_inp))
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
            # split current name and check if there is word "copy" in it at the end
            name_split = name.split("_")
            if name_split[-1] == "copy":
                continue
            name = "_".join(name_split[:-1]) # remove the user id
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

        dataset_file_name = get_dataset_name(user.id, dataset_name) # dataset_name = iris_1
        
        df = None
        err = None
        # Look if the copy of dataset exists and if it does, load dataset copy otherwise load the original dataset
        if check_dataset_copy_exists(dataset_name, user.id, user.email):
            df,err = load_dataset_copy(dataset_name, user.id, user.email)
            metadata = MetaData.objects(dataset_file_name=dataset_file_name+"_copy").first_or_404(message=f"Dataset Metadata for {dataset_file_name}_copy not found")
        else:
            df,err = load_dataset(dataset_name, user.id, user.email)
            metadata = MetaData.objects(dataset_file_name=dataset_file_name).first_or_404(message=f"Dataset Metadata for {dataset_file_name} not found")
        
        if err: 
            raise
        
        
        #todo: Give all the categorical values in the response and give top 100 unique values in the frontend. As soon as someone types something we give next top 100
        values = {}
        for column in metadata.categorical_column_list:
            values[column] = df[column].unique().tolist()[0:100]


        res = {
            "categorical_columns":metadata.categorical_column_list,
            "numerical_columns":metadata.numerical_column_list,
            "categorical_values": values,
            "all_columns":metadata.column_list,
            "n_rows":metadata.n_rows,
            "n_columns":metadata.n_columns,
            "dtypes":metadata.column_datatypes,
            "deleted_column_list":metadata.deleted_column_list 
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

        dataset_file_name = get_dataset_name(user.id, dataset_name) # dataset_name = iris_1
        
        metadata = None
         # Look if the copy of dataset exists and if it does, load dataset copy metadata otherwise load the original dataset metadata
        if check_dataset_copy_exists(dataset_name, user.id, user.email):
            metadata = MetaData.objects(dataset_file_name=dataset_file_name+"_copy").first_or_404(message=f"Dataset Metadata for {dataset_file_name}_copy not found")
        else:
            metadata = MetaData.objects(dataset_file_name=dataset_file_name).first_or_404(message=f"Dataset Metadata for {dataset_file_name} not found")

        res = {
            "categorical_columns":metadata.categorical_column_list,
            "n_categorical_columns":len(metadata.categorical_column_list),
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

        dataset_file_name = get_dataset_name(user.id, dataset_name) # dataset_name = iris_1
        
        metadata = None
         # Look if the copy of dataset exists and if it does, load dataset copy metadata otherwise load the original dataset metadata
        if check_dataset_copy_exists(dataset_name, user.id, user.email):
            metadata = MetaData.objects(dataset_file_name=dataset_file_name+"_copy").first_or_404(message=f"Dataset Metadata for {dataset_file_name}_copy not found")
        else:
            metadata = MetaData.objects(dataset_file_name=dataset_file_name).first_or_404(message=f"Dataset Metadata for {dataset_file_name} not found")

        res = {
            "numerical_columns":metadata.numerical_column_list,
            "n_numerical_columns":len(metadata.numerical_column_list),
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
            err = "No changes to save"
            raise
        else:

            directory = get_user_directory(user.email)

            # delete the current dataset
            dataset_name = get_dataset_name(user.id, dataset_name) # iris_1
            copy_dataset_name = dataset_name + "_copy" #iris_1_copy

            # Get Copy Metadata and Replace it with the original metadata
            copy_metadata_obj = MetaData.objects(dataset_file_name=copy_dataset_name).first()
            # if Copy Metadata Exists then replace it in place of original metadata and delete the copy metadata
            if copy_metadata_obj:
                og_metadata_obj = MetaData.objects(dataset_file_name=dataset_name).first()
                copy_metadata_dict = copy_metadata_obj.to_mongo().to_dict()
                del copy_metadata_dict["_id"]
                copy_metadata_dict["last_modified"] = datetime.now()
                copy_metadata_dict["is_copy"] = False
                copy_metadata_dict["is_copy_modified"] = False
                copy_metadata_dict["dataset_file_name"] = dataset_name
                copy_metadata_dict["deleted_column_list"] = []
                og_metadata_obj.update(**copy_metadata_dict)
                copy_metadata_obj.delete()
                app.logger.info("Dataset '%s' metadata deleted successfully",str(dataset_name))
            else:
                err = f"Metadata for '{dataset_name}' does not exists"
                raise

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
            copy_dataset_file = get_parquet_dataset_file_name(copy_dataset_name, user.email)
            os.rename(copy_dataset_file, dataset_file) # dataset_file is the og dataset file
            
            res = {
                "msg":"Dataset changes Saved Successfully"
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
            err = "No changes detected in the current dataset"
            raise
        
        copy_metadata_obj = MetaData.objects(dataset_file_name=dataset_name).first_or_404(message=f"Metadata for '{dataset_name}' does not exists")
        
        copy_metadata_obj.delete()
                
        res = {
            "msg":"Dataset changes Reverted Successfully"
        }

        app.logger.info("Reverted the changes in %s dataset", dataset_name)
        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in reverting the changes", error=err, exception=e)
        if not err:
            err = "Error in reverting the changes"
        return respond(error=err)


# Api to search the for a specific value in the dataset using the pattern matching
@datasetAPI.route("/search-categorical-value", methods=["POST"])
@jwt_required()
def search_categorical_value():
    """
        NEED API TRHOTELLING FOR THIS API : https://www.section.io/engineering-education/implementing-rate-limiting-in-flask/
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

        '''
            Request Body : {
                "dataset_name":"iris",
                "column_name":"sepal_length",
                "search_value":"5.1"
            }
        '''

        column_name = request.json.get("column_name")
        if not column_name:
            err = "Column name is required"
            raise

        search_value = request.json.get("search_value")
        if not search_value:
            err = "Search value is required"
            raise
        
        # Check if dataset copy exists
        if check_dataset_copy_exists(dataset_name, user.id, user.email):
            df, err = load_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
        else:
            df, err = load_dataset(dataset_name, user.id, user.email)
            if err:
                raise
            
        # print(dataset_name, column_name, search_value)
        # fetch the categorical columns
        categorical_columns = df.select_dtypes(include=['object', 'bool']).columns.tolist()
        if column_name not in categorical_columns:
            err = "Column is not a categorical column"
            raise

        # check if the value is present in the column and if yes then return the values
        values = []
        values = df[df[column_name].apply(str.lower).str.contains(f'^{search_value.lower()}.*')][column_name].unique().tolist()

        res = {
            "search_result":values
        }

        return respond(data=res)
    
    except Exception as e:
        # log_error(err_msg="Error in searching the categorical value. Sending empty list", error=err, exception=e)
        # NO NEED TO RAISE EXCEPTION HERE AS IT IS NOT A REGULAR API
        res = {
            "search_result":[]
        }
        return respond(data=res)
    

# Api to fetch the metadata of the dataset using the copy flag
@datasetAPI.route("/get-metadata", methods=["POST"])
@jwt_required()
def get_metadata():
    """
        TAKES the dataset name and copy flag returns the metadata of the dataset
        PERFORMS the following operations
            1. Fetch the metadata of the dataset
            2. Return the metadata
        RETURNS the metadata of the dataset
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

        # check if copy of the dataset exists
        if check_dataset_copy_exists(dataset_name, user.id, user.email):
            dataset_name = get_dataset_name(user.id, dataset_name) + "_copy"
        else:
            dataset_name = get_dataset_name(user.id, dataset_name)
        
        metadata_obj = MetaData.objects(dataset_file_name=dataset_name).first_or_404(message=f"Metadata for '{dataset_name}' does not exists")
        metadata = metadata_obj.to_mongo().to_dict()

        res = {
            "metadata":metadata
        }

        return respond(data=res)

    except Exception as e:
        log_error(err_msg="Error in fetching the metadata", error=err, exception=e)
        if not err:
            err = "Error in fetching the metadata"
        return respond(error=err)
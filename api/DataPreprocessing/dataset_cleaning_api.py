""" Dataset cleaning API """
# FLASK
from flask import Blueprint, request
from flask_jwt_extended import jwt_required, get_jwt_identity
from flask_restful import Api
from flask import current_app as app

# UTILITIES
from utilities.respond import respond
from utilities.methods import load_dataset_copy, load_dataset, log_error, make_dataset_copy, check_dataset_copy_exists, save_dataset_copy

# MODELS
from models.user_model import Users

# constants

# OTHER
import json

# BLUEPRINT
dataCleaningAPI = Blueprint("dataCleaningAPI", __name__)
dataCleaningAPI_restful = Api(dataCleaningAPI)


# Api to drop rows by categorical column values
@dataCleaningAPI.route("/drop-by-column-value", methods=["POST"])
@jwt_required()
def drop_by_column_value():
    """
        TAKES dataset name and column name and column value as input
        DROPS the rows with the given column value in the given dataset
        RETURNS the success as response
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

        '''
            Request body:{
                "dataset_name": "iris_1",
                "col_value_info": {
                    "col_name_1": ["value_1", "value_2"],
                    "col_name_2": ["value_1", "value_2"]
                }
            }
        '''
        col_value_info = request.json.get("col_value_info")
        if not col_value_info:
            err = "Column value info is required"
            raise

        # Load the dataset
        # Look if the copy of dataset exists and if it does, then rename the columns in that copy otherwise rename make a copy and rename the columns in that copy
        if not check_dataset_copy_exists(dataset_name, user.id, user.email):
            app.logger.info("Dataset copy of %s does not exist. Trying to make a copy of the dataset", dataset_name)
            err = make_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
    
        df, err = load_dataset_copy(dataset_name, user.id, user.email)
        if err:
            raise
        
        # ================== Business Logic Start ==================

        # Check if the column name is valid
        for col_name in col_value_info:
            if col_name not in df.columns:
                err = "Column name {} is not valid".format(col_name)
                raise
        
        # Drop the rows with the given column value
        for col_name in col_value_info:
            df = df[~df[col_name].isin(col_value_info[col_name])]

        # ================== Business Logic End ==================
        
        save_dataset_copy(df, dataset_name, user.id, user.email)

        res = {
            "msg": "Rows dropped by column value"
        }
        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in dropping the rows by columns values", error=err, exception=e)
        if not err:
            err = "Error in dropping the rows by columns values"
        return respond(error=err)


# Api to drop rows by numerical column values and range
@dataCleaningAPI.route("/drop-by-numerical-value", methods=["POST"])
@jwt_required()
def drop_by_numerical_value():
    """
        TAKES dataset name and column name and numerical column value as input
        PERFORMS the given operation on the given column
        RETURNS the success as response
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
            Request body:{
                "dataset_name": "iris_1",
                "col_value_info": {
                    "col_name_1": [from_value_1, to_value_1],
                    "col_name_2": [from_value_2, to_value_2]
                }
            }
        '''

        col_value_info = request.json.get("col_value_info")
        if not col_value_info:
            err = "Column value info is required"
            raise

        # Load the dataset
        # Look if the copy of dataset exists and if it does, then rename the columns in that copy otherwise rename make a copy and rename the columns in that copy
        if not check_dataset_copy_exists(dataset_name, user.id, user.email):
            app.logger.info("Dataset copy of %s does not exist. Trying to make a copy of the dataset", dataset_name)
            err = make_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
    
        df, err = load_dataset_copy(dataset_name, user.id, user.email)
        if err:
            raise

        # ================== Business Logic Start ==================

        # Check if the column name is valid
        for col_name in col_value_info:
            if col_name not in df.columns:
                err = "Column name {} is not valid".format(col_name)
                raise
        
        # Drop the rows with the given numerical column range
        for col_name in col_value_info:
            df = df[(df[col_name] < col_value_info[col_name][0]) | (df[col_name] > col_value_info[col_name][1])]
        
        # ================== Business Logic End ==================

        save_dataset_copy(df, dataset_name, user.id, user.email)

        res = {
            "msg": "Rows dropped by numerical value"
        }
        return respond(data=res)

    except Exception as e:
        log_error(err_msg="Error in dropping the rows by numerical values", error=err, exception=e)
        if not err:
            err = "Error in dropping the rows by numerical values"
        return respond(error=err)


# Api to drop rows by index
@dataCleaningAPI.route("/drop-by-row-index", methods=["POST"])
@jwt_required()
def drop_by_row_index():
    """
        TAKES dataset name and row_start and row_end index as input
        DROPS the given row in the given dataset
        RETURNS the success as response
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

        '''
            Request body:{
                "dataset_name": "iris_1",
                "row_start": 0,
                "row_end": 10
            }
        '''
        row_start = request.json.get("row_start")
        row_end = request.json.get("row_end")
        if row_start is None or row_end is None:
            err = "Row start and row end are required"
            raise

        # Load the dataset
        # Look if the copy of dataset exists and if it does, then rename the columns in that copy otherwise rename make a copy and rename the columns in that copy
        if not check_dataset_copy_exists(dataset_name, user.id, user.email):
            app.logger.info("Dataset copy of %s does not exist. Trying to make a copy of the dataset", dataset_name)
            err = make_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
    
        df, err = load_dataset_copy(dataset_name, user.id, user.email)
        if err:
            raise

        # ================== Business Logic Start ==================
        
        # Check if the row_start and row_end are valid
        if row_start < 0 or row_end < 0:
            err = "Row start and row end must be greater than 0"
            raise

        if row_start > row_end:
            err = "Row start must be less than row end"
            raise

        if row_end > len(df):
            err = "Row end must be less than the length of the dataset"
            raise

        df.drop(df.index[row_start:row_end+1], inplace=True)

        # ================== Business Logic End ==================

        save_dataset_copy(df, dataset_name, user.id, user.email)

        res = {
            "msg": "Rows dropped successfully",
        }

        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in dropping the rows and columns", error=err, exception=e)
        if not err:
            err = "Error in dropping the rows and columns"
        return respond(error=err)

# Api to derop columns by column name
@dataCleaningAPI.route("/drop-by-column-name", methods=["POST"])
@jwt_required()
def drop_by_column_name():
    """
        TAKES dataset name and column name as input
        DROPS the given column in the given dataset
        RETURNS the updated dataset as response
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

        '''
            Request body:{
                "dataset_name": "dataset_name",
                "col_list": ["col1", "col2", "col3"]
        '''
        
        col_list = request.json.get("col_list")
        if not col_list:
            err = "Column list is required"
            raise

        # Load the dataset
        # Look if the copy of dataset exists and if it does, then rename the columns in that copy otherwise rename make a copy and rename the columns in that copy
        if not check_dataset_copy_exists(dataset_name, user.id, user.email):
            app.logger.info("Dataset copy of %s does not exist. Trying to make a copy of the dataset", dataset_name)
            err = make_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
    
        df, err = load_dataset_copy(dataset_name, user.id, user.email)
        if err:
            raise

        # ================== Business Logic Start ==================

        # Check if all the columns exits in dataframe
        for col in col_list:
            if col not in df.columns:
                err = "Column {} does not exist in the dataset".format(col)
                raise

        df.drop(columns=col_list, inplace=True)

        # ================== Business Logic End ==================

        save_dataset_copy(df, dataset_name, user.id, user.email)

        res = {
            "msg": "Columns dropped successfully",
        }
        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in dropping the rows and columns", error=err, exception=e)
        if not err:
            err = "Error in dropping the rows and columns"
        return respond(error=err)
    

# Api to rename column names
@dataCleaningAPI.route("/rename-column", methods=["POST"])
@jwt_required()
def rename_column():
    """
        TAKES dataset name and column name and new name as input
        RENAMES the given column in the given dataset
        RETURNS the updated dataset as response
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

        '''
         Request body:{
            "dataset_name": "dataset_name",
            "col_name_change_info": {
                "column_name_1": "new_name_1",
                "column_name_2": "new_name_2",
            }
         }
        '''
        col_name_change_info = request.json.get("col_name_change_info")
        if not col_name_change_info:
            err = "Column name change info is required"
            raise

        # Look if the copy of dataset exists and if it does, then rename the columns in that copy otherwise rename make a copy and rename the columns in that copy
        if not check_dataset_copy_exists(dataset_name, user.id, user.email):
            app.logger.info("Dataset copy of %s does not exist. Trying to make a copy of the dataset", dataset_name)
            err = make_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
    
        df, err = load_dataset_copy(dataset_name, user.id, user.email)
        if err:
            raise
        
        # ================== Business Logic Start ==================

        df.rename(columns=col_name_change_info, inplace=True)

        # ================== Business Logic End ==================

        save_dataset_copy(df, dataset_name, user.id, user.email)
        
        res = {
            "msg": "Column names changed successfully",
        }
        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in renaming the columns", error=err, exception=e)
        if not err:
            err = "Error in renaming the columns"
        return respond(error=err)


# Api to find and replace
@dataCleaningAPI.route("/find-and-replace", methods=["POST"])
@jwt_required()
def find_and_replace():
    """
        TAKES dataset_name, column_name, find_value, replace_value as input 
        PERFOMS find and replace operation on the given column
        RETURNS the updated dataset as response
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
         Request body:{
            "dataset_name": "dataset_name",
            "find_relace_info": {
                "column_name_1": [
                    {
                        "find": "old value",
                        "replace_value": "new value",
                    }, 
                    {
                        "find": "old value",
                        "replace_value": "new value",
                    }
                ].
                "column_name_2": [
                    {
                        "find": "old value",
                        "replace_value": "new value",
                    }, 
                    {
                        "find": "old value",
                        "replace_value": "new value",
                    }
                ]
            }
         }
        '''
        find_relace_info = request.json.get("find_relace_info")
        if not find_relace_info:
            err = "Find and replace info is required"
            raise

        # Look if the copy of dataset exists and if it does, then rename the columns in that copy otherwise rename make a copy and rename the columns in that copy
        if not check_dataset_copy_exists(dataset_name, user.id, user.email):
            app.logger.info("Dataset copy of %s does not exist. Trying to make a copy of the dataset", dataset_name)
            err = make_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
    
        df, err = load_dataset_copy(dataset_name, user.id, user.email)
        if err:
            raise
        
        # ================== Business Logic Start ==================

        for column_name, find_replace_list in find_relace_info.items():
            find_list = []
            replace_list = []
            for find_replace in find_replace_list:
                find_list.append(find_replace["find"])
                replace_list.append(find_replace["replace_value"])
            df[column_name].replace(find_list, replace_list, inplace=True)

        # ================== Business Logic End ==================

        save_dataset_copy(df, dataset_name, user.id, user.email)

        res = {
            "msg": "Find and replace operation performed successfully",
        }
        return respond(data=res)

    except Exception as e:
        log_error(err_msg="Error in find and replace", error=err, exception=e)
        if not err:
            err = "Error in find and replace"
        return respond(error=err)
    

# Api to change the data type of a column
@dataCleaningAPI.route("/change-data-type", methods=["POST"])
@jwt_required()
def change_data_type():
    """
        TAKES dataset_name, column_name, new_data_type as input
        CHANGES the data type of the given column in the given dataset
        RETURNS the updated dataset as response
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
            Request body:{
                "dataset_name": "dataset_name",
                "col_data_type_change_info": {
                    "column_name_1": "new_data_type_1",
                    "column_name_2": "new_data_type_2",
                }
            }
        '''

        col_data_type_change_info = request.json.get("col_data_type_change_info")
        if not col_data_type_change_info:
            err = "Column data type change info is required"
            raise
        
        # Look if the copy of dataset exists and if it does, then rename the columns in that copy otherwise rename make a copy and rename the columns in that copy
        if not check_dataset_copy_exists(dataset_name, user.id, user.email):
            app.logger.info("Dataset copy of %s does not exist. Trying to make a copy of the dataset", dataset_name)
            err = make_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
        
        df, err = load_dataset_copy(dataset_name, user.id, user.email)
        if err:
            raise
        
        # ================== Business Logic Start ==================

        try:
            df = df.astype(col_data_type_change_info)
        except ValueError as v:
            err = str(v)
            raise

        # ================== Business Logic End ==================
    
        save_dataset_copy(df, dataset_name, user.id, user.email)

        res = {
            "msg": "Data type of the columns changed successfully",
        }
        return respond(data=res)

    except Exception as e:
        log_error(err_msg="Error in changing the data type of the columns", error=err, exception=e)
        if not err:
            err = "Error in changing the data type of the columns"
        return respond(error=err)
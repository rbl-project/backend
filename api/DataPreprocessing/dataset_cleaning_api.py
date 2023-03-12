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

# OTHER
import json

# BLUEPRINT
dataCleaningAPI = Blueprint("dataCleaningAPI", __name__)
dataCleaningAPI_restful = Api(dataCleaningAPI)


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

        df.rename(columns=col_name_change_info, inplace=True)

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

        for column_name, find_replace_list in find_relace_info.items():
            find_list = []
            replace_list = []
            for find_replace in find_replace_list:
                find_list.append(find_replace["find"])
                replace_list.append(find_replace["replace_value"])
            df[column_name].replace(find_list, replace_list, inplace=True)
        
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
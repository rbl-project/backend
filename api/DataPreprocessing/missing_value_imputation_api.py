""" Missing Value Imputation API """

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
missingValueImputationAPI = Blueprint("missingValueImputationAPI", __name__)
missingValueImputationAPI_restful = Api(missingValueImputationAPI)


# Api to rename column names
@missingValueImputationAPI.route("/missing-value-percentage", methods=["POST"])
@jwt_required()
def get_missing_value_percentage():
    """
        TAKES dataset name and column wise missing value as input
        RETURNS the percentage of missing values in each column of the given dataset as well as the total percentage of missing values in the dataset
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
        app.logger.info("Dataset name: {}".format(dataset_name))
        
        column_wise_missing_value_type = request.json.get("column_wise_missing_value_type")
        if not column_wise_missing_value_type:
            err = "Column wise Missing value type is required"
            raise
        
        
        all_columns_missing_value_type = request.json.get("all_columns_missing_value_type")
        if not all_columns_missing_value_type:
            err = "All columns Missing value type is required"
            raise

        df = None
        err = None
        # Look if the copy of dataset exists and if it does, load dataset copy otherwise load the original dataset
        if check_dataset_copy_exists(dataset_name, user.id, user.email):
            df,err = load_dataset_copy(dataset_name, user.id, user.email)
        else:
            df,err = load_dataset(dataset_name, user.id, user.email)
            
        
            
        # ================================== Main Logic Start HERE ==================================
    
        # Get the percentage of missing values in each column
        cols = df.columns.tolist()
        column_wise_missing_value_data = []
        for col in cols:
            missing_percentage = round(df[col].eq(column_wise_missing_value_type[col]["missing_value"]).sum()/len(df[col]) * 100, 2)
            non_missing_percentage = 100 - missing_percentage
            column_wise_missing_value_data.append({
                "column_name": col, 
                "missing_value_percentage": missing_percentage, 
                "correct_value_percentage": non_missing_percentage,
                "missing_value_type": column_wise_missing_value_type[col]["missing_value"]
            })
        
        # Sort the column wise missing value data in descending order of missing value percentage
        column_wise_missing_value_data.sort(key=lambda x: x["missing_value_percentage"], reverse=True)
        
        # Get the total percentage of missing values in the dataset
        total_missing_value_percentage = round(df.eq(all_columns_missing_value_type["missing_value"]).sum().sum()/df.size * 100, 2)
        total_non_missing_value_percentage = 100 - total_missing_value_percentage
        total_missing_value_data = {
            "column_name": "all_columns",
            "missing_value_percentage": total_missing_value_percentage,
            "correct_value_percentage": total_non_missing_value_percentage,
            "missing_value_type": all_columns_missing_value_type["missing_value"]
        }
        
        # ================================ Main Logic Ends HERE =================================
        
        res = {
            "column_wise_missing_value_data": column_wise_missing_value_data,
            "total_missing_value_data": total_missing_value_data,
        }   
        
        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in Getting Missing Value Percentage", error=err, exception=e)
        if not err:
            err = "Error in Getting Missing Value Percentage"
        return respond(error=err)


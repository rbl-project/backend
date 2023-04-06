""" Data Descretization API """
# FLASK
from flask import Blueprint, request
from flask_jwt_extended import jwt_required, get_jwt_identity
from flask_restful import Api
from flask import current_app as app

# UTILITIES
from utilities.respond import respond
from utilities.methods import get_dataset_name, load_dataset_copy, load_dataset, log_error, make_dataset_copy, check_dataset_copy_exists, save_dataset_copy, get_row_column_metadata

# MODELS
from models.user_model import Users
from models.dataset_metadata_model import MetaData
# constants

# OTHER
import pandas as pd

# BLUEPRINT
dataDescretizationAPI = Blueprint("dataDescretizationAPI", __name__)
dataDescretizationAPI_restful = Api(dataDescretizationAPI)


# Api to perform data descretization
@dataDescretizationAPI.route("/data-descretization", methods=["POST"])
@jwt_required()
def data_descretization():
    """
        TAKES dataset_name, column_name, and range_list (start,end,category) as input
        PERFORMS data descretization on the given column_name
        RETURNS the dataset with the column_name discretized
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
        
        column_name = request.json.get("column_name")
        if not column_name:
            err = "Column Name is required"
            raise    
        
        range_list = request.json.get("range_list")
        if not range_list:
            err = "Range List is required"
            raise

        dataset_file_name = get_dataset_name(user.id, dataset_name) # dataset_name = iris_1
        
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
        
        metadata = None
         # Look if the copy of dataset exists and if it does, load dataset copy metadata otherwise load the original dataset metadata
        if check_dataset_copy_exists(dataset_name, user.id, user.email):
            metadata = MetaData.objects(dataset_file_name=dataset_file_name+"_copy").first_or_404(message=f"Dataset Metadata for {dataset_file_name}_copy not found")
        else:
            metadata = MetaData.objects(dataset_file_name=dataset_file_name).first_or_404(message=f"Dataset Metadata for {dataset_file_name} not found")
        
        
        
        # =============================================== # Data Descretization Logic Start Here ===============================================
        
        
        # ================================================ # Data Descretization Logic End Here ================================================
        
        res={
            "msg": "Data Descretization Successful",
        }
        
        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in Data Descretization", error=err, exception=e)
        if not err:
            err = "Error in Data Descretization"
        return respond(error=err)
    

    

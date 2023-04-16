""" Data transformation API """
# FLASK
from flask import Blueprint, request
from flask_jwt_extended import jwt_required, get_jwt_identity
from flask_restful import Api
from flask import current_app as app

# UTILITIES
from utilities.respond import respond
from utilities.methods import get_dataset_name, load_dataset_copy, log_error, make_dataset_copy, check_dataset_copy_exists, save_dataset_copy, get_row_column_metadata

# MODELS
from models.user_model import Users
from models.dataset_metadata_model import MetaData
# constants

# OTHER
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler

# BLUEPRINT
dataTransformationAPI = Blueprint("dataTransformationAPI", __name__)
dataTransformationAPI_restful = Api(dataTransformationAPI)


# Api to perform data transformation
@dataTransformationAPI.route("/data-transformation", methods=["POST"])
@jwt_required()
def data_transformation():
    """
        TAKES dataset_name, transformation_method, column_list as input
        PERFORMS transformation on dataset
        RETURNS success as response
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

        transformation_method = request.json.get("transformation_method")
        if not transformation_method:
            err = "Transformation method is required"
            raise
        
        column_list = request.json.get("column_list")
        if not column_list:
            err = "Column list is required"
            raise

        transformation_options = ["normalization","standardization", "log-transformation", "exponential-transformation"]
        if transformation_method not in transformation_options:
            err = "Invalid Transformation method"
            raise

        # Load the dataset
        # Look for the copy of dataset if it does not exist then create a copy for the dataset if copy already exists do not create another

        if not check_dataset_copy_exists(dataset_name, user.id, user.email):
            app.logger.info("Dataset copy of %s does not exist. Trying to make a copy of the dataset", dataset_name)
            err = make_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
    
        df, err = load_dataset_copy(dataset_name, user.id, user.email)
        if err:
            raise

        dataset_file_name = get_dataset_name(user_id=user.id, dataset_name=dataset_name) # iris_1
        copy_dataset_file_name = dataset_file_name + "_copy" # iris_1_copy
        
        # Load the metadata of Copy of the dataset
        metadata = MetaData.objects(dataset_file_name=copy_dataset_file_name).first_or_404(message=f"Metadata of {dataset_name} not found")

        #Checking column_list from request-body JSON with original column list 
        og_column_list= metadata.column_list
        for column_name in column_list:
            if column_name not in og_column_list:
                err="Column not found"
                raise


        # ================================Main logic starts here==============================

        if transformation_method == "log-transformation":
            df[column_list]=np.log(df[column_list])

        elif transformation_method == "exponential-transformation":
            df[column_list]=np.exp(df[column_list])
            
        elif transformation_method == "standardization":
            scaler = StandardScaler()  
            df[column_list] = scaler.fit_transform(df[column_list])

        elif transformation_method == "normalization":
            scaler = MinMaxScaler()
            df[column_list] = scaler.fit_transform(df[column_list])

        # ================================Main logic ends here==============================

        #Saving updated dataset
        save_dataset_copy(df, dataset_name, user.id, user.email)

        # Metadata updation
        updated_row_column_metadata = get_row_column_metadata(df)
        for key, value in updated_row_column_metadata.items():
            metadata[key] = value

        # Update the metadata of the dataset
        metadata.is_copy_modified = True
        metadata.save()

        res = {"msg":"Data Transformation Successfull"}
        return respond(data = res)

    except Exception as e:
        log_error(err_msg="Error in Missing Value Imputation", error=err, exception=e)
        if not err:
            err = "Error in Missing Value Imputation"
        return respond(error=err)

   
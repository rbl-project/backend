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
from models.dataset_metadata_model import MetaData
# constants

# OTHER
import json
from datetime import datetime as dt

# BLUEPRINT
missingValueImputationAPI = Blueprint("missingValueImputationAPI", __name__)
missingValueImputationAPI_restful = Api(missingValueImputationAPI)


# Api to drop rows by categorical column values
@missingValueImputationAPI.route("/create", methods=["GET"])
@jwt_required()
def create():
    print("create")
    new_metadata = MetaData(
        id = "id",
        user_id = "user_id",
        user_email = "user_email",
        is_copy = False,
        date_created = dt.now(),
        last_modified = dt.now(),
        dataset_name = "dataset_name",
        dataset_extension = "dataset_extension",
        dataset_file_name = "dataset_file_name",
        dataset_size = 100.5,
        n_rows = 100,
        n_columns = 100,
        n_values = 100,
        column_list = ["colum1","colum2"],
        column_datatypes = {"colum1":"int","colum2":"string"},
        numerical_column_list = ["colum1"],
        categorical_column_list = ["colum2"],
        column_wise_missing_value = {"colum1":0,"colum2":0},
        all_columns_missing_value = {"missing_value":0}
    )
    
    new_metadata.save()
    
    return respond(data={"message": "Created","data":new_metadata})


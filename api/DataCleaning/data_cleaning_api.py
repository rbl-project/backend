from flask import (
    Blueprint,
    request,
    current_app as app
)
from flask_restful import Api
from models.user_model import Users
from utilities.constants import DEFAULT_FILL_METHOD
from utilities.methods import (
    load_dataset, 
    save_dataset_copy
)
from utilities.respond import respond
from flask_jwt_extended import get_jwt_identity, jwt_required

dataCleaningAPI = Blueprint('dataCleaningAPI', __name__)
dataCleaningAPI_restful = Api(dataCleaningAPI)

# Api to get the missing values in dataset
@dataCleaningAPI.route('/check-missing-values', methods=['POST'])
@jwt_required()
def check_missing_values():
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

        df, err = load_dataset(dataset_name, user.id, user.email)
        if err:
            raise

        total_rows_with_missing_values = df.isnull().any(axis = 1).sum()
        col_with_count_of_missing_values = df.isnull().sum(axis = 0).to_dict()
        null_data = df[df.isnull().any(axis=1)].to_dict(orient='records')

        res = {
            "total_rows_with_missing_values": str(total_rows_with_missing_values),
            "col_with_count_of_missing_values": col_with_count_of_missing_values,
            "null_data": null_data
        }
        
        return respond(data=res, code=200)
    
    except Exception as e:
        app.logger.error("Error in getting the missing values. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = "Error in getting the missing values"
        return respond(error=err)


# Api to fill the missing values in dataset [ CURRENTLY FOR TESTING PURPOSE ONLY MODE VALUE IS ADDED FOR EACH COLUMN ]
@dataCleaningAPI.route('/fill-missing-values', methods=['POST'])
@jwt_required()
def fill_missing_values():
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
        
        method = request.json.get("method") if request.json.get("method") else DEFAULT_FILL_METHOD

        df, err = load_dataset(dataset_name, user.id, user.email)
        if err:
            raise

        if method == "mode":
            for i in df.columns:
                df[i].fillna(df[i].mode()[0], inplace=True)
        elif method == "mean":
            for i in df.columns:
                df[i].fillna(df[i].mean(), inplace=True)
        elif method == "median":
            for i in df.columns:
                df[i].fillna(df[i].median(), inplace=True)
        
        save_dataset_copy(df, dataset_name, user.id, user.email)
        
        return respond(data={"message": "Missing values filled successfully"}, code=200)
    
    except Exception as e:
        app.logger.error("Error in filling the missing values. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = "Error in filling the missing values"
        return respond(error=err)
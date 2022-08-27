from flask import (
    Blueprint,
    request,
    current_app as app,
    Response
)
from flask_restful import Api
from flask_login import login_required, current_user
from models.user_model import Users
from manage.db_setup import db
from utilities.methods import get_dataset, get_dataset_name
from utilities.respond import respond

dataCleaningAPI = Blueprint('dataCleaningAPI', __name__)
dataCleaningAPI_restful = Api(dataCleaningAPI)

# Api to get the missing values in dataset
@dataCleaningAPI.route('/check-missing-values', methods=['POST'])
@login_required
def check_missing_values():
    err = None
    try:
        user = Users.query.filter_by(id=current_user.id).first()
        if not user:
            err = "No such user exits"
            raise

        dataset_name = request.json.get("dataset_name")
        if not dataset_name:
            err = "Dataset name is required"
            raise

        dataset_name = get_dataset_name(user.id, dataset_name, db)
        if not dataset_name:
            err = f"Dataset not found"
            raise
        
        df = get_dataset(dataset_name, db)

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
@login_required
def fill_missing_values():
    err = None
    try:
        user = Users.query.filter_by(id=current_user.id).first()
        if not user:
            err = "No such user exits"
            raise

        dataset_name = request.json.get("dataset_name")
        if not dataset_name:
            err = "Dataset name is required"
            raise

        dataset_name = get_dataset_name(user.id, dataset_name, db)
        if not dataset_name:
            err = f"Dataset not found"
            raise
        
        df = get_dataset(dataset_name, db)

        # Custom missing value
        # fill_value = request.json.get("fill_value")
        # if not fill_value:
        #     err = "Fill value is required"
        #     raise
        for i in df.columns:
            df[i].fillna(df[i].mode()[0], inplace=True)
            
        df.to_sql(dataset_name, db.engine)

        return respond(data={"message": "Missing values filled successfully"}, code=200)
    
    except Exception as e:
        app.logger.error("Error in filling the missing values. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = "Error in filling the missing values"
        return respond(error=err)
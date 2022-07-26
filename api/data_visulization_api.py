from flask import Blueprint
from flask import current_app as app
from models.user_model import Users
from utilities.respond import respond
from flask_restful import  Api
from flask import request
from flask_login import current_user, login_required
import pandas as pd
from manage.db_setup import db
from sqlalchemy import text

dataVisulizationAPI = Blueprint("dataVisulizationAPI", __name__)
dataVisulizationAPI_restful = Api(dataVisulizationAPI)

@dataVisulizationAPI.route("/describe", methods=['POST'])
@login_required
def data_describe():
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
        
        dataset_name = f'{dataset_name.split(".")[0]}_{user.id}'
        if not dataset_name in db.engine.table_names():
            err = "No such database exists"
            raise
        
        try:
            sql_query = text(f'select * from "{dataset_name}"')
            dataset = db.engine.execute(sql_query)
        except Exception as e:
            err = "Error in describe functionality"
            app.logger.info("Error in executing the SQL Query")
            raise e
        
        df = pd.DataFrame(dataset)
        describe_data = df.describe()

        describe_data = describe_data.to_dict()

        res = {
            "msg": "Describe Functionality Implemented Successfully",
            "describe": describe_data
        }
        return respond(data=res)

    except Exception as e:
        app.logger.error("Error in data describe. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = 'Error in data describe.'
        return respond(error=err)

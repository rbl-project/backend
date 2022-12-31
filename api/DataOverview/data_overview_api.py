import io
from flask import (
    Blueprint,
    request,
    current_app as app
)
from utilities.methods import (
    load_dataset
)
from utilities.respond import respond
from flask_restful import Api
from models.user_model import Users
from flask_jwt_extended import get_jwt_identity, jwt_required


dataOverviewAPI = Blueprint("dataOverviewAPI", __name__)
dataOverviewAPI_restful = Api(dataOverviewAPI)

# Api to get the head of dataset
@dataOverviewAPI.route('/get-dataset-overview', methods=['POST'])
@jwt_required()
def get_dataset_overview():
    """
        TAKES dataset name as input
        PERFORMS the dataset overview operation such as head, tail, info, shape, dtypes
        RETURNS the overview dictionary as response
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

        df, err = load_dataset(dataset_name, user.id, user.email)
        if err:
            raise

        head = df.head().to_dict(orient="records") # to send each row as a dictionary

        tail = df.tail().to_dict(orient="records")

        buf = io.StringIO()
        df.info(buf=buf)
        info = buf.getvalue().split("\n")

        shape = df.shape

        temp_dtypes = df.dtypes.to_dict()
        dtypes = {}
        for key, value in temp_dtypes.items():
            dtypes[key]=str(value)

        res = {
            "head": head,
            "tail": tail,
            "info": info,
            "shape": shape,
            "dtypes": dtypes
        }

        return respond(data=res)

    except Exception as e:
        app.logger.error("Error in getting the dataset overview. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = 'Error in getting the dataset overview'
        return respond(error=err)


# Api to get the columns of dataset
# Api to fetch all the columns in the table
@dataOverviewAPI.route("/get-columns", methods=["POST"])
@jwt_required()
def get_columns():
    """
        TAKES dataset name as input
        PERFORMS the operation to get the columns of the dataset
        RETURNS the columns list as response
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
        
        df, err = load_dataset(dataset_name, user.id, user.email)
        if err:
            raise 
        
        columns = df.columns.to_list()
        res = {
            "columns":columns
        }
        return respond(data=res)

    except Exception as e:
        app.logger.error("Error in getting the columns. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = 'Error in getting the columns'
        return respond(error=err)
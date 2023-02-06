""" Tabular Represenation APIs"""

# FLASK
from flask import Blueprint, request
from flask_jwt_extended import jwt_required, get_jwt_identity
from flask_restful import Api

# UTILITIES
from utilities.respond import respond
from utilities.methods import load_dataset, log_error

# MODELS
from models.user_model import Users

# OTHER
import json

# BLUEPRINT
tabularRepresentationAPI = Blueprint("tabularRepresentationAPI", __name__)
tabularRepresentationAPI_restful = Api(tabularRepresentationAPI)

# ROUTES
@tabularRepresentationAPI.route("/filtered-tabular-representation", methods=["POST"])
@jwt_required()
def tabular_representation():
    """
        TAKES dataset name as input
        PERFORMS the filtering, sorting and searching on dataset
        RETURNS the filtered dataset as response
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

        df, err = load_dataset(dataset_name, user.id, user.email)
        if err:
            raise

        temp_df = df.copy()    

        # 1. SEARCHING 2. SORTING 3. FILTERING it haas to be in this order only
        
        # 1. SEARCHING
            # "search": {
            #     "col1":["value1", "value2", "value3"],
            #     "col2":["value1", "value2", "value3"]
            # },
        search = request.json.get("search", None)
        if search:
            for col, values in search.items():
                temp_df = temp_df[temp_df[col].isin(values)]


        # 2. SORTING
            # "sort": {
            #     "col1": "True",
            #     "col2": "False"
            # }
        sort = request.json.get("sort", None)
        if sort:
            columns = list(sort.keys())
            order = list(sort.values())
            temp_df = temp_df.sort_values(by=columns, ascending=order)

        # 3. FILTERING
            # "filter":{
            #     "end":true,
            #     "columns": ["col1", "col2"],
            #     "row_start": 0,
            #     "row_end": 10
            # }
        filter_obj = request.json.get("filter", None)
        if filter_obj:
            if filter_obj["end"]:
                temp_df = temp_df[filter_obj["columns"]].iloc[filter_obj["row_start"]:]
            else:
                temp_df = temp_df[filter_obj["columns"]].iloc[filter_obj["row_start"]:filter_obj["row_end"]]

        

        # Preparing the dataframe for sending as response
        shape = temp_df.shape
        n_rows = shape[0]
        n_columns = shape[1]

        if n_rows <= 50:
            data = None
            head = temp_df.head(10)
            head = head.to_json(orient='split')
            head = json.loads(head)

            tail = temp_df.tail(10)
            tail = tail.to_json(orient='split')
            tail = json.loads(tail)
        else:
            head = None
            tail = None
            temp_df = temp_df.to_json(orient='split')
            temp_df = json.loads(temp_df)
            data = temp_df

        res = {
            "data": data,
            "n_rows": n_rows,
            "n_columns": n_columns,
            "head": head,
            "tail": tail
        }

        return respond(data=res)
        
    except Exception as e:
        log_error(err_msg="Error in performing the dataset filtering operation", error=err, exception=e)
        if not err:
            err = "Error in performing the dataset filtering operation"
        return respond(error=err)


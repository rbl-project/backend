""" Tabular Represenation APIs"""

# FLASK
from flask import Blueprint, request
from flask_jwt_extended import jwt_required, get_jwt_identity
from flask_restful import Api
from api.EDA.utilities_eda import ROW_END

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
        PERFORMS the searching, sorting and then filtering in that order on the given dataset. 
                 Filtering is performed on searched and sorted dataset
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
            #     "numerical_col" : {
            #       "SepalLengthCm" : [5.1, 6.1],
            #       "SepalWidthCm": [2.5, 3.0]
            #      },
            #     "categorical_col" : {
            #       "Species": ["Iris-setosta", "Iris-verginica"]
            #      }
            # },
        search = request.json.get("search", None)
        if search:
            # categorical columns
            for col, values in search['categorical_col'].items():
                temp_df = temp_df[temp_df[col].isin(values)]

            # numerical columns
            for col, values in search['numerical_col'].items():
                temp_df = temp_df[temp_df[col].between(float(values[0]), float(values[1]))]
                
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
            # if no columns are given then consider all columns
            if not filter_obj["columns"]:
                filter_obj["columns"] = df.columns.tolist()

            if filter_obj["row_end"] == ROW_END:
                temp_df = temp_df[filter_obj["columns"]].iloc[int(filter_obj["row_start"]):]
            else:
                temp_df = temp_df[filter_obj["columns"]].iloc[int(filter_obj["row_start"]):int(filter_obj["row_end"])]

        

        # Preparing the dataframe for sending as response
        shape = temp_df.shape
        n_rows = shape[0]
        n_columns = shape[1]
        dataframe = None

        from flask import current_app as app
        if n_rows > 50:
            head = temp_df.head(5)
            head = head.to_json(orient='split')
            head = json.loads(head)

            tail = temp_df.tail(5)
            tail = tail.to_json(orient='split')
            tail = json.loads(tail)

            # we are sending head + 2 dotted rows + tail
            final_data = head["data"] + [['...']*n_columns]*2 + tail["data"]
            final_cols = head["columns"]

            dataframe = {
                "data": final_data,
                "columns": final_cols
            }

        else:
            head = None
            tail = None
            temp_df = temp_df.to_json(orient='split')
            temp_df = json.loads(temp_df)
            dataframe = temp_df

        res = {
            "dataframe": dataframe,
            "n_rows": n_rows,
            "n_columns": n_columns,
        }

        return respond(data=res)
        
    except Exception as e:
        log_error(err_msg="Error in performing the dataset filtering operation", error=err, exception=e)
        if not err:
            err = "Error in performing the dataset filtering operation"
        return respond(error=err)



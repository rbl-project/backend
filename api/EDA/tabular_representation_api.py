""" Tabular Represenation APIs"""

# FLASK
from flask import Blueprint, request
from flask_jwt_extended import jwt_required, get_jwt_identity
from flask_restful import Api
from api.EDA.utilities_eda import ROW_END

# UTILITIES
from utilities.respond import respond
from utilities.methods import check_dataset_copy_exists, get_dataset_name, load_dataset, load_dataset_copy, log_error

# MODELS
from models.user_model import Users

# CONTANTS
from api.EDA.utilities_eda import ROWS_PER_PAGE, get_row_index

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

        # ================== Business Logic Start ==================

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

        # ================== Business Logic End ==================

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


# Api for gloabl data representation
@tabularRepresentationAPI.route("/global-data-representation", methods=["POST"])
@jwt_required()
def global_data_representation():
    """
        TAKES dataset name and the parameters for global data representation as input
        FETCHES the dataset as per the pagination and filters
        RETURNS the global data representation and total number of pages as response
    """
    err=None
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
        
        '''
            Request Body Format:{
                "dataset_name": "iris.csv",
                "categorical_values": {
                    "Species": ["Iris-setosa", "Iris-versicolor"]
                    "Col2": ["val1", "val2"]
                },
                "numerical_values": {
                    "SepalLengthCm": [5.1, 6.1],
                    "SepalWidthCm": [2.5, 3.0]
                },
                "page": 1,

            }
        '''
        # check if copy of the dataset exists
        if check_dataset_copy_exists(dataset_name, user.id, user.email):
            df, err = load_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
        else:
            df, err = load_dataset(dataset_name, user.id, user.email)
            if err:
                raise
        
        # ================== Business Logic Start ==================

        categorical_values = request.json.get("categorical_values", None)
        numerical_values = request.json.get("numerical_values", None)
        page = request.json.get("page", 0)


        # categorical columns
        if categorical_values:
            for col, values in categorical_values.items():
                df = df[df[col].isin(values)]
        # numerical columns
        if numerical_values:
            for col, values in numerical_values.items():
                df = df[df[col].between(float(values[0]), float(values[1]))]

        # calculate the total number of pages
        n_rows = df.shape[0]
        n_columns = df.shape[1]
        n_pages = n_rows//ROWS_PER_PAGE + 1
        column_list = df.columns.tolist()

        temp_dtypes = df.dtypes.to_dict()
        dtypes = {}
        for key, value in temp_dtypes.items():
            dtypes[key]=str(value)

        # get the row indices of the dataframe for the current page
        row_indices = get_row_index(df, page, ROWS_PER_PAGE)
        df = df.loc[row_indices]

        n_datapoints_this_page = df.shape[0]

        # cateogrical columns and numerical columns
        categorical_columns = []
        numerical_columns = []
        categorical_columns = df.select_dtypes(include=['object', 'bool']).columns.tolist()
        numerical_columns = df.select_dtypes(exclude=['object', 'bool']).columns.tolist()

        result = df.to_json(orient='split', index=True, )
        result = json.loads(result)

        # Inserting the index column. Doing manually because the index column is not included in the data,
        # it is only 10 rows so no performance issue

        index = result.get("index")
        data = result.get("data") # data = [ [], [] , [] ]
        for inst in data:
            inst.insert(0, index.pop(0))

        columns = result.get("columns")
        columns.insert(0, "Index")

        result["columns"] = columns
        result["data"] = data
        
        # ================== Business Logic End ==================

        res = {
            "n_rows": n_rows,
            "n_columns": n_columns,
            "n_pages": n_pages,
            "n_datapoints_this_page": n_datapoints_this_page,
            "column_list": column_list,
            "dtypes": dtypes,
            "result": result,
            "current_page": page,
            "categorical_columns": categorical_columns,
            "numerical_columns": numerical_columns
        }

        return respond(data=res)

    except Exception as e:
        log_error(err_msg="Error in performing the global data representation operation", error=err, exception=e)
        if not err:
            err = "Error in performing the global data representation operation"
        return respond(error=err)


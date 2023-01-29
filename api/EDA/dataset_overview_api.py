""" Dataset overview API """

from flask import (
    Blueprint,
    request,
)
from flask_jwt_extended import (
    jwt_required,
    get_jwt_identity
)
from flask_restful import Api
from models.user_model import Users
from utilities.respond import respond
from utilities.methods import load_dataset, log_error

datasetOverviewAPI = Blueprint("datasetOverviewAPI", __name__)
datasetOverviewAPI_restful = Api(datasetOverviewAPI)

@datasetOverviewAPI.route("/basic-information", methods=['POST'])
@jwt_required()
def basic_information():
    """
        TAKES dataset name as input
        PERFORMS the fetching of basic information
        RETURNS the basic information as response
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

        # n_columns, n_rows
        shape = df.shape
        n_rows = shape[0]
        n_columns = shape[1]
        
        # columns with data types
        temp_col_dtypes = df.dtypes.to_dict()

        col_with_dtypes = []

        for key, value in temp_col_dtypes.items():
            temp = {
                "column_name":key,
                "data_type":str(value)
            }
            col_with_dtypes.append(temp)

        # head
        head = df.head().to_numpy().tolist()
        columns = df.columns.tolist()
        head.insert(0, columns)

        res = {
            "dataset_name":dataset_name,
            "n_columns":n_columns,
            "n_rows": n_rows,
            "columns":col_with_dtypes,
            "head":head
        }

        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in fetching basic information", error=err, exception=e)
        if not err:
            err = "Error in fetching basic information"
        return respond(error=err)


@datasetOverviewAPI.route("/describe-numerical-data", methods=['POST'])
@jwt_required()
def describe_numerical_data():
    """
        TAKES dataset name as input
        PERFORMS the fetching of describe numerical data
        RETURNS the describe numerical data as response
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

        df_numerical = df.select_dtypes(exclude=['bool', 'object']) # https://note.nkmk.me/en/python-pandas-dtype-astype/#:~:text=Sponsored%20Link-,List%20of%20basic%20data%20types%20(dtype)%20in%20pandas,-The%20following%20is
   
        df_numerical_described = df_numerical.describe().to_dict()

        column = df_numerical.columns.tolist()
        col_sorted_desciption = []
        for col in column:
            temp = df_numerical_described[col]
            temp["name"] = col
            temp["data_type"] = str(df_numerical[col].dtype)
            col_sorted_desciption.append(temp)

        res = {
            "columns":col_sorted_desciption,
            "n_numerical_columns": df_numerical.shape[1],
            "n_categorical_columns" : df.shape[1] - df_numerical.shape[1]
        }

        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in describing numerical data", error=err, exception=e)
        if not err:
            err = "Error in describing numerical data"
        return respond(error=err)


@datasetOverviewAPI.route("/describe-categorical-data", methods=['POST'])
@jwt_required()
def describe_categorical_data():
    """
        TAKES dataset name as input
        PERFORMS the fetching of describe categorical data
        RETURNS the describe categorical data as response
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

        df_categorical = df.select_dtypes(include=['bool', 'object']) # https://note.nkmk.me/en/python-pandas-dtype-astype/#:~:text=Sponsored%20Link-,List%20of%20basic%20data%20types%20(dtype)%20in%20pandas,-The%20following%20is
   
        df_categorical_described = df_categorical.describe().to_dict()

        column = df_categorical.columns.tolist()
        col_sorted_desciption = []
        for col in column:
            temp = df_categorical_described[col]
            temp["name"] = col
            temp["mode"] = temp["top"]
            temp["mode_count"] = temp["freq"]
            temp["unique_count"] = temp["unique"]
            temp["data_type"] = str(df_categorical[col].dtype)

            del temp['top']
            del temp['unique']
            del temp['freq']

            col_sorted_desciption.append(temp)

        res = {
            "columns":col_sorted_desciption,
            "n_categorical_columns": df_categorical.shape[1],
            "n_numerical_columns": df.shape[1] - df_categorical.shape[1]
        }

        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in describing categorical data", error=err, exception=e)
        if not err:
            err = "Error in describing categorical data"
        return respond(error=err)
    

@datasetOverviewAPI.route("/graphical-representation", methods=['POST'])
@jwt_required()
def graphical_representation():
    """
        TAKES dataset name as input
        PERFORMS the fetching of graphical representation
        RETURNS the graphical representation and other dataset information as response
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
        
        # Columns
        n_columns = len(df.columns)
        n_numerical_columns = len(df.select_dtypes(exclude=['bool', 'object']).columns)
        n_categorical_columns = len(df.select_dtypes(include=['bool', 'object']).columns)
        percent_numerical_columns = round(n_numerical_columns/n_columns*100, 2)
        percent_categorical_columns = round(n_categorical_columns/n_columns*100, 2)

        # Numerical vs Categorical Pie Chart Data
        numerical_vs_categorical_pie_chart = [
            {
                "id": "Numerical Columns",
                "label":"Numerical Columns",
                "value": n_numerical_columns
            },
            {
                "id": "Categorical Columns",
                "label":"Categorical Columns",
                "value": n_categorical_columns
            }
        ]

        # Shape
        shape = df.shape
        n_values = shape[0]*shape[1]

        # Count of null values in a dataset
        n_null_values = int(df.isnull().sum().sum())
        n_non_null_values = n_values - n_null_values

        percent_null_values = round(n_null_values/n_values*100, 2)
        percent_non_null_values = round(n_non_null_values/n_values*100, 2)

        # non_null_vs_null_pie_chart
        non_null_vs_null_pie_chart = [
            {
                "id": "Non Null Values",
                "label": "Non Null Values",
                "value": n_non_null_values
            },
            {
                "id": "Null Values",
                "label": "Null Values",
                "value": n_null_values
            }, 
        ]


        res = {
            "n_columns": n_columns,
            "n_numerical_columns": n_numerical_columns,
            "n_categorical_columns": n_categorical_columns,
            "percent_numerical_columns": percent_numerical_columns,
            "percent_categorical_columns": percent_categorical_columns,
            "numerical_vs_categorical_pie_chart": numerical_vs_categorical_pie_chart,
            "n_values": n_values,
            "n_non_null_values": n_non_null_values,
            "n_null_values": n_null_values,
            "percent_non_null_values": percent_non_null_values,
            "percent_null_values": percent_null_values,
            "non_null_vs_null_pie_chart": non_null_vs_null_pie_chart,
        }

        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in fetching graphical representation", error=err, exception=e)
        if not err:
            err = "Error in fetching graphical representation"
        return respond(error=err)
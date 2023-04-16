""" Dat Correlation APIs for EDA """

from api.EDA.utilities_eda import get_image
from flask import (
    Blueprint,
    request,
)
from flask_jwt_extended import (
    jwt_required,
    get_jwt_identity
)
import json
import matplotlib.pyplot as plt
import seaborn as sns
from flask_restful import Api
from models.user_model import Users
from utilities.respond import respond
from utilities.methods import check_dataset_copy_exists, load_dataset, load_dataset_copy, log_error

dataCorrelationAPI = Blueprint("dataCorrelationAPI", __name__)
dataCorrelationAPI_restful = Api(dataCorrelationAPI)

@dataCorrelationAPI.route("/numerical-columns", methods=['POST'])
@jwt_required()
def numerical_columns():
    """
        TAKES dataset name as input
        PERFORMS fetchig of numerical columns
        RETURNS the numerical columns as response
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

        
        # check if copy of the dataset exists
        if check_dataset_copy_exists(dataset_name, user.id, user.email):
            df, err = load_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
        else:
            df, err = load_dataset(dataset_name, user.id, user.email)
            if err:
                raise

        # numerical columns
        numerical_columns = df.select_dtypes(exclude=['bool', 'object']).columns.tolist()
        
        res = {
            "n_numerical_columns": len(numerical_columns),
            "numerical_columns": numerical_columns
        }
        
        return respond(data=res)
        
    
    except Exception as e:
        log_error(err_msg="Error in Numerical Columns List", error=err, exception=e)
        if not err:
            err = "Error in Numerical Columns List"
        return respond(error=err)
    
    
@dataCorrelationAPI.route("/correlation-matrix", methods=['POST'])
@jwt_required()
def correlation_matrix():
    """
        TAKES dataset name and column list as input
        PERFORMS calculation of correlation matrix
        RETURNS correlation matrix as response
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
        
        included_column_list = request.json.get("column_list")
        # if not included_column_list:
        #     err = "Column list is required"
        #     raise

        
        # check if copy of the dataset exists
        if check_dataset_copy_exists(dataset_name, user.id, user.email):
            df, err = load_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
        else:
            df, err = load_dataset(dataset_name, user.id, user.email)
            if err:
                raise
        
        # Correlation Matrix && Column List
        column_list = []
        column_list.append({"id":"column_name","label":"","included": True if len(included_column_list) > 0 else False})
        correlation_matrix = []
        correlation_matrix_json = json.loads(df[included_column_list].corr().round(2).to_json(orient='index'))
        
        for column in included_column_list:
            
            column_obj = {}
            column_obj["id"] = column
            column_obj["label"] = column
            column_obj["included"] = True
            column_list.append(column_obj)
            
            correlations_of_column = correlation_matrix_json[column]
            correlations_of_column["column_name"] = column
            correlations_of_column["included"] = True
            correlation_matrix.append(correlations_of_column)
            
        
        # Not Included Numerical Columns List
        numerical_columns = df.select_dtypes(exclude=['bool', 'object']).columns.tolist()
        non_included_numerical_columns = list(set(numerical_columns) - set(included_column_list))
        
        for column in non_included_numerical_columns:
            
            column_obj = {}
            column_obj["id"] = column
            column_obj["label"] = column
            column_obj["included"] = False
            column_list.append(column_obj)
            
            correlations_of_column = {}
            correlations_of_column["column_name"] = column
            correlations_of_column["included"] = False
            correlation_matrix.append(correlations_of_column)
        
        res = {
            "column_list": column_list,
            "correlation_matrix": correlation_matrix
        }
        
        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in Correlation Matrix", error=err, exception=e)
        if not err:
            err = "Error in Correlation Matrix"
        return respond(error=err)
    
# Create Scatter plot
@dataCorrelationAPI.route("/scatter-plot", methods=['POST'])
@jwt_required()
def scatter_plot():
    """
        TAKES dataset name, column1 and column2 as input
        PERFORMS generation of scatter plot
        RETURNS scatter plot image as response
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
        
        coulmn1 = request.json.get("column1")
        if not coulmn1:
            err = "Column1 is required"
            raise
        
        coulmn2 = request.json.get("column2")
        if not coulmn2:
            err = "Column2 is required"
            raise

        
        # check if copy of the dataset exists
        if check_dataset_copy_exists(dataset_name, user.id, user.email):
            df, err = load_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
        else:
            df, err = load_dataset(dataset_name, user.id, user.email)
            if err:
                raise
        
        # Scatter Plot
        df.plot.scatter(x=coulmn1, y=coulmn2)
        
        # Converting scatter plot to base64 string
        plt.tight_layout()
        scatter_plot_image = get_image(plt)
        
        plt.close()
        res = {
            "scatter_plot":scatter_plot_image
        }
        
        return respond(data=res) 
        
    except Exception as e:
        log_error(err_msg="Error in Scatter Plot", error=err, exception=e)
        if not err:
            err = "Error in Scatter Plot"
        return respond(error=err)
        
@dataCorrelationAPI.route("/correlation-heatmap", methods=['POST'])
@jwt_required()
def correlation_heatmap():
    """
        TAKES dataset name and column list as input
        PERFORMS generation of heatmap
        RETURNS heatmap as response
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
        
        included_column_list = request.json.get("column_list")
        if not included_column_list:
            err = "Column list is required"
            raise

        
        # check if copy of the dataset exists
        if check_dataset_copy_exists(dataset_name, user.id, user.email):
            df, err = load_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
        else:
            df, err = load_dataset(dataset_name, user.id, user.email)
            if err:
                raise
        
        # Correlation Matrix && Heatmap
        correlation_matrix = df[included_column_list].corr().round(2)
        # plt.matshow(correlation_matrix)
        sns.heatmap(correlation_matrix, annot=True)
        plt.tight_layout()
        # Converting heatmap to base64 string
        heatmap_image = get_image(plt)
        
        plt.close()
        
        res = {
            "heatmap":heatmap_image
        }
        
        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in Heatmap", error=err, exception=e)
        if not err:
            err = "Error in Map"
        return respond(error=err)
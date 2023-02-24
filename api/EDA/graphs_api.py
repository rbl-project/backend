""" Graphs for EDA"""

from api.EDA.utilities_eda import get_image
from flask import (
    Blueprint,
    request,
)
from flask_jwt_extended import (
    jwt_required,
    get_jwt_identity
)
import matplotlib.pyplot as plt
import seaborn as sns
from flask_restful import Api
from models.user_model import Users
from utilities.respond import respond
from utilities.methods import load_dataset, log_error

graphsAPI = Blueprint("graphsAPI", __name__)
graphsAPI_restful = Api(graphsAPI)

# Generate Graph
@graphsAPI.route("/generate-graph", methods=['POST'])
@jwt_required()
def generate_graph():
    """
        TAKES dataset name, column1, column2 and type of graph as input
        PERFORMS generation of graph of given type
        RETURNS graph as image in response
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
        
        graph_type = request.json.get("graph_type")
        if not graph_type:
            err = "Graph Type is required"
            raise
        
        n_columns = request.json.get("n_columns")
        if not n_columns:
            err = "Number of columns is required"
            raise
            
        column1 = request.json.get("column1")
        if not column1:
            err = "Column1 is required"
            raise
        
        column2 = request.json.get("column2")
        if n_columns == 2 and not column2:
            err = "Column2 is required"
            raise

        df, err = load_dataset(dataset_name, user.id, user.email)
        if err:
            raise
        
        # Chart Styling
        plt.style.use('ggplot')
        sns.set_style('darkgrid') # darkgrid, white grid, dark, white and ticks
        
        # Generating Graph
        if n_columns == 1:
            if graph_type == "bar":
                df[column1].value_counts().plot(kind=graph_type,xlabel=column1,ylabel="count")
            elif graph_type == "pie":
                df[column1].value_counts().plot(kind=graph_type, autopct='%.1f%%', startangle=45,  wedgeprops={'linewidth': 6})
            elif graph_type == "hist" or graph_type == "density":
                df.plot(kind=graph_type,y=column1,xlabel=column1)
            elif graph_type == "line":
                df.plot(kind=graph_type,y=column1,xlabel="index",ylabel=column1)
            else:
                df.plot(kind=graph_type,y=column1 ,xlabel=column1,ylabel="count")
        else:
            df.plot(kind=graph_type,x=column1, y=column2, xlabel=column1, ylabel=column2)
        
       
        # Converting scatter plot to base64 string
        plt.tight_layout()
        graph_image = get_image(plt)
        
        plt.close()
        res = {
            "graph_type":graph_type,
            "n_columns":n_columns,
            "column1":column1,
            "column2":column2,
            "graph":graph_image
        }
        
        return respond(data=res) 
        
    except Exception as e:
        log_error(err_msg="Error in Graph Generation", error=err, exception=e)
        if not err:
            err = "Error in Graph Generation"
        return respond(error=err)
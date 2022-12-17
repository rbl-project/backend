from flask import (
    Blueprint,
    request,
    current_app as app,
    Response
)
from models.user_model import Users
from api.DataVisulization.utilities import getImage
from utilities.respond import respond
from utilities.methods import (
    load_dataset,
    save_dataset
)
from flask_restful import  Api
from flask_login import current_user, login_required
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

dataVisulizationAPI = Blueprint("dataVisulizationAPI", __name__)
dataVisulizationAPI_restful = Api(dataVisulizationAPI)

@dataVisulizationAPI.route("/data-describe", methods=['POST'])
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
        
        df, err = load_dataset(dataset_name, user.id, user.email)
        if err:
            raise
        
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


# API to show the correlation between two columns 
@dataVisulizationAPI.route("/two-var-correlation", methods=['POST'])
@login_required 
def two_var_correlation():
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
        
        plot_type = request.json.get("plot_type", "scatter")

        df, err = load_dataset(dataset_name, user.id, user.email)
        if err:
            raise

        col1 = request.json.get("col1")
        col2 = request.json.get("col2")
        if not col1 or not col2:
            err = "Two Column names are required"
            raise
        
        if not col1 in df.columns or not col2 in df.columns:
            err = "No such column exists"
            raise
        
        if plot_type == "scatter":
            plt.scatter(df[col1].tolist(), df[col2].tolist())

            plt.title(f"{col1} vs {col2}")
            plt.xlabel(col1)
            plt.ylabel(col2)

            plot_image = getImage(plt)
            filename = f"scatter_plot_{dataset_name}.png"
            
        elif plot_type == "matrix":
            correlation = df[[col1, col2]].corr()
            plt.matshow(correlation)

            plot_image = getImage(plt)
            filename = f"correlation_matrix_{dataset_name}.png"
  
        return Response(plot_image, mimetype='image/png',headers={
                            "Content-disposition": f"attachment; filename={filename}"})

    except Exception as e:
        app.logger.error("Error in two var correlation. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = 'Error in two var correlation.'
        return respond(error=err)


# Api to show correlation between all columns
@dataVisulizationAPI.route("/all-var-correlation", methods=['POST'])
@login_required
def all_var_correlation():
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
        
        df, err = load_dataset(dataset_name, user.id, user.email)
        if err:
            raise
        
        corr = df.corr()

        f, ax = plt.subplots(figsize=(11, 9))
        cmap = sns.diverging_palette(220, 10, as_cmap=True)
        mask = np.zeros_like(corr, dtype=np.bool)
        mask[np.triu_indices_from(mask)] = True

        sns.heatmap(corr, mask=mask, cmap=cmap, vmax=.3, center=0,
                    square=True, linewidths=.5, cbar_kws={"shrink": .5})
        
        
        plot_image = getImage(plt)
        filename = f"all_var_correlation_{dataset_name}.png"

        return Response(plot_image, mimetype='image/png',headers={
                            "Content-disposition":f"attachment; filename={filename}"})

    except Exception as e:
        app.logger.error("Error in all var correlation. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = 'Error in all var correlation.'
        return respond(error=err)

# Api to plot Pie Chart for any columns data
@dataVisulizationAPI.route("/pie-chart-col", methods=['POST'])
@login_required
def pie_chart_col():
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
        
        df, err = load_dataset(dataset_name, user.id, user.email)
        if err:
            raise
        
        col = request.json.get("col")
        if not col:
            err = "Column name is required"
            raise
        
        if not col in df.columns:
            err = "No such column exists"
            raise
        
        df[col].value_counts().plot(kind='pie', autopct='%1.1f%%',
                                    shadow=True, startangle=90)
        plt.title(f"{col}")
        plt.ylabel(col)
        plt.xlabel("Count")

        plot_image = getImage(plt)
        filename = f"pie_chart_{dataset_name}.png"

        return Response(plot_image, mimetype='image/png',headers={
                            "Content-disposition": f"attachment; filename={filename}"})

    except Exception as e:
        app.logger.error("Error in pie chart col. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = 'Error in pie chart col.'
        return respond(error=err)
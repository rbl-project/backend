from flask import Blueprint, send_file
from flask import current_app as app
from models.user_model import Users
from api.DataVisulization.utilities import getImage
from utilities.respond import respond
from utilities.methods import (
    get_dataset,
    get_dataset_name
)
from flask_restful import  Api
from flask import request
from flask_login import current_user, login_required
import pandas as pd
from manage.db_setup import db
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
        
        dataset_name = get_dataset_name(user.id, dataset_name, db)
        df = get_dataset(dataset_name, db)

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
        
        dataset_name = get_dataset_name(user.id, dataset_name, db)
        df = get_dataset(dataset_name, db)

        col1 = request.json.get("col1")
        col2 = request.json.get("col2")
        if not col1 or not col2:
            err = "Two Column names are required"
            raise
        
        if not col1 in df.columns or not col2 in df.columns:
            err = "No such column exists"
            raise
        
        correlation = df[[col1, col2]].corr()
        plt.matshow(correlation)
        
        plot_image = getImage(plt)
  
        return send_file(plot_image,
                     attachment_filename='plot.png',
                     mimetype='image/png')

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
        
        dataset_name = get_dataset_name(user.id, dataset_name, db)
        df = get_dataset(dataset_name, db)
        
        corr = df.corr()

        f, ax = plt.subplots(figsize=(11, 9))
        cmap = sns.diverging_palette(220, 10, as_cmap=True)
        mask = np.zeros_like(corr, dtype=np.bool)
        mask[np.triu_indices_from(mask)] = True

        sns.heatmap(corr, mask=mask, cmap=cmap, vmax=.3, center=0,
                    square=True, linewidths=.5, cbar_kws={"shrink": .5})
        
        
        plot_image = getImage(plt)
  
        return send_file(plot_image,
                     attachment_filename='plot.png',
                     mimetype='image/png')

    except Exception as e:
        app.logger.error("Error in all var correlation. Error=> %s. Exception=> %s", err, str(e))
        if not err:
            err = 'Error in all var correlation.'
        return respond(error=err)


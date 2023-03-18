""" Dataset cleaning API """
# FLASK
from flask import Blueprint, request
from flask_jwt_extended import jwt_required, get_jwt_identity
from flask_restful import Api
from flask import current_app as app

# UTILITIES
from utilities.respond import respond
from utilities.methods import get_dataset_name, load_dataset_copy, load_dataset, log_error, make_dataset_copy, check_dataset_copy_exists, save_dataset_copy, get_row_column_metadata

# MODELS
from models.user_model import Users
from models.dataset_metadata_model import MetaData
# constants

# OTHER
import pandas as pd

# BLUEPRINT
missingValueImputationAPI = Blueprint("missingValueImputationAPI", __name__)
missingValueImputationAPI_restful = Api(missingValueImputationAPI)


# Api to get the percentage of missing values in each column of the given dataset
@missingValueImputationAPI.route("/missing-value-percentage", methods=["POST"])
@jwt_required()
def get_missing_value_percentage():
    """
        TAKES dataset name and get_all_columns and as input
        if get_all_columns is False, it also takes column_name as input
        RETURNS the percentage of missing values in each column of the given dataset as well as the total percentage of missing values in the dataset if get_all_columns is True
        else it returns the percentage of missing values in the given column of the given dataset
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
        
        get_all_columns = request.json.get("get_all_columns")
        if get_all_columns == None:
            err = "get_all_columns is required"
            raise
        
        column_name = request.json.get("column_name")
        if get_all_columns == False and not column_name:
            err = "column_name is required"
            raise       

        dataset_file_name = get_dataset_name(user.id, dataset_name) # dataset_name = iris_1

        df = None
        err = None
        metadata = None

        # ======= Get Metada =======
        # Look if the copy of dataset exists and if it does, load dataset copy otherwise load the original dataset
        if check_dataset_copy_exists(dataset_name, user.id, user.email):
            df,err = load_dataset_copy(dataset_name, user.id, user.email)
            metadata = MetaData.objects(dataset_file_name=dataset_file_name+"_copy").first_or_404(message=f"Dataset Metadata for {dataset_file_name}_copy not found")
            
        else:
            df,err = load_dataset(dataset_name, user.id, user.email)
            metadata = MetaData.objects(dataset_file_name=dataset_file_name).first_or_404(message=f"Dataset Metadata for {dataset_file_name} not found")
        
        if err: 
            raise 
    
        ''' ========================== Older code ============================== '''
        # # ========== Update Metadata ==========
        # # Update the missing value in the metadata file
        # if get_all_columns == False:
            
        #     # If column_name is not "all_columns" and the missing value is different from the one in the metadata file, update the metadata file
        #     if column_name != "all_columns" and missing_value != metadata["column_wise_missing_value"][column_name]:
        #         column_wise_missing_value = metadata["column_wise_missing_value"] 
        #         column_wise_missing_value[column_name] = missing_value # Update the metadata object
        #         metadata.update(column_wise_missing_value=column_wise_missing_value)
            
        #     # If column_name is "all_columns" and the missing value is different from the one in the metadata file, update the metadata file
        #     elif column_name == "all_columns" and missing_value != metadata["all_columns_missing_value"]["missing_value"]:
        #         metadata["all_columns_missing_value"]["missing_value"] = missing_value # Update the metadata object
        #         metadata.update(all_columns_missing_value={"missing_value": missing_value})
        
        
        # # Get the column wise and all columns missing value from metadata file    
        # column_wise_missing_value = metadata["column_wise_missing_value"]
        # all_columns_missing_value = metadata["all_columns_missing_value"]["missing_value"]
        """ ========================== Older code End ==============================""" 
        
        # ================================== Main Logic Start HERE ==================================
        
        
        # If get_all_columns is False, set cols to the column_name else set cols to all columns of the dataset
        cols = []
        if get_all_columns == False and column_name != "all_columns":
            cols = [column_name]
        else:
            cols = df.columns.tolist()
    
    
        """ ========================== Older code ============================== """
        # # Get the percentage of missing values in each column
        # column_wise_missing_value_data = []
        # for col in cols:
        #     missing_percentage = round(df[col].eq(column_wise_missing_value[col]).sum()/len(df[col]) * 100, 2)
        #     non_missing_percentage = 100 - missing_percentage
        #     column_wise_missing_value_data.append({
        #         "column_name": col, 
        #         "missing_value_percentage": missing_percentage, 
        #         "correct_value_percentage": non_missing_percentage,
        #         "missing_value_type": column_wise_missing_value[col]
        #     })
        
        # # Sort the column wise missing value data in descending order of missing value percentage
        # column_wise_missing_value_data.sort(key=lambda x: x["missing_value_percentage"], reverse=True)
        
        
        # # Get the total percentage of missing values in the dataset
        # all_columns_missing_value_data = {}
    
        # if get_all_columns == True or column_name == "all_columns":
        #     all_columns_missing_value_percentage = round(df.eq(all_columns_missing_value).sum().sum()/df.size * 100, 2)
        #     all_columns_non_missing_value_percentage = 100 - all_columns_missing_value_percentage
        #     all_columns_missing_value_data = {
        #         "column_name": "all_columns",
        #         "missing_value_percentage": all_columns_missing_value_percentage,
        #         "correct_value_percentage": all_columns_non_missing_value_percentage,
        #         "missing_value_type": all_columns_missing_value
        #     }
        
        """ ========================== Older code End =============================="""
        
        # Get the percentage of missing values in each column
        column_wise_missing_value_data = []
        for col in cols:
            missing_percentage = round(df[col].isna().sum()/len(df[col]) * 100, 2)
            non_missing_percentage = 100 - missing_percentage
            column_wise_missing_value_data.append({
                "column_name": col, 
                "missing_value_percentage": missing_percentage, 
                "correct_value_percentage": non_missing_percentage,
            })
        
        # Sort the column wise missing value data in descending order of missing value percentage
        column_wise_missing_value_data.sort(key=lambda x: x["missing_value_percentage"], reverse=True)
        
        
        # Get the total percentage of missing values in the dataset
        all_columns_missing_value_data = {}
    
        if get_all_columns == True or column_name == "all_columns":
            all_columns_missing_value_percentage = round(df.isna().sum().sum()/df.size * 100, 2)
            all_columns_non_missing_value_percentage = 100 - all_columns_missing_value_percentage
            all_columns_missing_value_data = {
                "column_name": "all_columns",
                "missing_value_percentage": all_columns_missing_value_percentage,
                "correct_value_percentage": all_columns_non_missing_value_percentage,
            }
        
        
        # ================================ Main Logic Ends HERE =================================

        
        res = {}
        
        # When user wants to get the missing value percentage of a particular column and that column is NOT "all_columns"
        if get_all_columns == False and column_name != "all_columns": 
            res = column_wise_missing_value_data[0]
        # When user wants to get the missing value percentage of a particular column and that column is  "all_columns"
        elif get_all_columns == False and column_name == "all_columns":
            res = all_columns_missing_value_data
            
        # When user wants to get the missing value percentage of column wise and all columns
        else:
            res = {
                "column_wise_missing_value_data": column_wise_missing_value_data,
                "all_columns_missing_value_data": all_columns_missing_value_data,
            }   
        
        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in Getting Missing Value Percentage", error=err, exception=e)
        if not err:
            err = "Error in Getting Missing Value Percentage"
        return respond(error=err)
    

# API to Impute Missing Value in a Dataset
@missingValueImputationAPI.route("/impute-missing-value", methods=["POST"]) # type: ignore
@jwt_required() # type: ignore
def impute_missing_value():
    """
        TAKES dataset_name, column_name, missing_value, imputation_method as input
        PERFORMS imputation on the dataset
        RETURNS success as response
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

        column_name = request.json.get("column_name")
        if not column_name:
            err = "Column name is required"
            raise
        
        imputation_method = request.json.get("imputation_method")
        if not imputation_method:
            err = "Imputation method is required"
            raise
        
        
        # Load the dataset
        # Look if the copy of dataset exists and if it does, then rename the columns in that copy otherwise rename make a copy and rename the columns in that copy
        if not check_dataset_copy_exists(dataset_name, user.id, user.email):
            app.logger.info("Dataset copy of %s does not exist. Trying to make a copy of the dataset", dataset_name)
            err = make_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
    
        df, err = load_dataset_copy(dataset_name, user.id, user.email)
        if err:
            raise
        
        
        dataset_file_name = get_dataset_name(user_id=user.id, dataset_name=dataset_name) # iris_1
        copy_dataset_file_name = dataset_file_name + "_copy" # iris_1_copy
        
        # Load the metadata of Copy of the dataset
        metadata = MetaData.objects(dataset_file_name=copy_dataset_file_name).first_or_404(message=f"Metadata of {dataset_name} not found")
        
        # Get the datatype of the column
        column_dtype = None
        if column_name != "all_columns":
            column_dtype = metadata.column_datatypes[column_name]
        # ================== Business Logic Start ==================
        
        # When user wants to impute missing value of a particular column
        if column_name != "all_columns":
            
            # Imputattion is DROP ROWS or DROP COLUMN
            if imputation_method == "drop_rows" or imputation_method == "drop_column":
                
                # Drop the rows with missing value
                if imputation_method == "drop_rows":
                    
                    df.dropna(subset=[column_name], inplace=True)
                    # update the metadata of the dataset
                    if metadata.n_rows != df.shape[0]:
                        metadata.n_rows = df.shape[0]
                        metadata.n_values = df.size
                        metadata.save()
                 
                # Drop entire column   
                elif imputation_method == "drop_column":
                    df.drop(column_name, axis=1, inplace=True)
                
                    # Update the metadata of the dataset
                    updated_row_column_metadata = get_row_column_metadata(df)
                    for key, value in updated_row_column_metadata.items():
                        metadata[key] = value
                    
                    metadata.save()
                
            # Imputation is MEAN, MEDIAN , MODE or Custom_Value    
            else:  
                   
                imputation_value = None
                
                # Imputation is MEAN
                if imputation_method == "mean":
                    
                    # Check if the column is of type int or float
                    if column_dtype == "object" or column_dtype == "bool":
                        err = "Cannot Impute Mean value in a Categorical column"
                        raise
                    
                    imputation_value = df[column_name].mean()
                    
                # Imputation is MEDIAN
                elif imputation_method == "median":
                    
                    # Check if the column is of type int or float
                    if column_dtype == "object" or column_dtype == "bool":
                        err = "Cannot Impute Median value in a Categorical column"
                        raise
                    
                    imputation_value = df[column_name].median()

                # Imputation is MODE
                elif imputation_method == "mode":
                    imputation_value = df[column_name].mode()[0]
            
                # Imputation is Custom Value ( imputation_method == "custom_value" )
                elif imputation_method == "custom_value":
                    
                    imputation_value = request.json.get("imputation_value")
                    
                    # Check if we are imputing a numeric value in a categorical column
                    if (column_dtype == "object" or column_dtype == "bool") and isinstance(imputation_value, (int, float )):
                        err = "Cannot Impute a Numeric value in a Categorical column"
                        raise
                    
                    # Check if we are imputing a categorical value in a numeric column
                    if (column_dtype != "object" and column_dtype != "bool") and not isinstance(imputation_value, (int, float )): 
                        err = "Cannot Impute a Categorical value in a Numeric column"
                        raise
                
                else:
                    err = "Invalid Imputation Method"
                    raise
                
                # Impute the missing value
                df[column_name].fillna(imputation_value, inplace=True)
            
        # When user wants to impute missing value of all columns
        else:
            
            # Imputattion is DROP ROWS or DROP COLUMN
            if imputation_method == "drop_rows" or imputation_method == "drop_column":
                
                # Drop the rows with missing value
                if imputation_method == "drop_rows":
                    
                    df.dropna(inplace=True)
                    # update the metadata of the dataset
                    if metadata.n_rows != df.shape[0]:
                        metadata.n_rows = df.shape[0]
                        metadata.n_values = df.size
                        metadata.save()
                 
                # Drop entire column   
                elif imputation_method == "drop_column":
                    
                    # Getting Columns with missing value
                    columns_with_missing_value = []
                    for column in df.columns:
                        if df[column].isnull().values.any():
                            columns_with_missing_value.append(column)

                    df.drop(columns_with_missing_value, axis=1, inplace=True)
                                   
                    # Update the metadata of the dataset
                    updated_row_column_metadata = get_row_column_metadata(df)
                    for key, value in updated_row_column_metadata.items():
                        metadata[key] = value
                    
                    metadata.save()
                
            # Imputation is MEAN, MEDIAN , MODE or Custom_Value    
            else:  
                   
                imputation_values = None
                
                # Imputation is MEAN
                if imputation_method == "mean":
                    
                    # For numerical columns impute mean value
                    numercical_imputation_values = df[metadata.numerical_column_list].mean()
                    
                    # For categorical columns impute mode value
                    categorical_imputation_values = df[metadata.categorical_column_list].mode().iloc[0]
                    
                    # Concatenate the imputation values
                    imputation_values = pd.concat([numercical_imputation_values, categorical_imputation_values])
                
                # Imputation is MEDIAN
                elif imputation_method == "median":
                    
                    # For numerical columns impute median value
                    numercical_imputation_values = df[metadata.numerical_column_list].median()
                    
                    # For categorical columns impute mode value
                    categorical_imputation_values = df[metadata.categorical_column_list].mode().iloc[0]
                    
                    # Concatenate the imputation values
                    imputation_values = pd.concat([numercical_imputation_values, categorical_imputation_values])

                # Imputation is MODE
                elif imputation_method == "mode":
                    imputation_values = df.mode().iloc[0]
    
                else:
                    err = "Invalid Imputation Method"
                    raise
                    
                # Impute the missing value
                df.fillna(imputation_values, inplace=True)
                
        #  ========================== Older code Start ==============================
        """
        # When user wants to impute missing value of a particular column
        if column_name != "all_columns":
            
            # Imputattion is DROP ROWS or DROP COLUMN
            if imputation_method == "drop_rows" or imputation_method == "drop_column":
                
                # Drop the rows with missing value
                if imputation_method == "drop_rows":
                    df = df[df[column_name] != missing_value]
                 
                # Drop entire column   
                elif imputation_method == "drop_column":
                    df.drop(column_name, axis=1, inplace=True)
                
                    # Update the metadata of the dataset
                    metadata.column_names = list(df.columns)
                    metadata.save()
                
            # Imputation is MEAN, MEDIAN , MODE or Custom_Value    
            else:  
                   
                imputation_value = None
                
                # Imputation is MEAN
                if imputation_method == "mean":
                    imputation_value = pd.to_numeric(df[column_name], errors='coerce').mean()
                
                # Imputation is MEDIAN
                elif imputation_method == "median":
                    imputation_value = pd.to_numeric(df[column_name], errors='coerce').median()

                # Imputation is MODE
                elif imputation_method == "mode":
                    imputation_value = df[column_name].replace(missing_value, np.nan).mode()[0]
            
                # Imputation is Custom Value ( imputation_method == "custom_value" )
                else:
                    imputation_value = request.json.get("imputation_value")
                    
                # Impute the missing value
                df.replace(missing_value, imputation_value, inplace=True)
            
        # When user wants to impute missing value of all columns
        else:
            
            # Imputattion is DROP ROWS or DROP COLUMN
            if imputation_method == "drop_rows" or imputation_method == "drop_column":
                
                # Drop the rows with missing value
                if imputation_method == "drop_rows":
                    
                    cols = df.columns.tolist()
                    for col in cols:
                        df = df[df[col] != missing_value]
                 
                # Drop entire column   
                elif imputation_method == "drop_column":
                    
                    # Get the columns with missing value
                    cols_with_missing_value = []
                    cols = df.columns.tolist()
                    
                    for col in cols:
                        if df[col].eq(missing_value).sum() > 0:
                            cols_with_missing_value.append(col)
                    
                    df.drop(cols_with_missing_value, axis=1, inplace=True)
                                   
                    # Update the metadata of the dataset
                    metadata.column_names = list(df.columns)
                    metadata.save()
                
            # Imputation is MEAN, MEDIAN , MODE or Custom_Value    
            else:  
                   
                imputation_value = None
                
                # Imputation is MEAN
                if imputation_method == "mean":
                    imputation_value = pd.to_numeric(df[column_name], errors='coerce').mean()
                
                # Imputation is MEDIAN
                elif imputation_method == "median":
                    imputation_value = pd.to_numeric(df[column_name], errors='coerce').median()

                # Imputation is MODE
                elif imputation_method == "mode":
                    imputation_value = df[column_name].replace(missing_value, np.nan).mode()[0]
            
                # Imputation is Custom Value ( imputation_method == "custom_value" )
                else:
                    imputation_value = request.json.get("imputation_value")
                    
                # Impute the missing value
                df.replace(missing_value, imputation_value, inplace=True)
                
        """
        #  ========================== Older code End ==============================
        
        # ================== Business Logic End ==================
        save_dataset_copy(df, dataset_name, user.id, user.email)

        res = {
            "msg": "Missing value imputation successful",
            "res": df.to_dict(orient="records"),
        }
        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in Missing Value Imputation", error=err, exception=e)
        if not err:
            err = "Error in Missing Value Imputation"
        return respond(error=err)
    

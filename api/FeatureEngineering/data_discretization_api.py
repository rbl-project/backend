""" Data Discretization API """
# FLASK
from flask import Blueprint, request
from flask_jwt_extended import jwt_required, get_jwt_identity
from flask_restful import Api
from flask import current_app as app

# UTILITIES
from utilities.respond import respond
from utilities.methods import get_dataset_name, load_dataset_copy, log_error, make_dataset_copy, check_dataset_copy_exists, save_dataset_copy, get_row_column_metadata

# MODELS
from models.user_model import Users
from models.dataset_metadata_model import MetaData
# constants

# OTHER
import pandas as pd
from sklearn.preprocessing import KBinsDiscretizer

# BLUEPRINT
dataDiscretizationAPI = Blueprint("dataDiscretizationAPI", __name__)
dataDiscretizationAPI_restful = Api(dataDiscretizationAPI)


# Api to perform data discretization
@dataDiscretizationAPI.route("/data-discretization", methods=["POST"])
@jwt_required()
def data_discretization():
    """
        TAKES dataset_name, column_name, descritization_strategy, number_of_bins, encoding_type, prefix, default_category, range_list as input
        PERFORMS data discretization on the given column_name with the given descritization_strategy and input parameters
        RETURNS the dataset with the column_name discretized
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
            err = "Column Name is required"
            raise    
        
        strategy = request.json.get("strategy")
        if not strategy:
            err = "Discretization Strategy is required"
            raise
        
        encoding_type = request.json.get("encoding_type")
        if not encoding_type:
            err = "Encoding type is required"
            raise
        
        n_bins = request.json.get("n_bins")
        if strategy != "custom" and not n_bins:
            err = "Number of bins is required"
            raise
        
        prefix = request.json.get("prefix")
        if encoding_type =="onehot" and prefix == None:
            err = "Prefix is required"
            raise
        
        default_category = request.json.get("default_category")
        if strategy == "custom" and not default_category:
            err = "Default Category is required"
            raise
        
        range_list = request.json.get("range_list")
        if strategy == "custom" and not range_list:
            err = "Range List is required"
            raise

        dataset_file_name = get_dataset_name(user.id, dataset_name) # dataset_name = iris_1
        
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
        
        metadata = None
         # Look if the copy of dataset exists and if it does, load dataset copy metadata otherwise load the original dataset metadata
        if check_dataset_copy_exists(dataset_name, user.id, user.email):
            metadata = MetaData.objects(dataset_file_name=dataset_file_name+"_copy").first_or_404(message=f"Dataset Metadata for {dataset_file_name}_copy not found")
        else:
            metadata = MetaData.objects(dataset_file_name=dataset_file_name).first_or_404(message=f"Dataset Metadata for {dataset_file_name} not found")
        
        # Check if Column Contains Null Values
        if df[column_name].isnull().values.any():
            err = "Cannot descritize a column with null values. Please remove Null values using Missing Value Imputation Functionality"
            raise
        
        ### Check if Valid Inputs are given
        # Strategy should be one of the following
        strategy_list = ["uniform", "quantile", "kmeans", "custom"]
        if strategy not in strategy_list:
            err = "Invalid Strategy"
            raise
        
        # Encoding type should be one of the following
        encoding_type_list = ["onehot", "ordinal"]
        if encoding_type not in encoding_type_list:
            err = "Invalid Encoding Type"
            raise
        
        # No of bins should be an integer and greater than 1 and less than number of rows
        if strategy in ["uniform", "quantile", "kmeans"]:
            n_bins = int(n_bins)
            if n_bins < 2:
                err = "Number of bins should be greater than 1"
                raise
            elif n_bins > metadata.n_rows:
                err = "Number of bins should be less than number of Rows"
                raise
        
        # range_list should not be empty
        if strategy == "custom" and len(range_list) == 0:
            err = "Range List should not be empty"
            raise
        
        # =============================================== # Data Discretization Logic Start Here ===============================================
        
        # If Descritization Strategy is "uniform" or "quantile" or "kmeans"
        if strategy in ["uniform", "quantile", "kmeans"]:
            
            # If user wants onehot encoding. we will use "onehot-dense" encoding
            if encoding_type == "onehot":
                encoding_type = "onehot-dense"
                
            # Create an instance of KBinsDiscretizer
            kbins = KBinsDiscretizer(n_bins=n_bins, encode=encoding_type, strategy=strategy)
            # Fit n Transfomrm the KBinsDiscretizer
            descretised_data = kbins.fit_transform(df[[column_name]])
            
            # Column names of the new discretized columns
            new_column_names = []
            if encoding_type == "onehot" or encoding_type == "onehot-dense":
                for i in range(n_bins):
                    new_column_names.append(prefix+str(i))
            else:
                new_column_names.append(column_name)
            
            # Create a new dataframe with the discretized data
            new_df = pd.DataFrame(descretised_data, columns=new_column_names)
            new_df = new_df.astype(int)
            
            # Drop the original column from the dataframe
            df = df.drop(column_name, axis=1)

            # Concatenate the original dataframe with the new dataframe
            df = pd.concat([df, new_df], axis=1)
            
        # If Descritization Strategy is "custom"
        elif strategy == "custom":
            
            # Check if category names are unique
            category_list = [rangeObj["category"] for rangeObj in range_list]
            for category in category_list:
                if category in metadata.column_list:
                    err = "Category name already exists"
                    raise
            
            sorted_range_list = sorted(range_list, key=lambda x: x["start"]["value"])
            min_to_max_range_list = []
        
            min_value = int(df[column_name].min())
            max_value = int(df[column_name].max())
            
            # Check if range list contains range from min_value to max_value
            if not (sorted_range_list[0]["start"]["value"] >= min_value and sorted_range_list[-1]["end"]["value"] <= max_value):
                err = "Range List should contain range between Column Min and Column Max"
                raise
            
            start_value = sorted_range_list[0]["start"]["value"]
            start_included = sorted_range_list[0]["start"]["included"]
            
            index = 0
            # IF first range does not start from min_value, then add a range from min_value to start_value, else add the first range
            if not (start_value == min_value and start_included == True):
                min_to_max_range_list.append({"start": {"value": min_value, "included": True}, "end": {"value": start_value, "included": not start_included}, "category": default_category})
            else :
                min_to_max_range_list.append(sorted_range_list[0])
                index = 1
                
            total_range_items = len(sorted_range_list)
            
            while index < total_range_items:
                
                prev_end_value = min_to_max_range_list[-1]["end"]["value"]
                prev_end_included = min_to_max_range_list[-1]["end"]["included"]
                
                curr_start_value = sorted_range_list[index]["start"]["value"]
                curr_start_included = sorted_range_list[index]["start"]["included"]
                
                # If Prev ending and current starting values are same, 
                if prev_end_value == curr_start_value:
                    # if one of them is included and other is not included, then add the current range
                    if prev_end_included != curr_start_included:
                        min_to_max_range_list.append(sorted_range_list[index])
                        index += 1
                    # if both are not included, then add the an range with curr_start/prev_end value and included as True in both start and end with default category
                    else:
                        min_to_max_range_list.append({"start": {"value": curr_start_value, "included": True}, "end": {"value": curr_start_value, "included": True}, "category": default_category})
                
                # if prev ending is smaller than current starting, that means there is a gap between them, so add a range from prev_end to curr_start with default category        
                elif prev_end_value < curr_start_value:
                    min_to_max_range_list.append({"start": {"value": prev_end_value, "included": not prev_end_included}, "end": {"value": curr_start_value, "included": not curr_start_included}, "category": default_category})
            
            # Check if last range ends with max_value, if not, then add a range from last range end to max_value with default category
            last_range_end_value = min_to_max_range_list[-1]["end"]["value"]
            last_range_end_included = min_to_max_range_list[-1]["end"]["included"]
            if not (last_range_end_value == max_value and last_range_end_included == True):
                min_to_max_range_list.append({"start": {"value": last_range_end_value, "included": not last_range_end_included}, "end": {"value": max_value, "included": True}, "category": default_category})
         
            
            # Convert column to categorical column based on range_list
            def get_category(x, min_to_max_range_list, default_category):
                for i in range(len(min_to_max_range_list)):
                    
                    start_value = min_to_max_range_list[i]["start"]["value"]
                    start_included = min_to_max_range_list[i]["start"]["included"]
                    end_value = min_to_max_range_list[i]["end"]["value"]
                    end_included = min_to_max_range_list[i]["end"]["included"]
                    
                    if start_included and end_included:
                        if start_value <= x <= end_value:
                            return min_to_max_range_list[i]["category"]
                    elif start_included and not end_included:
                        if start_value <= x < end_value:
                            return min_to_max_range_list[i]["category"]
                    elif not start_included and end_included:
                        if start_value < x <= end_value:
                            return min_to_max_range_list[i]["category"]
                    elif not start_included and not end_included:
                        if start_value < x < end_value:
                            return min_to_max_range_list[i]["category"]
                        
                return default_category
            
            df[column_name] = df[column_name].apply(lambda x: get_category(x, min_to_max_range_list, default_category))
                
            # If user wants onehot encoding
            if encoding_type == "onehot":
                # Create a new dataframe with the onehot encoding of the new column
                new_df = pd.get_dummies(df[column_name], prefix=prefix)
                # Concatenate the original dataframe with the new dataframe
                df = pd.concat([df, new_df], axis=1)
                # Drop the original column
                df = df.drop(column_name, axis=1)
                
        
        # ================================================ # Data Discretization Logic End Here ================================================
        
        # Metadata updation
        updated_row_column_metadata = get_row_column_metadata(df)
        for key, value in updated_row_column_metadata.items():
            metadata[key] = value
            
        # Update the metadata for Deleted Columns
        if encoding_type == "onehot" or encoding_type == "onehot-dense":
            metadata["column_deleted_status"][column_name] = True
            
        # Update the metadata of the dataset
        metadata.is_copy_modified = True
        metadata.save()
        
        # Save the dataset copy
        save_dataset_copy(df, dataset_name, user.id, user.email)
        
        res={
            "msg": "Data Discretization Successful",
        }
        
        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in Data Discretization", error=err, exception=e)
        if not err:
            err = "Error in Data Discretization"
        return respond(error=err)
    

    

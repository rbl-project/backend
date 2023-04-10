"""  Feature Encoding API"""
# FLASK
from flask import Blueprint, request
from flask_restful import Api
from flask import current_app as app
from flask_jwt_extended import jwt_required, get_jwt_identity

# UTILITIES
from utilities.respond import respond
from utilities.methods import (
    get_dataset_name, 
    load_dataset_copy, 
    log_error, 
    make_dataset_copy, 
    check_dataset_copy_exists, 
    save_dataset_copy, 
    get_row_column_metadata
)

# MODELS
from models.user_model import Users
from models.dataset_metadata_model import MetaData

# OTHERS
from category_encoders import (
    OneHotEncoder,
    OrdinalEncoder,
    BinaryEncoder,
    TargetEncoder,
    LeaveOneOutEncoder,
    CountEncoder
)

# BLUEPRINT
featureEncodingAPI = Blueprint("fetatureEncodingAPI", __name__)
featureEncodingAPI_restful = Api(featureEncodingAPI)

# Api for one hot encoding
@featureEncodingAPI.route("/one-hot-encoding", methods=["POST"])
@jwt_required()
def one_hot_encoding():
    """
        TAKES dataset_name, column list and dummy column name rule as input
        PERFORMS one hot encoding on the dataset
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

        """
            Request Body {
                "dataset_name": "dataset_name",
                "encoding_info": {
                    "column_list": ["column1", "column2"],
                    "use_cat_name": true,
                }
            }
        """
        encoding_info = request.json.get("encoding_info")
        if not encoding_info:
            err = "Encoding info is required"
            raise
        if not isinstance(encoding_info, dict):
            err = "Encoding info should be a dictionary"
            raise

        column_list = encoding_info.get("column_list")
        if not column_list:
            err = "Column list is required"
            raise
        if not isinstance(column_list, list):
            err = "Column list should be a list"
            raise

        use_cat_name = encoding_info.get("use_cat_name", False)
        if not isinstance(use_cat_name, bool):
            err = "use_cat_name should be boolean"
            raise
        
        # Look if the copy of dataset exists and if it does, then rename the columns in that copy otherwise rename make a copy and rename the columns in that copy
        if not check_dataset_copy_exists(dataset_name, user.id, user.email):
            app.logger.info("Dataset copy of %s does not exist. Trying to make a copy of the dataset", dataset_name)
            err = make_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
    
        df, err = load_dataset_copy(dataset_name, user.id, user.email)
        if err:
            raise

        # ===================== Business Logic Starts Here =====================

        # Check if the column list is valid
        for col in column_list:
            if col not in df.columns:
                err = f"Column '{col}' does not exists in the dataset"
                raise

        df = OneHotEncoder(cols=column_list, use_cat_names=use_cat_name, return_df=True).fit(df).transform(df)

        # ===================== Business Logic Ends Here =====================

        # Save the dataset copy and update the metadata
        update_metadata(user, dataset_name, df)

        res = {
            "msg": "One hot encoding successful"
        }
        return respond(data=res)

    except Exception as e:
        log_error(err_msg="Error in one hot encoding", error=err, exception=e)
        if not err:
            err = "Error in one hot encoding"
        return respond(error=err)


# Api for target encoding
@featureEncodingAPI.route("/target-encoding", methods=["POST"])
@jwt_required()
def target_encoding():
    """
        TAKES dataset_name, column list and target column name and method as input
        PERFORMS target encoding on the dataset
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

        """
            Request Body {
                "dataset_name": "dataset_name",
                "encoding_info": {
                    "column_list": ["column1", "column2"],
                    "target_column": "target_column",
                    "leaveOneOut": true,
                }
            }
        """
        encoding_info = request.json.get("encoding_info")
        if not encoding_info:
            err = "Encoding info is required"
            raise
        if not isinstance(encoding_info, dict):
            err = "Encoding info should be a dictionary"
            raise

        column_list = encoding_info.get("column_list")
        if not column_list:
            err = "Column list is required"
            raise
        if not isinstance(column_list, list):
            err = "Column list should be a list"
            raise

        target_column = encoding_info.get("target_column")
        if not target_column:
            err = "Target column is required"
            raise

        leaveOneOut = encoding_info.get("leaveOneOut", False)
        if not isinstance(leaveOneOut, bool):
            err = "leaveOneOut should be boolean"
            raise

        # Look if the copy of dataset exists and if it does, then rename the columns in that copy otherwise rename make a copy and rename the columns in that copy
        if not check_dataset_copy_exists(dataset_name, user.id, user.email):
            app.logger.info("Dataset copy of %s does not exist. Trying to make a copy of the dataset", dataset_name)
            err = make_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
    
        df, err = load_dataset_copy(dataset_name, user.id, user.email)
        if err:
            raise

        # ===================== Business Logic Starts Here =====================

        # Check if the column list is valid
        for col in column_list:
            if col not in df.columns:
                err = f"Column '{col}' does not exists in the dataset"
                raise
        
        if target_column not in df.columns:
            err = f"Target column '{target_column}' does not exists in the dataset"
            raise
        
        if not leaveOneOut:
            df = TargetEncoder(cols=column_list, return_df=True).fit(df, df[target_column]).transform(df)
        else:
            df = LeaveOneOutEncoder(cols=column_list, return_df=True).fit(df, df[target_column]).transform(df)
        
        # ===================== Business Logic Ends Here =====================

        # Save the dataset copy and update the metadata
        update_metadata(user, dataset_name, df)

        res = {
            "msg": "Target encoding successful"
        }
        return respond(data=res)
        
    except Exception as e:
        log_error(err_msg="Error in target encoding", error=err, exception=e)
        if not err:
            err = "Error in target encoding"
        return respond(error=err)


# Api for binary encoding
@featureEncodingAPI.route("/binary-encoding", methods=["POST"])
@jwt_required()
def binary_encoding():
    """
        TAKES dataset_name, column list aas input
        PERFORMS binary encoding on the dataset
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

        """
            Request Body {
                "dataset_name": "dataset_name",
                "encoding_info": {
                    "column_list": ["column1", "column2"]
                }
            }
        """
        encoding_info = request.json.get("encoding_info")
        if not encoding_info:
            err = "Encoding info is required"
            raise
        if not isinstance(encoding_info, dict):
            err = "Encoding info should be a dictionary"
            raise

        column_list = encoding_info.get("column_list")
        if not column_list:
            err = "Column list is required"
            raise
        if not isinstance(column_list, list):
            err = "Column list should be a list"
            raise

        # Look if the copy of dataset exists and if it does, then rename the columns in that copy otherwise rename make a copy and rename the columns in that copy
        if not check_dataset_copy_exists(dataset_name, user.id, user.email):
            app.logger.info("Dataset copy of %s does not exist. Trying to make a copy of the dataset", dataset_name)
            err = make_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
    
        df, err = load_dataset_copy(dataset_name, user.id, user.email)
        if err:
            raise

        # ===================== Business Logic Starts Here =====================

        # Check if the column list is valid
        for col in column_list:
            if col not in df.columns:
                err = f"Column '{col}' does not exists in the dataset"
                raise
        
        df = BinaryEncoder(cols=column_list, return_df=True).fit(df).transform(df)
        
        # ===================== Business Logic Ends Here =====================

        # Save the dataset copy and update the metadata
        update_metadata(user, dataset_name, df)

        res = {
            "msg": "Binary encoding successful"
        }
        return respond(data=res)
        
    except Exception as e:
        log_error(err_msg="Error in binary encoding", error=err, exception=e)
        if not err:
            err = "Error in binary encoding"
        return respond(error=err)


# Api for binary encoding
@featureEncodingAPI.route("/frequency-encoding", methods=["POST"])
@jwt_required()
def frequency_encoding():
    """
        TAKES dataset_name, column list and normalize status as input
        PERFORMS frequency encoding on the dataset
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

        """
            Request Body {
                "dataset_name": "dataset_name",
                "encoding_info": {
                    "column_list": ["column1", "column2"],
                    "normalize": true
                }
            }
        """
        encoding_info = request.json.get("encoding_info")
        if not encoding_info:
            err = "Encoding info is required"
            raise
        if not isinstance(encoding_info, dict):
            err = "Encoding info should be a dictionary"
            raise

        column_list = encoding_info.get("column_list")
        if not column_list:
            err = "Column list is required"
            raise
        if not isinstance(column_list, list):
            err = "Column list should be a list"
            raise

        normalize = encoding_info.get("normalize", False)
        if not isinstance(normalize, bool):
            err = "Normalize status should be a boolean"
            raise

        # Look if the copy of dataset exists and if it does, then rename the columns in that copy otherwise rename make a copy and rename the columns in that copy
        if not check_dataset_copy_exists(dataset_name, user.id, user.email):
            app.logger.info("Dataset copy of %s does not exist. Trying to make a copy of the dataset", dataset_name)
            err = make_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
    
        df, err = load_dataset_copy(dataset_name, user.id, user.email)
        if err:
            raise

        # ===================== Business Logic Starts Here =====================

        # Check if the column list is valid
        for col in column_list:
            if col not in df.columns:
                err = f"Column '{col}' does not exists in the dataset"
                raise
        
        df = CountEncoder(cols=column_list, normalize=normalize, return_df=True).fit(df).transform(df)
        
        # ===================== Business Logic Ends Here =====================

        # Save the dataset copy and update the metadata
        update_metadata(user, dataset_name, df)

        res = {
            "msg": "Frequency encoding successful"
        }
        return respond(data=res)
        
    except Exception as e:
        log_error(err_msg="Error in frequency encoding", error=err, exception=e)
        if not err:
            err = "Error in frequency encoding"
        return respond(error=err)


# Api for ordinal  encoding
@featureEncodingAPI.route("/ordinal-encoding", methods=["POST"])
@jwt_required()
def ordinal_encoding():
    """
        TAKES dataset_name, column list and mapping status as input
        PERFORMS ordinal encoding on the dataset
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

        """
            Request Body {
                "dataset_name": "dataset_name",
                "encoding_info": {
                    "column_name": "col_name",
                    "custom_mapping": True,
                    "mapping": {
                        "cat_1": 1,
                        "cat_2": 2
                    }
                }
            }
        """
        encoding_info = request.json.get("encoding_info")
        if not encoding_info:
            err = "Encoding info is required"
            raise
        if not isinstance(encoding_info, dict):
            err = "Encoding info should be a dictionary"
            raise
        
        column_name = encoding_info.get("column_name")
        if not column_name:
            err = "Column name is required"
            raise

        custom_mapping = encoding_info.get("custom_mapping", False)
        if not isinstance(custom_mapping, bool):
            err = "Custom mapping status should be a boolean"
            raise

        mapping = encoding_info.get("mapping")
        if custom_mapping and not mapping:
            err = "Mapping information is required for custom mapping"
            raise
        if custom_mapping and not isinstance(mapping, dict):
            err = "Mapping should be a dictionary"
            raise

         # Look if the copy of dataset exists and if it does, then rename the columns in that copy otherwise rename make a copy and rename the columns in that copy
        if not check_dataset_copy_exists(dataset_name, user.id, user.email):
            app.logger.info("Dataset copy of %s does not exist. Trying to make a copy of the dataset", dataset_name)
            err = make_dataset_copy(dataset_name, user.id, user.email)
            if err:
                raise
    
        df, err = load_dataset_copy(dataset_name, user.id, user.email)
        if err:
            raise

        # ===================== Business Logic Starts Here =====================

        # Check if the column name is valid
        if column_name not in df.columns:
            err = f"Column '{column_name}' does not exists in the dataset"
            raise

        if custom_mapping:
            custom_mapping_obj = [
                {
                    'col': column_name, 
                    'mapping': mapping
                }
            ]
            df = OrdinalEncoder(mapping=custom_mapping_obj, return_df=True).fit(df).transform(df)
        
        else: 
            from sklearn.preprocessing import OrdinalEncoder as sklearn_OrdinalEncoder
            df[column_name] = sklearn_OrdinalEncoder().fit(df[[column_name]]).transform(df[[column_name]])
        
        # ===================== Business Logic Ends Here =====================

        # Save the dataset copy and update the metadata
        update_metadata(user, dataset_name, df)

        res = {
            "msg": "Ordinal encoding successful"
        }
        return respond(data=res)
    
    except Exception as e:
        log_error(err_msg="Error in ordinal encoding", error=err, exception=e)
        if not err:
            err = "Error in ordinal encoding"
        return respond(error=err)

def update_metadata(user, dataset_name, df):
    try:
        # get new column data
        new_column_list = df.columns.tolist()
        new_column_datatypes = df.dtypes.astype(str).to_dict() # Dictionary of column name and its type
        new_numerical_column_list = df.select_dtypes(exclude=['object', 'bool']).columns.tolist() # List of numerical columns
        new_categorical_column_list = df.select_dtypes(include=['object', 'bool']).columns.tolist() # List of categorical columns
        new_n_columns = len(new_column_list)
        new_n_rows = len(df)
        new_n_values = new_n_columns * new_n_rows

        dataset_name_copy = get_dataset_name( user.id, dataset_name) + "_copy"
        metadata_obj = MetaData.objects(dataset_file_name=dataset_name_copy).first_or_404(message=f"Metadata for '{dataset_name}' does not exists")
        metadata_dict = metadata_obj.to_mongo().to_dict()

        metadata_dict["column_list"] = new_column_list
        metadata_dict["column_datatypes"] = new_column_datatypes
        metadata_dict["numerical_column_list"] = new_numerical_column_list
        metadata_dict["categorical_column_list"] = new_categorical_column_list
        metadata_dict["n_columns"] = new_n_columns
        metadata_dict["n_rows"] = new_n_rows
        metadata_dict["n_values"] = new_n_values

        # ===== save the metadata and dataframe =====
        save_dataset_copy(df, dataset_name, user.id, user.email)

        # Update the metadata of the copy
        metadata_dict["is_copy_modified"] = True
        del metadata_dict["_id"]
        metadata_obj.update(**metadata_dict)
    
    except Exception as e:
        log_error(err_msg="Error in updating metadata", exception=e)
        raise e
"""Common Methods among all the APIs"""

import re
from sqlalchemy import text
import pandas as pd
from flask import current_app as app
from pathlib import Path

def is_email_valid(email):
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    if(re.fullmatch(regex, email)):
        return True
    else:
        return False

def get_dataset_name(user_id, dataset_name):
    dataset_name = f'{dataset_name.split(".")[0]}_{user_id}'
    return dataset_name

def get_parquet_dataset_file_name(dataset_name, user_email):
    dataset_file = app.config['UPLOAD_FOLDER'] + f'/{user_email}/{dataset_name}.parquet'
    return dataset_file

def get_home_directory():
    home_directory = Path.home()
    return home_directory

def get_user_directory(user_email):
    user_directory = app.config['UPLOAD_FOLDER'] + f'/{user_email}'
    return user_directory

def load_dataset(dataset_name, user_id, user_email):
    try:
        dataset_name = get_dataset_name(user_id, dataset_name)
        dataset_file = get_parquet_dataset_file_name(dataset_name, user_email)
        
        if not Path(dataset_file).is_file():
            err = "This dataset does not exists"
            return None, err

        df = pd.read_parquet(dataset_file)
        return df, None

    except Exception as e:
        app.logger.error("Error in loading the dataset")
        raise Exception(e)

def save_dataset_copy(df, dataset_name, user_id, user_email):
    try:
        dataset_name = get_dataset_name(user_id, dataset_name)
        dataset_name = dataset_name + "_copy"
        dataset_file = get_parquet_dataset_file_name(dataset_name, user_email)
        df.to_parquet(dataset_file, compression="snappy", index=False)
        return None
    except Exception as e:
        app.logger.error("Error in saving the dataset copy")
        raise Exception(e)

def log_error(err_msg = None, error=None, exception=None):
    app.logger.error("%s. Error=> %s. Exception=> %s",err_msg, error, str(exception))


def make_dataset_copy(dataset_name, user_id, user_email):
    try:
        df, err = load_dataset(dataset_name, user_id, user_email)
        if err:
            raise Exception(err)
        
        dataset_name = get_dataset_name(user_id, dataset_name) # dataset_name = iris_1
        dataset_name = dataset_name + "_copy" # dataset_name = iris_1_copy
        dataset_file_copy = get_parquet_dataset_file_name(dataset_name, user_email)

        df.to_parquet(dataset_file_copy, compression="snappy", index=False)
        app.logger.info("Dataset copy %s created successfully", dataset_name)
        return None
    
    except Exception as e:
        app.logger.error("Error in making the dataset copy")
        raise Exception(e)

def check_dataset_copy_exists(dataset_name, user_id, user_email):
    try:
        dataset_name = get_dataset_name(user_id, dataset_name) # dataset_name = iris_1
        dataset_name = dataset_name + "_copy" # dataset_name = iris_1_copy
        dataset_file_copy = get_parquet_dataset_file_name(dataset_name, user_email)

        if Path(dataset_file_copy).is_file():
            return True
        else:
            return False
    except Exception as e:
        app.logger.error("Error in checking the dataset copy exists")
        raise Exception(e)

def get_dataset_copy(dataset_name, user_id, user_email):
    try:
        dataset_name = get_dataset_name(user_id, dataset_name) # dataset_name = iris_1
        dataset_name = dataset_name + "_copy" # dataset_name = iris_1_copy
        dataset_file_copy = get_parquet_dataset_file_name(dataset_name, user_email)

        if Path(dataset_file_copy).is_file():
            df = pd.read_parquet(dataset_file_copy)
            return df, None
        else:
            err = "Dataset copy does not exists"
            return None, err
    except Exception as e:
        app.logger.error("Error in getting the dataset copy")
        raise Exception(e)

# =========================OLDER UNNECESSARY CODE=========================
def get_dataset(dataset_name, db):
    try:
        sql_query = text(f'select * from "{dataset_name}"')
        dataset = db.engine.execute(sql_query)
        df = pd.DataFrame(dataset)
        return df
    except Exception as e:
        app.logger.info("Error in fetching the dataset %s", dataset_name)
        raise e
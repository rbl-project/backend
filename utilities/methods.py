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

def get_dataset(dataset_name, db):
    try:
        sql_query = text(f'select * from "{dataset_name}"')
        dataset = db.engine.execute(sql_query)
        df = pd.DataFrame(dataset)
        return df
    except Exception as e:
        app.logger.info("Error in fetching the dataset %s", dataset_name)
        raise e

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
    dataset_name = get_dataset_name(user_id, dataset_name)
    dataset_file = get_parquet_dataset_file_name(dataset_name, user_email)
    if not Path(dataset_file).is_file():
        err = "This dataset does not exists"
        raise err
    df = pd.read_parquet(dataset_file)
    return df

def save_dataset():
    pass
"""Common Methods among all the APIs"""

import re
from sqlalchemy import text
import pandas as pd
from flask import current_app as app

def is_email_valid(email):
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    if(re.fullmatch(regex, email)):
        return True
    else:
        return False

def get_dataset_name(user_id, dataset_name, db):
    dataset_name = f'{dataset_name.split(".")[0]}_{user_id}'
    if not dataset_name in db.engine.table_names():
        return None
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
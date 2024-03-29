from manage.mongodb_setup import db

class MetaData(db.Document): # <dataset_name>_<user_id> OR <dataset_name>_<user_id>_copy
    id = db.SequenceField(primary_key=True,)
    user_id = db.IntField()
    user_email = db.StringField()
    is_copy = db.BooleanField()
    is_copy_modified = db.BooleanField()
    date_created = db.DateTimeField()   
    last_modified = db.DateTimeField()
    dataset_name = db.StringField()
    dataset_extension = db.StringField()
    dataset_file_name = db.StringField()
    dataset_size = db.FloatField()  # in MB
    n_rows = db.IntField()
    n_columns = db.IntField()
    n_values = db.IntField()
    column_list = db.ListField()
    column_datatypes = db.DictField()
    numerical_column_list = db.ListField()
    categorical_column_list = db.ListField()
    deleted_column_list =  db.ListField()
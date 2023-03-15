from manage.mongodb_setup import db

class Man(db.Document):
    name = db.StringField()
    email = db.StringField()
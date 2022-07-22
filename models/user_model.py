from manage.db_setup import db
import datetime
from werkzeug.security import generate_password_hash, check_password_hash


class Users(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    email = db.Column(db.String(120), nullable=False, unique=True)
    phone = db.Column(db.String(12))
    date_added = db.Column(db.DateTime, default=datetime.datetime.now())
    password_hash = db.Column(db.String(128))

    def save(self):
        db.session.add(self)
        try:
            db.session.commit()
        except:
            db.session.rollback()
            raise
        
    def setPassword(self, password):
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password):
        return check_password_hash(self.password_hash, password)
    
	# Create A String
    def __repr__(self):
        return '<Name %r>' % self.name
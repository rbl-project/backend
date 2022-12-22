import datetime
from manage.db_setup import db

class TokenBlocklist(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    jti = db.Column(db.String(36))
    expiry = db.Column(db.DateTime)
    revoked_at = db.Column(db.DateTime, default=datetime.datetime.now())

    def save(self):
        db.session.add(self)
        try:
            db.session.commit()
        except:
            db.session.rollback()
            raise
    
    def delete(self):
        db.session.delete(self)
        try:
            db.session.commit()
        except:
            db.session.rollback()
            raise
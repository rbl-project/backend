"""Common Methods, Constants, and Utilities for User API"""
import datetime
from flask_jwt_extended import get_jwt
from models.jwt_blocklist_model import TokenBlocklist
from flask import current_app as app

def revoke_jwt_token():
    try:
            
        jti = get_jwt()["jti"]
        expiry_timestap = get_jwt()["exp"]
        expiry = datetime.datetime.fromtimestamp(expiry_timestap)

        blocked_token = TokenBlocklist(jti=jti, expiry=expiry)

        blocked_token.save()
    except Exception as e:
        app.logger.error("Error in revoking the JWT token. Exception=> %s", str(e))
        raise

def delete_expired_jwt_tokens():
    try:
        all_revoked_token = TokenBlocklist.objects()
        for token in all_revoked_token:
            expiry = token.expiry
            if expiry <= datetime.datetime.now():
                token.delete()

    except Exception as e:
        app.logger.error("Error in deleting the expired JWT token. Exception=> %s", str(e))
        
        # no need to raise the exception as this has to be done as an extra fucntionality to present 
        # logout api call

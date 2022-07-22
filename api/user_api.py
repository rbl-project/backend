from flask import Blueprint
from flask import current_app as app
from models.user_model import Users
from utilities.respond import respond
from flask_restful import  Api
from flask import request

userAPI = Blueprint("userAPI", __name__)
userAPI_restful = Api(userAPI)

@userAPI.route("/", methods=['GET'])
def welcome():
    res = {
        'msg':'Welcome to RBL Project backend'
    }
    return respond(data=res)

@userAPI.route("/register", methods=['POST'])
def register():
    try:
        email = request.json.get("email")
        password = request.json.get("password")
        name = request.json.get("name")
        phone = request.json.get("phone")
        other = request.json.get("other")
        user = Users(name=name, email=email, phone=phone, other=other)
        user.setPassword(password)

        user.save()
        app.logger.info("User Registered Successfully")
        res = {
            "msg":"User Registered Successfully"
        }
        return respond(data=res)
    except Exception as ex:
        app.logger.error("Error in registering the user. Exception: ", str(ex))
        return respond(error="Error in registering the user")
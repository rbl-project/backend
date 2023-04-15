from flask import Blueprint
from flask import current_app as app
from models.user_model import Users
from utilities.methods import is_email_valid, log_error
from utilities.respond import respond
from flask_restful import  Api
from flask import request
from flask_jwt_extended import (
    create_access_token,
    get_jwt_identity,
    jwt_required
)

from api.User.utilities import (
    revoke_jwt_token, 
    delete_expired_jwt_tokens, 
    DELETE_CONFIRMATION_MSG
)
userAPI = Blueprint("userAPI", __name__)
userAPI_restful = Api(userAPI)

# @jwt_required()
@userAPI.route("/", methods=['GET'])
def welcome():
    """
        TAKES no input
        PERFORMS the welcome operation
        RETURNS the welcome message as response
    """
    # current_user = get_jwt_identity()
    # user = Users.query.filter_by(id = current_user["id"]).first()

    # app.logger.info("Welcome %s", user.name)

    # res = { 
    #     'msg':f'Hey {user.name}! Wish you the great data science session!'
    # }
    res={ 'message':'hello'}
    return respond(data=res)


@userAPI.route("/register", methods=['POST'])
def register():
    """
        TAKES name, email, password as input
        PERFORMS the registration operation
        RETURNS the success/failure message as response
    """
    err = None
    try:
        if not request.is_json:
            err="Missing JSON in request"
            raise

        email = request.json.get("email")
        if not email:
            err = "Email is required"
            raise
        if not is_email_valid(email):
            err="Invalid Email"
            raise

        name = request.json.get("name")
        if not name:
            err = "Name is required"
            raise

        password = request.json.get("password")
        if not password:
           err = "Password is required"
           raise
        
        check_user = Users.query.filter_by(email=email).first()
        if check_user:
            err="User already present"
            raise
        
        user = Users(name=name, email=email)
        user.setPassword(password)

        user.save()

        app.logger.info("User Registered Successfully")

        res = {
            "msg":"User Registered Successfully"
        }
        return respond(data=res, code=201)
    except Exception as e:
        log_error(err_msg="Error in registering the user", error=err, exception=e)
        if not err:
            err = "Error in registration"
        return respond(error=err)


@userAPI.route('/logout', methods=['GET', 'POST'])
@jwt_required()
def logout():
    """
        TAKES no input
        PERFORMS the logout operation
        RETURNS the success/failure message as response
    """
    err = None
    try:
        current_user = get_jwt_identity()
        user = Users.query.filter_by(id=current_user["id"]).first()
        if not user:
            err = "No such user exits"
            raise
        # logout_user()
        revoke_jwt_token()
        delete_expired_jwt_tokens()

        app.logger.info("%s Have Been Logged Out!  Thanks For Stopping By...",str(user.email))

        return respond(data="Logged Out Successfully")
    except Exception as e:
        log_error(err_msg="Error in logout", error=err, exception=e)
        if not err:
            err = "Error in logout."
        return respond(error=err)
	

@userAPI.route('/login', methods=['POST'])
def login():
    """
        TAKES email, password as input
        PERFORMS the login operation
        RETURNS the access_token, user_details as response
    """
    err = None
    try:
        email = request.json.get("email", None)
        if not email:
            err = "Email is required"
            raise

        password = request.json.get("password", None)
        if not password:
            err = "Password is required"
            raise

        user = Users.query.filter_by(email=email).first()
        if not user:
            err = "Bad Credentials. Try again"
            raise

        if not user.verify_password(password):
            err = "Bad Credentials. Try again"
            raise

        # login_user(user)
        access_token = create_access_token(identity=user.to_json())
       
        app.logger.info(" %s Login Successfull", user.name)
        
        res = {
            "msg":'User Logged In successfully',
            "access_token": access_token,
            "user_details": user.to_json()
        }
        return respond(data=res)
    except Exception as e:
        log_error(err_msg="Error in login", error=err, exception=e)
        if not err:
            err = "Error in Login"
        return respond(error=err)


@userAPI.route("/all-users", methods=["GET"])
def get_all_users():
    """
        TAKES no input
        PERFORMS the fetch all users operation
        RETURNS the list of all users as response
    """
    err = None
    try:
        all_users = Users.query.all()
        users = []
        for user in all_users:
            users.append(user.to_json())
        res = {
            "msg":"Fethced all the users",
            "users": users
        }
        return respond(data=res)
    except Exception as e:
        log_error(err_msg="Error in fetching all the users", error=err, exception=e)
        if not err:
            err = "Error in fetching all the users"
        return respond(error=err)


@userAPI.route("/delete-user", methods=["DELETE"])
@jwt_required()
def delete_user():
    """
        TAKES eamil, password and confirmation_text as input
        PERFORMS the delete user operation
        RETURNS the success/failure message as response
    """
    err = None
    try:
        current_user = get_jwt_identity()
        user = Users.query.filter_by(id=current_user["id"]).first()
        if not user:
            err = "No such user exits"
            raise

        email = request.json.get("email", None)
        if not email:
            err = "Email is required"
            raise

        if email != user.email:
            err = "Bad Credentials. Try again"
            raise

        password = request.json.get("password", None)
        if not password:
            err = "Password is required"
            raise

        confirmation_text = request.json.get("confirmation_text", None)
        if not confirmation_text:
            err = "Confirmation text is required"
            raise

        if confirmation_text != DELETE_CONFIRMATION_MSG:
            err = "Confirmation text is invalid"
            raise

        if not user.verify_password(password):
            err = "Bad Credentials. Try again"
            raise

        user.delete()

        app.logger.info("User Deleted Successfully and Logged Out!")

        revoke_jwt_token()
        delete_expired_jwt_tokens()

        res = {
            "msg":"User Deleted Successfully"
        }
        return respond(data=res)

    except Exception as e:
        log_error(err_msg="Error in deleting the user", error=err, exception=e)
        if not err:
            err = "Error in deleting the user"
        return respond(error=err)
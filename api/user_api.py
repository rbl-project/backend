from flask import Blueprint
from flask import current_app as app
from models.user_model import Users
from utilities.methods import is_email_valid
from utilities.respond import respond
from flask_restful import  Api
from flask import request
from flask_login import login_user, login_required, logout_user, current_user

userAPI = Blueprint("userAPI", __name__)
userAPI_restful = Api(userAPI)

@userAPI.route("/", methods=['GET'])
@login_required
def welcome():
    id = current_user.id
    user = Users.query.filter_by(id=id).first()
    app.logger.info("Welcome %s", user.name)
    res = { 
        'msg':'Welcome to RBL Project backend.'
    }
    return respond(data=res)

@userAPI.route("/register", methods=['POST'])
def register():
    err = None
    try:
        # logout current session is someone is logged in
        if current_user.is_authenticated:
            logout()
            
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
        app.logger.error("Error in registering the user. Error=> %s. Exception=> %s",err, str(e))
        if not err:
            err = "Error in registration"
        return respond(error=err)


@userAPI.route('/logout', methods=['GET', 'POST'])
@login_required
def logout():
    err = None
    try:
        logout_user()

        app.logger.info("You Have Been Logged Out!  Thanks For Stopping By...")

        return respond(data="Logged Out Successfully")
    except Exception as e:
        app.logger.error("Error in logout. Error= %s. Exception= %s", err, str(e))
        if not err:
            err = "Error in logout."
        return respond(error=err)
	

@userAPI.route('/login', methods=['POST'])
def login():
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

        login_user(user)
        app.logger.info("Login Successfull")
        
        res = {
            "msg":'User Logged In successfully'
        }
        return respond(data=res)
    except Exception as e:
        app.logger.error("Error in login. Error=> %s. Exception=> %s",err, str(e))
        if not err:
            err = "Error in Login"
        return respond(error=err)
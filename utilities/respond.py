from flask import jsonify

def respond(data=None, error=None, code=200):
    if error:
        status = False
        data = None
        # if code == 200:
        #     code = 500
    else:
        status = True
    res = {
        "data": data,
        "error": error,
        "status": status
    }
    # return jsonify(res) # removed code due to error handeling in frontend
    return jsonify(res), code
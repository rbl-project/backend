from flask import jsonify

def respond(data=None, error=None, code=200):
    if error:
        success = False
        data = None
        if code == 200:
            code = 500
    else:
        success = True
    res = {
        "data": data,
        "error": error,
        "success": success
    }
    return jsonify(res), code
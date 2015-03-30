from flask import request, jsonify, abort
from app import app, neo4j
from app.error_objects import *
from global_vars import *
from network_objects import Networks

@app.route("/")
def index():
    return "Index"

@app.route("/_api/send_request/", methods=["PUT"])
def send_request():
    req = request.form
    req_networks = {}
    if not req or not req.viewkeys() > {"to", "from"}:
        abort(404)
    from_mekid = req["from"]
    to_mekid = req["to"]
    
    for key, value in req.iteritems():
        if key in networks:
            req_networks[key] = int(value)

    try:
        neo4j.send_request(from_mekid, to_mekid, **req_networks)
    except UserError, arg:
        return json_response(json_error, arg.message)
    except RequestError, arg:
        return json_response(json_error, arg.message)
    except Exception, arg:
        print arg.message
        return json_response(json_error, unknown_error_message)
    else:
        return json_response(json_success, "You have sent a request to {}".format(neo4j.get_user(to_mekid)[name_key]))
    
@app.route("/_api/accept_request/", methods=["PUT"])
def accept_request():
    req = request.form
    req_networks = {}
    if not req or not req.viewkeys() > {"to", "from"}:
        abort(404)
    from_mekid = req["from"]
    to_mekid = req["to"]
    
    for key, value in req.iteritems():
        if key in networks:
            req_networks[key] = str(value)

    try:
        neo4j.accept_request(to_mekid, from_mekid, **req_networks)
    except UserError, arg:
        return json_response(json_error, arg.message)
    except LookupError, arg:
        return json_response(json_error, arg.message)
    except RequestError, arg:
        return json_response(json_error, arg.message)
    except Exception, arg:
        print arg.args
        return json_response(json_error, unknown_error_message)
    else:
        return json_response(json_success, "You have accepted {}'s request".format(neo4j.get_user(from_mekid)[name_key]))

@app.route("/_api/add_user/", methods=["POST"])
def add_user():
    neo4j.clear_db()
    req = request.form
    if not req or not all(key in req for key in ("name", "mekid")):
        abort(400)
    name = req.get('name', "error")
    mekid = req.get("mekid", "error")
    fb = req.get("fb", no_network_value)
    twit = req.get("twit", no_network_value)
    try:
        neo4j.create_user(name, mekid, **Networks(**{fb_key:fb, twit_key:twit}))
    except UserError, arg:
        return json_response(json_error, arg.message)
    except Exception, arg:
        print arg.message
        return json_response(json_error, unknown_error_message)
    else:
        return json_response(json_success, "User created successfully")
    
def json_response(status, msg=None):
    if msg:
        return jsonify(result = status, msg = msg)
    else:
        return jsonify(result = status)

@app.route("/hello/")
def hello():
    return "Hello World!"

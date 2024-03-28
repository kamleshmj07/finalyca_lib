import datetime
from flask import json, request, g, current_app
from flask.wrappers import Response
import socket
import time
import logging
from fin_models.controller_transaction_models import APILog
from werkzeug.exceptions import HTTPException, InternalServerError
from utils.authorize import AuthEntityType, authorize
import werkzeug

def fn_before_request():
    # setup timer
    g.req_time = time.time()

    # authenticate request
    # authorize(current_app.config["JWT_PUBLIC_KEY"])
    authorize()
    
    # logging.warning("Before request")

def fn_after_request(response: Response):
    # TODO: Currently we are using flask-cors, but it can be removed and CORS policy should be set here.

    # logging.warning("After request")
    g.resp_status_code = response.status_code
    # g.resp_data = response.get_data(as_text=True)
    # TODO: needs an alternative in the case the data is in binary
    g.resp_data = None
    g.resp_size = response.calculate_content_length()

    return response

def fn_teardown_request(err: None):
    # if 'access' in g and g.access.entity_type == AuthEntityType.api:
        # store data in API Log
    host = socket.gethostbyaddr(request.remote_addr)
    fqdn = socket.getfqdn(host[0])

    if request.method != 'OPTIONS':
        api_log = APILog()
        api_log.entity_id = g.access.entity_id if "access" in g else 1
        api_log.entity_type = g.access.entity_type.name if "access" in g else AuthEntityType.user.name
        # api_log.remote_addr = str(request.remote_addr)
        api_log.remote_addr = str(request.access_route) 
        api_log.http_method = request.method
        api_log.url_path = request.path
        api_log.query_str = request.query_string.decode('UTF-8') if request.query_string else None
        api_log.req_ts = datetime.datetime.fromtimestamp(g.req_time)
        api_log.req_payload = json.dumps(request.json) if request.json else json.dumps(request.form)
        api_log.req_has_files = True if request.files else False
        api_log.resp_status_code = g.resp_status_code
        api_log.resp_payload = g.resp_data
        api_log.resp_error = str(err) if err else None
        api_log.resp_time_ms = (time.time() - g.req_time)*1000
        api_log.resp_size_bytes = g.resp_size
        api_log.fqdn = fqdn

        current_app.store.db.add(api_log)
        current_app.store.db.commit()

    # log request with response or uncaught exception
    if err:
        current_app.logger.error(err)
        logging.error(err)
    else:
        time_taken = time.time() - g.req_time
        # current_app.logger.info(F"{request.endpoint}: {request.method} took {time_taken} seconds")
        current_app.logger.info(F"{request.endpoint}: {request.method} took {time_taken} seconds for addresses {list(request.access_route)}. origin: {request.origin}. referrer:{request.referrer}. remote_addr:{request.remote_addr}.")
    
    # remove existing DB connection 
    current_app.store.db.remove()

    # see if anything else to be done by the app


def exception_jsonifier(err):
    """Return JSON instead of HTML for HTTP errors."""
    # start with the correct headers and status code from the error
    # TODO: not the best version. looks like this will make code slow. optimize it.
    if isinstance(err, werkzeug.exceptions.HTTPException):
        response = err.get_response()
        response.data = json.dumps({
            "code": err.code,
            "name": err.name,
            "description": err.description,
        })
        response.content_type = "application/json"
        return response
    else:
        s = str(err)
        raise InternalServerError(description=s)

    # try:
    #     response = err.get_response()
    # except AttributeError as e:
    #     # any exception which is created by flask (or werkzeug) will have response. others will be logged here.
    #     s = str(err)
    #     raise InternalServerError(description=s)

    # # replace the body with JSON
    # response.data = json.dumps({
    #     "code": err.code,
    #     "name": err.name,
    #     "description": err.description,
    # })
    # response.content_type = "application/json"
    # return response

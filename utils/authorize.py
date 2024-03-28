from werkzeug.exceptions import Unauthorized, Forbidden
from flask import request, current_app, g, json
import fnmatch
from fin_models.controller_master_models import User
from .utils import AuthObj, AuthEntityType, validate_jwt_token, allow_multiple_logins_to_emails
from .access_control import APIAccessForbiddenException, verify_api_key
import logging
import socket

def authorize():
    # allow localhost request for testing
    allowed_clients = ['127.0.0.1', 'localhost', "demo.finalyca.com",  "portal.finalyca.com", "demoui.finalyca.com"]

    allowed_origins = [
        "https://demo.finalyca.com",
        # "https://portal.finalyca.com",    # Commented to not set the is_auth_ignored to "True"
        "https://preprod.finalyca.com",
        "https://finalyca.com",
        "https://demoui.finalyca.com"
    ]

    # OPTIONS are the pre-flight requests made by browser.
    allowed_methods = ["OPTIONS"]

    # Following requests may not be required as they are handled by IIS, but we may have to allow certain requests for remote debugging.
    allowed_wildcards = ["/static/*"]
    allowed_endpoints = ["/", "/favicon.ico", "/manifest.json", "/login/generate_otp", "/login/validate_otp", "/login/generate_token", "/login/validate_token", "/get_portfolio_report"]



    is_auth_ignored = False
    if not is_auth_ignored:
        for addr in allowed_clients:
            if request.remote_addr in addr:
                is_auth_ignored = True
                break

    # Allow whitelisted Perfios host to access whitelisted callbacks
    for ip_addr in request.access_route:    
        if ip_addr == current_app.config['PERFIOS_WHITELISTED_IP']:
            if request.path in current_app.config['PERFIOS_CALLBACKS_ENDPOINTS']:
                is_auth_ignored = True

    if not is_auth_ignored:
        if request.origin in allowed_origins:
            is_auth_ignored = True
    
    if not is_auth_ignored:
        if request.method in allowed_methods:
            is_auth_ignored = True

    if not is_auth_ignored:
        if request.path in allowed_endpoints:
            is_auth_ignored = True
        else:
            for ep in allowed_wildcards:
                if fnmatch.fnmatch(request.path, ep):
                    is_auth_ignored = True

    if not is_auth_ignored:
        is_auth_ignored = auth_gsquare_request()

    token_found = False
    token_valid = False
    # is_auth_ignored = False
    
    # TODO Whether we need this or not for API calls?
    # Should we take it off later?
    if "X_API_Key" in request.headers:
        token_found = True        
        token = request.headers["X_API_Key"]
        try:
            host = socket.gethostbyaddr(request.remote_addr)
            fqdn = socket.getfqdn(host[0])

            auth_obj = verify_api_key(current_app.store.db, token, request.access_route, fqdn)
        except APIAccessForbiddenException as api_ex:
            raise Forbidden(description=str(api_ex))
        
        if auth_obj:
            token_valid = True

    elif "X_User_Id" in request.headers:
        token_found = True

        user_id = request.headers.get("X_User_Id", type=int)

        sql_user = current_app.store.db.query(User).filter(User.User_Id == user_id).one_or_none()
        if sql_user:
            token_valid = True

            auth_obj = AuthObj()
            auth_obj.entity_type = AuthEntityType.user
            auth_obj.entity_id = sql_user.User_Id
            auth_obj.entity_org_id = sql_user.Organization_Id
            # TODO: Make sure following field comes from the database
            # auth_obj.entity_access_level = 3
            auth_obj.entity_access_level = sql_user.Access_Level
            auth_obj.entity_info["role_id"] = sql_user.Role_Id
    
    elif "token" in request.cookies:
        # check if call from frontend
        pass
    elif "X_Token" in request.headers:
        token = request.headers['X_Token']
        token_found = True
        try:
            result = validate_jwt_token(current_app.store.db, token, current_app.config['SECRET_KEY'])
            is_token_valid = result[0]
            user_id = result[1]
            data = result[2]

            if is_token_valid:
                # sql_user = allow_multiple_logins_to_emails(current_app.store.db, data.get("Email_Address"), user_id, data.get("Session_Id"))
                sql_user = current_app.store.db.query(User).filter(User.User_Id == user_id, User.Session_Id == data.get("Session_Id")).one_or_none()
                if sql_user:
                    token_valid = True

                    auth_obj = AuthObj()
                    auth_obj.entity_type = AuthEntityType.user
                    auth_obj.entity_id = sql_user.User_Id
                    auth_obj.entity_org_id = sql_user.Organization_Id
                    # TODO: Make sure following field comes from the database
                    # auth_obj.entity_access_level = 3
                    auth_obj.entity_access_level = sql_user.Access_Level
                    auth_obj.entity_info["role_id"] = sql_user.Role_Id

        except:
            token_valid = False


    # create dummy auth obj if is_auth_ignored is True
    if not token_valid and is_auth_ignored:
        # TODO: Think about giving sensible defaults
        token_found= True
        token_valid = True

        auth_obj = AuthObj()      
        auth_obj.entity_type = AuthEntityType.api
        auth_obj.entity_id = 1
        auth_obj.entity_org_id = 1
        auth_obj.entity_access_level = 3 

    if not token_found:
        raise Unauthorized(description="No authentication was provided.")

    if not token_valid:
        raise Unauthorized(description="Authentication was not validated.")
    
    g.access = auth_obj

def get_gsquare_requestobject(request_data):
    b = request_data.decode('utf-8')
    b = b.replace('\\', '').replace('"[', '[').replace(']"', ']').replace('"["', '["').replace('"]"', '"]').replace('"{', '{').replace('}"', '}').replace('\r\n', '')
    return json.loads(b)

#TODO please change below logic this is temporary solution
def auth_gsquare_request():
    gspy_endpoints = ['/api_layer/Gsquare/M00', '/api_layer/Gsquare/M01', '/api_layer/Gsquare/M02', '/api_layer/Gsquare/M03', '/api_layer/Gsquare/M04', '/api_layer/Gsquare/M05']

    if request.path in gspy_endpoints:        
        request_data = get_gsquare_requestobject(request.data) if request.data else None

        if str(request.remote_addr).startswith('172.31.'):            
            #validate token
            if request_data['RequestAuthorization']['Token'] == 'py2JQJDF8qP/UeaKjbsVUWRZjIYVQLW+3U54e1uj/Hs=':
                return True
            
    return False

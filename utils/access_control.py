import base64
import hashlib
import random
from fin_models.controller_master_models import API, Organization, User
import json
from .utils import AuthObj, AuthEntityType

class APIAccessForbiddenException(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info

def generate_key():
    # Generate random numbers from default random number generator
    r = str(random.getrandbits(256)).encode()
    # convert the random numbers into random text
    b = hashlib.sha256(r).digest()
    c = base64.b64encode(b).decode()
    # Remove any special characters and ensure we have a alpha numerical values
    return ''.join(e for e in c if e.isalnum())

def encrypt_key(key):
    p = key.encode('utf-8')
    return hashlib.sha1(p).hexdigest()

def verify_api_key(db_sessoion, request_api_key, request_access_route, fqdn):
    auth_obj = None

    db_key = encrypt_key(request_api_key)

    sql_obj = db_sessoion.query(API, Organization).join(Organization, API.org_id==Organization.Organization_Id).filter(API.api_key==db_key).one_or_none()

    if sql_obj:
        whitelisted_remote_addresses = json.loads(sql_obj[1].api_remote_addr) if sql_obj[1].api_remote_addr else []

        ip_found = False

        for remote_ip in request_access_route:
            if remote_ip in whitelisted_remote_addresses:
                ip_found = True
                break
            elif fqdn in whitelisted_remote_addresses: #validate fully qualified domain name if it is whitelisted
                ip_found = True
                break

        
        if not ip_found:
            raise APIAccessForbiddenException(F"{list(request_access_route)} in not part of allowed remote addresses.")

        # TODO: Add a check to make sure API has available hits. If not, give a different exception
        
        auth_obj = AuthObj()
        auth_obj.entity_type = AuthEntityType.api
        auth_obj.entity_id = sql_obj[0].id
        auth_obj.entity_org_id = sql_obj[0].org_id
        auth_obj.entity_access_level = sql_obj[1].api_access_level

    return auth_obj

def set_api_key(db_session, sql_api):
    api_key = None

    # Keep checking till we find a unique api key
    while True:
        # get a randomly generate alphanumeric string
        api_key = generate_key()

        # encrypt the key and check in the database if it is already used or not.
        encrypted_key = encrypt_key(api_key)
        sql_obj = db_session.query(API).filter(API.api_key==encrypted_key).one_or_none()
        if not sql_obj:
            break

    sql_api.api_key = encrypted_key
    return api_key



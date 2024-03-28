import yaml
import sqlalchemy
from flask import Request
import datetime
import enum
import base64 
from zipfile import ZipFile
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad,unpad
from fin_models.controller_master_models import User
import jwt
from datetime import datetime 
from datetime import timedelta
from dateutil.relativedelta import relativedelta

def is_valid_str(value):
    is_valid = False
    skip_str =["none", "null", "-"]
    if value and value.lower() not in skip_str:
        is_valid = True
    return is_valid

def encrypt_aes(raw: str, key: str, iv: str):
    raw = pad(raw.encode(), 16)
    cipher = AES.new(key.encode('utf-8'), AES.MODE_CBC, iv.encode('utf-8'))
    return base64.b64encode(cipher.encrypt(raw))

def decrypt_aes(enco: str, key: str, iv: str):
    enc = base64.b64decode(enco)
    cipher = AES.new(key.encode('utf-8'), AES.MODE_CBC, iv.encode('utf-8'))
    u = unpad(cipher.decrypt(enc), 16)
    return u.decode('UTF-8')
    
class AuthEntityType(enum.Enum):
    user = "User"
    api = "API"

class AuthObj:
    def __init__(self) -> None:
        self.entity_type = None
        self.entity_id = None
        self.entity_org_id = None
        self.entity_access_level = None
        self.entity_info = {}
    
def get_user_id(req: Request, db_session, secret_key):
    user_id = None

    # For temp Python version
    if "X_User_Id" in req.headers:
        user_id = req.headers.get("X_User_Id", type=int)

    if "X_Token" in req.headers:
        token = req.headers.get("X_Token")
        user_id = validate_jwt_token(db_session, token, secret_key)[1]

    # For C# layer
    if not user_id:
        if "RequestAuthorization" in req.form:
            auth_obj = req.form["RequestAuthorization"]
            user_id = auth_obj["User_Id"]

    return user_id

def mssql_prod_uri(is_production, database_name):
    if is_production:
        odbc_str = "Driver={SQL Server Native Client 11.0};Server=localhost;Database="+ database_name + ";UID=finalyca;PWD=F!n@lyca;"
        #uncomment below while comparing data
        #odbc_str = "Driver={SQL Server Native Client 11.0};Server=access.finalyca.com;Database="+ database_name + ";UID=finalyca;PWD=F!n@lyca;"
        connection_url = sqlalchemy.engine.URL.create("mssql+pyodbc", query={"odbc_connect": odbc_str})
    else:
        connection_url = 'mssql+pyodbc://@localhost/' + database_name + '?trusted_connection=yes&driver=SQL Server Native Client 11.0'
    
    return connection_url

def get_unsafe_db_engine(config):
    if "deploy" in config and config["deploy"] == "prod":
        is_production = True
    else:
        is_production = False
        
    return sqlalchemy.engine.create_engine( mssql_prod_uri(is_production, "PMS_Base") )


def get_DB_URI(server, database):
    return 'mssql+pyodbc://@' + server + '/' + database + '?trusted_connection=yes&driver=SQL Server Native Client 11.0'

def truncate_table(table_name, is_production=True):
    # TODO: Clean following code. Check with the models and make some sort of look up table for match database table.
    engine = sqlalchemy.engine.create_engine( mssql_prod_uri(is_production, "PMS_Base") )
    connection = engine.raw_connection()
    connection.execute(F"TRUNCATE TABLE {table_name}")
    connection.commit()
    connection.close()

def get_config(config_file_path):
    config = dict()
    with open(config_file_path) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    # if production
    if "deploy" in config and config["deploy"] == "prod":
        is_production = True
    else:
        is_production = False

    # if is_production:
    #     config["RAZORPAY_KEY_ID"] = config["RAZORPAY_TEST_ID"]
    #     config["RAZORPAY_KEY_SECRET"] = config["RAZORPAY_TEST_SECRET"]
    # else:
    #     config["RAZORPAY_KEY_ID"] = config["RAZORPAY_PROD_ID"]
    #     config["RAZORPAY_KEY_SECRET"] = config["RAZORPAY_PROD_SECRET"]

    config["RAZORPAY_KEY_ID"] = config["RAZORPAY_PROD_ID"]
    config["RAZORPAY_KEY_SECRET"] = config["RAZORPAY_PROD_SECRET"]

    return config

def print_query(query):
    # engine = query.session.get_bind()
    q_str = str(query.statement.compile(
        compile_kwargs={"literal_binds": True},
        # dialect=query.session.bind[0].dialect
        ))
    print(q_str)
    return q_str


def to_float(num, defaultvalue=0.0):
    try:
        return float(num)        
    except ValueError:
        return defaultvalue


def comma_separator_inr(number):
    s, *d = str(number).partition(".")
    r = ",".join([s[x-2:x] for x in range(-3, -len(s), -2)][::-1] + [s[-3:]])
    return "".join([r] + d)


def get_last_day_for_next_month(current_month, current_year):
    # Get first date for the given month and year
    cur_date = datetime.date(current_year, current_month, 1)

    # go forward 2 months and come back one day to get the exact last date
    target_month = current_month + 2
    target_year = current_year

    # If target month is crossing December, make the correction
    if target_month > 12:
        target_month = target_month - 12
        target_year = target_year + 1      
        
    return cur_date.replace(month=target_month).replace(year=target_year) - datetime.timedelta(days=1)


def shift_date(current_date: datetime.date, months, years):
    target_month = current_date.month + months
    target_year = current_date.year + years

    # If target month is crossing December, make the correction
    if target_month > 12:
        target_month = target_month - 12
        target_year = target_year + 1 

    if target_month < 1:
        target_month = 12 - target_month
        target_year = target_year - 1      
        
    return current_date.replace(month=target_month).replace(year=target_year)


# TODO: following method is obsolate. Remote it.
def pretty_float(value):
    # if value:
    #     return float(round(value, 2))
    # else:
    #     return None
    return value


def generate_jwt_token(user_info, secret_key, for_sso_url=False, expiry=60):

    pay_load = {
        'User_Id':user_info.User_Id,
        'Organization_Id': user_info.Organization_Id,
        'Display_Name':user_info.Display_Name,
        'Email_Address': user_info.Email_Address,
        'Role_Id':user_info.Role_Id,
        'Access_Level':user_info.Access_Level,
        'Downloadnav_Enabled':user_info.downloadnav_enabled,
        'Organization_Name':user_info.Organization_Name,
        'AMC_Id':user_info.AMC_Id,
        'Contact_Number': user_info.Contact_Number,
        'Designation': user_info.Designation,
        'Profile_Picture': user_info.Profile_Picture,
        'City': user_info.City,
        'State': user_info.State,
        'Pin_Code': user_info.Pin_Code,
        # 'License_Expiry_Date': user_info.License_Expiry_Date,
        # 'User_Type_Id': user_info.usertype_id,
        'exp' : datetime.utcnow() + timedelta(minutes = expiry)
    }

    if not for_sso_url:
        pay_load['Session_Id'] = user_info.Session_Id

    # generates the JWT Token
    token = jwt.encode(pay_load, secret_key)

    return token

def generate_report_jwt_token(file_info, secret_key, expiry=1440):
    pay_load = {
        'User_Id':file_info['User_Id'],
        'Email_Address': file_info['Recipients'],
        'Request_Id' : file_info['Request_Id'],
        'File': file_info['File'],
        'exp' : datetime.utcnow() + timedelta(minutes = expiry)
    }

    # generates the JWT Token
    token = jwt.encode(pay_load, secret_key)

    return token

def validate_jwt_token(db_session, token, secret_key, get_full_data=False):
    # decoding the payload to fetch the stored details
    data = jwt.decode(token, secret_key, algorithms=['HS256'])

    if get_full_data:
        return data
    
    user_id = data.get('User_Id')
    sql_user = db_session.query(User).filter(User.User_Id == user_id).one_or_none()
    if sql_user:
        return [True, user_id, data]

    return [False, None, None]


def allow_multiple_logins_to_emails(db_session, email, user_id, session_id):
    '''
    created for iOS app review process to allow multiple logins
    '''
    allow_multiple_logins_email = ['demo1@finalyca.com']
    if email in allow_multiple_logins_email:
        sql_user = db_session.query(User).filter(User.User_Id == user_id).one_or_none()
    else:
        sql_user = db_session.query(User).filter(User.User_Id == user_id, User.Session_Id == session_id).one_or_none()
    
    return sql_user

def calculate_age(from_date, to_date, in_months=False):
    cal_age = relativedelta(to_date, from_date)
    if in_months:
        return (cal_age.years * 12) + cal_age.months
    else:        
        if cal_age.years > 0:
            if cal_age.months > 0:
                return str(cal_age.years) + " Year's " + str(cal_age.months) + " Month's"
            else:
                return str(cal_age.years) + " Year's"
        else:
                return str(cal_age.months) + " Month's"


def remove_stop_words(sentence: str, stop_words: list[str]):
    '''
    Provide a sentence and list of stop words that need to be kicked out.
    '''
    word_list = sentence.split()
    clean_sentence = ' '.join([w for w in word_list if w.upper() not in stop_words])
    return (clean_sentence)


def unzip(zip_file_path, extract_dir_path):
    file_names = None
    with ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir_path)
        file_names = zip_ref.namelist()
    
    return file_names

import requests
from bs4 import BeautifulSoup
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import csv
import os
import sqlalchemy
from sqlalchemy_utils import database_exists, create_database
import logging
from logging.handlers import RotatingFileHandler
import sys
from datetime import date, timedelta
import calendar
from sebi_lib.models import Base as Sebi_Base
from fin_models.masters_models import Base as PMS_Base
import socket
class LinuxRunnable():
    '''
    Create an instance of this class in the main file. When automatically releases socket when it goes out of the scope. 
    1. When script finishes execution
    2. when SigKill signal is caught.
    '''
    def __init__(self, app_name) -> None:
        self._lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            # The null byte (\0) means the socket is created in the abstract namespace instead of being created on the file system itself.
            self._lock_socket.bind('\0' + app_name)
            logging.info(F"{app_name} can start.")
        except socket.error:
            logging.info(F"{app_name} is already running.")
            sys.exit(0)

class MaxLevelFilter(logging.Filter):
    '''Filters (lets through) all messages with level < LEVEL'''
    def __init__(self, level):
        self.level = level

    def filter(self, record):
        # "<" instead of "<=": since logger.setLevel is inclusive, this should be exclusive
        return record.levelno < self.level 

def setup_file_logger(program_name, logs_directory, use_formatter = True):
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s P%(process)d l%(lineno)d@%(module)s.%(funcName)s : %(message)s")

    MIN_LEVEL= logging.DEBUG

    stdout_hdlr = logging.StreamHandler(sys.stdout)
    stderr_hdlr = logging.StreamHandler(sys.stderr)
    # stdout_hdlr = RotatingFileHandler(os.path.join(logs_directory, F'{program_name}.log'), maxBytes=102400, backupCount=10)
    # stderr_hdlr = RotatingFileHandler(os.path.join(logs_directory, F'{program_name}.err'), maxBytes=102400, backupCount=10)

    stdout_hdlr.addFilter( MaxLevelFilter(logging.WARNING) )    
    stdout_hdlr.setLevel( MIN_LEVEL )
    if use_formatter:
        stdout_hdlr.setFormatter(formatter)
    
    stderr_hdlr.setLevel( max(MIN_LEVEL, logging.WARNING) ) 
    if use_formatter:
        stderr_hdlr.setFormatter(formatter)

    logger = logging.getLogger()
    logger.addHandler(stdout_hdlr)
    logger.addHandler(stderr_hdlr)
    logger.setLevel(logging.INFO)


def write_csv(file_path, header, rows):
    file_exists = os.path.isfile(file_path)

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, 'a') as f:
        write = csv.writer(f, lineterminator="\n")
        if not file_exists:
            write.writerow(header)
        for r in rows:
            write.writerow(r)

def write_html(file_path, html):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, 'wb') as f:
        f.write(html)

def amc_reg_nr(amc_selection_value):
    sl = amc_selection_value.split("@@")
    return sl[0]

def to_float(td_element):
    t = td_element.text.strip()
    return float(t) if t else 0.0

def get_last_day_for_next_month(current_month, current_year):
    # Get first date for the given month and year
    cur_date = date(current_year, current_month, 1)

    # go forward 2 months and come back one day to get the exact last date
    target_month = current_month + 2
    target_year = current_year

    # If target month is crossing December, make the correction
    if target_month > 12:
        target_month = target_month - 12
        target_year = target_year + 1      
        
    return cur_date.replace(month=target_month).replace(year=target_year) - timedelta(days=1)

def get_last_day_for_prev_month(current_month, current_year):
    # go forward 2 months and come back one day to get the exact last date
    target_month = current_month - 1
    target_year = current_year

    # If target month is crossing December, make the correction
    if not target_month > 0:
        target_month = 12
        target_year = target_year - 1

    last_day = calendar.monthrange(target_year, target_month)[1]

    return date(target_year, target_month, last_day)

def get_selection_value(sel_tag):
    values = list()
    options = sel_tag.find_all(['option'])
    for option in options:
        value = option.attrs['value']
        if value:
            values.append(value)
    return values

def get_amc_list():
    url = "https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doPmr=yes"
    
    # verify=False to ignore SSL Errors
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    res = requests.get(url, verify=False)
    html=BeautifulSoup(res.text,'html.parser')

    sel=html.find_all(['select'])

    return get_selection_value(sel[0])

def get_sebi_amc_html(sebi_nr, year, month):
    url="https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doPmr=yes"
    f = {
        "currdate": None,
        "loginflag": 0,
        "searchValue": None,
        "pmrId": sebi_nr,
        "year": year,
        "month": month,
        "loginEmail": None,
        "loginPassword": None,
        "cap_login": None,
        "moduleNo": -1,
        "moduleId": None,
        "link": None,
        "yourName": None,
        "friendName": None,
        "friendEmail": None,
        "mailmessage": None,
        "cap_email": None
    }
    # TODO: add check to make sure when we have connection error we handle it wisely.
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    res = requests.post(url, f, verify=False)
    return res.content

def cleanify(text: str) -> str:
    t = text.strip()
    op = " ".join(t.split())
    return op

def get_database_scoped_session(sqlalchemy_uri):
    engine = sqlalchemy.engine.create_engine(sqlalchemy_uri)
    SessionLocal = sqlalchemy.orm.sessionmaker(bind=engine, autocommit=False, autoflush=False)
    return sqlalchemy.orm.scoped_session(SessionLocal)

def get_db_session(DB_URL: str, ORM_Base, auto_create: bool):
    if not database_exists(DB_URL):
        if auto_create:
            create_database(DB_URL)
            engine = sqlalchemy.engine.create_engine(DB_URL)
            ORM_Base.metadata.create_all(engine)
        else:
            raise Exception("Database does not exist.")
    
    return get_database_scoped_session(DB_URL)

def create_tables(DB_URL: str, ORM_Base):
    engine = sqlalchemy.engine.create_engine(DB_URL)
    ORM_Base.metadata.create_all(engine)

def mssql_prod_uri(is_production, database_name):
    if is_production:
        odbc_str = "Driver={SQL Server Native Client 11.0};Server=localhost;Database="+ database_name + ";UID=finalyca;PWD=F!n@lyca;"
        #uncomment below while comparing data
        #odbc_str = "Driver={SQL Server Native Client 11.0};Server=access.finalyca.com;Database="+ database_name + ";UID=finalyca;PWD=F!n@lyca;"
        connection_url = sqlalchemy.engine.URL.create("mssql+pyodbc", query={"odbc_connect": odbc_str})
    else:
        connection_url = 'mssql+pyodbc://@localhost/' + database_name + '?trusted_connection=yes&driver=SQL Server Native Client 11.0'
    
    return connection_url

def get_sebi_database_scoped_session():
    pms_base_engine = sqlalchemy.engine.create_engine( mssql_prod_uri(True, "PMS_Base") )
    sebi_pms_engine = sqlalchemy.engine.create_engine( mssql_prod_uri(True, "SEBI_PMS") )

    session_factory = sqlalchemy.orm.sessionmaker( binds= { 
        PMS_Base: pms_base_engine, Sebi_Base: sebi_pms_engine,
        }, autocommit=False, autoflush=False)

    return sqlalchemy.orm.scoped_session(session_factory)
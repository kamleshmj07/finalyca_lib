from datetime import date
import time
import os
from sqlalchemy import func
from sebi_lib import *
from sebi_lib.models import AMCScrapperStatus 
import logging

def process_sebi_pms_page(sebi_amc_name, as_of_date, cooldown_seconds, db_session, html_export_dir, verbose: bool) -> bool:
    sebi_code = amc_reg_nr(sebi_amc_name)

    if verbose:
        logging.info(F"scrapping {sebi_amc_name} for {as_of_date.month} {as_of_date.year}")

    try:
        html_str = get_sebi_amc_html(sebi_amc_name, as_of_date.year, as_of_date.month)
        if verbose:
            logging.info("Fetched.")

        scrap_pms_info(sebi_code, as_of_date, html_str, db_session, html_export_dir, verbose)
        # return True if successful
        return True

    except requests.exceptions.ConnectionError as err:
        if verbose:
            logging.info(str(err))
        logging.info(F"--- cooling down for {cooldown_seconds} seconds")
        time.sleep(cooldown_seconds)
        return False

    except requests.exceptions.Timeout as err:
        if verbose:
            logging.info(str(err))
        logging.info(F"--- cooling down for {cooldown_seconds} seconds")
        time.sleep(cooldown_seconds)
        return False
    except requests.exceptions.ConnectTimeout as err:
        if verbose:
            logging.info(str(err))
        logging.info(F"--- cooling down for {cooldown_seconds} seconds")
        time.sleep(cooldown_seconds)
        return False
        
    except requests.exceptions.HTTPError as err:
        if verbose:
            logging.info(str(err))

        return False

        

def scrap_pms_info(sebi_code, as_of_date, html_str, db_session, export_dir, verbose: bool):
    # Check if scrapper tried to scrap this page
    sql_obj = db_session.query(AMCScrapperStatus).filter(AMCScrapperStatus.sebi_nr == sebi_code).filter(AMCScrapperStatus.as_of==as_of_date).one_or_none()
    if not sql_obj:
        sql_obj = AMCScrapperStatus()
        sql_obj.sebi_nr = sebi_code
        sql_obj.as_of = as_of_date
        sql_obj.created_at = datetime.now()

    try:
        process_amc_html(db_session, sebi_code, as_of_date, html_str)
        sql_obj.has_data = True
        sql_obj.is_successful = True
        if verbose:
            logging.info("Processed.")

    except NoInfoException as err:
        if verbose:
            logging.info("No data.")
        sql_obj.has_data = False
        sql_obj.is_successful = False        

    except Exception as e:
        logging.exception(F"Exception.{str(e)}")
        db_session.rollback()
        sql_obj.has_data = True
        sql_obj.is_successful = False
        sql_obj.error_description = str(e)

    finally:
        file_path = os.path.join( export_dir, sebi_code, F"{as_of_date.year}_{as_of_date.month}.html" )
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(html_str)

        sql_obj.last_tried_on = datetime.now()
        sql_obj.raw_html_path = file_path
        if not sql_obj.id:
            db_session.add(sql_obj)
        db_session.commit()

def check_missing_data(db_session, html_export_dir, verbose: bool):
    COOL_DOWN_PERIOD = 60

    # Check the current status of the SEBI AMC List
    amc_list = get_amc_list()

    # Prepare a lookup dictionary for sebi code to sebi name 
    # INP000000993 : 'INP000000993@@INP000000993@@JEETAY INVESTMENTS PVT LTD'
    amc_lookups = dict()
    for amc_item in amc_list:
        amc = amc_item.strip()
        sebi_code = amc_reg_nr(amc)
        amc_lookups[sebi_code] = amc

    # Check history for the unsuccessful entries (if they have uploaded data or we have fixed the errors)
    sql_objs = db_session.query(AMCScrapperStatus).filter(AMCScrapperStatus.is_successful == 0).filter(AMCScrapperStatus.to_ignore == 0).all()
    for sql_obj in sql_objs:
        as_of = sql_obj.as_of
        sebi_amc_name = amc_lookups[sql_obj.sebi_nr]

        process_sebi_pms_page(sebi_amc_name, as_of, COOL_DOWN_PERIOD, db_session, html_export_dir, verbose)   

def scrap_whole_website(db_session, html_export_dir, verbose: bool):
    # Sebi started putting data out for all PMS from Nov 2020. We may get some data for some AMC prior to that, but it is not consistent. Therefore the first possible data we can get is from Nov 2020. So we start with the October to make sure looping works without special conditions.
    SEBI_DATA_START_DATE = date(2020, 10, 1)
    
    # If scrapper makes too many queries in short time, website may stop responding. In that case, use cool down period to sleep for a while. Unit is in seconds.
    COOL_DOWN_PERIOD = 60
    today = date.today()
    amc_list = get_amc_list()
    for amc_item in amc_list:
        sebi_amc_name = amc_item.strip()
        sebi_code = amc_reg_nr(sebi_amc_name)

        # Check the last date for which data was fetched. Else start from beginning.
        
        # TODO: Think about an alternative. If scrapper crashes during scrapping the website, we will end up having half data from the webpage and start scrapping a new page rather than reimporting the previous page.
        start_date = db_session.query(func.max(AMCInfo.as_of)).filter(AMCInfo.sebi_nr==sebi_code).scalar()
        if not start_date:
            start_date = SEBI_DATA_START_DATE

        while True:
            # next_date = start_date + relativedelta.relativedelta(months=1)
            next_date = get_last_day_for_next_month(start_date.month, start_date.year)
            
            # never read data for the current month and future
            if next_date.month >= today.month and next_date.year == today.year:
                break

            if verbose:
                logging.info(F"scrapping {sebi_amc_name} for {next_date.month} {next_date.year}")

            if process_sebi_pms_page(sebi_amc_name, next_date, COOL_DOWN_PERIOD, db_session, html_export_dir, verbose):
                start_date = next_date
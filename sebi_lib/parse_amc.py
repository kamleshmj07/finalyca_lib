from sqlalchemy import and_

from sebi_lib.helper import find_or_create_amc
from sebi_lib.models import *
from datetime import datetime

from bs4 import BeautifulSoup
from .utils import *

from .helper import *

class NoInfoException(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info

class ParsingException(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info

def process_amc_html(db_session, sebi_code, as_of_date, html_str):
    html=BeautifulSoup(html_str,'html.parser')
    tables = html.find_all(['table'])

    if not len(tables):
        raise NoInfoException(F"{sebi_code} has no information for {as_of_date}")

    save_general_info(db_session, sebi_code, as_of_date, tables)
    save_client_breakup(db_session, sebi_code, as_of_date, tables)
    save_service_breakup(db_session, sebi_code, as_of_date, tables)
    save_amc_schemes_info(db_session, sebi_code, as_of_date, tables)
    save_amc_transaction_data(db_session, sebi_code, as_of_date, tables)
    save_non_discretionary_details(db_session, sebi_code, as_of_date, tables)
    save_amc_complaints(db_session, sebi_code, as_of_date, tables)

def check_bool(text):
    # if no text, then consider it as False
    if not text:
        return False

    if text.lower() == "yes":
        return True
    elif text.lower() == "no":
        return False
    else:
        raise Exception(F"{text} is not supported bool option.")

def save_general_info(db_session, sebi_code, as_of, tables):
    main_th = tables[0].find('thead')
    main_tr = main_th.findAll(['tr'])

    amc_name = main_tr[0].td.text.strip()
    amc_registration_date = datetime.fromisoformat(main_tr[2].td.text.strip())

    sql_a = find_or_create_amc(db_session, amc_name, sebi_code, amc_registration_date, as_of)

    sql_amc = db_session.query(AMCInfo).filter(and_(AMCInfo.sebi_nr == sebi_code, AMCInfo.as_of == as_of)).one_or_none()
    if sql_amc:
        # Nothing to do if already stored
        return 

    sql_amc = AMCInfo()
    sql_amc.created_at = datetime.now()
    sql_amc.sebi_nr = sebi_code
    sql_amc.as_of = as_of
    sql_amc.address = main_tr[3].td.text.strip()
    sql_amc.principal_officer_name = cleanify(main_tr[4].td.text)
    sql_amc.principal_officer_email = main_tr[5].td.text.strip()
    sql_amc.principal_officer_contact = main_tr[6].td.text.strip()
    sql_amc.compliance_officer_name = cleanify(main_tr[7].td.text)
    sql_amc.compliance_officer_email = main_tr[8].td.text.strip()
    sql_amc.total_client = int(float(main_tr[9].td.text.strip()))
    sql_amc.total_aum_in_cr = float(main_tr[10].td.text.strip())

    if len(tables) > 3:
        service_table = tables[3]
        service_body = service_table.find(["tbody"])
        service_trs = service_body.find_all(["tr"])
        discre_td = service_trs[0].find_all(['td'])[2].text.strip()
        nondiscre_td = service_trs[1].find_all(['td'])[2].text.strip()
        advisor_td = service_trs[2].find_all(['td'])[2].text.strip()
        sql_amc.is_discretionary_active = check_bool(discre_td.lower())
        sql_amc.is_non_discretionary_active = check_bool(nondiscre_td.lower())
        sql_amc.is_advisory_active = check_bool(advisor_td.lower())

    db_session.add(sql_amc)
    db_session.commit()

def save_client_breakup(db_session, sebi_code, as_of_date, tables):   
    save_single_client_breakup(db_session, sebi_code, as_of_date, ServiceType.total.name, tables[1])

    if not len(tables) > 3:
        return
    
    # We have specific information about each service
    save_single_client_breakup(db_session, sebi_code, as_of_date, ServiceType.discretionary.name, tables[4])
    save_single_client_breakup(db_session, sebi_code, as_of_date, ServiceType.non_discretionary.name, tables[9])
    save_single_client_breakup(db_session, sebi_code, as_of_date, ServiceType.advisory.name, tables[14])


def save_service_breakup(db_session, sebi_code, as_of_date, tables):
    sql_services = db_session.query(AMCAssetClasses).filter(and_(AMCAssetClasses.sebi_nr == sebi_code, AMCAssetClasses.as_of == as_of_date)).all()
    # It may happen that while saving 3 rows it may crash at any moment. So, making sure we have 3 rows per amc and date. if not, save all 3 again.
    if len(sql_services) == 3:
        return
    
    service_table = tables[2]
    service_body = service_table.find('tbody')
    service_rows = service_body.findAll(['tr'])

    for service_row in service_rows:
        # Check if we have same entry. if yes, update it else insert it.
        is_advisory = False
        th = service_row.find('th')
        if th:
            # for advisory services
            service_type = th.text.strip()
            is_advisory = True
        else:
            # for non advisory services
            tds = service_row.findAll(['td'])
            service_type = tds[0].text.strip()

        sql_service = db_session.query(AMCAssetClasses).filter(and_(AMCAssetClasses.sebi_nr == sebi_code, AMCAssetClasses.as_of == as_of_date, AMCAssetClasses.service_type==service_type)).one_or_none()
        is_create = False
        if not sql_service:
            sql_service = AMCAssetClasses()
            sql_service.created_at = datetime.now()
            is_create = True
                
        sql_service.sebi_nr = sebi_code
        sql_service.as_of = as_of_date
        sql_service.service_type = ServiceType(service_type).name
        if not is_advisory:
            sql_service.listed_equity = tds[1].text.strip() 
            sql_service.unlisted_equity = tds[2].text.strip() 
            sql_service.listed_plain_debt = tds[3].text.strip() 
            sql_service.unlisted_plain_debt = tds[4].text.strip() 
            sql_service.listed_structured_debt  = tds[5].text.strip() 
            sql_service.unlisted_structured_debt = tds[6].text.strip() 
            sql_service.equity_derivatives = tds[7].text.strip()  
            sql_service.commodity_derivatives = tds[8].text.strip() 
            sql_service.other_derivatives = tds[9].text.strip() 
            sql_service.mutual_funds = tds[10].text.strip()  
            sql_service.others = tds[11].text.strip() 
        sql_service.total = tds[-1].text.strip() 

        if is_create:
            db_session.add(sql_service)
        db_session.commit()
    
def save_amc_schemes_info(db_session, sebi_code, as_of_date, tables):
    if not len(tables) > 3:
        return
        
    # Do not save anything with name 'Total'
    scheme_assets_table = tables[5]
    scheme_assets_body = scheme_assets_table.find('tbody')
    scheme_assets_rows = scheme_assets_body.findAll(['tr'])

    sql_discretionary = DiscretionaryDetails()
    sql_discretionary.created_at = datetime.now()
    sql_discretionary.sebi_nr = sebi_code
    sql_discretionary.as_of = as_of_date

    for asset_row in scheme_assets_rows:
        tds = asset_row.findAll(['th'])
        save_scheme_asset_breakup(db_session, sebi_code, as_of_date, asset_row)

    flows_table = tables[6]
    flow_body = flows_table.find('tbody')
    flow_rows = flow_body.findAll(['tr'])
    skip_first = True
    for flow in flow_rows:
        if skip_first:
            skip_first = False
        else:
            tds = flow.findAll(['td'])
            scheme_name = tds[0].text.strip()
            if scheme_name == "Total":
                sql_discretionary.inflow_month_in_cr = tds[1].text.strip()
                sql_discretionary.outflow_month_in_cr = tds[2].text.strip()
                sql_discretionary.net_flow_month_in_cr = tds[3].text.strip()
                sql_discretionary.inflow_fy_in_cr = tds[4].text.strip()
                sql_discretionary.outflow_fy_in_cr = tds[5].text.strip()
                sql_discretionary.net_flow_fy_in_cr = tds[6].text.strip()
            else:
                save_scheme_flow(db_session, sebi_code, as_of_date, flow)

    perform_table = tables[8]
    perform_body = perform_table.find('tbody')
    perform_rows = perform_body.findAll(['tr'])
    row_cnt = len(perform_rows)

    for i in range(row_cnt):
        if (i*2 + 1) >= row_cnt:
            break

        row_1 = perform_rows[i*2]
        row_2 = perform_rows[i*2 + 1]
        save_scheme_performance(db_session, sebi_code, as_of_date, row_1, row_2)

    # read last row to get total AUM
    row = perform_rows[-1]
    tds = row.findAll(['td'])
    scheme_name = tds[0].text.strip()
    if scheme_name == "Total":
        sql_discretionary.aum = tds[1].text.strip()
    db_session.add(sql_discretionary)
    db_session.commit()

def save_amc_transaction_data(db_session, sebi_code, as_of_date, tables):
    if not len(tables) > 3:
        return

    discretionary = tables[7]
    discretbody = discretionary.find('tbody')
    discretrows = discretbody.findAll(['tr'])
    save_amc_transaction(db_session, sebi_code, as_of_date, ServiceType.discretionary.name, discretrows[0], discretrows[1], discretrows[2])

    discretionary = tables[12]
    discretbody = discretionary.find('tbody')
    discretrows = discretbody.findAll(['tr'])
    save_amc_transaction(db_session, sebi_code, as_of_date, ServiceType.non_discretionary.name, discretrows[0], discretrows[1], discretrows[2])

def save_amc_complaints(db_session, sebi_code, as_of_date, tables):
    if not len(tables) > 3:
        return
    
    complaints_table = tables[15]
    complaints_body = complaints_table.find('tbody')
    complaints_rows = complaints_body.findAll(['tr'])
    tds1 = complaints_rows[0].findAll(['td'])
    tds2 = complaints_rows[1].findAll(['td'])
    tds3 = complaints_rows[2].findAll(['td'])
    tds4 = complaints_rows[3].findAll(['td'])
    tds5 = complaints_rows[4].findAll(['td'])
    tds6 = complaints_rows[5].findAll(['td'])

    sql_obj = db_session.query(AMCComplaints).filter(and_(AMCComplaints.sebi_nr == sebi_code, AMCComplaints.as_of == as_of_date)).one_or_none()
    if sql_obj:
        return

    sql_obj = AMCComplaints()
    sql_obj.as_of = as_of_date
    sql_obj.sebi_nr = sebi_code
    sql_obj.pf_pending_at_month_start = int(tds1[1].text.strip())
    sql_obj.pf_received_during_month = int(tds1[2].text.strip())
    sql_obj.pf_resolved_during_month = int(tds1[3].text.strip())
    sql_obj.pf_pending_at_month_end = int(tds1[4].text.strip())
    sql_obj.corporates_pending_at_month_start = int(tds2[1].text.strip())
    sql_obj.corporates_received_during_month = int(tds2[2].text.strip())
    sql_obj.corporates_resolved_during_month = int(tds2[3].text.strip())
    sql_obj.corporates_pending_at_month_end = int(tds2[4].text.strip())
    sql_obj.non_corporates_pending_at_month_start = int(tds3[1].text.strip())
    sql_obj.non_corporates_received_during_month = int(tds3[2].text.strip())
    sql_obj.non_corporates_resolved_during_month = int(tds3[3].text.strip())
    sql_obj.non_corporates_pending_at_month_end = int(tds3[4].text.strip())
    sql_obj.nri_pending_at_month_start = int(tds4[1].text.strip())
    sql_obj.nri_received_during_month = int(tds4[2].text.strip())
    sql_obj.nri_resolved_during_month = int(tds4[3].text.strip())
    sql_obj.nri_pending_at_month_end = int(tds4[4].text.strip())
    sql_obj.fpi_pending_at_month_start = int(tds5[1].text.strip())
    sql_obj.fpi_received_during_month = int(tds5[2].text.strip())
    sql_obj.fpi_resolved_during_month = int(tds5[3].text.strip())
    sql_obj.fpi_pending_at_month_end = int(tds5[4].text.strip())
    sql_obj.others_pending_at_month_start = int(tds6[1].text.strip())
    sql_obj.others_received_during_month = int(tds6[2].text.strip())
    sql_obj.others_resolved_during_month = int(tds6[3].text.strip())
    sql_obj.others_pending_at_month_end = int(tds6[4].text.strip())
    sql_obj.created_at = datetime.now()

    db_session.add(sql_obj)
    db_session.commit()

def save_non_discretionary_details(db_session, sebi_code, as_of_date, tables):
    if not len(tables) > 3:
        return
    
    sql_obj = db_session.query(NonDiscretionaryDetails).filter(and_(NonDiscretionaryDetails.sebi_nr == sebi_code, NonDiscretionaryDetails.as_of == as_of_date)).one_or_none()

    if sql_obj:
        return
    
    sql_obj = NonDiscretionaryDetails()
    sql_obj.created_at = datetime.now()
    sql_obj.sebi_nr = sebi_code
    sql_obj.as_of = as_of_date

    flow_table = tables[11]
    flow_body = flow_table.find('tbody')
    flow_row = flow_body.findAll(['tr'])
    flow_tds = flow_row[1].findAll(['td'])

    sql_obj.inflow_month_in_cr = flow_tds[0].text.strip()
    sql_obj.outflow_month_in_cr = flow_tds[1].text.strip()
    sql_obj.net_flow_month_in_cr = flow_tds[2].text.strip()
    sql_obj.inflow_fy_in_cr = flow_tds[3].text.strip()
    sql_obj.outflow_fy_in_cr = flow_tds[4].text.strip()
    sql_obj.net_flow_fy_in_cr = flow_tds[5].text.strip()

    perform_table = tables[13]
    perform_body = perform_table.find('tbody')
    perform_row = perform_body.find('tr')
    if perform_row:
        perform_tds = perform_row.findAll(['th'])
        sql_obj.aum = perform_tds[0].text.strip()
        sql_obj.returns_1_month = perform_tds[1].text.strip()
        sql_obj.returns_1_yr = perform_tds[2].text.strip()
        sql_obj.ptr_1_month = perform_tds[3].text.strip()
        sql_obj.ptr_1_yr = perform_tds[4].text.strip()

    db_session.add(sql_obj)
    db_session.commit()
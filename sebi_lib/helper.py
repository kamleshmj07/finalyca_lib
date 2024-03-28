from datetime import datetime
from .models import AMC, Scheme, AMCTransactions, AMCClients, SchemePerformance, SchemeFlow, SchemeAssetClasses
from sqlalchemy import and_, func
from .utils import to_float

def find_or_create_amc(db_session, amc_name, sebi_nr, register_date, as_of):
    # Converting name into title case
    name = amc_name.strip().upper()

    if "PRIVATE" in name:
        name = name.replace("PRIVATE", "PVT")

    if "PVT." in name:
        name = name.replace("PVT.", "PVT")
    
    if "LIMITED" in name:
        name = name.replace("LIMITED", "LTD")

    if "LTD." in name:
        name = name.replace("LTD.", "LTD")

    if " & " in name:
        name = name.replace(" & ", " AND ")
    
    # Removing any additional whitespaces between words
    name = " ".join(name.split())

    sql_obj = db_session.query(AMC).filter(AMC.sebi_nr == sebi_nr).filter(AMC.is_active == True).one_or_none()

    # Check if AMC name has been changed.
    if sql_obj and sql_obj.name != name: 
        sql_obj.is_active = False
        sql_obj.deactivation_reason = F"AMC name changed from {sql_obj.name} to {name}"
        db_session.commit()
        sql_obj = None

    if not sql_obj:
        sql_obj = AMC()
        sql_obj.name = name
        sql_obj.sebi_nr = sebi_nr
        sql_obj.register_date = register_date
        sql_obj.name_change_date = as_of
        sql_obj.is_active = True
        sql_obj.created_at = datetime.now()
        db_session.add(sql_obj)
        db_session.commit()
    return sql_obj

def find_or_create_scheme(db_session, scheme_name, sebi_nr):
    name = scheme_name.strip().upper()
    
    # Removing any additional whitespaces between words
    name = " ".join(name.split())
    
    sql_obj = db_session.query(Scheme).filter(Scheme.name == name).filter(Scheme.sebi_nr == sebi_nr).one_or_none()
    if not sql_obj:
        sql_obj = Scheme()
        sql_obj.name = name
        sql_obj.sebi_nr = sebi_nr
        sql_obj.created_at = datetime.now()
        db_session.add(sql_obj)
        db_session.commit()
    return sql_obj

def save_amc_transaction(db_session, sebi_code, as_of_date, service_type, row1, row2, row3):
    sql_obj = db_session.query(AMCTransactions).filter(and_(AMCTransactions.sebi_nr == sebi_code, AMCTransactions.as_of == as_of_date, AMCTransactions.service_type==service_type)).one_or_none()
    if sql_obj:
        return

    tds1 = row1.findAll(['td'])
    tds2 = row2.findAll(['td'])
    tds3 = row3.findAll(['td'])

    sql_obj = AMCTransactions()
    sql_obj.sebi_nr = sebi_code
    sql_obj.as_of = as_of_date
    sql_obj.service_type = service_type
    sql_obj.sales_in_month_in_cr = tds1[2].text.strip()
    sql_obj.purchase_in_month_in_cr = tds2[2].text.strip()
    sql_obj.ptr = tds3[2].text.strip()
    sql_obj.created_at = datetime.now()

    db_session.add(sql_obj)
    db_session.commit()

def save_single_client_breakup(db_session, sebi_code, as_of_date, service_type, table):
    sql_client = db_session.query(AMCClients).filter(and_(AMCClients.sebi_nr == sebi_code, AMCClients.as_of == as_of_date, AMCClients.service_type==service_type)).one_or_none()
    if sql_client:
        return
    
    client_body = table.find('tbody')
    client_trs = client_body.findAll(['tr'])
    td_list_0 = client_trs[0].findAll(['td'])
    td_list_1 = client_trs[1].findAll(['td'])

    sql_client = AMCClients()
    sql_client.sebi_nr = sebi_code
    sql_client.as_of = as_of_date
    sql_client.service_type = service_type
    sql_client.pf_clients = td_list_0[1].text.strip()
    sql_client.corporates_clients = td_list_0[2].text.strip()
    sql_client.non_corporates_clients = td_list_0[3].text.strip()
    sql_client.nri_clients = td_list_0[4].text.strip()
    sql_client.fpi_clients = td_list_0[5].text.strip()
    sql_client.others_clients = td_list_0[6].text.strip()
    sql_client.pf_aum = td_list_1[1].text.strip()
    sql_client.corporates_aum = td_list_1[2].text.strip()
    sql_client.non_corporates_aum = td_list_1[3].text.strip()
    sql_client.nri_aum = td_list_1[4].text.strip()
    sql_client.fpi_aum = td_list_1[5].text.strip()
    sql_client.others_aum = td_list_1[6].text.strip()
    sql_client.created_at = datetime.now()

    db_session.add(sql_client)
    db_session.commit()

def save_scheme_flow(db_session, sebi_code, as_of_date, row):
    tds = row.findAll(['td'])
    
    scheme_name = tds[0].text.strip()
    # Do not save schemes without name
    if not scheme_name:
        return

    # Do not save Total of all schemes
    if scheme_name == "Total":
        return

    sql_scheme = find_or_create_scheme(db_session, scheme_name, sebi_code)
    scheme_id = sql_scheme.id

    sql_obj = db_session.query(SchemeFlow).filter(and_(SchemeFlow.scheme_id == scheme_id, SchemeFlow.as_of == as_of_date)).one_or_none()
    is_create = False
    if not sql_obj:
        sql_obj = SchemeFlow()
        sql_obj.created_at = datetime.now()
        is_create = True
            
    sql_obj.scheme_id = scheme_id
    sql_obj.as_of = as_of_date
    sql_obj.inflow_month_in_cr = tds[1].text.strip() 
    sql_obj.outflow_month_in_cr = tds[2].text.strip() 
    sql_obj.net_flow_month_in_cr = tds[3].text.strip() 
    sql_obj.inflow_fy_in_cr = tds[4].text.strip() 
    sql_obj.outflow_fy_in_cr = tds[5].text.strip() 
    sql_obj.net_flow_fy_in_cr = tds[6].text.strip() 

    if is_create:
        db_session.add(sql_obj)
    db_session.commit()

def save_scheme_performance(db_session, sebi_code, as_of_date, row1, row2):
    tds_1 = row1.findAll(['td'])
    tds_2 = row2.findAll(['td'])

    scheme_name = tds_1[0].text.strip()
    # Do not save schemes without name
    if not scheme_name:
        return
        
    # Do not save Total of all schemes
    if scheme_name == "Total":
        return

    sql_scheme = find_or_create_scheme(db_session, scheme_name, sebi_code)
    scheme_id = sql_scheme.id

    sql_obj = db_session.query(SchemePerformance).filter(and_(SchemePerformance.scheme_id == scheme_id, SchemePerformance.as_of == as_of_date)).one_or_none()
    is_create = False
    if not sql_obj:
        sql_obj = SchemePerformance()
        sql_obj.created_at = datetime.now()
        is_create = True
            
    sql_obj.scheme_id = scheme_id
    sql_obj.as_of = as_of_date
    sql_obj.benchmark_name = tds_2[0].text.strip()
    sql_obj.aum = to_float(tds_1[1])
    sql_obj.scheme_returns_1_month = to_float(tds_1[2])
    sql_obj.scheme_returns_1_yr = to_float(tds_1[3])
    sql_obj.benchmark_returns_1_month = to_float(tds_2[2])
    sql_obj.benchmark_returns_1_yr = to_float(tds_2[3])
    sql_obj.ptr_1_month = to_float(tds_1[4])
    sql_obj.ptr_1_yr = to_float(tds_1[5])

    if is_create:
        db_session.add(sql_obj)
    db_session.commit()

def save_scheme_asset_breakup(db_session, sebi_code, as_of_date, row):
    tds = row.findAll(['th'])

    scheme_name = tds[0].text.strip()
    # Do not save schemes without name
    if not scheme_name:
        return
    # Do not save Total of all schemes
    if scheme_name == "Total":
        return

    sql_scheme = find_or_create_scheme(db_session, scheme_name, sebi_code)
    scheme_id = sql_scheme.id

    sql_service = db_session.query(SchemeAssetClasses).filter(and_(SchemeAssetClasses.scheme_id == scheme_id, SchemeAssetClasses.as_of == as_of_date)).one_or_none()
    is_create = False
    if not sql_service:
        sql_service = SchemeAssetClasses()
        sql_service.created_at = datetime.now()
        is_create = True
            
    sql_service.scheme_id = scheme_id
    sql_service.as_of = as_of_date
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
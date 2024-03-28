from datetime import datetime, timedelta
import json
from re import I
from flask import Blueprint, current_app, jsonify, request
from sqlalchemy import desc, or_
from werkzeug.exceptions import BadRequest
from .models import *
import pandas as pd
import numpy as np

api_bp = Blueprint("api_bp", __name__)

@api_bp.route('/website/<sebi_nr>')
def get_amc_trend(sebi_nr):
    as_of = list()
    aum = list()
    clients = list()
    inflows = list()
    outflows = list()

    results = dict()

    sql_amc_info = current_app.db.query(AMCInfo).filter(AMCInfo.sebi_nr==sebi_nr).order_by(desc(AMCInfo.as_of)).limit(1).one()
    results["sebi_nr"] = sebi_nr
    results["as_on"] = sql_amc_info.as_of
    results["address"] = sql_amc_info.address
    results["total_client"] = sql_amc_info.total_client
    results["total_aum_in_cr"] = sql_amc_info.total_aum_in_cr
    results["compliance_officer_name"] = sql_amc_info.compliance_officer_name
    results["compliance_officer_email"] = sql_amc_info.compliance_officer_email

    subq = current_app.db.query(
        AMCInfo.as_of.label('as_of'), 
        AMCInfo.total_aum_in_cr.label('total_aum_in_cr'), 
        AMCInfo.total_client.label('total_client'), 
        ( DiscretionaryDetails.inflow_month_in_cr + NonDiscretionaryDetails.inflow_month_in_cr ).label('inflow_month_in_cr'), 
        ( DiscretionaryDetails.outflow_month_in_cr + NonDiscretionaryDetails.outflow_month_in_cr ).label('outflow_month_in_cr')
        ).join(
            DiscretionaryDetails, 
            (AMCInfo.as_of==DiscretionaryDetails.as_of) & (AMCInfo.sebi_nr==DiscretionaryDetails.sebi_nr)
        ).join(
            NonDiscretionaryDetails, (AMCInfo.as_of==NonDiscretionaryDetails.as_of) & (AMCInfo.sebi_nr==NonDiscretionaryDetails.sebi_nr)
        ).filter(AMCInfo.sebi_nr==sebi_nr).order_by(
            AMCInfo.as_of.desc()
        ).limit(12).subquery()

    sql_obj = current_app.db.query(subq).order_by(subq.c.as_of).all()

    for sql_o in sql_obj:
        as_of.append(prettify_date(sql_o[0]))
        aum.append(prettify_float(sql_o[1]))
        clients.append(sql_o[2])
        inflows.append(prettify_float(sql_o[3]))
        outflows.append(prettify_float(sql_o[4]))

    results["as_of"] = as_of
    results["aum"] = aum
    results["clients"] = clients
    results["inflows"] = inflows
    results["outflows"] = outflows
    return jsonify(results)

@api_bp.route('/web/amc_list')
def get_amc():
    resp = list()

    amc_sql = current_app.db.query(AMC).join(Scheme, AMC.sebi_nr == Scheme.sebi_nr).filter(AMC.is_active == 1).all()
    for sql_obj in amc_sql:
        json_obj = dict()
        json_obj["id"] = sql_obj.id
        json_obj["sebi_nr"] = sql_obj.sebi_nr
        json_obj["name"] = sql_obj.name
        resp.append(json_obj)

    return jsonify(resp)

@api_bp.route('/web/amc_details/<sebi_nr>')
def get_schemes(sebi_nr):
    obj = dict()

    sql_amc_info = current_app.db.query(AMCInfo).filter(AMCInfo.sebi_nr==sebi_nr).order_by(desc(AMCInfo.as_of)).limit(1).one()
    obj["sebi_nr"] = sebi_nr
    as_of = obj["as_on"] = sql_amc_info.as_of
    obj["address"] = sql_amc_info.address
    obj["total_client"] = sql_amc_info.total_client
    obj["total_aum_in_cr"] = sql_amc_info.total_aum_in_cr
    obj["compliance_officer_name"] = sql_amc_info.compliance_officer_name
    obj["compliance_officer_email"] = sql_amc_info.compliance_officer_email

    obj["is_discretionary"] = sql_amc_info.is_discretionary_active
    obj["is_non_discretionary"] = sql_amc_info.is_non_discretionary_active
    obj["is_advisory"] = sql_amc_info.is_advisory_active

    sql_discretionary = current_app.db.query(AMCTransactions).filter(AMCTransactions.sebi_nr==sebi_nr).filter(AMCTransactions.service_type==ServiceType.discretionary.name).order_by(desc(AMCTransactions.as_of)).limit(1).one_or_none()
    discretionary = dict()
    discretionary["name"] = ServiceType.discretionary.value
    discretionary["is_active"] = sql_amc_info.is_discretionary_active
    discretionary["sales_in_month_in_cr"] = sql_discretionary.sales_in_month_in_cr if sql_discretionary else None
    discretionary["purchase_in_month_in_cr"] = sql_discretionary.purchase_in_month_in_cr if sql_discretionary else None
    sql_discretionary_details = current_app.db.query(DiscretionaryDetails).filter(DiscretionaryDetails.sebi_nr==sebi_nr).order_by(desc(DiscretionaryDetails.as_of)).limit(1).one_or_none()
    discretionary["inflow_month_in_cr"] = sql_discretionary_details.inflow_month_in_cr if sql_discretionary_details else None
    discretionary["outflow_month_in_cr"] = sql_discretionary_details.outflow_month_in_cr if sql_discretionary_details else None
    discretionary["net_flow_month_in_cr"] = sql_discretionary_details.net_flow_month_in_cr if sql_discretionary_details else None

    sql_non_discretionary = current_app.db.query(AMCTransactions).filter(AMCTransactions.sebi_nr==sebi_nr).filter(AMCTransactions.service_type==ServiceType.non_discretionary.name).order_by(desc(AMCTransactions.as_of)).limit(1).one_or_none()
    non_discretionary = dict()
    non_discretionary["name"] = ServiceType.non_discretionary.value
    non_discretionary["is_active"] = sql_amc_info.is_non_discretionary_active
    non_discretionary["sales_in_month_in_cr"] = sql_non_discretionary.sales_in_month_in_cr if sql_non_discretionary else None
    non_discretionary["purchase_in_month_in_cr"] = sql_non_discretionary.purchase_in_month_in_cr if sql_non_discretionary else None
    sql_non_discretionary_details = current_app.db.query(NonDiscretionaryDetails).filter(NonDiscretionaryDetails.sebi_nr==sebi_nr).order_by(desc(NonDiscretionaryDetails.as_of)).limit(1).one_or_none()
    non_discretionary["inflow_month_in_cr"] = sql_non_discretionary_details.inflow_month_in_cr if sql_non_discretionary_details else None
    non_discretionary["outflow_month_in_cr"] = sql_non_discretionary_details.outflow_month_in_cr if sql_non_discretionary_details else None
    non_discretionary["net_flow_month_in_cr"] = sql_non_discretionary_details.net_flow_month_in_cr if sql_non_discretionary_details else None

    sql_advisory = current_app.db.query(AMCTransactions).filter(AMCTransactions.sebi_nr==sebi_nr).filter(AMCTransactions.service_type==ServiceType.non_discretionary.name).order_by(desc(AMCTransactions.as_of)).limit(1).one_or_none()
    advisory = dict()
    advisory["name"] = ServiceType.advisory.value
    advisory["is_active"] = sql_amc_info.is_advisory_active
    advisory["sales_in_month_in_cr"] = sql_advisory.sales_in_month_in_cr if sql_advisory else None
    advisory["purchase_in_month_in_cr"] = sql_advisory.purchase_in_month_in_cr if sql_advisory else None
    advisory["inflow_month_in_cr"] = None
    advisory["outflow_month_in_cr"] = None
    advisory["net_flow_month_in_cr"] = None

    obj["services"] = [discretionary, non_discretionary, advisory]

    schemes = list()
    sql_schemes = current_app.db.query(SchemePerformance).join(Scheme).filter(SchemePerformance.as_of==as_of).filter(Scheme.sebi_nr==sebi_nr).all()
    for sql_scheme in sql_schemes:
        sql_obj = dict()
        sql_obj["name"] = sql_scheme.scheme.name
        sql_obj["benchmark"] = sql_scheme.benchmark_name
        sql_obj["aum"] = sql_scheme.aum
        sql_obj["scheme_returns_1_month"] = sql_scheme.scheme_returns_1_month
        sql_obj["scheme_returns_1_yr"] = sql_scheme.scheme_returns_1_yr
        sql_obj["benchmark_returns_1_month"] = sql_scheme.benchmark_returns_1_month
        sql_obj["benchmark_returns_1_yr"] = sql_scheme.benchmark_returns_1_yr
        sql_obj["ptr_1_month"] = sql_scheme.ptr_1_month
        sql_obj["ptr_1_yr"] = sql_scheme.ptr_1_yr
        schemes.append(sql_obj)

    obj["schemes"] = schemes
    
    return jsonify(obj)

def prettify_date(dt: datetime):
    return dt.strftime('%d %b %Y') if dt else None

def prettify_float(obj):
    return float(round(obj, 2)) if obj else None

@api_bp.route('/web/<sebi_nr>/scheme_performance/<field_name>')
def get_schemes_trend(sebi_nr, field_name):
    sql_schemes = current_app.db.query(Scheme).filter(Scheme.sebi_nr==sebi_nr).filter(Scheme.is_active==True).all()
    
    dataframes = list()
    final = pd.DataFrame()
    for sql_scheme in sql_schemes:
        scheme_id = sql_scheme.id
        scheme_name = sql_scheme.name
        # get last 12 values for trend
        sql_objs = current_app.db.query(SchemePerformance).filter(SchemePerformance.scheme_id==scheme_id).order_by(SchemePerformance.as_of.desc()).limit(12).all()

        results = list()
        for sql_obj in sql_objs:
            d = dict()
            d["as_of"] = prettify_date(sql_obj.as_of)
            d[scheme_name] = getattr(sql_obj, field_name)
            results.append(d)
            
        df = pd.DataFrame(results)
        # df.set_index('as_of', inplace=True)
        dataframes.append(df)
        if final.empty:
            final = df
        else:
            final = final.merge(df, how='outer', on='as_of')

    final = final.replace({np.nan: None})
    jStr = final.to_json(orient='columns')
    new_obj = json.loads(jStr)
    for col, value in new_obj.items():
        new_obj[col] = list(value.values())

    return jsonify(new_obj)
    

@api_bp.route('/web/<sebi_nr>/clients')
def get_amc_clients(sebi_nr):
    # get last 12 values for trend
    sql_objs = current_app.db.query(AMCClients).filter(AMCClients.sebi_nr==sebi_nr).filter(AMCClients.service_type==ServiceType.total.name).order_by(AMCClients.as_of.desc()).limit(12).all()

    dates = list()
    pf = list()
    corp = list()
    non_corp = list()
    nri = list()
    fpi = list()
    others = list()

    for sql_obj in sql_objs:
        dates.append(prettify_date(sql_obj.as_of))
        pf.append(sql_obj.pf_clients)
        corp.append(sql_obj.corporates_clients)
        non_corp.append(sql_obj.non_corporates_clients)
        nri.append(sql_obj.nri_clients)
        fpi.append(sql_obj.fpi_clients)
        others.append(sql_obj.others_clients)

    results = dict()
    results["as_of"] = dates
    results["pf_clients"] = pf
    results["corporates_clients"] = corp
    results["non_corporates_clients"] = non_corp
    results["nri_clients"] = nri
    results["fpi_clients"] = fpi
    results["others_clients"] = others
    return jsonify(results)
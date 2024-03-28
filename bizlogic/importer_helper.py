import os
import csv
import requests
import statistics
import pandas as pd
import numpy as np
import logging
import xml.etree.ElementTree as ET
from math import trunc
from operator import and_, or_
from time import strptime
from datetime import date, datetime, timedelta
from datetime import datetime as dt1
import sqlalchemy
from werkzeug.exceptions import BadRequest
from sqlalchemy import desc, extract, func, or_, case
from sqlalchemy.sql.expression import cast
from sqlalchemy.orm import aliased
from dateutil.relativedelta import relativedelta

from fin_models.transaction_models import *
from fin_models.controller_master_models import Organization, User
from fin_models.masters_models import *
from utils.utils import print_query
from pandas.tseries.offsets import BDay
from fin_models.servicemanager_models import DeliveryRequest
from bizlogic.common_helper import get_fund_category, get_funds_in_same_category, get_last_transactiondate,\
                                    get_max_navdate_tilldate, get_navbydate
from data.holdings import get_fund_underlying_holdings
from analytics.analytics import generate_portfolio_characteristics
from utils.time_func import last_date_of_month

def save_nav(db_session, plan_id, nav_type, nav_date, nav, user_id, dt, raw_nav=None):
    remark = ""
    sql_obj = db_session.query(NAV).filter(NAV.Plan_Id == plan_id).filter(NAV.NAV_Date==nav_date).filter(NAV.NAV_Type==nav_type).filter(NAV.Is_Deleted != 1).one_or_none()
    if sql_obj:
        if sql_obj.is_locked != 1:
            sql_obj.NAV = nav
            sql_obj.RAW_NAV = raw_nav
            sql_obj.Updated_By = user_id
            sql_obj.Updated_Date = dt
            sql_obj.Is_Deleted = 0
            db_session.commit()
            if nav_type == "I":
                remark = "Closing Prices for Benchmark Indices uploaded Successfully."
            else:
                remark = "NAV updated successfully."
        else:            
            remark = "NAV for this period is locked. Cannot update."
    else:
        sql_obj = NAV()
        sql_obj.Plan_Id = plan_id
        sql_obj.NAV_Date = nav_date
        sql_obj.NAV_PortfolioReturn = None
        sql_obj.Is_Deleted = 0
        sql_obj.NAV_Type = nav_type
        sql_obj.NAV = nav
        sql_obj.RAW_NAV = raw_nav
        sql_obj.Created_By = user_id
        sql_obj.Created_Date = dt
        db_session.add(sql_obj)
        db_session.commit()

        if nav_type == "I":
            remark = "Closing Prices for Benchmark Indices updated Successfully."
        else:
            remark = "NAV updated successfully."

    return remark

def save_planproductmapping(db_session, plan_id, product_id, user_id, dt):
    remark = ""
    sql_planproductmapping = db_session.query(PlanProductMapping).filter(PlanProductMapping.Plan_Id == plan_id).filter(PlanProductMapping.Is_Deleted != 1).one_or_none()

    if not sql_planproductmapping:
        sql_planproductmapping = PlanProductMapping()
        sql_planproductmapping.Plan_Id = plan_id
        sql_planproductmapping.Product_Id = product_id
        sql_planproductmapping.Is_Deleted = 0
        sql_planproductmapping.Created_By = user_id
        sql_planproductmapping.Created_Date = dt

        db_session.add(sql_planproductmapping)
        db_session.commit()

        remark = "Plan product mapping successful."
    return remark

def save_benchmarkindices(db_session, benchmarkindices_name, benchmarkindices_code, benchmarkindices_description, co_code, short_name, long_name,tri_co_code, bse_code, nse_symbol, bse_groupname, attribution_flag, user_id, dt):
    remark = ""
    insert_benchmark = db_session.query(BenchmarkIndices).filter(BenchmarkIndices.Co_Code == co_code).filter(BenchmarkIndices.TRI_Co_Code == None).filter(BenchmarkIndices.Is_Deleted != 1).all()

    if not insert_benchmark:
        insert_benchmark = BenchmarkIndices()
        insert_benchmark.BenchmarkIndices_Name = benchmarkindices_name
        insert_benchmark.BenchmarkIndices_Code = benchmarkindices_code
        insert_benchmark.BenchmarkIndices_Description = benchmarkindices_description
        insert_benchmark.Is_Deleted = 0
        insert_benchmark.Created_By = user_id
        insert_benchmark.Created_Date = dt
        insert_benchmark.Updated_By = None
        insert_benchmark.Updated_Date = None
        insert_benchmark.Co_Code = co_code
        insert_benchmark.Short_Name = short_name
        insert_benchmark.Long_Name = long_name
        insert_benchmark.TRI_Co_Code = tri_co_code
        insert_benchmark.BSE_Code = bse_code
        insert_benchmark.NSE_Symbol = nse_symbol
        insert_benchmark.BSE_GroupName = bse_groupname
        insert_benchmark.Attribution_Flag = attribution_flag

        db_session.add(insert_benchmark)
        db_session.commit()
        remark = "New Benchmarks uploaded successfully."

    return remark

def save_closingvalues(db_session, bse_code, isin_code, date, st_exchng, co_code, high, low, open, close, tdcloindi, volumne, no_trades, net_turnov, user_id, dt):
    remark = ""
    
    sql_closingvalues = db_session.query(ClosingValues).filter(ClosingValues.Co_Code == co_code).filter(ClosingValues.Date_ == date).filter(ClosingValues.ST_EXCHNG == st_exchng).filter(ClosingValues.Is_Deleted != 1).one_or_none()
    if not sql_closingvalues:
        sql_closingvalues = ClosingValues()
        sql_closingvalues.BSE_Code = bse_code
        sql_closingvalues.ISIN_Code = isin_code
        sql_closingvalues.Date_ = date
        sql_closingvalues.ST_EXCHNG = st_exchng
        sql_closingvalues.Co_Code = co_code
        sql_closingvalues.HIGH = high
        sql_closingvalues.LOW = low
        sql_closingvalues.OPEN = open
        sql_closingvalues.CLOSE = close
        sql_closingvalues.TDCLOINDI = tdcloindi
        sql_closingvalues.VOLUME = volumne
        sql_closingvalues.NO_TRADES = no_trades
        sql_closingvalues.NET_TURNOV = net_turnov
        sql_closingvalues.Is_Deleted = 0
        sql_closingvalues.Created_By = user_id
        sql_closingvalues.Created_Date = dt
        sql_closingvalues.Updated_By = None
        sql_closingvalues.Updated_Date = None
        
        db_session.add(sql_closingvalues)
        db_session.commit()
        remark = 'Closing Prices for Securities Uploaded Successfully.'
    else:
        sql_closingvalues.BSE_Code = bse_code
        sql_closingvalues.ISIN_Code = isin_code
        sql_closingvalues.HIGH = high
        sql_closingvalues.LOW = low
        sql_closingvalues.OPEN = open
        sql_closingvalues.CLOSE = close
        sql_closingvalues.TDCLOINDI = tdcloindi
        sql_closingvalues.VOLUME = volumne
        sql_closingvalues.NO_TRADES = no_trades
        sql_closingvalues.NET_TURNOV = net_turnov
        sql_closingvalues.Updated_By = user_id
        sql_closingvalues.Updated_Date = dt
        
        db_session.commit()
        remark = 'Closing Prices for Securities Updated Successfully.'
    return remark


def save_holdingsecurity(db_session, isin, holdingsecurity_type, bse_code, bse_groupname, co_code, nse_symbol, short_companyname, sub_sectorname, user_id, dt):
    sql_holdingsecurity = db_session.query(HoldingSecurity).filter(HoldingSecurity.ISIN_Code == isin).filter(HoldingSecurity.HoldingSecurity_Type == holdingsecurity_type).filter(HoldingSecurity.Is_Deleted != 1).order_by(desc(HoldingSecurity.HoldingSecurity_Id)).first()

    if not sql_holdingsecurity:
        remark = "Secuirities not found in Security Master."

    else:        
        sql_holdingsecurity.BSE_Code = bse_code
        sql_holdingsecurity.BSE_GroupName = bse_groupname
        sql_holdingsecurity.Co_Code = co_code
        sql_holdingsecurity.NSE_Symbol = nse_symbol
        sql_holdingsecurity.Short_CompanyName = short_companyname
        sql_holdingsecurity.Sub_SectorName = sub_sectorname
        sql_holdingsecurity.Updated_By = user_id
        sql_holdingsecurity.Updated_Date = dt
            
        db_session.commit()
        remark = "Security Master Updated Successfully."
    return remark


# TODO Remove this function as it will be deprecated after CMOTS Migration release
def save_sector(db_session, sector_code, sector_name, user_id, dt):
    remark = ""
    sql_sector = db_session.query(Sector).filter(Sector.Sector_Code == sector_code, Sector.Is_Deleted != 1).one_or_none()
    if not sql_sector and sector_code != "":
        sql_sector = Sector()
        sql_sector.Sector_Name = sector_name
        sql_sector.Sector_Code = sector_code
        sql_sector.Is_Deleted = 0
        sql_sector.Created_By = user_id
        sql_sector.Created_Date = dt

        db_session.add(sql_sector)
        db_session.commit()
        remark = "Sector uploaded successfully."
    return remark

# TODO : Remove this function call, as this will be deprecated after sector synchronization
def lookup_vrsector_using_amfi_sector(db_session, sector_code, row, isin_code=None):
    sector_info = None
    cm_sector_code = sector_code.lstrip('0') if sector_code else ""

    # if there is no sector, then assign sector based on instrument_type
    # for soveriegn securities specially in ulip holdings identify the sector based on security_name pattern
    # if still no sector then mark as Others
    if not sector_code or cm_sector_code == '39':
        security_name = row["Company_SecurityName"]
        security_asset_class = row["Security_AssetClass"]
        if security_asset_class == 'T Bills' or security_asset_class == 'Govt. Securities':
            sector_info = (8, 17, "Sovereign")
        elif "%" in security_name and ("GOI" in security_name or "Trep" in security_name or "TBill" in security_name):
            sector_info = (8, 17, "Sovereign")
        elif "INE" in isin_code or isin_code == "":
            sector_info = (6, 14, "Others")
        elif isin_code[2:].isdigit():
            sector_info = (8, 17, "Sovereign")
        else:
            sector_info = (None, None, None)

        return sector_info

    sql_amfisector = db_session.query(Industry_Classification)\
                               .filter(or_(Industry_Classification.Ind_Code == sector_code,Industry_Classification.CM_Sector_Code == cm_sector_code), Industry_Classification.Is_Deleted != 1).first()

    if sql_amfisector:
        vr_sector = db_session.query(Sector).filter(Sector.Sector_Id == sql_amfisector.Sector_Id, Sector.Is_Deleted != 1).one_or_none()

        if vr_sector:
            sector_info = (vr_sector.Sector_Id, vr_sector.Sector_Code, vr_sector.Sector_Name)
    else:
        sector_info = (6, 14, "Others")

    return sector_info


def lookup_sebi_sector_using_sebi_industry(db_session, sector_code, row, isin_code=None):
    sector_info = None
    cm_sector_code = sector_code.lstrip('0') if sector_code else ""
    sov_sector_info = (23, 'Sov_01', 'Sovereign', None)
    others_sector_info = (24, 'Others_01', 'Others', None)

    # if there is no sector, then assign sector based on instrument_type
    # for soveriegn securities specially in ulip holdings identify the sector based on security_name pattern
    # if still no sector then mark as Others
    if not sector_code or cm_sector_code == '39':
        security_name = row.get("Company_SecurityName") or row.get("CompanyName")
        security_asset_class = row.get("Security_AssetClass")
        if security_asset_class == 'T Bills' or security_asset_class == 'Govt. Securities':
            sector_info = sov_sector_info
        elif "%" in security_name and ("GOI" in security_name or "Trep" in security_name or "TBill" in security_name):
            sector_info = sov_sector_info
        elif "INE" in isin_code or isin_code == "":
            sector_info = others_sector_info
        elif isin_code[2:].isdigit():
            sector_info = sov_sector_info
        else:
            sector_info = (None, None, None, None)

        return sector_info

    sql_sebisector = db_session.query(Industry_Classification)\
                               .filter(or_(Industry_Classification.Ind_Code == sector_code,Industry_Classification.CM_Sector_Code == cm_sector_code), 
                                       Industry_Classification.Is_Deleted != 1).first()

    if sql_sebisector:
        sebi_sector = db_session.query(Sector).filter(Sector.Sector_Code == sql_sebisector.Sect_Code, Sector.Is_Deleted != 1).one_or_none()

        if sebi_sector:
            sector_info = (sebi_sector.Sector_Id, sebi_sector.Sector_Code, sebi_sector.Sector_Name, sql_sebisector.Industry)
    else:
        sector_info = others_sector_info

    return sector_info


def save_issuer(db_session, issuer_code, issuer_name, user_id, dt, commit_flag=True):
    remark = ""
    issuer_id = None

    if not issuer_code or issuer_code == "":
        raise Exception("No Issuer Code provided.")

    sql_issuer = db_session.query(Issuer).filter(Issuer.Issuer_Code == issuer_code, Issuer.Is_Deleted != 1).one_or_none()

    if not sql_issuer:
        sql_issuer = Issuer()
        sql_issuer.Issuer_Name = issuer_name
        sql_issuer.Issuer_Code = issuer_code
        sql_issuer.Is_Deleted = 0
        sql_issuer.Created_By = user_id
        sql_issuer.Created_Date = dt
        db_session.add(sql_issuer)
        db_session.flush()
        if commit_flag:
            db_session.commit()

        remark = "Issuer uploaded successfully."
    
    issuer_id = sql_issuer.Issuer_Id

    return (issuer_id, remark)


def save_assetclass(db_session, assetclass_name, assetclass_description, user_id, dt):
    remark = ""
    sql_assetclass = db_session.query(AssetClass).filter(AssetClass.AssetClass_Name == assetclass_name).filter(AssetClass.Is_Deleted != 1).one_or_none()
 
    if not sql_assetclass:
        sql_assetclass = AssetClass()
        sql_assetclass.AssetClass_Name = assetclass_name
        sql_assetclass.AssetClass_Description = assetclass_description
        sql_assetclass.Is_Deleted = 0
        sql_assetclass.Created_Date = dt
        sql_assetclass.Updated_By = user_id
        sql_assetclass.Updated_Date = None

        db_session.add(sql_assetclass)
        db_session.commit()
        remark = "Assetclass uploaded successfully."
    return remark

def save_classification(db_session, classification_name, classification_code, user_id, dt, asset_class_id):
    remark = ""
    sql_classification = db_session.query(Classification).filter(Classification.Classification_Name == classification_name).filter(Classification.Is_Deleted != 1).one_or_none()

    if not sql_classification:
        sql_classification = Classification()
        sql_classification.Classification_Name = classification_name
        sql_classification.Classification_Code = classification_code
        sql_classification.Is_Deleted = 0
        sql_classification.Created_By = user_id
        sql_classification.Created_Date = dt
        sql_classification.AssetClass_Id = asset_class_id

        db_session.add(sql_classification)
        db_session.commit()
        remark = "Classification uploaded successfully."
    return remark
 
def get_csvreader(filepath, file):
    csvreader = None
    with open(get_rel_path(filepath, file), 'r') as f:
        csvreader = csv.DictReader(f)
    
    return csvreader

def get_rel_path(file_path, file=None):
    if not file:
        file = file_path
    return os.path.join(
        os.path.dirname(os.path.abspath(file)),
        file_path
    )

def write_csv(readpath, header, items, file, is_pipeseparated=False):
    with open(get_rel_path(readpath, file), 'w') as f:
        if is_pipeseparated:
            csvwriter = csv.writer(f, lineterminator="\n")
            csvwriter.writerows(items)
        else:
            csvwriter = csv.DictWriter(f, fieldnames=header, lineterminator="\n")
            csvwriter.writeheader() 
            csvwriter.writerows(items)

def get_plan_codefornav(scheme_code, product_id):
    plan_code = ""
    if product_id == 2:
        plan_code = F"INS_{scheme_code}_01"
    elif product_id == 4:
        plan_code = F"{scheme_code}_01"
    elif product_id == 5:
        plan_code = F"{scheme_code}_01"
    elif product_id == 1:
        plan_code = scheme_code
    else:
        raise Exception("Unknown product id")
    return plan_code

def get_schemecode_factsheet(schemecode, productid):
    schemecode1 = "" 
    if productid == 4 or productid == 5:
        schemecode1 = F"{schemecode}_01"
    elif productid == 2:
        schemecode1 = F"INS_{schemecode}_01"
    else:
        schemecode1 = schemecode
    return schemecode1

def fundmanager_upload(db_session, fundid, fundmanagercode, fundname, fundmanagername, experience, qualification, designation, datefrom, dateto, update_fm_basic_info=False):
    remark = ""
    if update_fm_basic_info:
        update_values = {
            FundManager.FundManager_Designation : designation,
            FundManager.FundManager_Description : F'Qualification: {qualification}. Experience:{experience}'
            }
        
        sql_fund = db_session.query(FundManager).filter(FundManager.FundManager_Code == fundmanagercode).update(update_values)                    
        db_session.commit()

    else:
        sql_fund = db_session.query(Fund.Fund_Id, FundManager.FundManager_Id).join(FundManager,Fund.Fund_Id == FundManager.Fund_Id).filter(Fund.Fund_Id == fundid).filter(FundManager.FundManager_Code == fundmanagercode).filter(FundManager.Is_Deleted != 1).filter(Fund.Is_Deleted != 1).one_or_none()

        if sql_fund:            
            update_values = {
                FundManager.Fund_Id : sql_fund.Fund_Id,
                FundManager.Is_Deleted : 0,
                FundManager.DateFrom : datefrom,
                FundManager.DateTo : dateto
            }
            db_session.query(FundManager).filter(FundManager.FundManager_Id == sql_fund.FundManager_Id).update(update_values)
            db_session.commit()
            remark = "Fund Managers details succesfully updated in system."
            
        else:
            sql_fundmanager = FundManager()
            sql_fundmanager.FundManager_Name = fundmanagername
            sql_fundmanager.FundManager_Code = fundmanagercode
            sql_fundmanager.FundManager_Description = F'Qualification: {qualification}. Experience:{experience}'
            sql_fundmanager.Fund_Id = fundid
            sql_fundmanager.Is_Deleted = 0
            sql_fundmanager.FundManager_Designation = designation
            sql_fundmanager.DateFrom = datefrom
            sql_fundmanager.DateTo = dateto     
            sql_fundmanager.AUM = 0
            sql_fundmanager.Funds_Managed = 0
            db_session.add(sql_fundmanager)
            db_session.commit()
            remark = "Fund Managers successfully uploaded in system."

    return remark
    
def get_plan_id(db_session, plan_code, plan_name, scheme_id, scheme_code, scheme_name, **kwargs):
    plan_id = None

    isin = kwargs.get("isin")
    amfi_code = kwargs.get("amfi_code")

    obj = dict()
    query = db_session.query(Plans.Plan_Id,
                             Plans.Plan_Name,
                             Plans.Plan_Code,
                             Plans.ISIN,
                             Plans.AMFI_Code,
                             Fund.Fund_Id,
                             Fund.Fund_Name,
                             Fund.Fund_Code,
                             MFSecurity.AMC_Id,
                             MFSecurity.Classification_Id).join(MFSecurity, and_(MFSecurity.MF_Security_Id==Plans.MF_Security_Id, MFSecurity.Is_Deleted != 1)) \
                                                          .join(Options, Options.Option_Id == Plans.Option_Id) \
                                                          .join(Fund, Fund.Fund_Id==MFSecurity.Fund_Id) \
                                                          .filter(Fund.Is_Deleted!= 1).filter(Plans.Is_Deleted != 1)
    
    if plan_code:
        query = query.filter(Plans.Plan_Code == plan_code)
    elif plan_name:
        query = query.filter(Plans.Plan_Name == plan_name)
    elif scheme_id:
        query = query.filter(Fund.Fund_Id == scheme_id).filter(Options.Option_Name.like('%G%')).filter(MFSecurity.Status_Id==1) \
                     .filter(Plans.PlanType_Id == 1) # fetch only regular plans for scheme related filters
    elif scheme_code:
        query = query.filter(Fund.Fund_Code == scheme_code).filter(Options.Option_Name.like('%G%')).filter(MFSecurity.Status_Id==1) \
                     .filter(Plans.PlanType_Id == 1) # fetch only regular plans for scheme related filters
    elif scheme_name:
        query = query.filter(Fund.Fund_Name == scheme_name).filter(Options.Option_Name.like('%G%')).filter(MFSecurity.Status_Id==1) \
                     .filter(Plans.PlanType_Id == 1) # fetch only regular plans for scheme related filters
    elif amfi_code:
        query = query.filter(Plans.AMFI_Code == amfi_code)
    elif isin:
        query = query.filter(or_(Plans.ISIN == isin, Plans.ISIN2 == isin))

    sql_plans = query.first()

    if sql_plans:
        plan_id = sql_plans[0]

    return plan_id


def get_requestdata(request):  
    resp = dict()
    resp["plan_id"] = request.args.get("plan_id", type=str)
    resp["plan_code"] = request.args.get("plan_code", type=str)
    resp["plan_name"] = request.args.get("plan_name", type=str)
    resp["scheme_id"] = request.args.get("scheme_id", type=str)
    resp["scheme_code"] = request.args.get("scheme_code", type=str)
    resp["scheme_name"] = request.args.get("scheme_name", type=str)
    resp["date"] = request.args.get("date", type=str)  
                    
    return resp


def get_bsensecode(db_session, isin):
    bsecode = None
    nsecode = None
    id = None
    name = None
    instrument_type = None
    market_cap = None
    sector_name = None

    sql_security = db_session.query(HoldingSecurity.HoldingSecurity_Id, HoldingSecurity.HoldingSecurity_Name, HoldingSecurity.Instrument_Type, HoldingSecurity.ISIN_Code, HoldingSecurity.BSE_Code, HoldingSecurity.NSE_Symbol, HoldingSecurity.MarketCap, Sector.Sector_Name).join(Sector, Sector.Sector_Id == HoldingSecurity.Sector_Id).filter(HoldingSecurity.ISIN_Code == isin).order_by(desc(HoldingSecurity.HoldingSecurity_Id)).all() 

    if sql_security:
        for sql_sec in sql_security:
            if not bsecode:
                bsecode = sql_sec.BSE_Code

            if not nsecode:
                nsecode = sql_sec.NSE_Symbol

            if not market_cap:
                market_cap = sql_sec.MarketCap

            if not instrument_type:
                instrument_type = sql_sec.Instrument_Type

            if not id:
                id = sql_sec.HoldingSecurity_Id

            if not name:
                name = sql_sec.HoldingSecurity_Name

            if not sector_name:
                sector_name = sql_sec.Sector_Name

    resp = dict()
    resp["security_id"] = id
    resp["security_name"] = name
    resp["security_type"] = instrument_type
    resp["security_cap"] = market_cap
    resp["security_isin"] = isin
    resp["security_bse"] = bsecode
    resp["security_nse"] = nsecode
    resp["security_sector"] = sector_name
    
    return resp


def get_fundid_byplanid(db_session, plan_id):
    fund_id = db_session.query(Fund.Fund_Id).join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).filter(Fund.Is_Deleted != 1).filter(Plans.Plan_Id == plan_id).scalar()

    return fund_id


def get_sql_fund_byplanid(db_session, plan_id):
    sql_fund = db_session.query(Fund).join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).filter(Fund.Is_Deleted != 1).filter(Plans.Plan_Id == plan_id).one_or_none()

    return sql_fund


def generate_attributions(from_date, to_date, plan_id, benchmarkindices_id, period, gsquare_url, db_session, write_response_in_db=True):
    # logging.warning('generate_attributions M00 start')
    # add_log(F"generate_attributions start - plan_id={plan_id}, benchmark_id={benchmarkindices_id}, period={period}, from_date={from_date}, to_date={to_date}")
    if gsquare_url:
        # logging.warning('inside generate_attributions M00 start')
        # add_log(F"generate_attributions calling gsquare api directly - plan_id={plan_id}, benchmark_id={benchmarkindices_id}, period={period}, from_date={from_date}, to_date={to_date}")

        dates=list()        
        dates.append(from_date.strftime('%Y-%m-%d'))
        dates.append(to_date.strftime('%Y-%m-%d'))

        f = {
            "fund_id": plan_id,
            "index_id": benchmarkindices_id,
            "period": period,
            "dates": dates,
        }
        r = requests.post(gsquare_url, json=f)

        add_log(F"generate_attributions after request - plan_id={plan_id}, benchmark_id={benchmarkindices_id}, period={period}, from_date={from_date}, to_date={to_date}")
        if r.status_code == 200:
            logging.warning('generate_attributions M00 start - response 200')
            # data = r.json()
            logging.warning('generate_attributions M00 start response - ' + str(r.text))

            
            period_dates = "[\"" + str(from_date.strftime('%Y-%m-%d')) + "\",\""+ str(to_date.strftime('%Y-%m-%d')) +"\"]"

            factsheetattribution = FactsheetAttribution()
            factsheetattribution.Plan_Id = plan_id
            factsheetattribution.BenchmarkIndices_Id = benchmarkindices_id
            factsheetattribution.Period = period
            factsheetattribution.Dates = period_dates
            factsheetattribution.Response_Attribution = str(r.text)
            factsheetattribution.Is_Deleted = 0 
            factsheetattribution.Created_By = 1
            factsheetattribution.Created_Date = dt1.today()

            db_session.add(factsheetattribution)
            db_session.commit()
        else:
            # add_log(F"generate_attributions error - plan_id={plan_id}, benchmark_id={benchmarkindices_id}, period={period}, from_date={from_date}, to_date={to_date}, response = {r}")
            raise Exception(F"generate_attributions error - plan_id={plan_id}, benchmark_id={benchmarkindices_id}, period={period}, from_date={from_date}, to_date={to_date}, response = {r}")
        # factsheetattribution.Response_Attribution = 
        # "[\"" + str(from_date.strftime('%Y-%m-%d')) + "\",\""+ str(to_date.strftime('%Y-%m-%d')) +"\"]"

        #sachin write response in db
        return r
    else:
        f = {
                "RequestStatus": 1,
                "RequestMessage": "Success",
                "RequestAuthorization": {
                    "Token": "py2JQJDF8qP/UeaKjbsVUWRZjIYVQLW+3U54e1uj/Hs=",
                    "Application_Id": 1,
                    "User_Id": 0,
                    "Role_Id": 0,
                    "Controller": "Gsquare",
                    "Method": "M07"
                },
                "RequestObject": {
                    "Plan_Id": plan_id,
                    "BenchmarkIndices_Id": benchmarkindices_id,
                    "Period": period,
                    "Dates": "[\"" + str(from_date.strftime('%Y-%m-%d')) + "\",\""+ str(to_date.strftime('%Y-%m-%d')) +"\"]",
                    "User_Name": "X8tP/yI2/QomqPeXTV0AVw==",
                    "Password": "Iwoh0pJDJv8TTUfxPPTMYg=="
                }
            }
    
        r = requests.post('https://api.finalyca.com/api_layer/Gsquare/M07', json=f)
        return r
    # http://localhost:30011/api_layer/Gsquare/M07
    

def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month


def getbetweendate(months =0, years=0, transactiondate=None, isstartdate=True, ismonthly_factsheet=True):
    yr = transactiondate[0]
    mn = transactiondate[1]
    day = transactiondate[2]
    
    if years>0:
        yr = yr - years
        if mn == 12:
            mn = 1
            yr = yr + 1
        else:
            mn = mn + 1
        
    if months>0:
        if months < mn:
            mn = mn - months
        elif months == mn:
            mn = 12
            yr = yr - 1
        elif months > mn:
            mn = (12 +(mn - months))
            yr = yr - 1

    if isstartdate:
        dt = datetime(yr, mn,1)
    else:
        if ismonthly_factsheet:
            dt = last_date_of_month(yr, mn)
        else:
            dt = datetime(yr, mn, day)
    return dt


def get_fund_holdings(db_session, plan_id, portfolio_date, get_full_holding=False):

    if not portfolio_date:            
        transaction_date = get_last_transactiondate(db_session, plan_id)
        portfolio_date = get_portfolio_date(db_session, plan_id, transaction_date)

    if not portfolio_date:
        return []
    
    mf_security_id = db_session.query(Plans.MF_Security_Id).filter(Plans.Plan_Id == plan_id).scalar()
    fund_id = db_session.query(MFSecurity.Fund_Id).filter(MFSecurity.MF_Security_Id == mf_security_id).scalar()
    fund_qry = db_session.query(Fund.Fund_Id,
                                Fund.Top_Holding_ToBeShown,
                                Fund.HideHoldingWeightage,
                                Fund.AIF_CATEGORY,
                                Fund.AIF_SUB_CATEGORY)\
                        .filter(Fund.Fund_Id == fund_id).first()

    limit = 10
    hide_holding_weightage = False

    if fund_qry:
        limit = fund_qry.Top_Holding_ToBeShown if fund_qry.Top_Holding_ToBeShown else limit
        hide_holding_weightage = fund_qry.HideHoldingWeightage if fund_qry.HideHoldingWeightage else hide_holding_weightage

    if get_full_holding:
        limit = None

    lst_holdings = get_fund_underlying_holdings(db_session,
                                                fund_id=fund_id,
                                                portfolio_date=portfolio_date,
                                                limit=limit)

    resp = []
    if lst_holdings:
        for h in lst_holdings:
            data = dict()
            data["scheme_id"] = fund_id
            data["security_id"] = h.get('HoldingSecurity_Id')
            data["security_name"] = h.get('HoldingSecurity_Name') if h.get('HoldingSecurity_Name') else h.get('Company_Security_Name')
            data["security_sector"] = h.get('Sector_Name')
            data["security_cap"] = h.get('MarketCap')
            data["security_type"] = h.get('HoldingSecurity_Type')
            data["security_isin"] = h.get('ISIN_Code')
            data["security_bse"] = h.get('BSE_Code')
            data["security_nse"] = h.get('NSE_Symbol')
            data["debt_security_id"] = h.get('DebtSecurity_Id')
            data["as_on_date"] = portfolio_date
            if hide_holding_weightage:
                data["security_weight"] = None
                data["value_in_inr"] = None
            else:
                data["security_weight"] = round(h.get('Percentage_to_AUM'),2) if h.get('Percentage_to_AUM') else None
                data["value_in_inr"] = h.get('Value_in_INR')
            
            resp.append(data)

    return resp


def getfundmanager(db_session, plan_id):
    resp = list()
    fund_id = get_fundid_byplanid(db_session, plan_id)  
    sql_fundmanager = db_session.query(FundManager.FundManager_Id, 
                                       FundManager.FundManager_Name, 
                                       FundManager.FundManager_Code, 
                                       FundManager.FundManager_Description, 
                                       FundManager.Fund_Id, 
                                       Fund.Fund_Name, 
                                       Fund.Fund_Code, 
                                       FundManager.Is_Deleted, 
                                       FundManager.FundManager_Image, 
                                       FundManager.FundManager_Designation, 
                                       FundManager.DateFrom, 
                                       FundManager.DateTo,
                                       FundManager.AUM)\
                                        .select_from(FundManager)\
                                        .join(Fund, Fund.Fund_Id == FundManager.Fund_Id)\
                                        .filter(or_(FundManager.DateTo >= date.today(), FundManager.DateTo == None))\
                                        .group_by(FundManager.FundManager_Id, 
                                                  FundManager.FundManager_Name, 
                                                  FundManager.FundManager_Code, 
                                                  FundManager.FundManager_Description, 
                                                  FundManager.Fund_Id, 
                                                  Fund.Fund_Name,
                                                  Fund.Fund_Code, 
                                                  FundManager.Is_Deleted, 
                                                  FundManager.FundManager_Image, 
                                                  FundManager.FundManager_Designation, 
                                                  FundManager.DateFrom, 
                                                  FundManager.DateTo,
                                                  FundManager.AUM)\
                                        .filter(FundManager.Is_Deleted != 1).filter(Fund.Is_Deleted != 1).filter(Fund.Fund_Id == fund_id).order_by(FundManager.FundManager_Name).all()

    for sql_obj in sql_fundmanager:
        json_obj = dict()
        json_obj["scheme_id"] = fund_id
        json_obj["fund_manager_name"] = sql_obj.FundManager_Name
        json_obj["fund_manager_code"] = sql_obj.FundManager_Code
        if sql_obj.DateFrom:
            json_obj["fund_manager_from"] = sql_obj.DateFrom
        else:
            sql_opendate = db_session.query(MFSecurity.MF_Security_OpenDate).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).filter(MFSecurity.Is_Deleted != 1).filter(Plans.Is_Deleted != 1).filter(Plans.Plan_Id == plan_id).scalar()
            json_obj["fund_manager_from"] = sql_opendate

        json_obj["fund_manager_to"] = sql_obj.DateTo
        json_obj["fund_manager_desc"] = sql_obj.FundManager_Description
        json_obj["fund_manager_designation"] = sql_obj.FundManager_Designation
        json_obj["fund_manager_aum_managed"] = sql_obj.AUM

        resp.append(json_obj)

    return resp


def get_compositiondata(db_session, plan_id, transaction_date, composition_for):
    """
    composition_for: accepted values for the parameter can be 'aif_cat3', 'fund_level', 'category_wise_all_funds'
    """
    resp = list()

    if not transaction_date:
            transaction_date = get_last_transactiondate(db_session, plan_id) 

    if composition_for == 'aif_cat3':
        sql_long = db_session.query(sqlalchemy.sql.expression.literal("Long").label("Exposure"),
                                    func.sum(PortfolioSectors.Percentage_To_AUM).label('Percentage_to_AUM'))\
                             .group_by(func.coalesce(PortfolioSectors.LONG_SHORT,"L"))\
                             .filter(PortfolioSectors.Plan_Id == plan_id, PortfolioSectors.Portfolio_Date == transaction_date, PortfolioSectors.Is_Deleted != 1)\
                             .filter(or_( PortfolioSectors.LONG_SHORT == "L", PortfolioSectors.LONG_SHORT == None))

        sql_short = db_session.query(sqlalchemy.sql.expression.literal("Short").label("Exposure"),
                                     func.sum(PortfolioSectors.Percentage_To_AUM).label("Percentage_to_AUM"))\
                              .group_by(PortfolioSectors.LONG_SHORT)\
                              .filter(PortfolioSectors.Plan_Id == plan_id, PortfolioSectors.Portfolio_Date == transaction_date, PortfolioSectors.Is_Deleted != 1)\
                              .filter(PortfolioSectors.LONG_SHORT == "S")
        
        sql_datas = sql_long.union(sql_short)
        Long = 0
        Short = 0
        Gross = 0
        Net = 0
        
        for sql_data in sql_datas:
            if sql_data.Exposure == "Long":
                Long = sql_data.Percentage_to_AUM

            if sql_data.Exposure == "Short":
                Short = sql_data.Percentage_to_AUM

        if Long > 0 or Short > 0:
            Gross = Long + Short
            Net = Long - Short

        resp.append({"Exposure": "Long", "Percentage": Long})
        resp.append({"Exposure": "Short", "Percentage": Short})
        resp.append({"Exposure": "Gross Exposure", "Percentage": Gross})
        resp.append({"Exposure": "Net Exposure", "Percentage": Net})

        return resp

    elif composition_for == 'fund_level':
        sql_comp = db_session.query(FactSheet.Plan_Id,
                                    FactSheet.Equity,
                                    FactSheet.Debt,
                                    FactSheet.Cash)\
                             .join(PlanProductMapping, PlanProductMapping.Plan_Id == FactSheet.Plan_Id)\
                             .filter(FactSheet.Is_Deleted != 1, FactSheet.TransactionDate == transaction_date, FactSheet.Plan_Id == plan_id).first()

        json_dict = dict()
        sql_fund = get_sql_fund_byplanid(db_session, plan_id)
        json_dict["scheme_id"] = sql_fund.Fund_Id
        json_dict["scheme_code"] = sql_fund.Fund_Code
        json_dict["date"] = transaction_date
        json_dict["equity"] = round(sql_comp.Equity,2) if sql_comp.Equity else None
        json_dict["debt"] = round(sql_comp.Debt,2) if sql_comp.Debt else None
        json_dict["cash"] = round(sql_comp.Cash,2) if sql_comp.Cash else None

        return json_dict

    elif composition_for == 'category_wise_all_funds':
        fund_category_info = get_fund_category(db_session, plan_id)
        list_fund_ids = get_funds_in_same_category(db_session, fund_category_info)

        transaction_date = datetime.strptime(transaction_date, r'%Y-%m-%d')
        first_day_of_month = transaction_date.replace(day=1)
        prev_month_end_date = first_day_of_month - timedelta(days=1)

        sql_fund_composition = db_session.query(func.max(FactSheet.Equity).label('equity'),
                                                func.max(FactSheet.Debt).label('debt'),
                                                func.max(FactSheet.Cash).label('cash'),
                                                func.max(FactSheet.NetAssets_Rs_Cr).label('aum'),
                                                MFSecurity.Fund_Id.label('fund_id'))\
                                         .select_from(FactSheet)\
                                         .join(Plans, Plans.Plan_Id == FactSheet.Plan_Id)\
                                         .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                         .filter(Plans.Is_Deleted != 1, FactSheet.Is_Deleted != 1, MFSecurity.Is_Deleted != 1, MFSecurity.Status_Id == 1)\
                                         .filter(FactSheet.Portfolio_Date >= prev_month_end_date, MFSecurity.Fund_Id.in_(list_fund_ids))\
                                         .filter(FactSheet.NetAssets_Rs_Cr != 0)\
                                         .group_by(MFSecurity.Fund_Id, FactSheet.TransactionDate)\
                                         .having(FactSheet.TransactionDate == func.max(FactSheet.TransactionDate)).all()

        df_cat_comp = pd.DataFrame(sql_fund_composition)

        # clean some data
        df_cat_comp = df_cat_comp[~((df_cat_comp['equity'] != 0) & (df_cat_comp['debt'] != 0) & (df_cat_comp['cash'] != 0))]

        # process
        df_cat_comp['equity_aum'] = df_cat_comp['equity'].astype(float) * df_cat_comp['aum'].astype(float) / 100
        df_cat_comp['debt_aum'] = df_cat_comp['debt'].astype(float) * df_cat_comp['aum'].astype(float) / 100
        df_cat_comp['cash_aum'] = df_cat_comp['cash'].astype(float) * df_cat_comp['aum'].astype(float) / 100
        total_aum = df_cat_comp['aum'].astype(float).sum()

        json_dict = dict()
        sql_fund = get_sql_fund_byplanid(db_session, plan_id)
        json_dict["scheme_id"] = sql_fund.Fund_Id
        json_dict["scheme_code"] = sql_fund.Fund_Code
        json_dict["date"] = transaction_date
        json_dict["equity"] = round((df_cat_comp['equity_aum'].sum() / total_aum) * 100 , 2)
        json_dict["debt"] = round((df_cat_comp['debt_aum'].sum() / total_aum) * 100 , 2)
        json_dict["cash"] = round((df_cat_comp['cash_aum'].sum() / total_aum) * 100 , 2)

        return json_dict


def get_sectorweightsdata(db_session, plan_id, transaction_date, composition_for):
    """
    Expected values:
        composition_for >> 'aif_cat3', 'fund_level', 'category_wise_all_funds', 'product_wise_all_funds'
    """
    resp = list()
    sectorlist = list()
    sector_dict = dict()

    if not transaction_date:
        transaction_date = get_last_transactiondate(db_session, plan_id)

    if plan_id:
        fund_id = get_fundid_byplanid(db_session, plan_id)
        fund_category_info = get_fund_category(db_session, plan_id) # Gets the asset class id, classification id, product id for the respective plan_id

    if composition_for == 'aif_cat3':
        long_sectordata = list()
        short_sectordata = list()

        sql_sectors = db_session.query(PortfolioSectors.Sector_Code,
                                        PortfolioSectors.Sector_Name)\
                                .filter(PortfolioSectors.Plan_Id == plan_id, PortfolioSectors.Portfolio_Date == transaction_date)\
                                .filter(PortfolioSectors.Is_Deleted != 1).order_by(PortfolioSectors.Sector_Name).distinct().all()

        for sql_sector in sql_sectors:
            sql_data = db_session.query(PortfolioSectors.Sector_Code,
                                        PortfolioSectors.Sector_Name,
                                        func.coalesce(PortfolioSectors.LONG_SHORT, "L").label("LONG_SHORT"),
                                        func.sum(func.coalesce(PortfolioSectors.Percentage_To_AUM, 0)).label("Weights"),
                                        PortfolioSectors.Plan_Id)\
                                 .filter(PortfolioSectors.Plan_Id == plan_id, PortfolioSectors.Portfolio_Date == transaction_date)\
                                 .filter(PortfolioSectors.Is_Deleted != 1).group_by(PortfolioSectors.Sector_Code,
                                                                                    PortfolioSectors.Sector_Name,
                                                                                    func.coalesce(PortfolioSectors.LONG_SHORT, "L"))\
                                 .filter(PortfolioSectors.Sector_Code == sql_sector.Sector_Code)\
                                 .order_by(func.coalesce(PortfolioSectors.LONG_SHORT, "L"), desc(func.coalesce(PortfolioSectors.Percentage_To_AUM, 0))).all()

            long_value = 0
            short_value = 0
            if sql_data:
                sectorlist.append(sql_dt.Sector_Name)
                for sql_dt in sql_data:
                    if sql_dt.LONG_SHORT == "L":
                        long_value = sql_dt.Weights

                    if sql_dt.LONG_SHORT == "S":
                        short_value = sql_dt.Weights

                long_sectordata.append(long_value)
                short_sectordata.append(short_value)
        #Short
        datalabels = dict()
        datalabels["x"] = 0

        sector_dict["name"] = "Short"
        sector_dict["dataLabels"] = datalabels
        sector_dict["data"] = short_sectordata
        sector_dict["scheme_id"] = fund_id
        resp.append(sector_dict)

        #Long
        datalabels["x"] = 10

        sector_dict["name"] = "Long"
        sector_dict["dataLabels"] = datalabels
        sector_dict["data"] = long_sectordata

    elif composition_for == 'fund_level':
        sql_portfolio_sectors = db_session.query(PortfolioSectors.Sector_Code,
                                                 PortfolioSectors.Sector_Name,
                                                 PortfolioSectors.Sub_Sector_Name,
                                                 PortfolioSectors.Percentage_To_AUM.label('Weights'),
                                                 PortfolioSectors.Plan_Id)\
                                        .join(FactSheet, and_(FactSheet.Plan_Id == PortfolioSectors.Plan_Id,
                                                                FactSheet.Portfolio_Date == PortfolioSectors.Portfolio_Date))\
                                        .filter(PortfolioSectors.Is_Deleted != 1,
                                                  FactSheet.Is_Deleted != 1,
                                                  FactSheet.Plan_Id == plan_id,
                                                  FactSheet.TransactionDate == transaction_date,
                                                  func.coalesce(PortfolioSectors.Percentage_To_AUM,0) != 0)\
                                        .order_by(desc(PortfolioSectors.Percentage_To_AUM))

        # sector weights in df
        df = pd.DataFrame(sql_portfolio_sectors)

        if not df.empty:
            df.rename(columns={'Sector_Name':'sector_name',
                               'Weights':'sector_weight',
                               'Plan_Id':'scheme_id',
                               'Sub_Sector_Name':'sub_sector_name'}, inplace=True)
            df['scheme_id'] = fund_id

            # pass it into other dataframe
            df1 = df.groupby(by='sector_name').agg({'sector_weight': 'sum',
                                                    'scheme_id': 'max'})
            df1.reset_index(inplace=True)
            sector_dict = df1.to_dict(orient='records')

            # sub sectors weight in df
            df2 = df.groupby(by='sub_sector_name').agg({'sector_weight': 'sum',
                                                        'sector_name': 'max'})
            df2.reset_index(inplace=True)
            df2.rename(columns={'sector_weight':'sub_sector_weight'}, inplace=True)

            # if the logic is for fund level sectors then additionally need to execute the following for sub sectors
            empty_dict = {'sub_sectors' : {}}
            for d in sector_dict:
                d.update(empty_dict)

            sub_sector_dict = df2.to_dict(orient='records')

            for d in sector_dict:
                for d1 in sub_sector_dict:
                    tmp_ss = {}
                    if d1['sector_name'] == d['sector_name']:
                        tmp_ss[d1['sub_sector_name']] = d1['sub_sector_weight']
                        if not d['sub_sectors']:
                            d['sub_sectors'] = tmp_ss
                        else:
                            d['sub_sectors'].update(tmp_ss)
            
            sector_dict = sorted(sector_dict, key=lambda d: d['sector_weight'], reverse=True) if sector_dict else sector_dict
            resp = sector_dict

    elif composition_for == 'category_wise_all_funds':
        list_fund_ids = get_funds_in_same_category(db_session, fund_category_info)

        # get only one plan for each fund and store in a list of plan ids
        sql_plan_ids = db_session.query(func.max(Plans.Plan_Id))\
                                 .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                 .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                                 .filter(Plans.Is_Deleted != 1,
                                      MFSecurity.Is_Deleted != 1,
                                      Fund.Is_Deleted != 1,
                                      Fund.Fund_Id.in_(list_fund_ids))\
                                 .group_by(Fund.Fund_Id)

        list_plan_ids = [r for (r, ) in sql_plan_ids]

        sql_portfolio_sectors = db_session.query(func.max(PortfolioSectors.Sector_Code).label('sector_code'),
                                                 Sector.Sector_Name.label('sector_name'),
                                                 func.sum(PortfolioSectors.Percentage_To_AUM).label('sector_weight'),
                                                 Fund.Fund_Id.label('scheme_id'),
                                                 FactSheet.NetAssets_Rs_Cr.label('aum'))\
                                          .join(FactSheet, and_(FactSheet.Plan_Id == PortfolioSectors.Plan_Id,
                                                                FactSheet.Portfolio_Date == PortfolioSectors.Portfolio_Date))\
                                          .join(Plans, Plans.Plan_Id == FactSheet.Plan_Id)\
                                          .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                          .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                                          .join(Sector, Sector.Sector_Code == PortfolioSectors.Sector_Code)\
                                          .filter(PortfolioSectors.Is_Deleted != 1, FactSheet.Is_Deleted != 1, Fund.Is_Deleted != 1,
                                                  Plans.Is_Deleted != 1, MFSecurity.Is_Deleted != 1, MFSecurity.Status_Id == 1)\
                                          .filter(Plans.Plan_Id.in_(list_plan_ids),
                                                  FactSheet.TransactionDate == transaction_date,
                                                  func.coalesce(PortfolioSectors.Percentage_To_AUM,0) != 0,
                                                  func.coalesce(FactSheet.NetAssets_Rs_Cr,0) > 0)\
                                          .group_by(Fund.Fund_Id, Sector.Sector_Name, FactSheet.NetAssets_Rs_Cr)

        # sector weights in df
        df = pd.DataFrame(sql_portfolio_sectors)
        if not df.empty:
            total_aum = df['aum'].unique().sum()
            df['sector_aum'] = df['sector_weight'] * df['aum']
            df_ = df.groupby(by='sector_name').agg({'sector_aum': 'sum', 'scheme_id': 'max'})
            df_['scheme_id'] = fund_id
            df_['category_sector_weight'] = df_['sector_aum'] / total_aum
            df_.reset_index(inplace=True)
            df_.drop('sector_aum', axis=1, inplace=True)
            df_.rename(columns={'category_sector_weight': 'sector_weight'}, inplace=True)
            df_['sector_weight'] = round(df_['sector_weight'].astype(float), 2)
            df_ = df_[df_['sector_weight'] > 0]
            resp = df_.to_dict(orient='records')

    elif composition_for == 'product_wise_all_funds':

        resp = dict()
        product_codes = ['ALL', 'MF', 'ULIP', 'PMS', 'AIF']

        for product_code in product_codes:
            res = list()
            #Find better way
            if product_code == 'ALL':
                products = ['MF', 'ULIP', 'PMS', 'AIF'] 
            else:
                products = [product_code]

            aum = db_session.query(func.sum(FundStocks.Value_In_Inr))\
                                    .filter(FundStocks.InstrumentType == 'Equity', 
                                            FundStocks.Product_Code.in_(products)).scalar()
                                               
            sql_sector_wts = db_session.query(FundStocks.Sector_Names, 
                                            ((func.sum(FundStocks.Value_In_Inr) * 100)/aum).label('weight'))\
                                            .filter(FundStocks.InstrumentType == 'Equity', 
                                                    FundStocks.Product_Code.in_(products))\
                                            .group_by(FundStocks.Sector_Names, 
                                                      FundStocks.Sector_Code).all()
            
            for sector_wts in sql_sector_wts:
                data = dict()
                data['sector_name'] = sector_wts.Sector_Names
                data['sector_weight'] = sector_wts.weight
                res.append(data)

            res = sorted(res, key=lambda d: d['sector_weight'], reverse=True) 
            resp[product_code.lower()] = res
    
    return resp


def get_performancetrend_data(db_session, plan_id, transaction_date):

    factsheet_query1 = db_session.query(FactSheet).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.Is_Deleted != 1)

    if transaction_date:
        factsheet_query1 = factsheet_query1.filter(FactSheet.TransactionDate == transaction_date)

    factsheet = factsheet_query1.order_by(desc(FactSheet.TransactionDate)).first()

    res_factsheet = dict()    
    if factsheet:        
        if not transaction_date:
            transaction_date = factsheet.TransactionDate
        
        benchmarkdata = db_session.query(MFSecurity.MF_Security_Id,MFSecurity.MF_Security_OpenDate, Plans.Plan_Name, Product.Product_Code, Product.Product_Id, BenchmarkIndices.Co_Code, BenchmarkIndices.TRI_Co_Code, BenchmarkIndices.BenchmarkIndices_Name, TRIReturns.Return_1Month, TRIReturns.Return_3Month, TRIReturns.Return_6Month, TRIReturns.Return_1Year, TRIReturns.Return_3Year, FactSheet.SCHEME_BENCHMARK_RETURNS_1MONTH, FactSheet.SCHEME_BENCHMARK_RETURNS_3MONTH, FactSheet.SCHEME_BENCHMARK_RETURNS_6MONTH, FactSheet.SCHEME_BENCHMARK_RETURNS_1YEAR, FactSheet.SCHEME_BENCHMARK_RETURNS_3YEAR, FactSheet.SCHEME_BENCHMARK_RETURNS_2YEAR, FactSheet.SCHEME_BENCHMARK_RETURNS_5YEAR, FactSheet.SCHEME_BENCHMARK_RETURNS_10YEAR, FactSheet.SCHEME_BENCHMARK_RETURNS_SI,FactSheet.SCHEME_RETURNS_1MONTH ,FactSheet.SCHEME_RETURNS_3MONTH ,FactSheet.SCHEME_RETURNS_6MONTH ,FactSheet.SCHEME_RETURNS_1YEAR ,FactSheet.SCHEME_RETURNS_3YEAR ,FactSheet.SCHEME_RETURNS_5YEAR,FactSheet.SCHEME_RETURNS_10YEAR, FactSheet.SCHEME_RETURNS_since_inception , FactSheet.RANKING_RANK_1MONTH , FactSheet.RANKING_RANK_3MONTH , FactSheet.RANKING_RANK_6MONTH , FactSheet.RANKING_RANK_1YEAR , FactSheet.RANKING_RANK_3YEAR , FactSheet.RANKING_RANK_5YEAR , FactSheet.COUNT_1MONTH , FactSheet.COUNT_3MONTH , FactSheet.COUNT_6MONTH , FactSheet.COUNT_1YEAR , FactSheet.COUNT_3YEAR , FactSheet.COUNT_3YEAR , Fund.Fund_Id, Fund.Fund_Name, Fund.Fund_Code).select_from(FactSheet).join(Plans, Plans.Plan_Id == FactSheet.Plan_Id).join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id).join(Product, Product.Product_Id == PlanProductMapping.Product_Id).join(MFSecurity,MFSecurity.MF_Security_Id == Plans.MF_Security_Id).join(BenchmarkIndices, BenchmarkIndices.BenchmarkIndices_Id == MFSecurity.BenchmarkIndices_Id, isouter = True).join(Fund, Fund.Fund_Id==MFSecurity.Fund_Id).join(TRIReturns, and_(TRIReturns.TRI_Co_Code == BenchmarkIndices.TRI_Co_Code, TRIReturns.TRI_IndexDate == FactSheet.TransactionDate), isouter = True).filter(MFSecurity.Is_Deleted != 1).filter(MFSecurity.Status_Id == 1).filter(FactSheet.Is_Deleted != 1).filter(FactSheet.TransactionDate == transaction_date).filter(FactSheet.Plan_Id == plan_id).first()

        res_factsheet["plan_name"] = benchmarkdata.Plan_Name
        res_factsheet["benchmark_name"] = benchmarkdata.BenchmarkIndices_Name
        res_factsheet["inception_date"] = benchmarkdata.MF_Security_OpenDate
        res_factsheet["scheme_ret_1m"] = round(factsheet.SCHEME_RETURNS_1MONTH,2) if factsheet.SCHEME_RETURNS_1MONTH else None
        res_factsheet["scheme_ret_3m"] = round(factsheet.SCHEME_RETURNS_3MONTH,2) if factsheet.SCHEME_RETURNS_3MONTH else None
        res_factsheet["scheme_ret_6m"] = round(factsheet.SCHEME_RETURNS_6MONTH,2) if factsheet.SCHEME_RETURNS_6MONTH else None
        res_factsheet["scheme_ret_1y"] = round(factsheet.SCHEME_RETURNS_1YEAR,2) if factsheet.SCHEME_RETURNS_1YEAR else None
        res_factsheet["scheme_ret_2y"] = round(factsheet.SCHEME_RETURNS_2YEAR,2) if factsheet.SCHEME_RETURNS_2YEAR else None
        res_factsheet["scheme_ret_3y"] = round(factsheet.SCHEME_RETURNS_3YEAR,2) if factsheet.SCHEME_RETURNS_3YEAR else None
        res_factsheet["scheme_ret_5y"] = round(factsheet.SCHEME_RETURNS_5YEAR,2) if factsheet.SCHEME_RETURNS_5YEAR else None
        res_factsheet["scheme_ret_10y"] = round(factsheet.SCHEME_RETURNS_10YEAR,2) if factsheet.SCHEME_RETURNS_10YEAR else None
        res_factsheet["scheme_ret_ince"] = round(factsheet.SCHEME_RETURNS_since_inception,2) if factsheet.SCHEME_RETURNS_since_inception else None

        product_code = benchmarkdata.Product_Code
        tri_co_code = benchmarkdata.TRI_Co_Code if benchmarkdata.TRI_Co_Code else ''

        if product_code == "PMS" or product_code == "AIF":
            res_factsheet["bm_ret_1m"] = round(benchmarkdata.SCHEME_BENCHMARK_RETURNS_1MONTH,2) if benchmarkdata.SCHEME_BENCHMARK_RETURNS_1MONTH else None
            res_factsheet["bm_ret_3m"] = round(benchmarkdata.SCHEME_BENCHMARK_RETURNS_3MONTH,2) if benchmarkdata.SCHEME_BENCHMARK_RETURNS_3MONTH else None
            res_factsheet["bm_ret_6m"] = round(benchmarkdata.SCHEME_BENCHMARK_RETURNS_6MONTH,2) if benchmarkdata.SCHEME_BENCHMARK_RETURNS_6MONTH else None
            res_factsheet["bm_ret_1y"] = round(benchmarkdata.SCHEME_BENCHMARK_RETURNS_1YEAR,2) if benchmarkdata.SCHEME_BENCHMARK_RETURNS_1YEAR else None
            res_factsheet["bm_ret_2y"] = round(benchmarkdata.SCHEME_BENCHMARK_RETURNS_2YEAR,2) if benchmarkdata.SCHEME_BENCHMARK_RETURNS_2YEAR else None
            res_factsheet["bm_ret_3y"] = round(benchmarkdata.SCHEME_BENCHMARK_RETURNS_3YEAR,2) if benchmarkdata.SCHEME_BENCHMARK_RETURNS_3YEAR else None
            res_factsheet["bm_ret_5y"] = round(benchmarkdata.SCHEME_BENCHMARK_RETURNS_5YEAR,2) if benchmarkdata.SCHEME_BENCHMARK_RETURNS_5YEAR else None
            res_factsheet["bm_ret_10y"] = round(benchmarkdata.SCHEME_BENCHMARK_RETURNS_10YEAR,2) if benchmarkdata.SCHEME_BENCHMARK_RETURNS_10YEAR else None
            res_factsheet["bm_ret_ince"] = round(benchmarkdata.SCHEME_BENCHMARK_RETURNS_SI,2) if benchmarkdata.SCHEME_BENCHMARK_RETURNS_SI else None
        else:
            if not isinstance(transaction_date, datetime):
                transaction_date = datetime.strptime(transaction_date, '%Y-%m-%d')

            transactiondate1 = transaction_date
            transaction_date_1 = transactiondate1 - timedelta(days=1)
            transaction_date_2 = transactiondate1 - timedelta(days=2)

            co_code = get_co_code_by_plan_id(db_session, plan_id)

            benchmark_ret_query = db_session.query(TRIReturns.Return_1Month,
                                                   TRIReturns.Return_3Month,
                                                   TRIReturns.Return_6Month,
                                                   TRIReturns.Return_1Year,
                                                   TRIReturns.Return_3Year)

            if tri_co_code == '':
                benchmark_ret_query = benchmark_ret_query.join(BenchmarkIndices, TRIReturns.TRI_Co_Code == BenchmarkIndices.Co_Code).filter(BenchmarkIndices.Co_Code == benchmarkdata.Co_Code)
            else:
                benchmark_ret_query = benchmark_ret_query.join(BenchmarkIndices, TRIReturns.TRI_Co_Code == BenchmarkIndices.TRI_Co_Code).filter(BenchmarkIndices.Co_Code == co_code)

            benchmark_ret_data = benchmark_ret_query.filter(or_(TRIReturns.TRI_IndexDate == transaction_date, TRIReturns.TRI_IndexDate == transaction_date_1)).order_by(desc(TRIReturns.TRI_IndexDate)).first()

            if not benchmark_ret_data:
                benchmark_ret_data = benchmark_ret_query.filter(or_(TRIReturns.TRI_IndexDate == transaction_date, TRIReturns.TRI_IndexDate == transaction_date_2)).order_by(desc(TRIReturns.TRI_IndexDate)).first()

            if benchmark_ret_data:
                res_factsheet["bm_ret_1m"] = round(benchmark_ret_data.Return_1Month, 2) if benchmark_ret_data.Return_1Month else None
                res_factsheet["bm_ret_3m"] = round(benchmark_ret_data.Return_3Month, 2) if benchmark_ret_data.Return_3Month else None
                res_factsheet["bm_ret_6m"] = round(benchmark_ret_data.Return_6Month, 2) if benchmark_ret_data.Return_6Month else None
                res_factsheet["bm_ret_1y"] = round(benchmark_ret_data.Return_1Year, 2) if benchmark_ret_data.Return_1Year else None
                res_factsheet["bm_ret_2y"] = 0
                res_factsheet["bm_ret_3y"] = round(benchmark_ret_data.Return_3Year, 2) if benchmark_ret_data.Return_3Year else None
                res_factsheet["bm_ret_5y"] = 0
                res_factsheet["bm_ret_10y"] = 0
                res_factsheet["bm_ret_ince"] = 0
            else:
                res_factsheet["bm_ret_1m"] = None
                res_factsheet["bm_ret_3m"] = None
                res_factsheet["bm_ret_6m"] = None
                res_factsheet["bm_ret_1y"] = None
                res_factsheet["bm_ret_2y"] = None
                res_factsheet["bm_ret_3y"] = None
                res_factsheet["bm_ret_5y"] = None
                res_factsheet["bm_ret_10y"] = None
                res_factsheet["bm_ret_ince"] = None

        if (benchmarkdata.Product_Id == 1 or benchmarkdata.Product_Id == 2): #MF or ULIP
            res_factsheet["category_name"] = "Category Average"
            res_factsheet["cat_ret_1m"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_1MONTH
            res_factsheet["cat_ret_3m"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_3MONTH
            res_factsheet["cat_ret_6m"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_6MONTH
            res_factsheet["cat_ret_1y"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_1YEAR
            res_factsheet["cat_ret_3y"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_3YEAR
            res_factsheet["cat_ret_5y"] = factsheet.SCHEME_CATEGORY_AVERAGE_RETURNS_5YEAR
            res_factsheet["cat_ret_ince"] = 0

        #Active Returns
        scheme_return_1month = factsheet.SCHEME_RETURNS_1MONTH if factsheet.SCHEME_RETURNS_1MONTH else 0
        scheme_return_3month = factsheet.SCHEME_RETURNS_3MONTH if factsheet.SCHEME_RETURNS_3MONTH else 0
        scheme_return_6month = factsheet.SCHEME_RETURNS_6MONTH if factsheet.SCHEME_RETURNS_6MONTH else 0
        scheme_return_1year = factsheet.SCHEME_RETURNS_1YEAR if factsheet.SCHEME_RETURNS_1YEAR else 0
        scheme_return_3year = factsheet.SCHEME_RETURNS_3YEAR if factsheet.SCHEME_RETURNS_3YEAR else 0
        scheme_return_5year = factsheet.SCHEME_RETURNS_5YEAR if factsheet.SCHEME_RETURNS_5YEAR else 0
        scheme_return_10year = factsheet.SCHEME_RETURNS_10YEAR if factsheet.SCHEME_RETURNS_10YEAR else 0
        scheme_return_si = factsheet.SCHEME_RETURNS_since_inception if factsheet.SCHEME_RETURNS_since_inception else 0

        benchmark_return_1month = benchmarkdata.SCHEME_BENCHMARK_RETURNS_1MONTH if benchmarkdata.SCHEME_BENCHMARK_RETURNS_1MONTH else 0
        benchmark_return_3month = benchmarkdata.SCHEME_BENCHMARK_RETURNS_3MONTH if benchmarkdata.SCHEME_BENCHMARK_RETURNS_3MONTH else 0
        benchmark_return_6month = benchmarkdata.SCHEME_BENCHMARK_RETURNS_6MONTH if benchmarkdata.SCHEME_BENCHMARK_RETURNS_6MONTH else 0
        benchmark_return_1year = benchmarkdata.SCHEME_BENCHMARK_RETURNS_1YEAR if benchmarkdata.SCHEME_BENCHMARK_RETURNS_1YEAR else 0
        benchmark_return_3year = benchmarkdata.SCHEME_BENCHMARK_RETURNS_3YEAR if benchmarkdata.SCHEME_BENCHMARK_RETURNS_3YEAR else 0
        benchmark_return_5year = benchmarkdata.SCHEME_BENCHMARK_RETURNS_5YEAR if benchmarkdata.SCHEME_BENCHMARK_RETURNS_5YEAR else 0
        benchmark_return_10year = benchmarkdata.SCHEME_BENCHMARK_RETURNS_10YEAR if benchmarkdata.SCHEME_BENCHMARK_RETURNS_10YEAR else 0
        benchmark_return_si = benchmarkdata.SCHEME_BENCHMARK_RETURNS_SI if benchmarkdata.SCHEME_BENCHMARK_RETURNS_SI else 0
        
        res_factsheet["active_ret_1m"] = None if scheme_return_1month - benchmark_return_1month == 0 else round(scheme_return_1month - benchmark_return_1month,2)
        res_factsheet["active_ret_3m"] = None if scheme_return_3month - benchmark_return_3month == 0 else round(scheme_return_3month - benchmark_return_3month,2)
        res_factsheet["active_ret_6m"] = None if scheme_return_6month - benchmark_return_6month == 0 else round(scheme_return_6month - benchmark_return_6month,2)
        res_factsheet["active_ret_1y"] = None if scheme_return_1year - benchmark_return_1year == 0 else round(scheme_return_1year - benchmark_return_1year,2)
        res_factsheet["active_ret_3y"] = None if scheme_return_3year - benchmark_return_3year == 0 else round(scheme_return_3year - benchmark_return_3year,2)
        res_factsheet["active_ret_5y"] = None if scheme_return_5year - benchmark_return_5year == 0 else round(scheme_return_5year - benchmark_return_5year,2)
        res_factsheet["active_ret_10y"] = None if scheme_return_10year - benchmark_return_10year == 0 else round(scheme_return_10year - benchmark_return_10year, 2)
        res_factsheet["active_ret_ince"] = None if scheme_return_si - benchmark_return_si == 0 else round(scheme_return_si - benchmark_return_si,2)

        res_factsheet["ranking_rank_1m"] = factsheet.RANKING_RANK_1MONTH
        res_factsheet["ranking_rank_3m"] = factsheet.RANKING_RANK_3MONTH
        res_factsheet["ranking_rank_6m"] = factsheet.RANKING_RANK_6MONTH
        res_factsheet["ranking_rank_1y"] = factsheet.RANKING_RANK_1YEAR
        res_factsheet["ranking_rank_3y"] = factsheet.RANKING_RANK_3YEAR
        res_factsheet["ranking_rank_5y"] = factsheet.RANKING_RANK_5YEAR

        res_factsheet["count_1m"] = factsheet.COUNT_1MONTH
        res_factsheet["count_3m"] = factsheet.COUNT_3MONTH
        res_factsheet["count_6m"] = factsheet.COUNT_6MONTH
        res_factsheet["count_1y"] = factsheet.COUNT_1YEAR
        res_factsheet["count_3y"] = factsheet.COUNT_3YEAR
        res_factsheet["count_5y"] = factsheet.COUNT_5YEAR

    return res_factsheet


def get_fundriskratio_data(db_session, plan_id, transaction_date):
    transactiondate1 = None
    scheme_id = None
    date = None
    standard_deviation_1_y = "NA"
    standard_deviation_3_y = "NA"
    sharpe_ratio_1_y = "NA"
    sharpe_ratio_3_y = "NA"
    sortino_1_y = "NA"
    sortino_3_y = "NA"
    alpha_1_y = "NA"
    alpha_3_y = "NA"
    beta_1_y = "NA"
    beta_3_y = "NA"
    r_square_1_y = "NA"
    r_square_3_y = "NA"
    mean_1_y = "NA"
    mean_3_y = "NA"
    pe_ratio = "NA"
    total_stocks = "NA"
    treynor_ratio_1_y = "NA"
    treynor_ratio_3_y = "NA"

    resp_dic = dict()

    if plan_id:
        if not transaction_date:
            transaction_date = get_last_transactiondate(db_session, plan_id)   
            # transactiondate1 = str(transaction_date.year) + "-" + str(transaction_date.month) + "-" + str(transaction_date.day)
            
        # if transactiondate1:
        #     transactiondate1 = strptime(transactiondate1, '%Y-%m-%d')
        # else:
        #     transactiondate1 = strptime(transaction_date, '%Y-%m-%d')
                       
        # year1to = getbetweendate(0,1,transactiondate1) - timedelta(days = 1)
        # year1from = datetime(year1to.year, year1to.month,28)
        
        sql_current = db_session.query(FactSheet.StandardDeviation,FactSheet.StandardDeviation_1Yr,FactSheet.SharpeRatio,FactSheet.SharpeRatio_1Yr,FactSheet.Beta,FactSheet.Beta_1Yr,FactSheet.R_Squared,FactSheet.R_Squared_1Yr,FactSheet.Alpha,FactSheet.Alpha_1Yr,FactSheet.TotalStocks,FactSheet.PortfolioP_ERatio, FactSheet.Portfolio_Dividend_Yield, FactSheet.AvgMktCap_Rs_Cr, FactSheet.PortfolioP_BRatio, FactSheet.Mean, FactSheet.Mean_1Yr, FactSheet.Sortino, FactSheet.Sortino_1Yr, FactSheet.ModifiedDuration_yrs, FactSheet.Treynor_Ratio_1Yr, FactSheet.Treynor_Ratio).filter(FactSheet.TransactionDate == transaction_date).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.Is_Deleted != 1).order_by(desc(FactSheet.TransactionDate)).first()
        
        if not sql_current:
            transaction_date_1 = transaction_date - timedelta(days=1) 
            sql_current = db_session.query(FactSheet.StandardDeviation,FactSheet.StandardDeviation_1Yr,FactSheet.SharpeRatio,FactSheet.SharpeRatio_1Yr,FactSheet.Beta,FactSheet.Beta_1Yr,FactSheet.R_Squared,FactSheet.R_Squared_1Yr,FactSheet.Alpha,FactSheet.Alpha_1Yr,FactSheet.TotalStocks,FactSheet.PortfolioP_ERatio, FactSheet.Portfolio_Dividend_Yield, FactSheet.AvgMktCap_Rs_Cr, FactSheet.PortfolioP_BRatio, FactSheet.Mean, FactSheet.Mean_1Yr, FactSheet.Sortino, FactSheet.Sortino_1Yr, FactSheet.ModifiedDuration_yrs, FactSheet.Treynor_Ratio_1Yr, FactSheet.Treynor_Ratio).filter(FactSheet.TransactionDate == transaction_date_1).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.Is_Deleted != 1).order_by(desc(FactSheet.TransactionDate)).first()

        if not sql_current:
            transaction_date_2 = transaction_date - timedelta(days=2) 
            sql_current = db_session.query(FactSheet.StandardDeviation,FactSheet.StandardDeviation_1Yr,FactSheet.SharpeRatio,FactSheet.SharpeRatio_1Yr,FactSheet.Beta,FactSheet.Beta_1Yr,FactSheet.R_Squared,FactSheet.R_Squared_1Yr,FactSheet.Alpha,FactSheet.Alpha_1Yr,FactSheet.TotalStocks,FactSheet.PortfolioP_ERatio, FactSheet.Portfolio_Dividend_Yield, FactSheet.AvgMktCap_Rs_Cr, FactSheet.PortfolioP_BRatio, FactSheet.Mean, FactSheet.Mean_1Yr, FactSheet.Sortino, FactSheet.Sortino_1Yr, FactSheet.ModifiedDuration_yrs, FactSheet.Treynor_Ratio_1Yr, FactSheet.Treynor_Ratio).filter(FactSheet.TransactionDate == transaction_date_2).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.Is_Deleted != 1).order_by(desc(FactSheet.TransactionDate)).first()

        if sql_current:
            standard_deviation_3_y = round(sql_current.StandardDeviation,2) if sql_current.StandardDeviation else None
            sharpe_ratio_3_y = round(sql_current.SharpeRatio,2) if sql_current.SharpeRatio else None
            sortino_3_y = round(sql_current.Sortino,2) if sql_current.Sortino else None
            alpha_3_y = round(sql_current.Alpha,2) if sql_current.Alpha else None
            beta_3_y = round(sql_current.Beta,2) if sql_current.Beta else None
            r_square_3_y = round(sql_current.R_Squared,2) if sql_current.R_Squared else None
            mean_3_y = round(sql_current.Mean,2) if sql_current.Mean else None
            treynor_ratio_3_y = round(sql_current.Treynor_Ratio,2) if sql_current.Treynor_Ratio else None

            standard_deviation_1_y = round(sql_current.StandardDeviation_1Yr,2) if sql_current.StandardDeviation_1Yr else None
            sharpe_ratio_1_y = round(sql_current.SharpeRatio_1Yr,2) if sql_current.SharpeRatio_1Yr else None
            sortino_1_y = round(sql_current.Sortino_1Yr,2) if sql_current.Sortino_1Yr else None
            alpha_1_y = round(sql_current.Alpha_1Yr,2) if sql_current.Alpha_1Yr else None
            beta_1_y = round(sql_current.Beta_1Yr,2) if sql_current.Beta_1Yr else None
            r_square_1_y = round(sql_current.R_Squared_1Yr,2) if sql_current.R_Squared_1Yr else None
            mean_1_y = round(sql_current.Mean_1Yr,2) if sql_current.Mean_1Yr else None
            treynor_ratio_1_y = round(sql_current.Treynor_Ratio_1Yr,2) if sql_current.Treynor_Ratio_1Yr else None

            pe_ratio = round(sql_current.PortfolioP_ERatio,2) if sql_current.PortfolioP_ERatio else None
            total_stocks = round(sql_current.TotalStocks,0) if sql_current.TotalStocks else None
            modified_duration = sql_current.ModifiedDuration_yrs
        
        sql_fund = get_sql_fund_byplanid(db_session, plan_id)
        resp_dic["scheme_id"] = sql_fund.Fund_Id
        resp_dic["scheme_code"] = sql_fund.Fund_Code
        resp_dic["date"] = transaction_date
        resp_dic["standard_deviation_1_y"] = standard_deviation_1_y
        resp_dic["standard_deviation_3_y"] = standard_deviation_3_y
        resp_dic["sharpe_ratio_1_y"] =  sharpe_ratio_1_y
        resp_dic["sharpe_ratio_3_y"] = sharpe_ratio_3_y
        resp_dic["sortino_1_y"] = sortino_1_y
        resp_dic["sortino_3_y"] = sortino_3_y
        resp_dic["alpha_1_y"] = alpha_1_y
        resp_dic["alpha_3_y"] = alpha_3_y
        resp_dic["beta_1_y"] = beta_1_y
        resp_dic["beta_3_y"] = beta_3_y
        resp_dic["r_square_1_y"] = r_square_1_y
        resp_dic["r_square_3_y"] = r_square_3_y
        resp_dic["mean_1_y"] = mean_1_y
        resp_dic["mean_3_y"] = mean_3_y
        resp_dic["pe_ratio"] = pe_ratio
        resp_dic["total_stocks"] = total_stocks
        resp_dic["modified_duration"] = modified_duration
        resp_dic["treynor_ratio_3_y"] = treynor_ratio_3_y
        resp_dic["treynor_ratio_1_y"] = treynor_ratio_1_y
        
    return resp_dic


# TODO: Move following function in analytics.py
def get_rolling_returns(db_session, plan_id, is_benchmark, timeframe_in_yr, transaction_date, include_breakup, get_only_raw_data=False, is_annualized_return=False):
    result = dict()
    sql_q = None

    product_id = db_session.query(PlanProductMapping.Product_Id).select_from(PlanProductMapping).join(Plans, Plans.Plan_Id == PlanProductMapping.Plan_Id).filter(PlanProductMapping.Is_Deleted != 1, Plans.Plan_Id == plan_id).scalar()
    
    if (product_id == 2 or product_id == 4 or product_id == 5) or is_benchmark:
        sql_q = db_session.query(NAV.NAV_Date, NAV.NAV)
    elif product_id == 1:
        sql_q = db_session.query(NAV.NAV_Date, NAV.RAW_NAV.label('NAV'))

    if is_benchmark:
        sql_q = sql_q.filter(NAV.Plan_Id == plan_id, NAV.Is_Deleted != 1, NAV.NAV_Date <= transaction_date, NAV.NAV_Type == 'I')
    else:
        sql_q = sql_q.filter(NAV.Plan_Id == plan_id, NAV.Is_Deleted != 1, NAV.NAV_Date <= transaction_date, NAV.NAV_Type == 'P')
    sql_results = sql_q.order_by(NAV.NAV_Date).distinct().all()

    if not len(sql_results) > 0:
        return result

    df = pd.DataFrame(sql_results, columns =['NAV_Date', 'NAV'])
    df['NAV_Date'] = df['NAV_Date'].apply(pd.to_datetime)
    df = df.set_index('NAV_Date')

    # TODO: timeframe_in_yr rename to ROLLING_YEARS
    df["old_NAV"] = df.shift(periods= 365*timeframe_in_yr, freq='D')["NAV"]
    df = df.reset_index()

    if is_annualized_return:
        df["returns"] = (pow((df["NAV"] / df["old_NAV"]), (1/timeframe_in_yr)) - 1)*100
    else:
        df["returns"] = ((df["NAV"] - df["old_NAV"]) / df["old_NAV"])*100

    if get_only_raw_data: #added this to get data for active returns
        return df.to_dict(orient="records")
    
    return get_rolling_analysis(df, include_breakup)
    
def get_rolling_analysis(df, include_breakup):
    result = dict()
    ret_cnt = int(df["returns"].count())

    if not ret_cnt:
        return result

    result["total_observations_no"]  = ret_cnt
    result["median_returns"] = round(df["returns"].median(), 2)
    result["average_returns"] = round(df["returns"].mean(), 2)
    result["max_returns"] = round(df["returns"].max(), 2)
    result["min_returns"] = round(df["returns"].min(), 2)

    above75 = sum( df['returns'] > 75 )
    between50_75 = sum( (df['returns'] > 50) & (df['returns'] <= 75) )
    between40_50 = sum( (df['returns'] > 40) & (df['returns'] <= 50) )
    between30_40 = sum( (df['returns'] > 30) & (df['returns'] <= 40) )
    between20_30 = sum( (df['returns'] > 20) & (df['returns'] <= 30) )
    between10_20 = sum( (df['returns'] > 10) & (df['returns'] <= 20) )
    between1_10 = sum( (df['returns'] > 1) & (df['returns'] <= 10) )
    between_m1_1 = sum( (df['returns'] > -1) & (df['returns'] <= 1) )
    between_m1_m10 = sum( (df['returns'] > -10) & (df['returns'] <= -1) )
    between_m10_m20 = sum( (df['returns'] > -20) & (df['returns'] <= -10) )
    between_m20_m30 = sum( (df['returns'] > -30) & (df['returns'] <= -20) )
    less_m30 = sum( (df['returns'] <= -30) )

    if include_breakup:
        positives = above75 + between50_75 + between40_50 + between30_40 + between20_30 + between10_20 + between1_10
        neutrals = between_m1_1
        negatives =  between_m1_m10 + between_m10_m20 + between_m20_m30 + less_m30

        result["positive_observation_no"] = positives
        result["neutral_observation_no"] = neutrals
        result["negative_observation_no"] = negatives

        result["positive_observation_perc"] = round((positives * 100) / ret_cnt, 2) if ret_cnt else None
        result["neutral_observation_perc"] = round((neutrals * 100) / ret_cnt, 2) if ret_cnt else None
        result["negative_observation_perc"] = round((negatives * 100) / ret_cnt, 2) if ret_cnt else None

        breakup = dict()
        if above75 > 0:
            breakup["above75"] = above75 
            breakup["above75_perc"] = round((above75 * 100 ) / ret_cnt , 2) if ret_cnt else None
        if between50_75 > 0:
            breakup["between50_75"] = between50_75     
            breakup["between50_75_perc"] = round((between50_75 * 100 ) / ret_cnt , 2) if ret_cnt else None     
        if between40_50 > 0:
            breakup["between40_50"] = between40_50     
            breakup["between40_50_perc"] = round((between40_50 * 100 ) / ret_cnt, 2) if ret_cnt else None        
        if between30_40 > 0:
            breakup["between30_40"] = between30_40
            breakup["between30_40_perc"] = round((between30_40 * 100 ) / ret_cnt, 2) if ret_cnt else None        
        if between20_30 > 0:
            breakup["between20_30"] = between20_30
            breakup["between20_30_perc"] = round((between20_30 * 100 ) / ret_cnt, 2) if ret_cnt else None
        if between10_20 > 0:
            breakup["between10_20"] = between10_20
            breakup["between10_20_perc"] = round((between10_20 * 100 ) / ret_cnt, 2) if ret_cnt else None
        if between1_10 > 0:
            breakup["between1_10"] = between1_10
            breakup["between1_10_perc"] = round((between1_10 * 100 ) / ret_cnt, 2) if ret_cnt else None
        if between_m1_1 > 0:
            breakup["between_m1_1"] = between_m1_1
            breakup["between_m1_1_perc"] = round((between_m1_1 * 100 ) / ret_cnt, 2) if ret_cnt else None
        if between_m1_m10 > 0:
            breakup["between_m1_m10"] = between_m1_m10
            breakup["between_m1_m10_perc"] = round((between_m1_m10 * 100 ) / ret_cnt, 2) if ret_cnt else None
        if between_m10_m20 > 0:
            breakup["between_m10_m20"] = between_m10_m20
            breakup["between_m10_m20_perc"] = round((between_m10_m20 * 100 ) / ret_cnt, 2) if ret_cnt else None
        if between_m20_m30 > 0:
            breakup["between_m20_m30"] = between_m20_m30
            breakup["between_m20_m30_perc"] = round((between_m20_m30 * 100 ) / ret_cnt, 2) if ret_cnt else None

        result["observation_breakup"] = breakup

    return result


def get_rolling_returns_old(db_session, plan_id, is_benchmark, timeframe_in_yr, transaction_date, include_breakup):
    result = dict()

    above75 = between50_75 = between40_50 = between30_40 = between20_30 = between10_20 = between1_10 = between_m1_1 = between_m1_m10 = between_m10_m20 = between_m20_m30 = less_m30 = 0

    min_returns = float('inf')
    max_returns = - float('inf')
    total_ret = 0

    rolling_returns = list()

    sql_q = db_session.query(NAV.NAV, NAV.NAV_Date).filter(NAV.Plan_Id == plan_id).filter(NAV.Is_Deleted != 1).filter(NAV.NAV_Date <= transaction_date)
    if is_benchmark:
        sql_q = sql_q.filter(NAV.NAV_Type == 'I')
    else:
        sql_q = sql_q.filter(NAV.NAV_Type == 'P')
    sql_objs = sql_q.order_by(NAV.NAV_Date).all()

    for sql_obj in sql_objs:
        old_nav_date = sql_obj.NAV_Date - relativedelta(years = timeframe_in_yr)
        
        sql_old_q = db_session.query(NAV.NAV).filter(NAV.Plan_Id == plan_id).filter(NAV.Is_Deleted != 1).filter(NAV.NAV_Date == old_nav_date)
        if is_benchmark:
            sql_old_q =  sql_old_q.filter(NAV.NAV_Type == 'I')
        else:
            sql_old_q =  sql_old_q.filter(NAV.NAV_Type == 'P')
        old_nav_value = sql_old_q.scalar()

        if old_nav_value and old_nav_value > 0:
            diff_perc = ((sql_obj.NAV - old_nav_value) / old_nav_value ) * 100.00
            rolling_returns.append(diff_perc)

            total_ret = total_ret + diff_perc
            if min_returns > diff_perc:
                min_returns = diff_perc
            if max_returns < diff_perc:
                max_returns = diff_perc

            if include_breakup:
                if diff_perc > 75:
                    above75 = above75 + 1
                elif diff_perc > 50 and diff_perc <= 75:
                    between50_75 = between50_75 + 1
                elif diff_perc > 40 and diff_perc <= 50:
                    between40_50 = between40_50 + 1
                elif diff_perc > 30 and diff_perc <= 40:
                    between30_40 = between30_40 + 1
                elif diff_perc > 20 and diff_perc <= 30:
                    between20_30 = between20_30 + 1
                elif diff_perc > 10 and diff_perc <= 20:
                    between10_20 = between10_20 + 1
                elif diff_perc > 1 and diff_perc <= 10:
                    between1_10= between1_10 + 1
                elif diff_perc > -1 and diff_perc <= 1:
                    between_m1_1 = between_m1_1 + 1
                elif diff_perc > -10 and diff_perc <= -1:
                    between_m1_m10 = between_m1_m10 + 1
                elif diff_perc > -20 and diff_perc <= -10:
                    between_m10_m20= between_m10_m20 + 1
                elif diff_perc > -30 and diff_perc <= -20:
                    between_m20_m30= between_m20_m30 + 1
                elif diff_perc <= -30:
                    less_m30 = less_m30 + 1

    ret_cnt = len(rolling_returns)
    med = statistics.median(rolling_returns) if ret_cnt else None

    result["total_observations_no"]  = ret_cnt
    # for any more ratios, use in-built functions from statistics module from stdlib
    result["median_returns"] = round(med, 2) if ret_cnt else None
    result["average_returns"] = round(total_ret/ret_cnt, 2) if ret_cnt else None
    result["max_returns"] = round(max_returns, 2) if ret_cnt else None
    result["min_returns"] = round(min_returns, 2) if ret_cnt else None

    if include_breakup:
        positives = above75 + between50_75 + between40_50 + between30_40 + between20_30 + between10_20 + between1_10
        neutrals = between_m1_1
        negatives =  between_m1_m10 + between_m10_m20 + between_m20_m30 + less_m30

        result["positive_observation_no"] = positives
        result["neutral_observation_no"] = neutrals
        result["negative_observation_no"] = negatives

        result["positive_observation_perc"] = round((positives * 100) / ret_cnt, 2) if ret_cnt else None
        result["neutral_observation_perc"] = round((neutrals * 100) / ret_cnt, 2) if ret_cnt else None
        result["negative_observation_perc"] = round((negatives * 100) / ret_cnt, 2) if ret_cnt else None

        breakup = dict()
        if above75 > 0:
            breakup["above75"] = above75 
            breakup["above75_perc"] = round((above75 * 100 ) / ret_cnt , 2) if ret_cnt else None
        if between50_75 > 0:
            breakup["between50_75"] = between50_75     
            breakup["between50_75_perc"] = round((between50_75 * 100 ) / ret_cnt , 2) if ret_cnt else None     
        if between40_50 > 0:
            breakup["between40_50"] = between40_50     
            breakup["between40_50_perc"] = round((between40_50 * 100 ) / ret_cnt, 2) if ret_cnt else None        
        if between30_40 > 0:
            breakup["between30_40"] = between30_40
            breakup["between30_40_perc"] = round((between30_40 * 100 ) / ret_cnt, 2) if ret_cnt else None        
        if between20_30 > 0:
            breakup["between20_30"] = between20_30
            breakup["between20_30_perc"] = round((between20_30 * 100 ) / ret_cnt, 2) if ret_cnt else None
        if between10_20 > 0:
            breakup["between10_20"] = between10_20
            breakup["between10_20_perc"] = round((between10_20 * 100 ) / ret_cnt, 2) if ret_cnt else None
        if between1_10 > 0:
            breakup["between1_10"] = between1_10
            breakup["between1_10_perc"] = round((between1_10 * 100 ) / ret_cnt, 2) if ret_cnt else None
        if between_m1_1 > 0:
            breakup["between_m1_1"] = between_m1_1
            breakup["between_m1_1_perc"] = round((between_m1_1 * 100 ) / ret_cnt, 2) if ret_cnt else None
        if between_m1_m10 > 0:
            breakup["between_m1_m10"] = between_m1_m10
            breakup["between_m1_m10_perc"] = round((between_m1_m10 * 100 ) / ret_cnt, 2) if ret_cnt else None
        if between_m10_m20 > 0:
            breakup["between_m10_m20"] = between_m10_m20
            breakup["between_m10_m20_perc"] = round((between_m10_m20 * 100 ) / ret_cnt, 2) if ret_cnt else None
        if between_m20_m30 > 0:
            breakup["between_m20_m30"] = between_m20_m30
            breakup["between_m20_m30_perc"] = round((between_m20_m30 * 100 ) / ret_cnt, 2) if ret_cnt else None

        result["observation_breakup"] = breakup

    return result


def get_commonstock_between_two_plans(plans, all_plans_details):
    allisin_data = dict()
    res = dict()
    common_totalstocks = 0
    percentportfoliooverlap_total_percent = 0

    for plan in plans:        
        for plans_details in all_plans_details:
            if int(plans_details["plan_id"]) == int(plan):
                for holding_details in plans_details["holdings"]:
                    if holding_details["security_isin"]:
                        if holding_details["security_isin"] in allisin_data:
                            allisin_data[holding_details["security_isin"]] = int(allisin_data[holding_details["security_isin"]]) + 1
                        else:
                            allisin_data[holding_details["security_isin"]] = 1
    
    for isin, count in allisin_data.items():
        if count == 2:
            common_totalstocks = common_totalstocks + 1

            for plan in plans:
                for plans_details in all_plans_details:
                    if int(plan) == int(plans_details["plan_id"]):  
                        for holding_dt in plans_details["holdings"]:
                            if holding_dt["security_isin"] == isin:
                                percentportfoliooverlap_total_percent = percentportfoliooverlap_total_percent + holding_dt["security_percentage"]
    
    #keep below commented if required in future
    res["plan_b"] = plans[1]
    res["common_totalstocks"] = common_totalstocks
    res["percentportfoliooverlap"] = percentportfoliooverlap_total_percent/2
    
    return res


def get_co_code_by_plan_id(db_session, plan_id):

    BenchmarkIndices1 = aliased(BenchmarkIndices)

    sql_co_code = db_session.query(BenchmarkIndices1.Co_Code).select_from(BenchmarkIndices1).join(BenchmarkIndices, BenchmarkIndices1.TRI_Co_Code == BenchmarkIndices.Co_Code).join(MFSecurity, MFSecurity.BenchmarkIndices_Id == BenchmarkIndices.BenchmarkIndices_Id).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).filter(MFSecurity.Status_Id == 1).filter(Plans.Is_Deleted != 1).filter(Plans.Plan_Id == plan_id).first()

    if not sql_co_code:
        sql_co_code = db_session.query(BenchmarkIndices.Co_Code).select_from(BenchmarkIndices).join(MFSecurity, MFSecurity.BenchmarkIndices_Id == BenchmarkIndices.BenchmarkIndices_Id).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).filter(MFSecurity.Status_Id == 1).filter(Plans.Is_Deleted != 1).filter(Plans.Plan_Id == plan_id).first()
    
    return sql_co_code.Co_Code if sql_co_code else None


def get_max_indexweightage_bydate(db_session, transactiondate, plan_id):
    co_code = get_co_code_by_plan_id(db_session, plan_id)
    date_4 = transactiondate - timedelta(days=4)

    sql_index_weight_1m = db_session.query(func.max(IndexWeightage.WDATE).label('max_date'))\
                                    .join(BenchmarkIndices, BenchmarkIndices.Co_Code == IndexWeightage.Index_CO_CODE)\
                                    .filter(IndexWeightage.Is_Deleted != 1, IndexWeightage.WDATE >= date_4, IndexWeightage.WDATE <= transactiondate, BenchmarkIndices.Co_Code == co_code).scalar()

    return sql_index_weight_1m


def get_aumandfundcountbyproduct(db_session, isin_code, product_id):
    resp = dict()
    aum = 0 
    count = 0 

    sql_favoritestockdetail = db_session.query(FundStocks.Value_In_Inr, FundStocks.Fund_Id).filter(FundStocks.ISIN_Code == isin_code).filter(FundStocks.Product_Id == product_id).filter(FundStocks.ISIN_Code.like("INE%")).filter(FundStocks.InstrumentType == 'Equity').distinct().all()
    
    if sql_favoritestockdetail:
        for favoritestockdetail in sql_favoritestockdetail:
            if favoritestockdetail.Value_In_Inr != None:
                aum = aum + favoritestockdetail.Value_In_Inr                
            count = count + 1

    resp["aum"] = aum / 10000000 if aum != None and aum != 0 else 0
    resp["count"] = count

    return resp   


def get_aum_monthwise(db_session, plan_id, transaction_date, getdatein_milisecond=False):
    resp = list()

    if plan_id:
        if not transaction_date:
            transaction_date = get_last_transactiondate(db_session, plan_id)
    if transaction_date:
        sql_factsheetdata = db_session.query(func.max(FactSheet.NetAssets_Rs_Cr),
                                            extract('year', FactSheet.TransactionDate),
                                            extract('month', FactSheet.TransactionDate))\
                                            .filter(FactSheet.Plan_Id == plan_id, FactSheet.TransactionDate <= transaction_date, FactSheet.Is_Deleted != 1)\
                                            .group_by(extract('year', FactSheet.TransactionDate), extract('month', FactSheet.TransactionDate))\
                                            .order_by(extract('year', FactSheet.TransactionDate), extract('month', FactSheet.TransactionDate))\
                                            .all()

        if sql_factsheetdata:
            for sql_factsheet in sql_factsheetdata:
                if sql_factsheet[0]:
                    if sql_factsheet[0] > 0:
                        data_dict = {}

                        enddate = last_date_of_month(sql_factsheet[1], sql_factsheet[2])
                        new_enddate = datetime.combine(enddate,datetime.time(datetime.today()))

                        if getdatein_milisecond == True:
                            data_dict["asofdate"] = trunc(new_enddate.timestamp()) * 1000
                        else:
                            data_dict["asofdate"] = new_enddate.timestamp()

                        data_dict["aum_in_cr"] = sql_factsheet[0]

                        resp.append(data_dict)

    return resp


def get_fund_change(db_session, plan_id, portfolio_date, change_category, scheme_id, hide_holding_weightage):
    results = list()

    sql_objs = db_session.query(PortfolioHoldings.Plan_Id, PortfolioHoldings.Portfolio_Date, PortfolioHoldings.Current_Weight, PortfolioHoldings.Difference_Weight, PortfolioHoldings.Security_Name, PortfolioHoldings.ISIN_Code).select_from(PortfolioHoldings).join(FactSheet, and_(FactSheet.Plan_Id == PortfolioHoldings.Plan_Id, FactSheet.Portfolio_Date == PortfolioHoldings.Portfolio_Date)).filter(PortfolioHoldings.Plan_Id == plan_id).filter(PortfolioHoldings.Is_Deleted != 1).filter(PortfolioHoldings.Holding_Type == change_category).filter(PortfolioHoldings.Portfolio_Date == portfolio_date).filter(FactSheet.Is_Deleted != 1).distinct().order_by(PortfolioHoldings.Difference_Weight).all()

    for sql_obj in sql_objs:
        obj = dict()
        obj["security_new_weight"] = 0 if hide_holding_weightage else sql_obj.Current_Weight
        obj["weight_difference"] = sql_obj.Difference_Weight
        obj["security_name"] = sql_obj.Security_Name
        obj["security_isin"] = sql_obj.ISIN_Code
        obj["scheme_id"] = scheme_id
        if sql_obj.ISIN_Code:
            obj.update(get_bsensecode(db_session, sql_obj.ISIN_Code))

        results.append(obj)

    return results


def number_to_word(number):
    def get_word(n):
        words={ 0:"", 1:"One", 2:"Two", 3:"Three", 4:"Four", 5:"Five", 6:"Six", 7:"Seven", 8:"Eight", 9:"Nine", 10:"Ten", 11:"Eleven", 12:"Twelve", 13:"Thirteen", 14:"Fourteen", 15:"Fifteen", 16:"Sixteen", 17:"Seventeen", 18:"Eighteen", 19:"Nineteen", 20:"Twenty", 30:"Thirty", 40:"Forty", 50:"Fifty", 60:"Sixty", 70:"Seventy", 80:"Eighty", 90:"Ninty" }
        if n<=20:
            return words[n]
        else:
            ones=n%10
            tens=n-ones
            return words[tens]+" "+words[ones]
            
    def get_all_word(n):
        d=[100,10,100,100]
        v=["","Hundred And","Thousand","lakh"]
        w=[]
        for i,x in zip(d,v):
            t=get_word(n%i)
            if t!="":
                t+=" "+x
            w.append(t.rstrip(" "))
            n=n//i
        w.reverse()
        w=' '.join(w).strip()
        if w.endswith("And"):
            w=w[:-3]
        return w

    arr=str(number).split(".")
    number=int(arr[0])
    crore=number//10000000
    number=number%10000000
    word=""
    if crore>0:
        word+=get_all_word(crore)
        word+=" crore "
    word+=get_all_word(number).strip()+" Only"
    if len(arr)>1:
         if len(arr[1])==1:
            arr[1]+="0"
         word+=""
    return word


def get_last_transaction_portfolio_dates(db_session, plan_id):
    transaction_date = None
    portfolio_date = None

    # get last transactiondate and Portfolio_Date
    factsheet_query = db_session.query(FactSheet).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.Is_Deleted != 1).order_by(desc(FactSheet.TransactionDate)).first()

    if factsheet_query:        
        transaction_date = factsheet_query.TransactionDate
        portfolio_date = factsheet_query.Portfolio_Date
        
    return transaction_date, portfolio_date   

# TODO: KM: This function was left to be refactored in the underlying holdings refactor phase 1, needs review.
def prepare_plan_holdings(db_session, plan_list):
    plan_dfs = dict()
    for plan_id in plan_list:
        fund_id = get_fundid_byplanid(db_session, plan_id)
        transaction_date, portfolio_date = get_last_transaction_portfolio_dates(db_session, plan_id)

        if transaction_date and portfolio_date:
            sql_holdings = db_session.query(HoldingSecurity.HoldingSecurity_Name.label('name'),
                                            UnderlyingHoldings.ISIN_Code.label('isin'),
                                            UnderlyingHoldings.Percentage_to_AUM.label('weight'),
                                            Sector.Sector_Name.label('sector'),
                                            HoldingSecurity.HoldingSecurity_Type.label('holding_security_type'),
                                            UnderlyingHoldings.Portfolio_Date.label('portfolio_date'),
                                            FactSheet.NetAssets_Rs_Cr.label('aum'))\
                                      .join(Fund, Fund.Fund_Id == UnderlyingHoldings.Fund_Id)\
                                      .join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id)\
                                      .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                      .join(FactSheet, FactSheet.Plan_Id == Plans.Plan_Id)\
                                      .join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == UnderlyingHoldings.HoldingSecurity_Id)\
                                      .join(Sector, HoldingSecurity.Sector_Id == Sector.Sector_Id)\
                                      .filter(UnderlyingHoldings.Fund_Id == fund_id, UnderlyingHoldings.Portfolio_Date == portfolio_date)\
                                      .filter(UnderlyingHoldings.Is_Deleted != 1, FactSheet.TransactionDate == transaction_date)\
                                      .filter(Plans.Is_Deleted != 1, FactSheet.Is_Deleted != 1).distinct().all()

            plan_dfs[plan_id] = pd.DataFrame(sql_holdings)

    return plan_dfs


def get_weights_from_df(df, col_name):
    """
    col_name : This parameter refers to the column name for 'name' column in the dataframe
    """
    resp = list()
    for idx in df.index:
        resp.append({
            "name" : df[col_name][idx],
            "weight_a": round(df['weight_a'][idx], 2),
            "weight_b": round(df['weight_b'][idx], 2),
        })
    return resp


def find_portfolio_overlap_with_isin(df_a, df_b):
    overlap = dict()

    all_df = pd.merge(df_a, df_b, how='outer', on=["isin"], suffixes=['_a', '_b'])
    all_df.loc[pd.isnull(all_df["name_a"]), "name_a"] = all_df["name_b"]
    all_df.rename(columns={"name_a":"name"}, inplace=True)
    all_df = all_df.drop(["name_b", "sector_a", "sector_b"], axis=1)
    all_df = all_df.fillna(0)
    all_df["weight_diff"] = all_df["weight_a"] - all_df["weight_b"]

    overlap["securities_overlap"] = list()
    unique_df = all_df.loc[(all_df["weight_a"] != 0) & (all_df["weight_b"] != 0)]

    for idx in unique_df.index:
        overlap["securities_overlap"].append({
                "name" : unique_df['name'][idx],
                "weight_a": round(unique_df['weight_a'][idx], 2),
                "weight_b": round(unique_df['weight_b'][idx], 2),
        })

    unique_model_df = all_df.loc[(all_df["weight_a"] == 0) & (all_df["weight_b"] != 0)]
    unique_investor_df = all_df.loc[(all_df["weight_a"] != 0) & (all_df["weight_b"] == 0)]
    investor_overweight_df = all_df.loc[(all_df["weight_a"] != 0) & (all_df["weight_b"] != 0) & (all_df["weight_diff"] > 0)]
    investor_underweight_df = all_df.loc[(all_df["weight_a"] != 0) & (all_df["weight_b"] != 0) & (all_df["weight_diff"] < 0)]

    overlap["common_securities"] = len(unique_df.index)

    overlap["fund_a_common_weight"] = unique_df["weight_a"].sum()
    overlap["fund_a_unique_weight"] = df_a["weight"].sum() - unique_df["weight_a"].sum()
    overlap["fund_a_total_securities"] = len(df_a.index)

    overlap["fund_b_common_weight"] = unique_df["weight_b"].sum()
    overlap["fund_b_unique_weight"] = df_b["weight"].sum() - unique_df["weight_b"].sum()
    overlap["fund_b_total_securities"] = len(df_b.index)

    overlap["attribution"] = dict()
    overlap["attribution"]["model_unique"] = get_weights_from_df(unique_model_df, 'name')
    overlap["attribution"]["investor_unique"] = get_weights_from_df(unique_investor_df, 'name')
    overlap["attribution"]["investor_overweight"] = get_weights_from_df(investor_overweight_df, 'name')
    overlap["attribution"]["investor_underweight"] = get_weights_from_df(unique_model_df, 'name')

    return overlap


def find_portfolio_overlap(df_a, df_b, include_sector_overlap):
    overlap = dict()
    common_df = pd.merge(df_a, df_b, how='inner', on=["name", "isin", "sector"], suffixes=['_a', '_b'])

    overlap["common_securities"] = len(common_df.index)

    overlap["fund_a_common_weight"] = common_df["weight_a"].sum()
    overlap["fund_a_unique_weight"] = df_a["weight"].sum() - common_df["weight_a"].sum()
    overlap["fund_a_total_securities"] = len(df_a.index)

    overlap["fund_b_common_weight"] = common_df["weight_b"].sum()
    overlap["fund_b_unique_weight"] = df_b["weight"].sum() - common_df["weight_b"].sum()
    overlap["fund_b_total_securities"] = len(df_b.index)

    overlap["securities_overlap"] = list()
    for idx in common_df.index:
        overlap["securities_overlap"].append({
                "name" : common_df['name'][idx],
                "weight_a": round(common_df['weight_a'][idx], 2), 
                "weight_b": round(common_df['weight_b'][idx], 2), 
        })
    
    if include_sector_overlap:
        sector_a=df_a.groupby(['sector'], dropna=False, as_index=False).agg(weight = ("weight", "sum"))
        sector_b=df_b.groupby(['sector'], dropna=False, as_index=False).agg(weight = ("weight", "sum"))
        common_sectors = pd.merge(sector_a, sector_b, how='inner', on=["sector"], suffixes=['_a', '_b'])

        overlap["sector_overlap"] = list()
        for idx in common_sectors.index:
            overlap["sector_overlap"].append({
                    "name" : common_sectors['sector'][idx],
                    "weight_a": round(common_sectors['weight_a'][idx], 2), 
                    "weight_b": round(common_sectors['weight_b'][idx], 2), 
            })

    return overlap

# Format: isin, name, weight, sector (order is NOT important)
def find_portfolio_overlap_v2(left, right, include_detailed, include_sector_overlap, require_aum_weightage_overlap=True):
    overlap = dict()

    # Prepare master list
    if not left.empty:
        left["weight"] = left["weight"].apply(pd.to_numeric, downcast='float') 
        # left["aum"] = left["aum"].apply(pd.to_numeric, downcast='float') 

        # left['aum_weight'] = ((left['weight']*left['aum']) / 100)

    if not right.empty:    
        right["weight"] = right["weight"].apply(pd.to_numeric, downcast='float')
        # right["aum"] = right["aum"].apply(pd.to_numeric, downcast='float') 

        # right['aum_weight'] = ((right['weight']*right['aum']) / 100)

    # Merge the 2 datasets and prepare the data for the overlap
    if not left.empty and not right.empty:
        all_df = pd.merge(left, right, how='outer', on=["isin"], suffixes=['_a', '_b'])
        all_df.loc[pd.isnull(all_df["name_a"]), "name_a"] = all_df["name_b"]
        all_df.loc[pd.isnull(all_df["sector_a"]), "sector_a"] = all_df["sector_b"]
        all_df.rename(columns={"name_a":"name", "sector_a":"sector"}, inplace=True)
        all_df = all_df.drop(["name_b", "sector_b"], axis=1)
        all_df = all_df.fillna(0)
        all_df["weight_diff"] = all_df["weight_a"] - all_df["weight_b"]

        # Security Overlap for common stocks
        common_df = all_df.loc[(all_df["weight_a"] != 0) & (all_df["weight_b"] != 0)]
    
        # calculate the AUM based weights
        if require_aum_weightage_overlap:
            # common_df['aum_a'] = common_df['aum_a'].apply(pd.to_numeric, downcast='float')
            # common_df['aum_b'] = common_df['aum_b'].apply(pd.to_numeric, downcast='float')
            
            #uncomment below if aum based overlap is required
            # common_df['aum_weight_a'] = common_df["weight_a"] * common_df['aum_a']
            # common_df['aum_weight_b'] = common_df["weight_b"] * common_df['aum_b']
            
            # total_combined_aum = common_df.iloc[0, common_df.columns.get_loc('aum_a')] + \
            #                     common_df.iloc[0, common_df.columns.get_loc('aum_b')] if not common_df.empty else None
            # common_df['aum_weight_a'] = common_df['aum_weight_a']/total_combined_aum
            # common_df['aum_weight_b'] = common_df['aum_weight_b']/total_combined_aum

            # AUM based weight impacts only for reflecting common weight, rest logic remains same
            overlap["securities_info"] = dict()
            overlap["securities_info"]["common_securities"] = len(common_df.index)

            #uncomment below if aum based overlap is required
            # if require_aum_weightage_overlap:
            #     overlap["securities_info"]["left_common_weight"] = round(common_df["weight_a"].sum(), 4) if not common_df.empty else None 
            #     overlap["securities_info"]["right_common_weight"] = round(common_df["weight_b"].sum(), 4) if not common_df.empty else None
            # else:
            overlap["securities_info"]["left_common_weight"] = round(common_df["weight_a"].sum(), 4) if not common_df.empty else None 
            overlap["securities_info"]["right_common_weight"] = round(common_df["weight_b"].sum(), 4) if not common_df.empty else None

            

            # left_uncommon_df = pd.merge(left, common_df, how='left', on=["isin"], suffixes=['_a', '_b'], indicator=True)
            # left_uncommon_df = left_uncommon_df[left_uncommon_df['_merge']=='left_only']

            # right_uncommon_df = pd.merge(right, common_df, how='left', on=["isin"], suffixes=['_a', '_b'], indicator=True)
            # right_uncommon_df = right_uncommon_df[right_uncommon_df['_merge']=='left_only']

            overlap["securities_info"]["left_unique_weight"] = left["weight"].sum() - common_df["weight_a"].sum()
            overlap["securities_info"]["left_total_securities"] = len(left.index)
            overlap["securities_info"]["right_unique_weight"] = right["weight"].sum() - common_df["weight_b"].sum()

            if 'aum_a' in common_df.columns and 'aum_b' in common_df.columns:
                overlap["securities_info"]["left_aum"] = common_df.iloc[0]['aum_a']
                overlap["securities_info"]["right_aum"] = common_df.iloc[0]['aum_b']

            # overlap["securities_info"]["left_unique_weight"] = round(left_uncommon_df["aum_weight"].sum(), 4) / total_combined_aum
            # overlap["securities_info"]["left_total_securities"] = len(left.index)
            # overlap["securities_info"]["right_unique_weight"] = round(right_uncommon_df["aum_weight"].sum(), 4) / total_combined_aum

            overlap["securities_info"]["right_total_securities"] = len(right.index)
            overlap["securities_overlap"] = get_weights_from_df(common_df, 'name')

            # Detailed Security Overlap
            if include_detailed:
                unique_left = all_df.loc[(all_df["weight_a"] != 0) & (all_df["weight_b"] == 0)]
                unique_right = all_df.loc[(all_df["weight_a"] == 0) & (all_df["weight_b"] != 0)]
                left_overweight = all_df.loc[(all_df["weight_a"] != 0) & (all_df["weight_b"] != 0) & (all_df["weight_diff"] > 0)]
                right_overweight = all_df.loc[(all_df["weight_a"] != 0) & (all_df["weight_b"] != 0) & (all_df["weight_diff"] < 0)]

                overlap["securities_detailed"] = dict()
                overlap["securities_detailed"]["unique_left"] = get_weights_from_df(unique_left, 'name')
                overlap["securities_detailed"]["unique_right"] = get_weights_from_df(unique_right, 'name')
                overlap["securities_detailed"]["left_overweight"] = get_weights_from_df(left_overweight, 'name')
                overlap["securities_detailed"]["right_overweight"] = get_weights_from_df(right_overweight, 'name')
            
            # Sector Overlap
            if include_sector_overlap:
                sector_l=left.groupby(['sector'], dropna=False, as_index=False).agg(weight = ("weight", "sum"))
                sector_r=right.groupby(['sector'], dropna=False, as_index=False).agg(weight = ("weight", "sum"))
                common_sectors = pd.merge(sector_l, sector_r, how='inner', on=["sector"], suffixes=['_a', '_b'])
                overlap["sector_overlap"] = get_weights_from_df(common_sectors, 'sector')

    return overlap


def get_plan_overlap(db_session, plan_id_list):    
    plan_dfs = prepare_plan_holdings(db_session, plan_id_list)

    total_pfs = list(plan_dfs.keys())

    overlap_a = dict()
    for plan_a in total_pfs:
        overlap_b = dict()
        for plan_b in total_pfs:
            if plan_a == plan_b:
                continue

            df_a = plan_dfs[plan_a]
            df_b = plan_dfs[plan_b]

            overlap = find_portfolio_overlap_v2(df_a, df_b, True, True)
            overlap_b[str(plan_b)] = overlap

        overlap_a[str(plan_a)] = overlap_b

    return overlap_a

def get_trailing_return_and_riskanalysis(db_session, plan_list, portfolio_asof_date=None):
    trailing_return = list()
    riskanalysis = list()

    #get Trailing Returns (%)
    for plan in plan_list:
        res_factsheet = get_performancetrend_data(db_session, plan, None)
        if res_factsheet:
            res = dict()
            res["plan_id"] = plan
            res["plan_name"] = res_factsheet["plan_name"]
            res["inception_date"] = res_factsheet["inception_date"]

            res_fact = dict()
            res_fact["bm_ret_1m"] = res_factsheet["bm_ret_1m"]
            res_fact["bm_ret_3m"] = res_factsheet["bm_ret_3m"]
            res_fact["bm_ret_6m"] = res_factsheet["bm_ret_6m"]
            res_fact["bm_ret_1y"] = res_factsheet["bm_ret_1y"]
            res_fact["bm_ret_2y"] = res_factsheet["bm_ret_2y"]
            res_fact["bm_ret_3y"] = res_factsheet["bm_ret_3y"]
            res_fact["bm_ret_5y"] = res_factsheet["bm_ret_5y"]
            res_fact["bm_ret_ince"] = res_factsheet["bm_ret_ince"]

            res_fact["scheme_ret_1m"] = res_factsheet["scheme_ret_1m"]
            res_fact["scheme_ret_3m"] = res_factsheet["scheme_ret_3m"]
            res_fact["scheme_ret_6m"] = res_factsheet["scheme_ret_6m"]
            res_fact["scheme_ret_1y"] = res_factsheet["scheme_ret_1y"]
            res_fact["scheme_ret_2y"] = res_factsheet["scheme_ret_2y"]
            res_fact["scheme_ret_3y"] = res_factsheet["scheme_ret_3y"]
            res_fact["scheme_ret_5y"] = res_factsheet["scheme_ret_5y"]
            res_fact["scheme_ret_ince"] = res_factsheet["scheme_ret_ince"]

            res["returns"] = res_fact
            trailing_return.append(res)

            #get Risk Analysis
            res = dict()
            riskanal = dict()
            res["plan_id"] = plan
            res["plan_name"] = res_factsheet["plan_name"]
            resp_dic = get_fundriskratio_data(db_session,plan,None)

            riskanal["scheme_id"] = resp_dic["scheme_id"] 
            riskanal["scheme_code"] = resp_dic["scheme_code"] 
            riskanal["date"] = resp_dic["date"] 
            riskanal["standard_deviation_1_y"] = resp_dic["standard_deviation_1_y"]
            riskanal["standard_deviation_3_y"] = resp_dic["standard_deviation_3_y"]
            riskanal["sharpe_ratio_1_y"] = resp_dic["sharpe_ratio_1_y"] 
            riskanal["sharpe_ratio_3_y"] = resp_dic["sharpe_ratio_3_y"]     
            riskanal["beta_1_y"] = resp_dic["beta_1_y"] 
            riskanal["beta_3_y"] = resp_dic["beta_3_y"] 
            riskanal["r_square_1_y"] = resp_dic["r_square_1_y"]
            riskanal["r_square_3_y"] = resp_dic["r_square_3_y"]
            riskanal["r_square_3_y"] = resp_dic["r_square_3_y"]
            riskanal["treynor_ratio_1_y"] = resp_dic["treynor_ratio_1_y"]
            riskanal["treynor_ratio_3_y"] = resp_dic["treynor_ratio_3_y"]            


            factsheet_query = db_session.query(FactSheet).filter(FactSheet.Plan_Id == plan).filter(FactSheet.Is_Deleted != 1)

            if portfolio_asof_date:
                factsheet_query = factsheet_query.filter(FactSheet.Portfolio_Date == portfolio_asof_date)

            factsheet = factsheet_query.order_by(desc(FactSheet.TransactionDate)).first()

            if factsheet:
                transaction_date = factsheet.TransactionDate

            sql_current = db_session.query(FactSheet.ModifiedDuration_yrs, FactSheet.AvgMktCap_Rs_Cr, FactSheet.PortfolioP_ERatio).filter(FactSheet.TransactionDate == transaction_date).filter(FactSheet.Plan_Id == plan).filter(FactSheet.Is_Deleted != 1).order_by(desc(FactSheet.TransactionDate)).first()

            modifiedduration_yrs = None
            avgmktcap_rs_cr = None
            portfoliop_eratio = None

            if sql_current:
                modifiedduration_yrs = sql_current.ModifiedDuration_yrs if sql_current.ModifiedDuration_yrs else None
                avgmktcap_rs_cr = sql_current.AvgMktCap_Rs_Cr if sql_current.AvgMktCap_Rs_Cr else None
                portfoliop_eratio = sql_current.PortfolioP_ERatio if sql_current.PortfolioP_ERatio else None

            riskanal["modified_duration"] = modifiedduration_yrs
            riskanal["avgmktcap_rs_cr"] = avgmktcap_rs_cr
            riskanal["pe_ratio"] = portfoliop_eratio

            res["analysis"] = riskanal
            riskanalysis.append(res)

    return trailing_return, riskanalysis


def get_scheme_details(db_session, plan_id, transaction_date):
    
    fundid = get_fundid_byplanid(db_session, plan_id)
    
    sql_factsheet = db_session.query(Plans.Plan_Id, Plans.Plan_Name, FactSheet.TransactionDate, Fund.Fund_Id, Fund.fund_comments, FactSheet.Portfolio_Date, MFSecurity.BenchmarkIndices_Id, BenchmarkIndices.BenchmarkIndices_Name, BenchmarkIndices.TRI_Co_Code, Product.Product_Name, Product.Product_Code, Product.Product_Id, AMC.AMC_Id, AMC.AMC_Logo, MFSecurity.MF_Security_Investment_Strategy, MFSecurity.Fees_Structure, MFSecurity.Classification_Id, Classification.Classification_Name, FactSheet.NetAssets_Rs_Cr, MFSecurity.MF_Security_OpenDate, FactSheet.Risk_Grade, MFSecurity.MF_Security_Min_Purchase_Amount, FactSheet.Exit_Load, BenchmarkIndices.Attribution_Flag, Fund.HideAttribution, FactSheet.ExpenseRatio, PlanType.PlanType_Id, PlanType.PlanType_Name, Plans.ISIN, Fund.AIF_SPONSOR_COMMITMENT_IN_CR, Fund.AIF_TARGET_FUND_SIZE_IN_CR, Fund.AIF_INVESTMENT_THEME, Fund.AIF_MIN_INVESTMENT_AMOUNT, Fund.AIF_INITIAL_DRAWDOWN, Fund.AIF_TENURE_OF_FUND, Fund.AIF_CURRENCY, Fund.AIF_CATEGORY, Fund.AIF_SUB_CATEGORY, Fund.AIF_CLASS_OF_UNITS, Fund.AIF_MANAGEMENT_EXPENSES, Fund.AIF_ADMIN_EXPENSES, Fund.AIF_SET_UP_FEE, Fund.AIF_HURDLE_RATE, Fund.AIF_PERFORMANCE_FEES, FactSheet.AIF_COMMITEDCAPITAL_Rs_Cr, FactSheet.AIF_DRAWDOWNCAPITAL_Rs_Cr, FactSheet.AIF_CAPITALRETURNED_Rs_Cr, FactSheet.AIF_INITIALCLOSUREDATE, FactSheet.AIF_FUNDCLOSUREDATE, FactSheet.AIF_ALLOTMENTDATE, FactSheet.AIF_DOLLAR_NAV, FactSheet.AIF_FUND_RATING, Fund.Is_Closed_For_Subscription, Fund.AIF_NRI_INVESTMENT_ALLOWED, FundType.FundType_Name)\
    .select_from(FactSheet)\
    .join(Plans, Plans.Plan_Id == FactSheet.Plan_Id)\
    .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
    .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
    .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
    .join(Classification, MFSecurity.Classification_Id == Classification.Classification_Id)\
    .join(BenchmarkIndices, BenchmarkIndices.BenchmarkIndices_Id == MFSecurity.BenchmarkIndices_Id, isouter=True)\
    .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
    .join(AMC, AMC.AMC_Id == MFSecurity.AMC_Id)\
    .join(PlanType, PlanType.PlanType_Id == Plans.PlanType_Id)\
    .join(FundType, MFSecurity.FundType_Id == FundType.FundType_Id, isouter=True)\
    .filter(Plans.Is_Deleted != 1).filter(FactSheet.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).filter(Fund.Is_Deleted != 1).filter(MFSecurity.Status_Id == 1).filter(Plans.Plan_Id == plan_id).filter(FactSheet.TransactionDate == transaction_date).filter(FactSheet.Is_Deleted != 1).filter(PlanProductMapping.Is_Deleted != 1)\
    .one_or_none()

    
    
    res = dict()

    if sql_factsheet:
        res["plan_id"] = sql_factsheet.Plan_Id
        res["plan_name"] = sql_factsheet.Plan_Name
        res["transaction_date"] = sql_factsheet.TransactionDate
        res["transaction_date"] = sql_factsheet.TransactionDate
        res["fund_id"] = sql_factsheet.Fund_Id
        res["portfolio_date"] = sql_factsheet.Portfolio_Date
        res["benchmark_indices_id"] = sql_factsheet.BenchmarkIndices_Id
        res["benchmark_indices_name"] = sql_factsheet.BenchmarkIndices_Name
        res["product_name"] = sql_factsheet.Product_Name
        res["product_code"] = sql_factsheet.Product_Code
        res["product_id"] = sql_factsheet.Product_Id
        res["classification_id"] = sql_factsheet.Classification_Id
        res["classification_name"] = sql_factsheet.Classification_Name
        res["amc_id"] = sql_factsheet.AMC_Id
        res["amc_logo"] = sql_factsheet.AMC_Logo
        res["expense_ratio"] = sql_factsheet.ExpenseRatio
        res["investment_strategy"] = sql_factsheet.MF_Security_Investment_Strategy
        res["fees_structure"] = sql_factsheet.Fees_Structure
        res["aum"] = sql_factsheet.NetAssets_Rs_Cr
        res["inception_date"] = sql_factsheet.MF_Security_OpenDate
        res["risk_grade"] = sql_factsheet.Risk_Grade
        res["min_purchase_amount"] = sql_factsheet.MF_Security_Min_Purchase_Amount
        res["exit_load"] = sql_factsheet.Exit_Load
        res["comments"] = sql_factsheet.fund_comments
        res["plan_type_id"] = sql_factsheet.PlanType_Id
        res["plan_type_name"] = sql_factsheet.PlanType_Name
        res["fund_type"] = sql_factsheet.FundType_Name

        #AIF fund details
        res["isin"] = sql_factsheet.ISIN
        res["aif_sponsor_commitment_in_cr"] = sql_factsheet.AIF_SPONSOR_COMMITMENT_IN_CR
        res["aif_target_fund_size_in_cr"] = sql_factsheet.AIF_TARGET_FUND_SIZE_IN_CR
        res["aif_investment_theme"] = sql_factsheet.AIF_INVESTMENT_THEME
        res["aif_min_investment_amount_in_cr"] = sql_factsheet.AIF_MIN_INVESTMENT_AMOUNT
        res["aif_initial_drawdown"] = sql_factsheet.AIF_INITIAL_DRAWDOWN
        res["aif_tenure_of_fund_in_months"] = sql_factsheet.AIF_TENURE_OF_FUND
        res["aif_currency"] = sql_factsheet.AIF_CURRENCY
        res["aif_category"] = sql_factsheet.AIF_CATEGORY
        res["aif_sub_category"] = sql_factsheet.AIF_SUB_CATEGORY
        res["aif_class_of_units"] = sql_factsheet.AIF_CLASS_OF_UNITS
        res["aif_management_expenses"] = sql_factsheet.AIF_MANAGEMENT_EXPENSES
        res["aif_admin_expenses"] = sql_factsheet.AIF_ADMIN_EXPENSES
        res["aif_set_up_fees"] = sql_factsheet.AIF_SET_UP_FEE
        res["aif_hurdle_rate"] = sql_factsheet.AIF_HURDLE_RATE
        res["aif_performance_fees"] = sql_factsheet.AIF_PERFORMANCE_FEES
        res["aif_commitedcapital_in_cr"] = sql_factsheet.AIF_COMMITEDCAPITAL_Rs_Cr
        res["aif_drawdowncapital_in_cr"] = sql_factsheet.AIF_DRAWDOWNCAPITAL_Rs_Cr
        res["aif_capitalreturned_in_cr"] = sql_factsheet.AIF_CAPITALRETURNED_Rs_Cr
        res["aif_initialclosure_date"] = sql_factsheet.AIF_INITIALCLOSUREDATE
        res["aif_fundclosure_date"] = sql_factsheet.AIF_FUNDCLOSUREDATE
        res["aif_allotment_date"] = sql_factsheet.AIF_ALLOTMENTDATE
        res["aif_dollar_nav"] = sql_factsheet.AIF_DOLLAR_NAV
        res["aif_fund_rating"] = sql_factsheet.AIF_FUND_RATING
        res["aif_investment_style"] = None
        res["aif_subscription_status"] = 'Closed for Subscription' if sql_factsheet.Is_Closed_For_Subscription == 1 else 'Open for Subscription'
        res["aif_nri_investment_allowed"] = 'No' if sql_factsheet.Is_Closed_For_Subscription == 1 else 'Yes'
        

        min_purchase_amount_inwords = None
        if sql_factsheet.MF_Security_Min_Purchase_Amount:
            min_purchase_amount_inwords = number_to_word(sql_factsheet.MF_Security_Min_Purchase_Amount)
        res["min_purchase_amount_inwords"] = min_purchase_amount_inwords

        max_nav_date = get_max_navdate_tilldate(db_session, plan_id, transaction_date)
        res["nav_date"] = max_nav_date
        res["nav"] = get_navbydate(db_session, plan_id, max_nav_date) if max_nav_date else None
        res = get_default_attribution_dates(db_session, res, fundid, plan_id, transaction_date, sql_factsheet.HideAttribution)
        
    return res

def get_default_attribution_dates(db_session, res_dict, fundid, planid, transaction_date, hide_attribution):
    factsheet_fromdate = None
    factsheet_todate = None
    attribution_fromdate = None
    attribution_todate = None
    attribution_flag = 0

    if not fundid:
        sql_fund = get_sql_fund_byplanid(db_session, planid)
        fundid = sql_fund.Fund_Id
        hide_attribution = sql_fund.HideAttribution if not hide_attribution else hide_attribution

    attribution_todate = db_session.query(func.max(UnderlyingHoldings.Portfolio_Date).label("Portfolio_Date")).filter(UnderlyingHoldings.Portfolio_Date <= transaction_date).filter(UnderlyingHoldings.Fund_Id == fundid).filter(UnderlyingHoldings.Is_Deleted != 1).scalar()
    
    if attribution_todate:
        transactiondate1 = str(attribution_todate.year) + "-" + str(attribution_todate.month) + "-" + str(attribution_todate.day)
        transactiondate1 = strptime(transactiondate1, '%Y-%m-%d')

        month1from = getbetweendate(1,0,transactiondate1)
        month1to = getbetweendate(1,0,transactiondate1,False)

        attribution_fromdate = db_session.query(func.min(UnderlyingHoldings.Portfolio_Date).label("Portfolio_Date")).filter(UnderlyingHoldings.Portfolio_Date <= month1to).filter(UnderlyingHoldings.Portfolio_Date >= month1from).filter(UnderlyingHoldings.Fund_Id == fundid).filter(UnderlyingHoldings.Is_Deleted != 1).scalar()

        if attribution_fromdate and attribution_fromdate:
            attribution_fromdate_1 = attribution_fromdate - timedelta(days=1) 
            attribution_fromdate_2 = attribution_fromdate - timedelta(days=2)
            attribution_fromdate_3 = attribution_fromdate - timedelta(days=3) 

            factsheet_fromdate = db_session.query(func.max(FactSheet.TransactionDate)).filter(FactSheet.Is_Deleted != 1).filter(FactSheet.Plan_Id == planid).filter(or_(FactSheet.TransactionDate == attribution_fromdate, FactSheet.TransactionDate == attribution_fromdate_1)).filter(FactSheet.Is_Deleted != 1).scalar()

            if not factsheet_fromdate:
                factsheet_fromdate = db_session.query(func.max(FactSheet.TransactionDate)).filter(FactSheet.Is_Deleted != 1).filter(FactSheet.Plan_Id == planid).filter(or_(FactSheet.TransactionDate == attribution_fromdate_2, FactSheet.TransactionDate == attribution_fromdate_3)).filter(FactSheet.Is_Deleted != 1).scalar()

            attribution_todate_1 = attribution_todate - timedelta(days=1) 
            attribution_todate_2 = attribution_todate - timedelta(days=2)
            attribution_todate_3 = attribution_todate - timedelta(days=3) 

            factsheet_todate = db_session.query(func.max(FactSheet.TransactionDate)).filter(FactSheet.Is_Deleted != 1).filter(FactSheet.Plan_Id == planid).filter(or_(FactSheet.TransactionDate == attribution_todate, FactSheet.TransactionDate == attribution_todate_1)).filter(FactSheet.Is_Deleted != 1).scalar()

            if not factsheet_todate:
                factsheet_todate = db_session.query(func.max(FactSheet.TransactionDate)).filter(FactSheet.Is_Deleted != 1).filter(FactSheet.Plan_Id == planid).filter(or_(FactSheet.TransactionDate == attribution_todate_2, FactSheet.TransactionDate == attribution_todate_3)).filter(FactSheet.Is_Deleted != 1).scalar()

        #check attribution flag logic
        # if sql_factsheet.Attribution_Flag:
        #     if sql_factsheet.Attribution_Flag == 1:
        if attribution_fromdate and attribution_fromdate and factsheet_fromdate and factsheet_todate:
            if attribution_fromdate >= factsheet_fromdate and attribution_todate >= factsheet_todate and hide_attribution == 0:
                attribution_flag = 1
    
    res_dict["attribution_flag"] = attribution_flag
    res_dict["attribution_from"] = attribution_fromdate
    res_dict["attribution_to"] = attribution_todate

    return res_dict

def get_portfolio_characteristics(db_session, plan_id, transaction_date):
    '''
        Get the portfolio characteristics custom response for api and pdf output.
    '''

    df_factsheet = get_portfolio_characteristics_by_plan_ids(db_session, transaction_date, list_plan_ids=[plan_id])

    res = dict()
    if df_factsheet.shape[0] == 1:
        sql_fund = get_sql_fund_byplanid(db_session, plan_id)
        fund_id = sql_fund.Fund_Id
        asset_class_name = db_session.query(AssetClass.AssetClass_Name).join(MFSecurity, MFSecurity.AssetClass_Id == AssetClass.AssetClass_Id)\
                                                                       .filter(MFSecurity.Fund_Id == fund_id).first()[0]
        res["scheme_id"] = fund_id
        res["scheme_code"] = sql_fund.Fund_Code
        res["scheme_asset_class_name"] = asset_class_name
        res["auto_populate"] = sql_fund.AutoPopulate
        res["date"] = transaction_date.strftime("%Y-%m-%d")
        res["total_stocks"] = round(df_factsheet['total_stocks'].iloc[0],0) if df_factsheet['total_stocks'].iloc[0] else None
        res["avg_mkt_cap"] = round(df_factsheet['avg_mcap_cr'].iloc[0], 2) if df_factsheet['avg_mcap_cr'].iloc[0] else df_factsheet['avg_mcap_cr'].iloc[0]
        res["p_e"] = round(df_factsheet['pe'].iloc[0], 2) if df_factsheet['pe'].iloc[0] else df_factsheet['pe'].iloc[0]
        res["p_b"] = round(df_factsheet['pbv'].iloc[0], 2) if df_factsheet['pbv'].iloc[0] else df_factsheet['pbv'].iloc[0]
        res["dividend_yield"] = round(df_factsheet['div_yld'].iloc[0], 2) if df_factsheet['div_yld'].iloc[0] else df_factsheet['div_yld'].iloc[0]
        res["macaulay_duration_yrs"] = round(df_factsheet['macaulay_duration_yrs'].iloc[0], 2) if df_factsheet['macaulay_duration_yrs'].iloc[0] else df_factsheet['macaulay_duration_yrs'].iloc[0]
        res["avg_maturity_yrs"] = round(df_factsheet['avg_maturity_yrs'].iloc[0], 2) if df_factsheet['avg_maturity_yrs'].iloc[0] else df_factsheet['avg_maturity_yrs'].iloc[0]
        res["modified_duration_yrs"] = round(df_factsheet['modified_duration_yrs'].iloc[0], 2) if df_factsheet['modified_duration_yrs'].iloc[0] else df_factsheet['modified_duration_yrs'].iloc[0]
        res["avg_credit_rating"] = df_factsheet['avg_credit_rating'].iloc[0]
        res["yield_to_maturity"] = round(df_factsheet['ytm'].iloc[0], 2) if df_factsheet['ytm'].iloc[0] else df_factsheet['ytm'].iloc[0]

    return res


def get_portfolio_characteristics_by_plan_ids(db_session, asof_date, list_plan_ids):
    '''
        Get the portfolio characteristics data for a list of fund/plan isins/plan_ids.
        Dataframe columns: total_stocks, pe, pbv, div_yld, avg_mcap_cr, macaulay_duration_yrs, avg_maturity_yrs, modified_duration_yrs, avg_credit_rating, ytm
        Delta for backdated data is 3 days.
    '''
    delta = 3
    backdated_date = asof_date - timedelta(days=delta)

    sql_factsheet_sq = db_session.query(func.max(FactSheet.TransactionDate).label('TransactionDate'),
                                        FactSheet.Plan_Id)\
                                 .filter(FactSheet.Plan_Id.in_(list_plan_ids),
                                         FactSheet.TransactionDate <= asof_date,
                                         FactSheet.TransactionDate >= backdated_date,
                                         FactSheet.Is_Deleted != 1)\
                                 .group_by(FactSheet.Plan_Id).subquery()

    sql_factsheet_data = db_session.query(Plans.Plan_Id,
                                          FactSheet.TotalStocks.label('total_stocks'),
                                          FactSheet.PortfolioP_ERatio.label('pe'),
                                          FactSheet.Portfolio_Dividend_Yield.label('div_yld'),
                                          FactSheet.AvgMktCap_Rs_Cr.label('avg_mcap_cr'),
                                          FactSheet.PortfolioP_BRatio.label('pbv'),
                                          FactSheet.Macaulay_Duration_Yrs.label('macaulay_duration_yrs'),
                                          FactSheet.AvgMaturity_Yrs.label('avg_maturity_yrs'),
                                          FactSheet.ModifiedDuration_yrs.label('modified_duration_yrs'),
                                          FactSheet.AvgCreditRating.label('avg_credit_rating'),
                                          FactSheet.Yield_To_Maturity.label('ytm'))\
                                     .join(sql_factsheet_sq, and_(sql_factsheet_sq.c.Plan_Id == FactSheet.Plan_Id,
                                                                     sql_factsheet_sq.c.TransactionDate == FactSheet.TransactionDate))\
                                     .join(Plans, Plans.Plan_Id == sql_factsheet_sq.c.Plan_Id)\
                                     .filter(FactSheet.Is_Deleted != 1,
                                             Plans.Is_Deleted != 1)\
                                     .order_by(desc(FactSheet.TransactionDate)).all()

    df_factsheet = pd.DataFrame(sql_factsheet_data)
                                                                 
    return df_factsheet


def get_marketcap_composition(db_session, plan_id, transaction_date, composition_for):
    """
    composition_for: accepted values for the parameter can be 'fund_level', 'category_wise_all_funds'
    """
    if plan_id:
        if not transaction_date:
            transaction_date = get_last_transactiondate(db_session, plan_id)

        sql_factsheet_qry = db_session.query(PortfolioAnalysis.Attribute_Text,
                                             PortfolioAnalysis.Attribute_Value,
                                             PortfolioAnalysis.Plan_Id)\
                                      .join(FactSheet, and_(FactSheet.Plan_Id == PortfolioAnalysis.Plan_Id, FactSheet.Portfolio_Date == PortfolioAnalysis.Portfolio_Date))\
                                      .filter(FactSheet.TransactionDate == transaction_date, FactSheet.Is_Deleted != 1,
                                              PortfolioAnalysis.Attribute_Type == "Market_Cap", PortfolioAnalysis.Is_Deleted != 1)\

        if composition_for == 'fund_level':
            sql_factsheet = sql_factsheet_qry.filter(PortfolioAnalysis.Plan_Id == plan_id)\
                                             .order_by(desc(PortfolioAnalysis.Attribute_Value)).all()
        elif composition_for == 'category_wise_all_funds':
            fund_category_info = get_fund_category(db_session, plan_id)

            sql_factsheet = sql_factsheet_qry.join(Plans, Plans.Plan_Id == PortfolioAnalysis.Plan_Id)\
                                             .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                             .join(PlanProductMapping, and_(PlanProductMapping.Plan_Id == Plans.Plan_Id, PlanProductMapping.Product_Id == fund_category_info[2]))\
                                             .filter(Plans.Is_Deleted != 1, MFSecurity.Is_Deleted != 1, MFSecurity.Status_Id == 1)\
                                             .filter(MFSecurity.Classification_Id == fund_category_info[0], MFSecurity.AssetClass_Id == fund_category_info[1])\
                                             .filter(PortfolioAnalysis.Attribute_Text != '')\
                                             .order_by(desc(PortfolioAnalysis.Attribute_Value)).all()

        res = []
        no_value = None
        lst_response_keys = ['large_cap', 'mid_cap', 'small_cap', 'unlisted']

        if len(sql_factsheet) != 0:
            df_mcap_comp = pd.DataFrame(sql_factsheet)
            df_mcap_comp['Attribute_Text'] = df_mcap_comp['Attribute_Text'].str.lower()
            df_mcap_comp['Attribute_Text'] = df_mcap_comp['Attribute_Text'].str.replace(' ', '_')

            df_mcap_comp_pv = df_mcap_comp.pivot(index='Plan_Id', columns='Attribute_Text', values='Attribute_Value')
            df_mcap_comp_pv.reset_index(inplace=True)
            df_mcap_comp_pv.drop(['Plan_Id'], axis=1, inplace=True)
            df_mcap_comp_pv = df_mcap_comp_pv.mean().to_frame().T
            df_mcap_comp_pv = df_mcap_comp_pv.round(2)

            fund_id = get_fundid_byplanid(db_session, plan_id)
            res = df_mcap_comp_pv.to_dict(orient="records")
            res[0]["scheme_id"] = fund_id
            res[0]["date"] = transaction_date
            # add 'NA' to missing keys
            missing_keys = lst_response_keys - res[0].keys()
            res[0].update(dict.fromkeys(missing_keys, no_value))


    return res


def get_riskrating(db_session, plan_id, transaction_date):
    resp = list() 
    sql_factsheet = db_session.query(PortfolioAnalysis.Attribute_Text, PortfolioAnalysis.Attribute_Value).join(FactSheet, and_(FactSheet.Plan_Id == PortfolioAnalysis.Plan_Id, FactSheet.Portfolio_Date == PortfolioAnalysis.Portfolio_Date)).filter(FactSheet.TransactionDate == transaction_date).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.Is_Deleted != 1).filter(PortfolioAnalysis.Attribute_Type == "Risk_Ratings").filter(PortfolioAnalysis.Is_Deleted != 1).order_by(desc(PortfolioAnalysis.Attribute_Value)).all()  

    if sql_factsheet:
        fund_id = get_fundid_byplanid(db_session, plan_id)
        for sql_data in sql_factsheet:
            res = dict()
            res["risk_category"] = sql_data.Attribute_Text
            res["Percentage_to_AUM"] = round(sql_data.Attribute_Value, 2)
            res["scheme_id"] = fund_id
            res["date"] = transaction_date
        
            resp.append(res)
    return resp


def get_instrumenttype(db_session, plan_id, transaction_date):
    resp = list() 

    sql_factsheet = db_session.query(PortfolioAnalysis.Attribute_Text,
                                     PortfolioAnalysis.Attribute_Value)\
                              .join(FactSheet, and_(FactSheet.Plan_Id == PortfolioAnalysis.Plan_Id, FactSheet.Portfolio_Date == PortfolioAnalysis.Portfolio_Date))\
                              .filter(FactSheet.TransactionDate == transaction_date)\
                              .filter(FactSheet.Plan_Id == plan_id)\
                              .filter(FactSheet.Is_Deleted != 1)\
                              .filter(PortfolioAnalysis.Attribute_Type == "Instrument_Type")\
                              .filter(PortfolioAnalysis.Is_Deleted != 1).order_by(desc(PortfolioAnalysis.Attribute_Value)).all()

    if sql_factsheet:
        fund_id = get_fundid_byplanid(db_session, plan_id)
        for sql_data in sql_factsheet:
            res = dict()
            res["instrument_type"] = sql_data.Attribute_Text
            res["Percentage_to_AUM"] = round(sql_data.Attribute_Value, 2)
            res["scheme_id"] = fund_id
            res["date"] = transaction_date

            resp.append(res)

    return resp


def get_investmentstyle(db_session, plan_id, transaction_date):

    resp = list()
    resp_dic = dict()
    respons = list()
    scheme_id = None
    date = None
    largecap_blend = "NA"
    largecap_growth = "NA"
    largecap_value = "NA"
    midcap_blend = "NA"
    midcap_growth = "NA"
    midcap_value = "NA"
    smallcap_blend = "NA"
    smallcap_growth = "NA"
    smallcap_value = "NA"

    sql_factsheet = db_session.query(PortfolioAnalysis.Attribute_Text, PortfolioAnalysis.Attribute_Sub_Text, PortfolioAnalysis.Attribute_Value).join(FactSheet, and_(FactSheet.Plan_Id == PortfolioAnalysis.Plan_Id, FactSheet.Portfolio_Date == PortfolioAnalysis.Portfolio_Date)).filter(FactSheet.TransactionDate == transaction_date).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.Is_Deleted != 1).filter(PortfolioAnalysis.Attribute_Type == "Stocks_Rank").filter(PortfolioAnalysis.Is_Deleted != 1).order_by(PortfolioAnalysis.Attribute_Text).all()  

    if sql_factsheet:
        fund_id = get_fundid_byplanid(db_session, plan_id)
        for sql_data in sql_factsheet:
            res = dict()
            res["StocksRank"] = sql_data.Attribute_Text
            res["Equity_Style"] = sql_data.Attribute_Sub_Text
            res["Percentage_to_AUM"] = round(sql_data.Attribute_Value, 2)
            res["scheme_id"] = fund_id
            res["date"] = transaction_date
        
            resp.append(res)    

    if resp:
        for response in resp:            
            scheme_id = response["scheme_id"]
            date = response["date"]

            if response["StocksRank"] == "Large Cap" and response["Equity_Style"] == "Growth":
                largecap_growth = response["Percentage_to_AUM"]
            elif response["StocksRank"] == "Large Cap" and response["Equity_Style"] == "Value":
                largecap_value = response["Percentage_to_AUM"]
            elif response["StocksRank"] == "Large Cap" and response["Equity_Style"] == "Blend":
                largecap_blend = response["Percentage_to_AUM"]
            elif response["StocksRank"] == "Mid Cap" and response["Equity_Style"] == "Growth":
                midcap_growth = response["Percentage_to_AUM"]
            elif response["StocksRank"] == "Mid Cap" and response["Equity_Style"] == "Value":
                midcap_value = response["Percentage_to_AUM"]
            elif response["StocksRank"] == "Mid Cap" and response["Equity_Style"] == "Blend":
                midcap_blend = response["Percentage_to_AUM"]
            elif response["StocksRank"] == "Small Cap" and response["Equity_Style"] == "Growth":
                smallcap_growth = response["Percentage_to_AUM"]
            elif response["StocksRank"] == "Small Cap" and response["Equity_Style"] == "Value":
                smallcap_value = response["Percentage_to_AUM"]
            elif response["StocksRank"] == "Small Cap" and response["Equity_Style"] == "Blend":
                smallcap_blend = response["Percentage_to_AUM"]
           
        
        sql_fund = get_sql_fund_byplanid(db_session, plan_id)
        resp_dic["scheme_id"] = sql_fund.Fund_Id
        resp_dic["scheme_code"] = sql_fund.Fund_Code
        resp_dic["date"] = date
        resp_dic["large_cap_blend"] = largecap_blend
        resp_dic["large_cap_growth"] = largecap_growth
        resp_dic["large_cap_value"] = largecap_value

        resp_dic["mid_cap_blend"] = midcap_blend
        resp_dic["mid_cap_growth"] = midcap_growth
        resp_dic["mid_cap_value"] = midcap_value

        resp_dic["small_cap_blend"] = smallcap_blend
        resp_dic["small_cap_growth"] = smallcap_growth
        resp_dic["small_cap_value"] = smallcap_value
        
        respons.append(resp_dic)
    
    return respons


def get_fund_changes(db_session, plan_id, portfolio_date, change_category, scheme_id, hide_holding_weightage):
    results = list()

    sql_objs = db_session.query(PortfolioHoldings.Plan_Id, 
                                PortfolioHoldings.Portfolio_Date, 
                                PortfolioHoldings.Current_Weight, 
                                PortfolioHoldings.Difference_Weight, 
                                PortfolioHoldings.Security_Name, 
                                PortfolioHoldings.ISIN_Code).select_from(PortfolioHoldings)\
                                                            .join(FactSheet, and_(FactSheet.Plan_Id == PortfolioHoldings.Plan_Id, FactSheet.Portfolio_Date == PortfolioHoldings.Portfolio_Date))\
                                                            .filter(PortfolioHoldings.Plan_Id == plan_id)\
                                                            .filter(PortfolioHoldings.Is_Deleted != 1)\
                                                            .filter(PortfolioHoldings.Holding_Type == change_category)\
                                                            .filter(FactSheet.TransactionDate == portfolio_date)\
                                                            .filter(FactSheet.Is_Deleted != 1).distinct()\
                                                            
    if change_category == 'Increase_Exposure':
        sql_objs = sql_objs.order_by(PortfolioHoldings.Difference_Weight.desc()).limit(5).all()
    else:
        sql_objs = sql_objs.order_by(PortfolioHoldings.Difference_Weight).limit(5).all()


    for sql_obj in sql_objs:
        obj = dict()
        obj["security_new_weight"] = 0 if hide_holding_weightage else sql_obj.Current_Weight
        obj["weight_difference"] = sql_obj.Difference_Weight
        obj["security_name"] = sql_obj.Security_Name
        obj["security_isin"] = sql_obj.ISIN_Code
        obj["scheme_id"] = scheme_id
        if sql_obj.ISIN_Code:
            obj.update(get_bsensecode(db_session, sql_obj.ISIN_Code))

        results.append(obj)

    return results


def get_fund_portfolio_change(db_session, plan_id, change_category, transaction_date):
    resp = list()
    scheme_id = None
    hide_holding_weightage = False
    
    if plan_id:
        if not transaction_date:
            transaction_date = get_last_transactiondate(db_session, plan_id) 

    sql_fund = db_session.query(Fund.HideHoldingWeightage, Fund.HidePortfolioHoldingChanges, Fund.Fund_Id).select_from(Fund).join(MFSecurity, Fund.Fund_Id == MFSecurity.Fund_Id).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).filter(MFSecurity.Status_Id == 1).filter(Plans.Plan_Id == plan_id).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).all()

    sql_factsheet_count =  db_session.query(func.count(FactSheet.Plan_Id)).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.Is_Deleted != 1).scalar()
  
    if sql_fund:
        hide_holding_weightage = sql_fund[0].HideHoldingWeightage
        hide_portfolio_changes = sql_fund[0].HidePortfolioHoldingChanges
        scheme_id = sql_fund[0].Fund_Id

    if sql_factsheet_count < 2:
        hide_portfolio_changes = True

    if hide_portfolio_changes:
        return resp

    # TODO Code Refactor: Add an enum to represent the change category field and make appropriate changes to all the references.
    if change_category == "decrease":
        resp = get_fund_changes(db_session, plan_id, transaction_date, 'Decrease_Exposure', scheme_id, hide_holding_weightage)
    elif change_category == "increase":
        resp = get_fund_changes(db_session, plan_id, transaction_date, 'Increase_Exposure', scheme_id, hide_holding_weightage)
    elif change_category == "entry":
        resp = get_fund_changes(db_session, plan_id, transaction_date, 'New_Entrants', scheme_id, hide_holding_weightage)
    elif change_category == "exit":
        resp = get_fund_changes(db_session, plan_id, transaction_date, 'Complete_Exit', scheme_id, hide_holding_weightage)
    
    return resp


def get_fund_nav(db_session, plan_id, transaction_date, dataset_type=None, start_date=None, end_date=None):
    resp = list()

    if not transaction_date:
        transaction_date = get_last_transactiondate(db_session, plan_id)

    dt = db_session.query(MFSecurity.Classification_Id, Plans.Plan_Name).select_from(MFSecurity).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).filter(Plans.Plan_Id == plan_id).first()
    
    if dt.Classification_Id == 147:#Other: ETFs ---> showing adjusted nav bcoz ETF may include face value change, Bonus, dividend 
        sql_q = db_session.query(NAV.NAV_Date, NAV.RAW_NAV.label('NAV'))        
    else:
        sql_q = db_session.query(NAV.NAV_Date, NAV.NAV)
        

    sql_navdata = sql_q.filter(NAV.NAV_Type=="P", NAV.Plan_Id == plan_id, NAV.Is_Deleted != 1)\
                        .filter(NAV.NAV_Date <= transaction_date)
    
    if start_date and end_date:#in case of nav export we will pass dates
        sql_navdata = sql_navdata.filter(NAV.NAV_Date >= start_date, NAV.NAV_Date <= end_date)

    if sql_navdata:
        if dataset_type == "latest":
            sql_navdata = sql_navdata.order_by(NAV.NAV_Date.desc()).first()
            res = dict()
            res["scheme_name"] = dt.Plan_Name
            res["nav_date"] = sql_navdata.NAV_Date
            res["nav"] = round(sql_navdata.NAV, 2)
            resp.append(res)
        else:
            sql_navdata = sql_navdata.order_by(NAV.NAV_Date).all()
            for sql_nav in sql_navdata:
                res = dict()
                res["scheme_name"] = dt.Plan_Name
                res["nav_date"] = sql_nav.NAV_Date
                res["nav"] = round(sql_nav.NAV, 2)
                resp.append(res)

    return resp


def get_rollingreturn(db_session, plan_id, transaction_date, timeframe_in_yr, is_annualized_return=False):
    res = dict()

    if not transaction_date:
            transaction_date = get_last_transactiondate(db_session, plan_id)

    scheme_result = get_rolling_returns(db_session, plan_id, False, timeframe_in_yr, transaction_date, include_breakup=True, get_only_raw_data=False, is_annualized_return=is_annualized_return)

    sql_obj = db_session.query(BenchmarkIndices.BenchmarkIndices_Id, BenchmarkIndices.BenchmarkIndices_Name, Plans.Plan_Name, Fund.Fund_Id, Fund.Fund_Code).select_from(Plans).join(MFSecurity, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).join(BenchmarkIndices, BenchmarkIndices.BenchmarkIndices_Id == MFSecurity.BenchmarkIndices_Id).join(Fund, Fund.Fund_Id==MFSecurity.Fund_Id).filter(MFSecurity.Status_Id == 1).filter(Plans.Plan_Id == plan_id).first()

    res["scheme_name"] = sql_obj.Plan_Name
    res["benchmark_name"] = sql_obj.BenchmarkIndices_Name
    res["scheme_id"] = sql_obj.Fund_Id
    res["scheme_code"] = sql_obj.Fund_Code
    res["date"] = transaction_date

    benchmark_id = sql_obj.BenchmarkIndices_Id

    index_result = get_rolling_returns(db_session, benchmark_id, True, timeframe_in_yr, transaction_date, False, False, is_annualized_return)

    res["scheme_min_return"] = scheme_result["min_returns"] if scheme_result else None
    res["scheme_max_return"] = scheme_result["max_returns"] if scheme_result else None
    res["scheme_avg_return"] = scheme_result["average_returns"] if scheme_result else None
    res["scheme_med_return"] = scheme_result["median_returns"] if scheme_result else None

    res["benchmark_min_return"] = index_result["min_returns"] if index_result else None
    res["benchmark_max_return"] = index_result["max_returns"] if index_result else None
    res["benchmark_avg_return"] = index_result["average_returns"] if index_result else None
    res["benchmark_med_return"] = index_result["median_returns"] if index_result else None

    res["total_observations_no"] = scheme_result["total_observations_no"] if scheme_result else None
    res["positive_observation_no"] = scheme_result["positive_observation_no"] if scheme_result else None
    res["neutral_observation_no"] = scheme_result["neutral_observation_no"] if scheme_result else None
    res["negative_observation_no"] = scheme_result["negative_observation_no"] if scheme_result else None
    res["positive_observation_perc"] = scheme_result["positive_observation_perc"] if scheme_result else None
    res["neutral_observation_perc"] = scheme_result["neutral_observation_perc"] if scheme_result else None
    res["negative_observation_perc"] = scheme_result["negative_observation_perc"] if scheme_result else None

    res["observation_breakup"] = scheme_result["observation_breakup"] if scheme_result else None

    return res


def get_detailed_fund_risk_ratios(db_session, plan_id, transaction_date):
    sql_product_id = db_session.query(PlanProductMapping.Product_Id).filter(PlanProductMapping.Is_Deleted != 1).filter(PlanProductMapping.Plan_Id == plan_id).scalar()
    
    is_monthly_factsheet = True
    if sql_product_id:
        if sql_product_id == 1 or sql_product_id == 2:
            is_monthly_factsheet = False

    transactiondate1 = strptime(transaction_date, '%Y-%m-%d')
    det_period = dict()
    period = list()
    
    month1to = getbetweendate(0,0,transactiondate1,False, is_monthly_factsheet)
    det_period["desc"] = "current_date"
    det_period["date"] = month1to
    period.append(det_period)

    det_period = dict()
    month3to = getbetweendate(3,0,transactiondate1,False)
    det_period["desc"] = "3_month"
    det_period["date"] = month3to
    period.append(det_period)

    det_period = dict()
    month6to = getbetweendate(6,0,transactiondate1,False)
    det_period["desc"] = "6_month"
    det_period["date"] = month6to
    period.append(det_period)

    det_period = dict()
    year1to = getbetweendate(0,1,transactiondate1,False)
    det_period["desc"] = "1_year"
    det_period["date"] = year1to
    period.append(det_period)

    det_period = dict()
    year3to = getbetweendate(0,3,transactiondate1,False)
    det_period["desc"] = "3_year"
    det_period["date"] = year3to
    period.append(det_period)

    resp = dict()
    for item in period:        
        sql_factsheet = db_session.query(FactSheet).filter(FactSheet.Is_Deleted != 1).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.TransactionDate == item["date"]).one_or_none()

        if sql_factsheet:
            resp_dic = get_fundriskratio_data(db_session, plan_id, item["date"])

            res = dict()
            resp_dic["period"] = item["desc"]
            resp_dic["is_visible"] = 1
            # res["data"] = resp_dic
            resp[item["desc"]] = resp_dic
            # resp.append(resp_dic)
        else:
            res = dict()
            res["period"] = item["desc"]
            res["is_visible"] = 0
            # res["data"] = None
            resp[item["desc"]] = res
            # resp.append(res)
    return resp



def get_attributions(db_session, from_date, to_date, plan_id, gsquare_url, benchmark_id=None):
    resp = dict()

    if plan_id:
        #check if factsheet is available for given to date
        sql_factsheet = db_session.query(FactSheet).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.TransactionDate == to_date).filter(FactSheet.Is_Deleted != 1).one_or_none()
        if not sql_factsheet:
            to_date = to_date - timedelta(days=1)
            sql_factsheet = db_session.query(FactSheet).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.TransactionDate == to_date).filter(FactSheet.Is_Deleted != 1).one_or_none()

        if not sql_factsheet:
            to_date = to_date - timedelta(days=2)
            sql_factsheet = db_session.query(FactSheet).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.TransactionDate == to_date).filter(FactSheet.Is_Deleted != 1).one_or_none()
            
        if not sql_factsheet:
            raise BadRequest("Factsheet not available for given date.")
        
        sql_benchmark = db_session.query(MFSecurity.BenchmarkIndices_Id).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).filter(Plans.Plan_Id == plan_id).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).one_or_none()

        if not benchmark_id:
            benchmark_id = sql_benchmark.BenchmarkIndices_Id

        att_dates = "[\"" + str(from_date.strftime('%Y-%m-%d')) + "\",\""+ str(to_date.strftime('%Y-%m-%d')) +"\"]"
        sql_attribution = db_session.query(FactsheetAttribution.Response_Attribution).filter(FactsheetAttribution.Plan_Id == plan_id).filter(FactsheetAttribution.BenchmarkIndices_Id==benchmark_id).filter(FactsheetAttribution.Dates == att_dates).filter(FactsheetAttribution.Is_Deleted != 1).all()

        if not sql_attribution:
            months = diff_month(to_date, from_date)
            period = "NA"
            if months < 12:
                period = str(months) + "M"
            elif months > 11 and months < 24:
                period = "1Y"
            elif months == 24:
                period = "2Y"
            elif months > 24:
                period = "3Y"

            
            #TODO call Gsquare api directly from here
            attri_gsquare_resp = generate_attributions(from_date, to_date, plan_id, benchmark_id, period, gsquare_url, db_session)

            sql_attribution = db_session.query(FactsheetAttribution.Response_Attribution).filter(FactsheetAttribution.Plan_Id == plan_id).filter(FactsheetAttribution.Dates == "[\"" + str(from_date.strftime('%Y-%m-%d')) + "\",\""+ str(to_date.strftime('%Y-%m-%d')) +"\"]").filter(FactsheetAttribution.BenchmarkIndices_Id==benchmark_id).filter(FactsheetAttribution.Is_Deleted != 1).all()
            if sql_attribution:
                resp = sql_attribution[0][0]
            else:
                if attri_gsquare_resp:
                    raise BadRequest(F"Gsquare response - {attri_gsquare_resp.text}")
        else:
            resp = sql_attribution[0][0]

    return resp


def attribution_validations(db_session, plan_id, to_date):

    sql_benchmark = db_session.query(MFSecurity.BenchmarkIndices_Id).join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).filter(Plans.Plan_Id == plan_id).filter(Plans.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).one_or_none()

    if sql_benchmark:
        if not sql_benchmark.BenchmarkIndices_Id:
            return 'Benchmark not available.'
    else:
        return 'Benchmark not available.'

    bench_nav = get_navbydate(db_session, plan_id, to_date, 'P')
    if not bench_nav:
        return 'Strategy NAV not available.'

    # plan_nav = get_navbydate(db_session, sql_benchmark.BenchmarkIndices_Id, to_date, 'I')
    # if not plan_nav:
    #     return 'Benchmark NAV not available.'


def get_portfolio_date(db_session, plan_id, transactiondate):
    portfolio_date = None

    factsheet_query = db_session.query(FactSheet).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.Is_Deleted != 1, FactSheet.TransactionDate==transactiondate).first()

    if factsheet_query:        
        portfolio_date = factsheet_query.Portfolio_Date

    return portfolio_date


def get_organization_whitelabel(db_session, organization_id):
    resp = dict()

    disclaimer = None
    logo = None

    if organization_id:
        sql_organization = db_session.query(Organization).filter(Organization.Organization_Id == organization_id, Organization.Is_WhiteLabel_Value == 1).first()

        if sql_organization:
            disclaimer = sql_organization.disclaimer
            logo = sql_organization.Logo_Img 
    
    sql_organization = db_session.query(Organization).filter(Organization.Organization_Id == 10027).first()

    if sql_organization:
        if not disclaimer:
            disclaimer = sql_organization.disclaimer
        if not logo:
            if sql_organization.Logo_Img:
                logo = sql_organization.Logo_Img 
            else:
                logo = 'logo.png'

    resp["disclaimer"] = disclaimer
    resp["logo"] = logo 

    return resp


def get_organizationid_by_userid(db_session, user_id):
    resp = dict()

    organization_id = db_session.query(User.Organization_Id).filter(User.User_Id == user_id).scalar()

    return organization_id


def get_fundcomparedata_planwise(db_session, transactiondate, plan_id):
    data = dict()
    data["plan_id"] = plan_id
    portfolio_date = None
    transaction_date = None

    if transactiondate == '0':
        transaction_date = get_last_transactiondate( db_session, plan_id)
    else:
        transaction_date = datetime.strptime(transactiondate, '%Y-%m-%d')
    
    holding_query1 =  db_session.query(UnderlyingHoldings.Portfolio_Date)\
                                .join(MFSecurity,MFSecurity.Fund_Id == UnderlyingHoldings.Fund_Id)\
                                .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                .join(FactSheet, and_(Plans.Plan_Id == FactSheet.Plan_Id, FactSheet.Portfolio_Date == UnderlyingHoldings.Portfolio_Date))\
                                .filter(MFSecurity.Status_Id == 1,
                                        FactSheet.Is_Deleted != 1,
                                        Plans.Is_Deleted != 1,
                                        MFSecurity.Is_Deleted != 1,
                                        UnderlyingHoldings.Is_Deleted != 1,
                                        FactSheet.TransactionDate == transaction_date,
                                        Plans.Plan_Id == plan_id)\
                                .order_by(desc(UnderlyingHoldings.Portfolio_Date)).first()
    if holding_query1:        
        portfolio_date = holding_query1.Portfolio_Date

    #Get fund holdings 
    fund_holdings = list()       
    fund_holdings = get_fund_holdings(db_session, plan_id, portfolio_date)
    data["fund_holdings"] = fund_holdings

    #get Fund manager
    fundmanager = list()
    fundmanager = getfundmanager(db_session, plan_id)
    data["fund_manager"] = fundmanager

    #Get composition
    composition = list()
    composition = get_compositiondata(db_session, plan_id, transaction_date, composition_for='fund_level')
    data["composition"] = composition

    marketcap_composition = list()
    marketcap_composition = get_marketcap_composition(db_session, plan_id, transaction_date, composition_for='fund_level')
    data["marketcap_composition"] = marketcap_composition[0] if marketcap_composition else []

    #Get sector weights
    sector = list()
    sector = get_sectorweightsdata(db_session, plan_id, transaction_date, composition_for = 'fund_level')
    data["sector"] = sector

    sql_factsheet = db_session.query(Fund.Fund_Id, Fund.Fund_Code, Fund.Fund_Name, AMC.AMC_Id, AMC.AMC_Code, AMC.AMC_Logo, Classification.Classification_Name, MFSecurity.MF_Security_OpenDate, BenchmarkIndices.BenchmarkIndices_Name, MFSecurity.MF_Security_Min_Purchase_Amount,FactSheet.NetAssets_Rs_Cr,FactSheet.TransactionDate,FactSheet.ExpenseRatio, FactSheet.Exit_Load, MFSecurity.Fees_Structure, MFSecurity.MF_Security_Investment_Strategy, FactSheet.TotalStocks, FactSheet.PortfolioP_ERatio, FactSheet.Portfolio_Dividend_Yield, FactSheet.AvgMktCap_Rs_Cr, FactSheet.PortfolioP_BRatio, FactSheet.Risk_Grade).select_from(FactSheet).join(Plans, Plans.Plan_Id == FactSheet.Plan_Id).join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id).join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id).join(AMC, AMC.AMC_Id == MFSecurity.AMC_Id).join(Classification, Classification.Classification_Id == MFSecurity.Classification_Id).join(BenchmarkIndices, BenchmarkIndices.BenchmarkIndices_Id == MFSecurity.BenchmarkIndices_Id).filter(Plans.Is_Deleted != 1).filter(FactSheet.Is_Deleted != 1).filter(MFSecurity.Is_Deleted != 1).filter(Fund.Is_Deleted != 1).filter(Classification.Is_Deleted != 1).filter(BenchmarkIndices.Is_Deleted != 1).filter(MFSecurity.Status_Id == 1).filter(Plans.Plan_Id == plan_id).filter(FactSheet.TransactionDate == transaction_date).one_or_none()

    if sql_factsheet:
        data["amc_logo"] = sql_factsheet.AMC_Logo
        data["amc_id"] = sql_factsheet.AMC_Id
        data["classification"] = sql_factsheet.Classification_Name
        data["inception_date"] = sql_factsheet.MF_Security_OpenDate
        data["benchmark_name"] = sql_factsheet.BenchmarkIndices_Name
        data["min_investment"] = sql_factsheet.MF_Security_Min_Purchase_Amount
        data["exit_load"] = sql_factsheet.Exit_Load
        data["fee_structure"] = sql_factsheet.Fees_Structure
        data["asondate"] = sql_factsheet.TransactionDate
        data["aum"] = sql_factsheet.NetAssets_Rs_Cr
        data["risk_grade"] = sql_factsheet.Risk_Grade
        data["expense_ratio"] = sql_factsheet.ExpenseRatio

        port_chara = dict()
        
        port_chara["avgmktcap_rs_cr"] = sql_factsheet.AvgMktCap_Rs_Cr if sql_factsheet.AvgMktCap_Rs_Cr else None
        port_chara["portfoliop_eratio"] = sql_factsheet.PortfolioP_ERatio if sql_factsheet.PortfolioP_ERatio else None
        port_chara["total_stocks"] = sql_factsheet.TotalStocks if sql_factsheet.TotalStocks else None
                    
        data["portfolio_characteristics"] = port_chara

    nav = db_session.query(NAV.NAV).filter(NAV.NAV_Date == transaction_date).filter(NAV.Plan_Id == plan_id).filter(NAV.Is_Deleted != 1).filter(NAV.NAV_Type == 'P').scalar()    
    
    data["nav"] = round(nav,2) if nav else None

    sql_product = db_session.query(Product.Product_Code, Product.Product_Id).select_from(Product).join(PlanProductMapping, PlanProductMapping.Product_Id == Product.Product_Id).join(Plans, Plans.Plan_Id == PlanProductMapping.Plan_Id).filter(Plans.Plan_Id == plan_id).filter(Plans.Is_Deleted != 1).filter(PlanProductMapping.Is_Deleted != 1).all()

    data["product_code"] = sql_product[0][0]
    data["product_id"] = sql_product[0][1]

    res_factsheet = get_performancetrend_data(db_session, plan_id, transaction_date)  
    if res_factsheet:
        data["plan_name"] = res_factsheet["plan_name"]

        res_fact = dict()
        res_fact["bm_ret_1m"] = res_factsheet["bm_ret_1m"]
        res_fact["bm_ret_3m"] = res_factsheet["bm_ret_3m"]
        res_fact["bm_ret_6m"] = res_factsheet["bm_ret_6m"]
        res_fact["bm_ret_1y"] = res_factsheet["bm_ret_1y"]
        res_fact["bm_ret_2y"] = res_factsheet["bm_ret_2y"]
        res_fact["bm_ret_3y"] = res_factsheet["bm_ret_3y"]
        res_fact["bm_ret_5y"] = res_factsheet["bm_ret_5y"]
        res_fact["bm_ret_10y"] = res_factsheet["bm_ret_10y"]
        res_fact["bm_ret_ince"] = res_factsheet["bm_ret_ince"]

        if (sql_product[0][1] == 1 or sql_product[0][1] == 2): #MF or ULIP
            res_fact["cat_ret_1m"] = res_factsheet["cat_ret_1m"] if res_factsheet["cat_ret_1m"] else None
            res_fact["cat_ret_3m"] = res_factsheet["cat_ret_3m"] if res_factsheet["cat_ret_3m"] else None
            res_fact["cat_ret_6m"] = res_factsheet["cat_ret_6m"] if res_factsheet["cat_ret_6m"] else None
            res_fact["cat_ret_1y"] = res_factsheet["cat_ret_1y"] if res_factsheet["cat_ret_1y"] else None
            res_fact["cat_ret_3y"] = res_factsheet["cat_ret_3y"] if res_factsheet["cat_ret_3y"] else None
            res_fact["cat_ret_5y"] = res_factsheet["cat_ret_5y"] if res_factsheet["cat_ret_5y"] else None
            res_fact["cat_ret_ince"] = None

        res_fact["scheme_ret_1m"] = res_factsheet["scheme_ret_1m"]
        res_fact["scheme_ret_3m"] = res_factsheet["scheme_ret_3m"]
        res_fact["scheme_ret_6m"] = res_factsheet["scheme_ret_6m"]
        res_fact["scheme_ret_1y"] = res_factsheet["scheme_ret_1y"]
        res_fact["scheme_ret_2y"] = res_factsheet["scheme_ret_2y"]
        res_fact["scheme_ret_3y"] = res_factsheet["scheme_ret_3y"]
        res_fact["scheme_ret_5y"] = res_factsheet["scheme_ret_5y"]
        res_fact["scheme_ret_10y"] = res_factsheet["scheme_ret_10y"]
        res_fact["scheme_ret_ince"] = res_factsheet["scheme_ret_ince"]

        data["returns"] = res_fact

        #get Risk Analysis
        riskanal = dict()
        resp_dic = get_fundriskratio_data(db_session, plan_id, transaction_date)

        riskanal["scheme_id"] = resp_dic["scheme_id"] 
        riskanal["scheme_code"] = resp_dic["scheme_code"] 
        riskanal["date"] = resp_dic["date"] 
        riskanal["standard_deviation_1_y"] = resp_dic["standard_deviation_1_y"]
        riskanal["standard_deviation_3_y"] = resp_dic["standard_deviation_3_y"]
        riskanal["sharpe_ratio_1_y"] = resp_dic["sharpe_ratio_1_y"] 
        riskanal["sharpe_ratio_3_y"] = resp_dic["sharpe_ratio_3_y"]     
        riskanal["beta_1_y"] = resp_dic["beta_1_y"] 
        riskanal["beta_3_y"] = resp_dic["beta_3_y"] 
        riskanal["r_square_1_y"] = resp_dic["r_square_1_y"]
        riskanal["r_square_3_y"] = resp_dic["r_square_3_y"]
        riskanal["alpha_1_y"] = resp_dic["alpha_1_y"]
        riskanal["alpha_3_y"] = resp_dic["alpha_3_y"]
        riskanal["treynor_ratio_1_y"] = resp_dic["treynor_ratio_1_y"]
        riskanal["treynor_ratio_3_y"] = resp_dic["treynor_ratio_3_y"]

        data["risk_analysis"] = riskanal

    return data


def get_excel_report(data_frame, file_path, logo_path, report_title):    
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(file_path, engine='xlsxwriter')

    # Convert the dataframe to an XlsxWriter Excel object.
    data_frame.to_excel(writer, sheet_name=report_title, float_format="%.4f", index=False, header=False)

    # Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    worksheet = writer.sheets[report_title]

    # Insert an image.
    worksheet.insert_image('A1', logo_path, {'x_scale': 0.5, 'y_scale': 0.5})
    logo_format = workbook.add_format({
    'bold': 1,
    'border': 1,
    'align': 'center',
    'valign': 'vcenter',
    'fg_color': '#E4813B',
    'font_color': 'white'})

    header_format = workbook.add_format({
    'bold': True,
    'text_wrap': True,
    'align': 'center',
    'valign': 'top',
    'fg_color': '#1890ff',
    'border': 1,
    'font_color': 'white'})
    
    #Add empty row at top for logo 
    worksheet.write(0, 0, '', '')

    #Add style
    worksheet.set_column(0, 0, 25)
    worksheet.set_column(1, 1, 10)
    worksheet.set_column(2, 2, 10)
    worksheet.set_row(0, 50)
    worksheet.merge_range('B1:C1', report_title, logo_format)

    #Add header with styles
    for col_num, value in enumerate(data_frame.columns.values):
        worksheet.write(1, col_num, value, header_format)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

    return file_path


def get_business_day(asofdate : datetime, timeframe_in_days : int = 0, timeframe_in_mnths : int = 0):
    bday = BDay()
    is_business_day = False

    # if timeframe is provided then subtract get delta
    if timeframe_in_mnths or timeframe_in_days:
        asofdate -= relativedelta(months=timeframe_in_mnths, days=timeframe_in_days) # timedelta(delta_in_days)

    # if asofdate falls on weekend then pick a day prior
    while not is_business_day:
        is_business_day = bday.is_on_offset(asofdate)

        if is_business_day:
            break
        else:
            asofdate -= timedelta(1)

    return asofdate.strftime('%Y-%m-%d')


def investmentstyle_month_wise(db_session, plan_id, transaction_date):
    if not transaction_date:
        transaction_date = get_last_transactiondate(db_session, plan_id)

    sql_factsheetdata = db_session.query(PortfolioAnalysis.Attribute_Value.label('aum'),
                                                   PortfolioAnalysis.Attribute_Sub_Text.label('style'),
                                                   PortfolioAnalysis.Portfolio_Date,
                                                   extract('year', PortfolioAnalysis.Portfolio_Date).label('year'), 
                                                   extract('month', PortfolioAnalysis.Portfolio_Date).label('month')).filter(PortfolioAnalysis.Plan_Id == plan_id) \
                                                            .filter(PortfolioAnalysis.Portfolio_Date <= transaction_date) \
                                                            .filter(PortfolioAnalysis.Is_Deleted != 1) \
                                                            .filter(PortfolioAnalysis.Attribute_Type == 'Stocks_Rank') \
                                                            .filter(PortfolioAnalysis.Attribute_Text != '') \
                                                            .filter(PortfolioAnalysis.Attribute_Sub_Text != '-') \
                                                            .filter(PortfolioAnalysis.Attribute_Sub_Text != None) \
                                                            .all()

    # convert the query result to dataframe 
    df = pd.DataFrame(sql_factsheetdata)
    if not df.empty:
        # get the latest dates for every month of each year
        latest_dates_by_mnth = df.sort_values('Portfolio_Date').groupby(['year', 'month']).tail(1).Portfolio_Date
        df = df[df['Portfolio_Date'].isin(latest_dates_by_mnth)] \
                                    .groupby(['style', 'year', 'month'], as_index=False) \
                                    .agg({'aum': 'sum', 
                                        'style': 'first', 
                                        'year': 'first', 
                                        'month': 'first'})

    return df


def marketcapcomposition_month_wise(db_session, plan_id, transaction_date):
    resp = list()

    if not transaction_date:
        transaction_date = get_last_transactiondate(db_session, plan_id)

    sql_factsheetdata = db_session.query(PortfolioAnalysis.Attribute_Value, extract('year', PortfolioAnalysis.Portfolio_Date), extract('month', PortfolioAnalysis.Portfolio_Date), PortfolioAnalysis.Attribute_Text).filter(PortfolioAnalysis.Plan_Id == plan_id).filter(PortfolioAnalysis.Portfolio_Date <= transaction_date).filter(PortfolioAnalysis.Is_Deleted != 1).filter(PortfolioAnalysis.Attribute_Type == 'Market_Cap',PortfolioAnalysis.Attribute_Text != '').order_by(extract('year', PortfolioAnalysis.Portfolio_Date), extract('month', PortfolioAnalysis.Portfolio_Date), PortfolioAnalysis.Attribute_Text).all()

    if sql_factsheetdata:
        for sql_factsheet in sql_factsheetdata:
            data = dict()
            data["value"] = sql_factsheet.Attribute_Value
            data["year"] = sql_factsheet[1]
            data["month"] = sql_factsheet[2]
            data["market_cap"] = sql_factsheet.Attribute_Text
            
            resp.append(data)

    return resp


def get_holdings_sector_data(db_session, plan_id, portfolio_date):
    resp = list()

    fund_id = get_fundid_byplanid(db_session, plan_id)

    sql_factsheetquery = db_session.query(UnderlyingHoldings.Sector_Names, extract('year', UnderlyingHoldings.Portfolio_Date), extract('month', UnderlyingHoldings.Portfolio_Date), func.sum(UnderlyingHoldings.Percentage_to_AUM)).filter(UnderlyingHoldings.Fund_Id == fund_id).filter(UnderlyingHoldings.Sector_Names != None, UnderlyingHoldings.Sector_Names != '', UnderlyingHoldings.Is_Deleted != 1).group_by(extract('year', UnderlyingHoldings.Portfolio_Date), extract('month', UnderlyingHoldings.Portfolio_Date), UnderlyingHoldings.Sector_Names).order_by(extract('year', UnderlyingHoldings.Portfolio_Date), extract('month', UnderlyingHoldings.Portfolio_Date), UnderlyingHoldings.Sector_Names)
    
    if portfolio_date:
        sql_factsheetquery = sql_factsheetquery.filter(UnderlyingHoldings.Portfolio_Date == portfolio_date)

    sql_factsheetdata = sql_factsheetquery.all()

    if sql_factsheetdata:
        if portfolio_date:
            for sql_factsheet in sql_factsheetdata:
                data = dict()
                data["sector_name"] = sql_factsheet.Sector_Names
                data["sector_weight"] = sql_factsheet[3]
                resp.append(data)
        else:        
            for sql_factsheet in sql_factsheetdata:
                data = dict()
                data["value"] = sql_factsheet[3]
                data["year"] = sql_factsheet[1]
                data["month"] = sql_factsheet[2]
                data["sector_name"] = sql_factsheet.Sector_Names
                
                resp.append(data)
    return resp


def get_portfolio_instrumentrating_data(db_session, plan_id, portfolio_date):
    resp = list()

    fund_id = get_fundid_byplanid(db_session, plan_id)

    sql_factsheetquery = db_session.query(UnderlyingHoldings.Instrument_Rating, extract('year', UnderlyingHoldings.Portfolio_Date), extract('month', UnderlyingHoldings.Portfolio_Date), func.sum(UnderlyingHoldings.Percentage_to_AUM)).filter(UnderlyingHoldings.Fund_Id == fund_id).filter(UnderlyingHoldings.Instrument_Rating != None, UnderlyingHoldings.Instrument_Rating != '', UnderlyingHoldings.Is_Deleted != 1).group_by(extract('year', UnderlyingHoldings.Portfolio_Date), extract('month', UnderlyingHoldings.Portfolio_Date), UnderlyingHoldings.Instrument_Rating).order_by(extract('year', UnderlyingHoldings.Portfolio_Date), extract('month', UnderlyingHoldings.Portfolio_Date), UnderlyingHoldings.Instrument_Rating)
    
    if portfolio_date:
        sql_factsheetquery = sql_factsheetquery.filter(UnderlyingHoldings.Portfolio_Date == portfolio_date)

    sql_factsheetdata = sql_factsheetquery.all()

    if sql_factsheetdata:
        for sql_factsheet in sql_factsheetdata:
            data = dict()
            data["value"] = sql_factsheet[3]
            data["year"] = sql_factsheet[1]
            data["month"] = sql_factsheet[2]
            data["instrument_rating"] = sql_factsheet.Instrument_Rating
            
            resp.append(data)

    return resp


def get_portfolio_instrument_data(db_session, plan_id, portfolio_date):
    resp = list()

    fund_id = get_fundid_byplanid(db_session, plan_id)

    sql_factsheetquery = db_session.query(UnderlyingHoldings.Instrument, extract('year', UnderlyingHoldings.Portfolio_Date), extract('month', UnderlyingHoldings.Portfolio_Date), func.sum(UnderlyingHoldings.Percentage_to_AUM)).filter(UnderlyingHoldings.Fund_Id == fund_id).filter(UnderlyingHoldings.Instrument != None, UnderlyingHoldings.Instrument != '').filter(UnderlyingHoldings.Is_Deleted != 1).group_by(extract('year', UnderlyingHoldings.Portfolio_Date), extract('month', UnderlyingHoldings.Portfolio_Date), UnderlyingHoldings.Instrument).order_by(extract('year', UnderlyingHoldings.Portfolio_Date), extract('month', UnderlyingHoldings.Portfolio_Date), UnderlyingHoldings.Instrument)
    
    if portfolio_date:
        sql_factsheetquery = sql_factsheetquery.filter(UnderlyingHoldings.Portfolio_Date == portfolio_date)

    sql_factsheetdata = sql_factsheetquery.all()

    if sql_factsheetdata:
        for sql_factsheet in sql_factsheetdata:
            data = dict()
            data["value"] = sql_factsheet[3]
            data["year"] = sql_factsheet[1]
            data["month"] = sql_factsheet[2]
            data["instrument"] = sql_factsheet.Instrument
            
            resp.append(data)
    
    return resp


def get_securedunsecured_data(db_session, plan_id, portfolio_date):
    resp = list()

    fund_id = get_fundid_byplanid(db_session, plan_id)

    sql_factsheetquery = db_session.query(UnderlyingHoldings.secured_unsecured, extract('year', UnderlyingHoldings.Portfolio_Date), extract('month', UnderlyingHoldings.Portfolio_Date), func.sum(UnderlyingHoldings.Percentage_to_AUM)).filter(UnderlyingHoldings.Fund_Id == fund_id).filter(UnderlyingHoldings.secured_unsecured != None, UnderlyingHoldings.secured_unsecured != '', UnderlyingHoldings.Is_Deleted != 1).group_by(extract('year', UnderlyingHoldings.Portfolio_Date), extract('month', UnderlyingHoldings.Portfolio_Date), UnderlyingHoldings.secured_unsecured).order_by(extract('year', UnderlyingHoldings.Portfolio_Date), extract('month', UnderlyingHoldings.Portfolio_Date), UnderlyingHoldings.secured_unsecured)
    
    if portfolio_date:
        sql_factsheetquery = sql_factsheetquery.filter(UnderlyingHoldings.Portfolio_Date == portfolio_date)

    sql_factsheetdata = sql_factsheetquery.all()

    if sql_factsheetdata:
        if portfolio_date:
            for sql_factsheet in sql_factsheetdata:
                data = dict()
                data["secured_unsecured"] = sql_factsheet.secured_unsecured
                data["value"] = sql_factsheet[3]
                resp.append(data)  

    return resp


def schedule_email_activity(db_session, to_list: str, cc_list: str, bcc_list: str, subject, email_body, attachements):
    elem = ET.Element("Attachments")
    for attachment in attachements:
        child = ET.Element("File")
        child.text = attachment
        elem.append(child)

    sql_request = DeliveryRequest()
    # TODO: remove the hard coded part from the below list
    sql_request.Channel_Id = 2
    sql_request.Type = "EMAIL"
    sql_request.Recipients = to_list
    sql_request.Body = email_body
    sql_request.Request_Time = dt1.now()
    sql_request.Template_Id = 0
    sql_request.RecipientsCC = cc_list
    sql_request.RecipientsBCC = bcc_list
    sql_request.Subject = subject
    sql_request.IsBodyHTML = 1
    sql_request.Attachments = ET.tostring(elem).decode()
    sql_request.Parameters = None
    sql_request.Resources = None
    sql_request.Status = 0
    sql_request.Status_Message = "Pending"

    db_session.add(sql_request)
    db_session.commit()


def add_log(msg):
    logging.basicConfig(filename='error.log', encoding='utf-8', level=logging.INFO, format='%(message)s')
    logging.warning(msg)


def get_pms_aif_aum_fundwise(db_session, product_id=4, transactiondate=None, fundwise=True, plans=[], exclude_list=False):
    resp = list()
    if transactiondate:
        pms_query = db_session.query(Plans.Plan_Id, Plans.Plan_Name, FactSheet.NetAssets_Rs_Cr, MFSecurity.MF_Security_OpenDate.label('Inception Date'), FactSheet.TransactionDate)\
                            .select_from(FactSheet)\
                            .join(Plans, Plans.Plan_Id == FactSheet.Plan_Id)\
                            .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                            .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                            .join(PlanProductMapping, and_(PlanProductMapping.Plan_Id == Plans.Plan_Id, PlanProductMapping.Product_Id == product_id))\
                            .join(NAV, and_(NAV.Plan_Id == Plans.Plan_Id, NAV.NAV_Type == 'P'))\
                            .filter(Plans.Is_Deleted != 1, MFSecurity.Is_Deleted != 1, PlanProductMapping.Is_Deleted != 1, FactSheet.Is_Deleted != 1, MFSecurity.Status_Id == 1, Fund.Is_Active == 1, FactSheet.TransactionDate == transactiondate, Plans.Plan_Id.not_in([46163,44193]))
        
        if transactiondate:
            pms_query = pms_query.filter(func.month(NAV.NAV_Date) == func.month(transactiondate)).filter(func.year(NAV.NAV_Date) == func.year(transactiondate))

        if plans:
            if exclude_list:
                pms_query = pms_query.filter(Plans.Plan_Id.not_in(plans))
            else:
                pms_query = pms_query.filter(Plans.Plan_Id.in_(plans))

        resp = [u._asdict() for u in pms_query.order_by(Plans.Plan_Name).distinct().all()]

        if not fundwise:
            total_aum = sum(item['NetAssets_Rs_Cr'] if item['NetAssets_Rs_Cr'] else 0 for item in resp)
            return round(float(total_aum),2)

    
    return resp


def get_pms_aif_nav_fundwise(db_session, product_id=4, transactiondate=None, fundwise=True, plans=[], exclude_list=False):
    resp = list()
    
    pms_query = db_session.query(NAV.NAV_Date.label('Date'), Plans.Plan_Name.label('PMS Name'), NAV.NAV)\
                        .select_from(FactSheet)\
                        .join(Plans, Plans.Plan_Id == FactSheet.Plan_Id)\
                        .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                        .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                        .join(PlanProductMapping, and_(PlanProductMapping.Plan_Id == Plans.Plan_Id, PlanProductMapping.Product_Id == product_id))\
                        .join(NAV, and_(NAV.Plan_Id == Plans.Plan_Id, NAV.NAV_Type == 'P'))\
                        .filter(Plans.Is_Deleted != 1, MFSecurity.Is_Deleted != 1, PlanProductMapping.Is_Deleted != 1, FactSheet.Is_Deleted != 1, MFSecurity.Status_Id == 1, Fund.Is_Active == 1, NAV.Is_Deleted != 1, Plans.Plan_Id.not_in([46163,44193]))
    if transactiondate:
        pms_query = pms_query.filter(func.month(NAV.NAV_Date) == func.month(transactiondate)).filter(func.year(NAV.NAV_Date) == func.year(transactiondate))
    
    if plans:
        if exclude_list:
            pms_query = pms_query.filter(Plans.Plan_Id.not_in(plans))
        else:
            pms_query = pms_query.filter(Plans.Plan_Id.in_(plans))
    
    resp = [u._asdict() for u in pms_query.order_by(Plans.Plan_Name, NAV.NAV_Date).distinct().all()]

    return resp


def get_mf_ulip_aum_fundwise(db_session, product_id=1):
    product_wise_aum = 0
    
    product_wise_aum = db_session.query((func.sum(case((and_(FundStocks.Product_Id == product_id, FundStocks.ExitStockForFund != 1), FundStocks.Value_In_Inr), else_=0))/10000000).label('mf_aum')).select_from(FundStocks).join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == FundStocks.HoldingSecurity_Id).filter(HoldingSecurity.ISIN_Code.like("INE%")).filter(FundStocks.InstrumentType == 'Equity').filter(FundStocks.Product_Id == product_id).scalar()
    
    return round(float(product_wise_aum),2)


def get_newly_onboarded_plans(db_session, product_id=4):
    sql_newplans = db_session.query(Plans.Plan_Id)\
                            .select_from(Plans)\
                            .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                            .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                            .join(AMC, AMC.AMC_Id == MFSecurity.AMC_Id)\
                            .join(FactSheet, FactSheet.Plan_Id == Plans.Plan_Id)\
                            .join(PlanProductMapping, PlanProductMapping.Plan_Id == FactSheet.Plan_Id)\
                            .join(Report_Plans_status, Report_Plans_status.Plan_Id == Plans.Plan_Id,isouter = True)\
                            .join(NAV, and_(NAV.Plan_Id == Plans.Plan_Id, NAV.NAV_Type == 'P'),isouter = True)\
                            .filter(MFSecurity.Status_Id == 1, FactSheet.Is_Deleted != 1, PlanProductMapping.Product_Id == product_id, Report_Plans_status.Plan_Id == None, NAV.Is_Deleted != 1, Plans.Plan_Id.not_in([46163,44193]))\
                            .group_by(Plans.Plan_Id)\
                            .distinct().all()
    res = list()
    for sub in sql_newplans:        
        res.append(sub['Plan_Id'])

    return res


def get_plans_fo_which_data_not_received(db_session, product_id=4, report_date=None):
    sql_plans = db_session.query(Plans.Plan_Id, func.max(NAV.NAV_Date), func.max(FactSheet.TransactionDate))\
                            .select_from(Plans)\
                            .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                            .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                            .join(AMC, AMC.AMC_Id == MFSecurity.AMC_Id)\
                            .join(FactSheet, FactSheet.Plan_Id == Plans.Plan_Id)\
                            .join(PlanProductMapping, PlanProductMapping.Plan_Id == FactSheet.Plan_Id)\
                            .join(Report_Plans_status, Report_Plans_status.Plan_Id == Plans.Plan_Id)\
                            .join(NAV, and_(NAV.Plan_Id == Plans.Plan_Id, NAV.NAV_Type == 'P'),isouter = True)\
                            .filter(MFSecurity.Status_Id == 1, FactSheet.Is_Deleted != 1, PlanProductMapping.Product_Id == product_id, NAV.Is_Deleted != 1)\
                            .group_by(Plans.Plan_Id)\
                            .having(func.max(FactSheet.TransactionDate) <= report_date).having(func.max(NAV.NAV_Date) < report_date).distinct().all()
    
    
    res = list()
    for sub in sql_plans:        
        res.append(sub['Plan_Id'])

    return res


def check_if_fund_contains_nav_and_factsheet_by_date(db_session, plan_id=None, transaction_date=None):
    sql_plans = db_session.query(Plans.Plan_Id, func.max(FactSheet.TransactionDate))\
                            .select_from(Plans)\
                            .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                            .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                            .join(AMC, AMC.AMC_Id == MFSecurity.AMC_Id)\
                            .join(FactSheet, FactSheet.Plan_Id == Plans.Plan_Id)\
                            .join(PlanProductMapping, PlanProductMapping.Plan_Id == FactSheet.Plan_Id)\
                            .join(NAV, and_(NAV.Plan_Id == Plans.Plan_Id, NAV.NAV_Type == 'P'),isouter = True)\
                            .filter(MFSecurity.Status_Id == 1, FactSheet.Is_Deleted != 1, NAV.Is_Deleted != 1, FactSheet.TransactionDate == transaction_date, Plans.Plan_Id == plan_id)\
                            .group_by(Plans.Plan_Id).distinct()
                            
    # print_query(sql_plans)
    resp = [u._asdict() for u in sql_plans.all()]

    return resp
    

def get_performance_and_nav_movement_mismatch(db_session, from_date, to_date):
    plans_nav_performance_mismatch = list()
    
    all_dates = [from_date, to_date]

    sql_plans_monthly_movement = db_session.query(Plans.Plan_Id, Plans.Plan_Name, NAV.NAV_Date.label('Date'), NAV.NAV, func.lag(NAV.NAV).over(partition_by=(NAV.Plan_Id),order_by=NAV.NAV_Date).label('prev_nav'),     
    (((((NAV.NAV - func.lag(NAV.NAV).over(partition_by=(NAV.Plan_Id),order_by=NAV.NAV_Date)) / func.lag(NAV.NAV).over(partition_by=(NAV.Plan_Id),order_by=NAV.NAV_Date))) * 100)).label('Portfolio_Returns'), Product.Product_Name).select_from(Plans).join(MFSecurity, MFSecurity.MF_Security_Id==Plans.MF_Security_Id).join(NAV, and_(NAV.Plan_Id==Plans.Plan_Id, NAV.NAV_Type=='P')).join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id).join(Product, Product.Product_Id == PlanProductMapping.Product_Id).filter(NAV.NAV_Type=='P').filter(Plans.Is_Deleted != 1, NAV.Is_Deleted != 1, PlanProductMapping.Is_Deleted != 1).filter(MFSecurity.Status_Id==1).filter(NAV.NAV_Date.in_(all_dates)).filter(Product.Product_Id == 4).all()
 
    if sql_plans_monthly_movement:
        for plans_movement in sql_plans_monthly_movement:
            if plans_movement[5]:
                diff = 0
                return_1month = db_session.query(FactSheet.SCHEME_RETURNS_1MONTH).filter(FactSheet.Plan_Id == plans_movement.Plan_Id, FactSheet.TransactionDate == plans_movement[2], FactSheet.Is_Deleted != 1).scalar()
                if return_1month:
                    diff = float(return_1month) - float(plans_movement[5]) if float(return_1month) > float(plans_movement[5]) else float(plans_movement[5]) - float(return_1month)

                if diff >= 0.20 or diff <= -0.20:
                    data = dict()
                    data["Plan id"] = plans_movement.Plan_Id   
                    data["Plan Name"] = plans_movement.Plan_Name                
                    data["Current Nav Date"] = plans_movement[2]
                    data["Current NAV"] = plans_movement.NAV
                    data["last month end NAV"] = plans_movement[4]
                    data["Difference"] = diff
                
                    plans_nav_performance_mismatch.append(data)

    return plans_nav_performance_mismatch


def calculate_cagr_return(start, end, period):
    return (end/start)**(1/period)-1

def calculate_portfolio_level_analysis(db_session, plan_id, portfolio_dt, delta):
    df = pd.DataFrame()

    if portfolio_dt:
        asof_date = portfolio_dt - timedelta(days=delta)

        sql_latest_fundamental = db_session.query(Fundamentals.CO_CODE.label('co_code'),
                                                Fundamentals.ISIN_Code.label('isin'),
                                                func.min(Fundamentals.PriceDate).label('asof_date'))\
                                            .filter(Fundamentals.PriceDate >= asof_date)\
                                            .filter(Fundamentals.Is_Deleted != 1)\
                                            .group_by(Fundamentals.CO_CODE, Fundamentals.ISIN_Code).subquery()

        sql_holdings = db_session.query(Plans.Plan_Id.label('plan_id'),
                                        UnderlyingHoldings.Company_Security_Name.label('name'),
                                        UnderlyingHoldings.ISIN_Code.label('isin'),
                                        UnderlyingHoldings.MarketCap.label('marketcap'),
                                        UnderlyingHoldings.Percentage_to_AUM.label('weight'),
                                        HoldingSecurity.Co_Code.label('co_code'),
                                        Fundamentals.DivYield.label('div_yld'),
                                        Fundamentals.EPS.label('eps'),
                                        Fundamentals.PE.label('pe'),
                                        Fundamentals.PBV.label('pbv'),
                                        Fundamentals.mcap.label('mcap'))\
                                .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                                .join(UnderlyingHoldings, UnderlyingHoldings.Fund_Id == Fund.Fund_Id)\
                                .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                                .join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == UnderlyingHoldings.HoldingSecurity_Id , isouter=True)\
                                .join(Fundamentals, cast(Fundamentals.CO_CODE, sqlalchemy.String) == HoldingSecurity.Co_Code)\
                                .join(sql_latest_fundamental, and_(sql_latest_fundamental.c.co_code == Fundamentals.CO_CODE, sql_latest_fundamental.c.asof_date == Fundamentals.PriceDate))\
                                .filter(UnderlyingHoldings.Portfolio_Date == portfolio_dt, Plans.Plan_Id == plan_id,
                                        Plans.Is_Deleted != 1, MFSecurity.Is_Deleted != 1, Fund.Is_Deleted != 1, UnderlyingHoldings.Is_Deleted != 1).all()
                                #  .group_by(UnderlyingHoldings.Portfolio_Date, Plans.Plan_Id).all()

        df = pd.DataFrame(sql_holdings)

    if df.empty:
        return {
            "msg": "Holdings information is not available.",
            "median_mkt_cap": None            
            }

    result = generate_portfolio_characteristics(df)

    return result

def get_fund_manager_info_by_code(db_session, fund_manager_code, only_active_fund, config_image_path):
    # name, code, description, image, designation, total aum, amc id will be the same. so take anyone of the results.
    sql_objs = db_session.query(FundManager.FundManager_Name, 
                                            FundManager.FundManager_Code, 
                                            FundManager.FundManager_Description, 
                                            FundManager.FundManager_Designation, 
                                            FundManager.AUM, 
                                            FundManager.Linkedin_url,
                                            AMC.AMC_Name, 
                                            FundManager.FundManager_Image, 
                                            FundManager.Funds_Managed,
                                            Product.Product_Name)\
                                                .select_from(FundManager)\
                                                .join(AMC, FundManager.AMC_Id==AMC.AMC_Id)\
                                                .join(Product, Product.Product_Id == FundManager.Product_Id)\
                                                .filter(FundManager.FundManager_Code == fund_manager_code)\
                                                .filter(FundManager.Is_Deleted != 1)\
                                                .filter(Fund.Is_Deleted != 1)\
                                                .order_by(FundManager.FundManager_Id)\
                                                .first()

    q_all_fund = db_session.query(FundManager.FundManager_Id, 
                                            FundManager.FundManager_Name, 
                                            FundManager.FundManager_Code, 
                                            FundManager.FundManager_Description, 
                                            FundManager.Fund_Id, 
                                            Fund.Fund_Name, 
                                            Fund.Fund_Code, 
                                            FundManager.Is_Deleted, 
                                            FundManager.FundManager_Image, 
                                            FundManager.FundManager_Designation, 
                                            FundManager.DateFrom, 
                                            FundManager.DateTo,
                                            MFSecurity.MF_Security_OpenDate)\
                                                .select_from(FundManager)\
                                                .join(Fund, Fund.Fund_Id == FundManager.Fund_Id)\
                                                .join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id)\

    if only_active_fund:
        q_all_fund = q_all_fund.filter(or_(FundManager.DateTo >= date.today(), FundManager.DateTo == None))

    sql_allfund = q_all_fund.group_by(FundManager.FundManager_Id, 
                                      FundManager.FundManager_Name, 
                                      FundManager.FundManager_Code, 
                                      FundManager.FundManager_Description, 
                                      FundManager.Fund_Id, 
                                      Fund.Fund_Name,
                                      Fund.Fund_Code, 
                                      FundManager.Is_Deleted, 
                                      FundManager.FundManager_Image, 
                                      FundManager.FundManager_Designation, 
                                      FundManager.DateFrom, 
                                      FundManager.DateTo,
                                      MFSecurity.MF_Security_OpenDate)\
                                        .filter(FundManager.Is_Deleted != 1)\
                                        .filter(Fund.Is_Deleted != 1)\
                                        .filter(FundManager.FundManager_Code == fund_manager_code)\
                                        .filter(MFSecurity.Status_Id == 1)\
                                        .all()

    resp = dict()
    if sql_objs:
        # sql_obj = sql_objs[0]
        resp['fund_manager_name'] = sql_objs.FundManager_Name
        resp['fund_manager_code'] = sql_objs.FundManager_Code
        resp['fund_manager_description'] = sql_objs.FundManager_Description
        resp['fund_manager_image'] = F"{config_image_path}{sql_objs.FundManager_Image}" if sql_objs.FundManager_Image else None
        resp['fund_manager_designation'] = sql_objs.FundManager_Designation
        resp['fund_manager_amc'] = sql_objs.AMC_Name
        resp['fund_manager_aum'] = sql_objs.AUM
        resp['fund_manager_fund_count'] = sql_objs.Funds_Managed
        resp['fund_manager_product_name'] = sql_objs.Product_Name
        resp['fund_manager_linkedin_url'] = sql_objs.Linkedin_url

        funds = list()
        for sql_o in sql_allfund:
            f = dict()
            f["fund_code"] = sql_o.Fund_Code
            f["fund_name"] = sql_o.Fund_Name
            f["fund_manager_from"] = sql_o.DateFrom if sql_o.DateFrom else sql_o.MF_Security_OpenDate
            f["fund_manager_to"] = sql_o.DateTo
            funds.append(f)
        resp["funds"] = funds
    
    return resp

def get_fundmanager_list(db_session, dateto, amc_id, product_id, fundmanager_name):

    sql_objs = db_session.query(FundManager.FundManager_Name,
                                          FundManager.FundManager_Code,
                                          FundManager.Product_Id,
                                          FundManager.AUM,
                                          AMC.AMC_Name,
                                          AMC.AMC_Id,
                                          func.max(FundManager.FundManager_Image).label('FundManager_Image'),
                                          FundManager.Funds_Managed,
                                          Product.Product_Name)\
                                        .select_from(FundManager)\
                                        .join(AMC, FundManager.AMC_Id == AMC.AMC_Id)\
                                        .join(Fund, Fund.Fund_Id == FundManager.Fund_Id)\
                                        .join(MFSecurity, MFSecurity.Fund_Id == FundManager.Fund_Id)\
                                        .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                        .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                                        .join(Product, Product.Product_Id == FundManager.Product_Id)\
                                        .filter(FundManager.Is_Deleted != 1, 
                                                MFSecurity.Status_Id == 1,
                                                Plans.Is_Deleted != 1, 
                                                PlanProductMapping.Is_Deleted != 1,  
                                                FundManager.Funds_Managed>0, 
                                                or_(FundManager.DateTo == None, FundManager.DateTo >= dateto))\

    if amc_id:
        sql_objs = sql_objs.filter(AMC.AMC_Id == amc_id)

    if product_id:
        sql_objs = sql_objs.filter(FundManager.Product_Id == product_id)

    if fundmanager_name != None and fundmanager_name != '':
        sql_objs = sql_objs.filter(FundManager.FundManager_Name.like(F"%{fundmanager_name}%"))

    sql_fundmanagers = sql_objs.group_by(FundManager.FundManager_Name,
                                         FundManager.FundManager_Code,
                                         FundManager.AUM,
                                         FundManager.Product_Id,
                                         AMC.AMC_Name,
                                         AMC.AMC_Id,
                                         FundManager.Funds_Managed,
                                         Product.Product_Name).all()
    return sql_fundmanagers

def get_latest_factsheet_query(db_session, product_list, assetclass_list, classification_list, amc_list):
    max_pms_factsheet_date = db_session.query(func.max(FactSheet.TransactionDate).label('TransactionDate'))\
                      .join(Plans, Plans.Plan_Id == FactSheet.Plan_Id )\
                      .join(PlanProductMapping, Plans.Plan_Id == PlanProductMapping.Plan_Id)\
                      .filter(FactSheet.TransactionDate.isnot(None),
                            FactSheet.Is_Deleted != 1,
                            Plans.PlanType_Id == 1,
                            Plans.Is_Deleted != 1,
                            PlanProductMapping.Product_Id == 4,
                            PlanProductMapping.Is_Deleted != 1).scalar()
    
    sql_subquery = db_session.query(FactSheet.Plan_Id, 
                                              func.max(FactSheet.TransactionDate).label('max_transactiondate'))\
                                            .filter(FactSheet.Is_Deleted != 1, 
                                                    FactSheet.TransactionDate <= max_pms_factsheet_date)\
                                            .group_by(FactSheet.Plan_Id).subquery()

    sql_query = db_session.query(FactSheet.FactSheet_Id, 
                                            FactSheet.Plan_Id, 
                                            Plans.Plan_Name, 
                                            Product.Product_Id, 
                                            MFSecurity.AssetClass_Id, 
                                            MFSecurity.Classification_Id, 
                                            Product.Product_Code, 
                                            Product.Product_Name, 
                                            FactSheet.SCHEME_RETURNS_1MONTH, 
                                            FactSheet.SCHEME_RETURNS_3MONTH, 
                                            FactSheet.SCHEME_RETURNS_6MONTH, 
                                            FactSheet.SCHEME_RETURNS_1YEAR, 
                                            FactSheet.SCHEME_RETURNS_2YEAR, 
                                            FactSheet.SCHEME_RETURNS_3YEAR, 
                                            FactSheet.SCHEME_RETURNS_5YEAR, 
                                            FactSheet.SCHEME_RETURNS_10YEAR,
                                            FactSheet.SCHEME_RETURNS_since_inception, 
                                            Classification.Classification_Name, 
                                            FactSheet.TransactionDate,
                                            func.datediff(text('Day'), MFSecurity.MF_Security_OpenDate, FactSheet.TransactionDate).label('age_in_days'))\
                                                .select_from(FactSheet)\
                                                .join(Plans, Plans.Plan_Id == FactSheet.Plan_Id)\
                                                .join(Options, and_(Options.Option_Id == Plans.Option_Id, Options.Option_Name.like("%G%")))\
                                                .join(PlanProductMapping, and_(PlanProductMapping.Plan_Id == Plans.Plan_Id, PlanProductMapping.Is_Deleted != 1))\
                                                .join(Product, and_(Product.Product_Id == PlanProductMapping.Product_Id, FactSheet.SourceFlag == Product.Product_Code))\
                                                .join(MFSecurity, and_(MFSecurity.MF_Security_Id == Plans.MF_Security_Id, MFSecurity.Status_Id == 1))\
                                                .join(Classification, Classification.Classification_Id == MFSecurity.Classification_Id)\
                                                .join(sql_subquery, and_(sql_subquery.c.Plan_Id == Plans.Plan_Id, sql_subquery.c.max_transactiondate == FactSheet.TransactionDate))\
                                                .filter(FactSheet.Is_Deleted != 1)

    if product_list:
        sql_query = sql_query.filter(Product.Product_Id.in_(product_list))
    
    if assetclass_list:
        sql_query = sql_query.filter(MFSecurity.AssetClass_Id.in_(assetclass_list))
    
    if classification_list:
        sql_query = sql_query.filter(MFSecurity.Classification_Id.in_(classification_list))
    
    return sql_query

def generate_active_rolling_returns(db_session, plan_id, benchmark_id, timeframe_in_yr, transaction_date):
    res = dict()

    if not benchmark_id:
        sql_obj = db_session.query(BenchmarkIndices.BenchmarkIndices_Id, BenchmarkIndices.BenchmarkIndices_Name, Plans.Plan_Name, Fund.Fund_Id, Fund.Fund_Code).select_from(Plans).join(MFSecurity, Plans.MF_Security_Id == MFSecurity.MF_Security_Id).join(BenchmarkIndices, BenchmarkIndices.BenchmarkIndices_Id == MFSecurity.BenchmarkIndices_Id).join(Fund, Fund.Fund_Id==MFSecurity.Fund_Id).filter(MFSecurity.Status_Id == 1).filter(Plans.Plan_Id == plan_id).first()

        benchmark_id = sql_obj.BenchmarkIndices_Id

    index_result = get_rolling_returns(db_session, benchmark_id, True, timeframe_in_yr, transaction_date, False, True)
    fund_result = get_rolling_returns(db_session, plan_id, False, timeframe_in_yr, transaction_date, False, True)

    if index_result and fund_result:
        df_index = pd.DataFrame(index_result)
        df_fund = pd.DataFrame(fund_result)

        active_data_df = df_fund.merge(df_index, how='left', left_on='NAV_Date', right_on='NAV_Date')
        
        active_data_df['date'] = active_data_df['NAV_Date']
        active_data_df = active_data_df.set_index('date')

        #get all tri returns and fill missing returns for 1 and 3 yrs
        tri_returns_data = db_session.query(TRIReturns.TRI_IndexDate, TRIReturns.Return_1Year, TRIReturns.Return_3Year)\
                                                        .join(BenchmarkIndices, BenchmarkIndices.Co_Code == TRIReturns.TRI_Co_Code)\
                                                        .filter(BenchmarkIndices.BenchmarkIndices_Id == benchmark_id,
                                                                TRIReturns.Is_Deleted != 1)\
                                                        .order_by(TRIReturns.TRI_IndexDate)\
                                                        .all()
        
        df_index_returns_data = pd.DataFrame(tri_returns_data)

        if not df_index_returns_data.empty:
            active_data_df1 = active_data_df.merge(df_index_returns_data, how='left', left_on='NAV_Date', right_on='TRI_IndexDate')

            active_data_df1 = active_data_df1.fillna('')
            active_data_df1['returns_y'] = active_data_df1.apply(lambda x: x[F'Return_{timeframe_in_yr}Year'] if x['returns_y'] == '' else x['returns_y'], axis=1)

            #Fill holidays - tri returns
            for x in range(4):
                active_data_df1['returns_y'] = active_data_df1['returns_y'].shift().where(active_data_df1['returns_y'] == '', active_data_df1['returns_y'])

            active_data_df1['returns_y1'] = pd.to_numeric(active_data_df1['returns_y'],errors='coerce')
            active_data_df1['returns_x'] = pd.to_numeric(active_data_df1['returns_x'],errors='coerce')
            
            active_data_df1["returns"] = active_data_df1["returns_x"] - active_data_df1["returns_y1"]
            active_data_df1["consider"] = active_data_df1["returns"].notnull()

            res = get_rolling_analysis(active_data_df1.loc[active_data_df1['consider'] == True], True)
        
    return res

def get_fund_manager_details(db_session, fundmanager_code, ts):
    resp = dict()
    fact_performance_list = list()
    finalholding_list = list()
    finalsector_list = list()    

    today = datetime.today()
    sql_fund = db_session.query(Plans.Plan_Id,
                                          Plans.Plan_Name,
                                          Fund.Fund_Id,
                                          Fund.Fund_Name,
                                          FundManager.Funds_Managed,
                                          FundManager.AUM,
                                          Classification.Classification_Name, 
                                          Product.Product_Code,
                                          AMC.AMC_Logo)\
                                    .select_from(FundManager)\
                                    .join(Fund, Fund.Fund_Id == FundManager.Fund_Id)\
                                    .join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id)\
                                    .join(AMC, AMC.AMC_Id == MFSecurity.AMC_Id)\
                                    .join(Classification, Classification.Classification_Id == MFSecurity.Classification_Id)\
                                    .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                    .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id) \
                                    .join(Product, Product.Product_Id == PlanProductMapping.Product_Id) \
                                    .join(Options, Options.Option_Id == Plans.Option_Id)\
                                    .filter(Plans.Is_Deleted != 1, MFSecurity.Is_Deleted != 1)\
                                    .filter(MFSecurity.Status_Id == 1, Plans.PlanType_Id == 1)\
                                    .filter(or_(FundManager.DateTo >= today, FundManager.DateTo == None))\
                                    .filter(FundManager.FundManager_Code == fundmanager_code, FundManager.Is_Deleted != 1)\
                                    .filter(Plans.PlanType_Id==1).order_by(desc(Options.Option_Name)).all()
    
    if sql_fund:        
        unique_fund_id = list()
        fullholding_list = list()
        total_aum = 0

        for fund in sql_fund:
            # get performance
            res_factsheet = get_performancetrend_data(db_session, fund.Plan_Id, None)
            if res_factsheet:
                fact_performance = dict()
                fact_performance["plan_id"] = fund.Plan_Id
                fact_performance["plan_name"] = res_factsheet["plan_name"]
                fact_performance["inception_date"] = res_factsheet["inception_date"]
                fact_performance["fund_id"] = fund.Fund_Name

                fact_performance["scheme_ret_1m"] = res_factsheet["scheme_ret_1m"]
                fact_performance["scheme_ret_3m"] = res_factsheet["scheme_ret_3m"]
                fact_performance["scheme_ret_6m"] = res_factsheet["scheme_ret_6m"]
                fact_performance["scheme_ret_1y"] = res_factsheet["scheme_ret_1y"]
                fact_performance["scheme_ret_2y"] = res_factsheet["scheme_ret_2y"]
                fact_performance["scheme_ret_3y"] = res_factsheet["scheme_ret_3y"]
                fact_performance["scheme_ret_5y"] = res_factsheet["scheme_ret_5y"]
                fact_performance["scheme_ret_ince"] = res_factsheet["scheme_ret_ince"]

                transaction_date = get_last_transactiondate(db_session, fund.Plan_Id)
                sql_fund_aum = db_session.query(FactSheet.NetAssets_Rs_Cr)\
                                                    .filter(FactSheet.Is_Deleted != 1, FactSheet.Plan_Id == fund.Plan_Id)\
                                                    .filter(FactSheet.TransactionDate == transaction_date).one_or_none()
                fact_performance["aum"] = sql_fund_aum.NetAssets_Rs_Cr
                fact_performance["classification"] = fund.Classification_Name
                fact_performance["product_code"] = fund.Product_Code
                fact_performance["amc_logo"] = fund.AMC_Logo

                fact_performance_list.append(fact_performance)

                if not fund.Fund_Id in unique_fund_id:
                    portfolio_date = None

                    holding_query1 = db_session.query(UnderlyingHoldings.Portfolio_Date)\
                                                        .select_from(UnderlyingHoldings)\
                                                        .join(MFSecurity,MFSecurity.Fund_Id == UnderlyingHoldings.Fund_Id)\
                                                        .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                                        .filter(MFSecurity.Status_Id == 1, Plans.Is_Deleted != 1)\
                                                        .filter(MFSecurity.Is_Deleted != 1, UnderlyingHoldings.Is_Deleted != 1)\
                                                        .filter(Plans.Plan_Id == fund.Plan_Id)

                    holding = holding_query1.order_by(desc(UnderlyingHoldings.Portfolio_Date)).first()
                    if holding:
                        portfolio_date = holding.Portfolio_Date

                    holdings = get_fund_holdings(db_session, fund.Plan_Id, portfolio_date)
                    if holdings:
                        temp_dict = dict()
                        temp_dict["fund_id"] = fund.Fund_Id
                        temp_dict["holding"] = holdings
                        fullholding_list.append(temp_dict)

                    if sql_fund_aum:
                        total_aum = (total_aum + float(sql_fund_aum.NetAssets_Rs_Cr)) if sql_fund_aum.NetAssets_Rs_Cr else total_aum

                    unique_fund_id.append(fund.Fund_Id)

        resp["performance_data"] = fact_performance_list

        holding_final_data = dict()
        # get consolidated holdings
        for holdingdata in fullholding_list:
            a_fund_id = holdingdata["fund_id"]
            a_fund_holding = holdingdata["holding"]

            for securitydata in a_fund_holding:
                if not securitydata["security_name"] in holding_final_data:
                    holding_final_data[securitydata["security_name"]] = securitydata["value_in_inr"] if securitydata["value_in_inr"] != None else 0
                else:
                    value_in_inr = holding_final_data[securitydata["security_name"]]
                    if value_in_inr:
                        value_inr = (float(securitydata["value_in_inr"]) if securitydata["value_in_inr"] else 0) + (float(value_in_inr) if value_in_inr else 0)
                        holding_final_data[securitydata["security_name"]] = value_inr
        

        # loop through each dict item
        holding_sorted_desc = sorted(holding_final_data.items(), key=lambda x: x[1], reverse=True)
        for holding_data in holding_sorted_desc:
            holding_dict = dict()
            security_name = holding_data[0]
            value_in_inr = holding_data[1] / 10000000

            holding_percent_to_aum = None
            if total_aum:
                holding_percent_to_aum = float(value_in_inr * 100) / float(total_aum)
            holding_dict["security_name"] = security_name
            holding_dict["percent_to_aum"] = holding_percent_to_aum
            finalholding_list.append(holding_dict)
        
        resp["holding_data"] = finalholding_list

        # get sector data
        sector_final_data = dict()
        holding_list = list()
        unique_fund_id = list()

        for fund in sql_fund:
            if not fund.Fund_Id in unique_fund_id:
                portfolio_date = None

                transaction_date = get_last_transactiondate(db_session, fund.Plan_Id)

                holding_query1 = db_session.query(UnderlyingHoldings.Portfolio_Date)\
                                                    .select_from(UnderlyingHoldings)\
                                                    .join(MFSecurity,MFSecurity.Fund_Id == UnderlyingHoldings.Fund_Id)\
                                                    .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                                    .filter(MFSecurity.Status_Id == 1, Plans.Is_Deleted != 1)\
                                                    .filter(MFSecurity.Is_Deleted != 1, UnderlyingHoldings.Is_Deleted != 1)\
                                                    .filter(Plans.Plan_Id == fund.Plan_Id)

                holding = holding_query1.order_by(desc(UnderlyingHoldings.Portfolio_Date)).first()
                if holding:        
                    portfolio_date = holding.Portfolio_Date

                holdings = get_fund_holdings(db_session, fund.Plan_Id, portfolio_date, get_full_holding=True)
                if holdings:
                    temp_dict = dict()
                    temp_dict["fund_id"] = fund.Fund_Id
                    temp_dict["holding"] = holdings
                    holding_list.append(temp_dict)

                    unique_fund_id.append(fund.Fund_Id)

        for holdingdata in holding_list:
            a_fund_id = holdingdata["fund_id"]
            a_fund_holding = holdingdata["holding"]
        
            aum_value = db_session.query(FactSheet.NetAssets_Rs_Cr)\
                                            .select_from(FactSheet)\
                                            .join(Plans, Plans.Plan_Id == FactSheet.Plan_Id)\
                                            .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                                            .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                                            .filter(FactSheet.Is_Deleted != 1, MFSecurity.Status_Id == 1, Plans.Is_Deleted != 1, Fund.Is_Deleted != 1, Fund.Fund_Id == a_fund_id)\
                                            .order_by(desc(FactSheet.TransactionDate)).limit(1).scalar()

            for securitydata in a_fund_holding:
                aum = float(aum_value) if aum_value else 0
                if not securitydata["security_sector"] in sector_final_data:                    
                    valueinr = securitydata["value_in_inr"] if securitydata["value_in_inr"] else (float(securitydata["security_weight"]) * aum / 100 ) * 10000000 if securitydata["security_weight"] else 0

                    sector_final_data[securitydata["security_sector"]] = valueinr
                else:
                    value_in_inr = sector_final_data[securitydata["security_sector"]]                    
                    valueinr = securitydata["value_in_inr"] if securitydata["value_in_inr"] else (float(securitydata["security_weight"]) * aum / 100 ) * 10000000 if securitydata["security_weight"] else 0

                    sector_final_data[securitydata["security_sector"]] = float(value_in_inr) + float(valueinr)
        
        # loop through each dict item
        sector_final_data = sorted(sector_final_data.items(), key=lambda x: x[1], reverse=True)
        for holding_data in sector_final_data:
            holding_dict = dict()
            sector_name = holding_data[0]
            value_in_inr = holding_data[1] / 10000000

            holding_percent_to_aum = float(value_in_inr * 100) / float(total_aum) if value_in_inr else 0
            holding_dict["sector_name"] = sector_name
            holding_dict["percent_to_aum"] = round(holding_percent_to_aum,2)
            finalsector_list.append(holding_dict)

        resp["sector_data"] = finalsector_list

        # Fund AUM
        aum_planwise_list = list()
        for fund in sql_fund:            
            plan_aum = dict()
            get_date_in_milisecond = True if ts == 1 else False
            aum_resp = get_aum_monthwise(db_session, fund.Plan_Id, None, get_date_in_milisecond)

            plan_aum["aumdata"] = aum_resp
            plan_aum["plan_name"] = fund.Plan_Name
            plan_aum["plan_id"] = fund.Plan_Id
            plan_aum["classification"] = fund.Classification_Name
            aum_planwise_list.append(plan_aum)
        
        resp["aum_data"] = aum_planwise_list

        # Fund manager history
        fundmanager_hist = list()
        sql_fundmanager = db_session.query(FundManager.FundManager_Name, FundManager.FundManager_Code, Fund.Fund_Name, Fund.Fund_Id, Fund.Fund_Code, FundManager.DateFrom, FundManager.DateTo).select_from(FundManager).join(Fund, Fund.Fund_Id == FundManager.Fund_Id).filter(FundManager.FundManager_Code == fundmanager_code).filter(FundManager.Is_Deleted != 1).all()

        if sql_fundmanager:
            for fundmanager in sql_fundmanager:
                data = dict()
                data["fundmanager_name"] = fundmanager.FundManager_Name
                data["fundmanager_Code"] = fundmanager.FundManager_Code
                data["fund_name"] = fundmanager.Fund_Name
                data["fund_id"] = fundmanager.Fund_Id
                data["fund_code"] = fundmanager.Fund_Code
                data["date_to"] = fundmanager.DateTo if fundmanager.DateTo else today

                if fundmanager.DateFrom:
                    data["date_from"] = fundmanager.DateFrom
                else:
                    sql_opendate = db_session.query(MFSecurity.MF_Security_OpenDate).filter(MFSecurity.Fund_Id == fundmanager.Fund_Id).filter(MFSecurity.Is_Deleted != 1).limit(1).scalar()
                    data["date_from"] = sql_opendate if sql_opendate else None

                fundmanager_hist.append(data)
        
        resp["fundmanager_history_data"] = fundmanager_hist
    return resp


def get_fund_portfolio_movement_data_by_date(db_session, prev_date, cur_date, product_ids=[]):
    ClosingValues1 = aliased(ClosingValues)

    fundstock_query = db_session.query(
            FundStocks.Fund_Id,
            FundStocks.Fund_Name,
            FundStocks.Product_Name,
            FundStocks.Product_Id,
            HoldingSecurity.ISIN_Code.label('isin_code'),
            FundStocks.HoldingSecurity_Name.label('holdingsecurity_name'), 
            FundStocks.HoldingSecurity_Id.label('holdingsecurity_id'), 
            FundStocks.MarketCap.label('marketcap'),  
            FundStocks.Equity_Style.label('investmentstyle'),
            HoldingSecurity.Co_Code.label('co_code'),
            FundStocks.Sector_Code.label('Sector_Code'),
            FundStocks.Sector_Code.label('Sector_Names'),
            FundStocks.Classification_Name.label('Classification_Name'),
            FundStocks.Portfolio_Date.label('Portfolio_Date'),
            FundStocks.Percentage_to_AUM.label('percentage_to_aum'),
            FundStocks.Value_In_Inr.label('value_in_inr'),
            (FundStocks.Value_In_Inr/ClosingValues.CLOSE).label('units'),
            ClosingValues.CLOSE.label('portfolio_unit_price'),
            ClosingValues1.CLOSE.label('current_unit_price'),
            ((FundStocks.Value_In_Inr/ClosingValues.CLOSE)*ClosingValues1.CLOSE).label('current_value_in_inr')      
        ).join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == FundStocks.HoldingSecurity_Id)\
        .join(ClosingValues, and_(cast(ClosingValues.Co_Code, sqlalchemy.String) == HoldingSecurity.Co_Code, ClosingValues.Date_ == prev_date))\
        .join(ClosingValues1, and_(cast(ClosingValues1.Co_Code, sqlalchemy.String) == HoldingSecurity.Co_Code, ClosingValues1.Date_ == cur_date))\
        .filter(HoldingSecurity.ISIN_Code.like("INE%"),
                FundStocks.InstrumentType == 'Equity',
                ClosingValues.ST_EXCHNG == 'NSE',
                ClosingValues1.ST_EXCHNG == 'NSE',
                FundStocks.Product_Id.in_(product_ids)
                )
    df = pd.DataFrame(fundstock_query.all())

    return df    
                                             
#TODO we can use below function in equity analysis. Need to modify below function little as per requirement. Currently we are not using it.
def get_security_movement_data_by_date(db_session):
    # get last two dates for which closing value is available
    max_closing_nav_dates = db_session.query((ClosingValues.Date_).label('max_date'))\
                                                        .filter(ClosingValues.Is_Deleted != 1, 
                                                                ClosingValues.ST_EXCHNG == 'NSE',
                                                                HoldingSecurity.Is_Deleted != 1).distinct().order_by(desc(ClosingValues.Date_))\
                                                        .limit(2).all()
    
    dates = []
    for max_dates in max_closing_nav_dates:
        dates.append(datetime.strftime(max_dates.max_date ,'%Y-%m-%d'))
    
    securities_subquery = db_session.query(HoldingSecurity.HoldingSecurity_Id)\
                                                        .join(FundStocks, FundStocks.HoldingSecurity_Id == HoldingSecurity.HoldingSecurity_Id)\
                                                        .filter(HoldingSecurity.Is_Deleted != 1,
                                                                HoldingSecurity.active == 1,
                                                                HoldingSecurity.Co_Code != None).subquery()
                                                                    
    sql_data = db_session.query(HoldingSecurity.Co_Code,
                                            HoldingSecurity.ISIN_Code,
                                            HoldingSecurity.HoldingSecurity_Name,
                                            Sector.Sector_Name,
                                            HoldingSecurity.Sub_SectorName,
                                            HoldingSecurity.MarketCap,
                                            HoldingSecurity.Issuer_Name,
                                            HoldingSecurity.Asset_Class,
                                            HoldingSecurity.Instrument_Type,
                                            HoldingSecurity.Equity_Style,
                                            ClosingValues.Date_,
                                            ClosingValues.CLOSE,
                                            func.lag(ClosingValues.CLOSE).over(partition_by=(HoldingSecurity.Co_Code),order_by=ClosingValues.Date_).label('prev_close'),
                                            (((((ClosingValues.CLOSE - func.lag(ClosingValues.CLOSE).over(partition_by=(HoldingSecurity.Co_Code),order_by=ClosingValues.Date_)) / func.lag(ClosingValues.CLOSE).over(partition_by=(HoldingSecurity.Co_Code),order_by=ClosingValues.Date_))) * 100)).label('returns_1d'),
                                            Fundamentals.PE,
                                            Fundamentals.PE_CONS,
                                            Fundamentals.EPS,
                                            Fundamentals.EPS_CONS,
                                            Fundamentals.PBV,
                                            Fundamentals.PBV_CONS,
                                            Fundamentals.DivYield,
                                            Fundamentals.mcap,)\
                                            .join(HoldingSecurity, HoldingSecurity.Co_Code == ClosingValues.Co_Code)\
                                            .join(securities_subquery, securities_subquery.c.HoldingSecurity_Id == HoldingSecurity.HoldingSecurity_Id)\
                                            .join(Fundamentals, and_(Fundamentals.CO_CODE == ClosingValues.Co_Code, Fundamentals.PriceDate == ClosingValues.Date_))\
                                            .join(Sector, HoldingSecurity.Sector_Id == Sector.Sector_Id, isouter=True)\
                                            .filter(ClosingValues.Is_Deleted != 1,
                                                    ClosingValues.ST_EXCHNG == 'NSE',
                                                    ClosingValues.Is_Deleted != 1,
                                                    HoldingSecurity.Is_Deleted != 1,
                                                    HoldingSecurity.active != 0,
                                                    ClosingValues.Date_.in_(dates),
                                                    Fundamentals.PriceDate.in_(dates),
                                                    )\
                                            .distinct().all()
    df = pd.DataFrame(sql_data)
    if not df.empty:        
        df = df.fillna(np.nan).replace([np.nan], [None])
        df = df.loc[(df['returns_1d'] > 0) | (df['returns_1d'] < 0)]
    
    return df

def get_plans_list_product_wise(db_session, product_id=1, report_date=None, previous_month=None):    
    sql_plans_subquery = db_session.query(Plans.Plan_Id, func.max(FactSheet.TransactionDate).label('TransactionDate'))\
                            .select_from(Plans)\
                            .join(MFSecurity, and_(MFSecurity.MF_Security_Id == Plans.MF_Security_Id, MFSecurity.Status_Id == 1))\
                            .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                            .join(FactSheet, FactSheet.Plan_Id == Plans.Plan_Id)\
                            .filter(FactSheet.Is_Deleted != 1,
                                    PlanProductMapping.Is_Deleted != 1,
                                    PlanProductMapping.Product_Id == product_id,
                                    FactSheet.TransactionDate <= report_date
                                    # ,FactSheet.TransactionDate >= previous_month
                                    )\
                            .group_by(Plans.Plan_Id).subquery()
    
    sql_plans = db_session.query(AMC.AMC_Name,
                                Plans.Plan_Id, 
                                 Plans.Plan_Name, 
                                 Plans.ISIN, 
                                 Plans.ISIN2, 
                                 FactSheet.TransactionDate, 
                                 FactSheet.ExpenseRatio, 
                                 AssetClass.AssetClass_Name, 
                                 Classification.Classification_Name,
                                 FactSheet.NetAssets_Rs_Cr,
                                 FactSheet.StandardDeviation_1Yr,
                                 FactSheet.Beta_1Yr,
                                 FactSheet.StandardDeviation,
                                 FactSheet.Beta,
                                 NAV.NAV,
                                 MFSecurity.MF_Security_OpenDate,
                                 FactSheet.Alpha_1Yr,
                                 FactSheet.Alpha,
                                 FactSheet.SCHEME_RETURNS_1YEAR,
                                 FactSheet.SCHEME_RETURNS_3YEAR,
                                 FactSheet.SCHEME_RETURNS_5YEAR,
                                 FactSheet.SCHEME_BENCHMARK_RETURNS_1YEAR,
                                 FactSheet.SCHEME_BENCHMARK_RETURNS_3YEAR,
                                 FactSheet.SCHEME_BENCHMARK_RETURNS_5YEAR,
                                 FundType.FundType_Name,
                                 BenchmarkIndices.BenchmarkIndices_Name,
                                 Fund.Fund_Id,
                                 FactSheet.SCHEME_RETURNS_1MONTH,
                                 Plans.Plan_Code)\
                            .select_from(Plans)\
                            .join(MFSecurity, and_(MFSecurity.MF_Security_Id == Plans.MF_Security_Id, MFSecurity.Status_Id == 1))\
                            .join(BenchmarkIndices, MFSecurity.BenchmarkIndices_Id == BenchmarkIndices.BenchmarkIndices_Id)\
                            .join(Fund, and_(Fund.Fund_Id == MFSecurity.Fund_Id, Fund.Is_Deleted != 1))\
                            .join(AMC, and_(AMC.AMC_Id == MFSecurity.AMC_Id, AMC.Is_Deleted != 1))\
                            .join(PlanProductMapping, and_(PlanProductMapping.Plan_Id == Plans.Plan_Id, PlanProductMapping.Is_Deleted != 1))\
                            .join(sql_plans_subquery, sql_plans_subquery.c.Plan_Id == Plans.Plan_Id)\
                            .join(FactSheet, (FactSheet.Plan_Id == Plans.Plan_Id) & (FactSheet.Is_Deleted != 1) & (FactSheet.TransactionDate == sql_plans_subquery.c.TransactionDate), isouter = True)\
                            .join(AssetClass, and_(AssetClass.AssetClass_Id == MFSecurity.AssetClass_Id, 
                                                   AssetClass.Is_Deleted != 1), isouter = True)\
                            .join(Classification, and_(Classification.Classification_Id == MFSecurity.Classification_Id, 
                                                       Classification.Is_Deleted != 1), isouter = True)\
                            .join(NAV, (NAV.Plan_Id == Plans.Plan_Id) & (NAV.NAV_Date == FactSheet.TransactionDate) & (NAV.NAV_Type == 'P') & (NAV.Is_Deleted != 1), isouter = True)\
                            .join(FundType, and_(FundType.FundType_Id == MFSecurity.FundType_Id,
                                            FundType.Is_Deleted != 1), isouter = True)\
                            .filter(MFSecurity.Status_Id == 1,
                                    PlanProductMapping.Product_Id == product_id,                                     
                                    AMC.Is_Deleted != 1,
                                    Plans.Is_Deleted != 1,
                                    Plans.Heartbeat_Date != None
                                    ).order_by(Plans.Plan_Name).all()

    return sql_plans




'''
    Fixed income importer related functions below
'''

# seperate the datasets in separate files
def split_files_for_bilav(file_path, asof_date, dict_columns):
    '''
        The encoding of file is found to be changing, use the link below to figure out the encoding.
        https://stackoverflow.com/a/68726315
        Ask bilav to keep the encoding consistent as per the data's best suitability
    '''
    print('File path to split files is ', file_path)
    with open(file_path) as f:
        r = f.readlines()

    dict_filenames = {}
    for key, val in dict_columns.items():
        print(f'Splitting data for {key}')
        is_column_set = 0
        for i in range(len(r) + 1):
            # setting column header
            if is_column_set:
                row = r[i-1]
                data_type = r[i-1].split('|')[0]
            else:
                row = '|'.join(val)
                row += '\n'
                is_column_set = 1
                data_type = key

            if key == data_type.upper():
                # write the row in the file
                location = os.path.dirname(file_path)  # /sample/test/
                file_name = "bilav_" + data_type.upper() + "_" + asof_date.strftime("%d%m%Y") + ".csv"
                file_name = os.path.join(location, file_name)
                with open(file_name, 'a') as f:
                    f.write(row)

                # capture new filenames
                if not (key in dict_filenames):
                    dict_filenames[key] = file_name

    return dict_filenames


# data reader method for bilav files
def get_fi_pricedata(file_name):
    '''
        The encoding of file is found to be changing, use the link below to figure out the encoding.
        https://stackoverflow.com/a/68726315
        Ask bilav to keep the encoding consistent as per the data's best suitability
    '''
    print(file_name)
    if os.path.isfile(file_name):
        print('Reading the file into df...')
        df = pd.read_csv(file_name, sep =',', header=0, encoding='unicode_escape') # encoding='utf-8-sig'   encoding= 'unicode_escape'
    else:
        df = pd.DataFrame()

    return df


# data reader method for bilav files
def get_fi_data_df(file_name):
    '''
        The encoding of file is found to be changing, use the link below to figure out the encoding.
        https://stackoverflow.com/a/68726315
        Ask bilav to keep the encoding consistent as per the data's best suitability
    '''
    print(file_name)
    if os.path.isfile(file_name):
        print('Reading the file into df...')
        df = pd.read_csv(file_name, sep ='|', header=0, encoding='mbcs', index_col=False)
    else:
        df = pd.DataFrame()

    return df


# insert/update the datasets in the database
def import_debt_security_price_data(db_session, df_price, user_id, asof_date):
    lst_exception = []
    lst_data = df_price.to_dict(orient='records')

    for record in lst_data:
        isin = record.get('ISIN')
        today = dt1.now()

        try:
            print(f'Checking if the security is available in debt master - {isin}')
            sql_debt = db_session.query(DebtSecurity).filter(DebtSecurity.ISIN == isin).one_or_none()

            if sql_debt:
                record['DebtSecurity_Id'] = sql_debt.DebtSecurity_Id
                record['Created_By'] = user_id
                record['Created_Date'] = today
                record['Updated_By'] = None
                record['Updated_Date'] = None
                record['AsofDate'] = asof_date
                record['Is_Deleted'] = False
                record['ISIN'] = isin

                debt_price = DebtPrice(**record)
                db_session.add(debt_price)
            else:
                lst_exception.append({
                                        'isin':isin,
                                        'table':'DebtPrice',
                                        'exception_info':f'The record with {isin} does not exist in the debt master table.'
                                    })


        except Exception as ex:
            print(f'Exception while importing debt security price data {isin} - {ex}')
            ex_record = {}
            ex_record['isin'] = isin
            ex_record['table'] = 'DebtPrice'
            ex_record['exception_info'] = str(ex)
            lst_exception.append(ex_record)

    db_session.commit()

    return lst_exception


# insert/update the datasets in the database
def create_or_update_holding_security_for_debt(db_session, lst_data, user_id):
    lst_exception_data = []

    for record in lst_data:
        isin = record.get('ISIN_Code')
        today = dt1.now()
        record['HoldingSecurity_Type'] = 'DEBT'

        try:
            db_session.begin()

            print(f'Checking if the security is available in master - {isin}')
            sql_holding = db_session.query(HoldingSecurity).filter(HoldingSecurity.ISIN_Code == isin,
                                                                   HoldingSecurity.active == 1,
                                                                   HoldingSecurity.Is_Deleted != 1)

            # if holdings exists then update else create a new one
            holding = sql_holding.one_or_none()
            if holding:
                record['Updated_By'] = user_id
                record['Updated_Date'] = today
                record['Is_Deleted'] = False

                update_query = sql_holding.update(record)
                print(f'Successful update for isin {isin}')
            else:
                record['Created_By'] = user_id
                record['Created_Date'] = today
                record['Is_Deleted'] = False
                record['active'] = True

                holding_security = HoldingSecurity(**record)
                db_session.add(holding_security)

                print(f"Successful insert for isin {isin}")

            db_session.commit()
        except Exception as ex:
            print(f'Exception while importing holding security data {isin} - {ex}')
            lst_exception_data.append({
                'isin': isin,
                'dataset': 'HoldingSecurity',
                'exception_info': str(ex)
            })
            db_session.rollback()

    return lst_exception_data


def create_or_update_debt_security(db_session, lst_data, user_id):
    lst_exception_data = []

    for record in lst_data:
        isin = record.get('ISIN')
        record_action = record.pop('Record_Action', None)
        today = dt1.now()

        try:
            db_session.begin()
            
            print(f'Checking if the security is available in debt master - {isin}')
            print(f'Record Action is {record_action}')
            sql_debt = db_session.query(DebtSecurity).filter(DebtSecurity.ISIN == isin)

            # check record action then perform create/update/delete
            debt_sec = sql_debt.one_or_none()
            if debt_sec:
                record['Updated_By'] = user_id
                record['Updated_Date'] = today
                record['Is_Deleted'] = 0

                update_query = sql_debt.update(record)
                print(f'Successful debt master update for isin {isin}')
            elif record_action == 'D':
                record['Updated_By'] = user_id
                record['Updated_Date'] = today
                record['Is_Deleted'] = True

                update_query = sql_debt.update(record)
                print(f'Successful debt master delete for isin {isin}')
            else:
                record['Created_By'] = user_id
                record['Created_Date'] = today
                record['Is_Deleted'] = False

                debt_sec = DebtSecurity(**record)
                db_session.add(debt_sec)
                print(f"Successful debt master insert for isin {isin}")

            db_session.commit()
        except Exception as ex:
            print(f'Exception while importing debt security master data {isin} - {ex}')
            lst_exception_data.append({
                'isin': isin,
                'dataset': 'DebtSecurity',
                'exception_info': str(ex)
            })
            db_session.rollback()

    return lst_exception_data


def create_or_update_debt_redemption(db_session, lst_data, user_id):
    lst_exception_data = []

    for record in lst_data:
        isin = record.get('ISIN')
        redemption_id = record.get('DebtRedemption_Id')
        record_action = record.pop('Record_Action', None)
        today = dt1.now()

        try:
            db_session.begin()

            print(f'Checking if the redemption record is available - {isin}')
            print(f'Record Action is {record_action}')
            sql_redmt = db_session.query(DebtRedemption).filter(DebtRedemption.ISIN == isin, DebtRedemption.DebtRedemption_Id == redemption_id)
            sql_debt = db_session.query(DebtSecurity).filter(DebtSecurity.ISIN == isin).one_or_none()

            # check record action then perform create/update/delete
            debt_redmt = sql_redmt.one_or_none()
            if debt_redmt and sql_debt:
                record['Updated_By'] = user_id
                record['Updated_Date'] = today
                record['Is_Deleted'] = False

                update_query = sql_redmt.update(record)
                print(f'Successful debt redemption update for isin {isin}')
            elif debt_redmt and record_action == 'D':
                record['Updated_By'] = user_id
                record['Updated_Date'] = today
                record['Is_Deleted'] = True

                update_query = sql_redmt.update(record)
                print(f'Successful debt redemption delete for isin {isin}')
            elif not debt_redmt and sql_debt:
                record['Created_By'] = user_id
                record['Created_Date'] = today
                record['Is_Deleted'] = False

                debt_redmt = DebtRedemption(**record)
                db_session.add(debt_redmt)
                print(f"Successful debt redemeption insert for isin {isin}")

            db_session.commit()
        except Exception as ex:
            print(f'Exception while importing debt redemption data {isin} - {ex}')
            lst_exception_data.append({
                'isin': isin,
                'dataset': 'DebtRedemption',
                'exception_info': str(ex)
            })
            db_session.rollback()

    return lst_exception_data


def create_or_update_debt_calloption(db_session, lst_data, user_id):
    lst_exception_data = []

    for record in lst_data:
        isin = record.get('ISIN')
        calloption_id = record.get('DebtCallOption_Id')
        record_action = record.pop('Record_Action', None)
        today = dt1.now()

        try:
            db_session.begin()

            print(f'Checking if the debt call option record is available - {isin}')
            print(f'Record Action is {record_action}')
            sql_call = db_session.query(DebtCallOption).filter(DebtCallOption.ISIN == isin, DebtCallOption.DebtCallOption_Id == calloption_id)
            sql_debt = db_session.query(DebtSecurity).filter(DebtSecurity.ISIN == isin).one_or_none()

            # check record action then perform create/update/delete
            debt_call = sql_call.one_or_none()
            if debt_call and sql_debt:
                record['Updated_By'] = user_id
                record['Updated_Date'] = today
                record['Is_Deleted'] = False

                # print(f'Record to be updated {record}')
                update_query = sql_call.update(record)
                print(f'Successful debt call option update for isin {isin}')
            elif debt_call and record_action == 'D':
                record['Updated_By'] = user_id
                record['Updated_Date'] = today
                record['Is_Deleted'] = True

                update_query = sql_call.update(record)
                print(f'Successful debt call option delete for isin {isin}')
            elif not debt_call and sql_debt:
                record['Created_By'] = user_id
                record['Created_Date'] = today
                record['Is_Deleted'] = False

                debt_call = DebtCallOption(**record)
                db_session.add(debt_call)
                print(f"Successful debt call option insert for isin {isin}")

            db_session.commit()
        except Exception as ex:
            print(f'Exception while importing debt call option data {isin} - {ex}')
            lst_exception_data.append({
                'isin': isin,
                'dataset': 'DebtCallOption',
                'exception_info': str(ex)
            })
            db_session.rollback()

    return lst_exception_data


def create_or_update_debt_putoption(db_session, lst_data, user_id):
    lst_exception_data = []

    for record in lst_data:
        isin = record.get('ISIN')
        putoption_id = record.get('DebtPutOption_Id')
        record_action = record.pop('Record_Action', None)
        today = dt1.now()

        try:
            db_session.begin()

            print(f'Checking if the debt put option record is available - {isin}')
            print(f'Record Action is {record_action}')
            sql_put = db_session.query(DebtPutOption).filter(DebtPutOption.ISIN == isin, DebtPutOption.DebtPutOption_Id == putoption_id)
            sql_debt = db_session.query(DebtSecurity).filter(DebtSecurity.ISIN == isin).one_or_none()

            # check record action then perform create/update/delete
            debt_put = sql_put.one_or_none()
            if debt_put and sql_debt:
                record['Updated_By'] = user_id
                record['Updated_Date'] = today
                record['Is_Deleted'] = False

                update_query = sql_put.update(record)
                print(f'Successful debt put option update for isin {isin}')
            elif debt_put and record_action == 'D':
                record['Updated_By'] = user_id
                record['Updated_Date'] = today
                record['Is_Deleted'] = True

                update_query = sql_put.update(record)
                print(f'Successful debt put option delete for isin {isin}')
            elif not debt_put and sql_debt:
                record['Created_By'] = user_id
                record['Created_Date'] = today
                record['Is_Deleted'] = False

                debt_put = DebtPutOption(**record)
                db_session.add(debt_put)
                print(f"Successful debt put option insert for isin {isin}")

            db_session.commit()
        except Exception as ex:
            print(f'Exception while importing debt put option data {isin} - {ex}')
            lst_exception_data.append({
                'isin': isin,
                'dataset': 'DebtPutOption',
                'exception_info': str(ex)
            })
            db_session.rollback()

    return lst_exception_data


def create_or_update_debt_credit_ratings(db_session, lst_data, user_id, asof_date):
    lst_exception_data = []

    for record in lst_data:
        isin = record.get('ISIN')
        rating_id = record.get('DebtCreditRating_Id')
        today = dt1.now()
        record_action = record.pop('Record_Action')
        try:
            db_session.begin()

            print(f'Checking if the security is available in debt master - {isin}')
            sql_debt = db_session.query(DebtSecurity).filter(DebtSecurity.ISIN == isin).one_or_none()
            sql_rating = db_session.query(DebtCreditRating).filter(DebtCreditRating.ISIN == isin, DebtCreditRating.DebtCreditRating_Id == rating_id)
            debt_rating = sql_rating.one_or_none()

            if sql_debt and debt_rating:
                record['Updated_By'] = user_id
                record['Updated_Date'] = today
                record['Is_Deleted'] = False

                update_query = sql_rating.update(record)
                print(f'Successful debt credit ratings update for isin {isin}')
            elif debt_rating and record_action == 'D':
                record['Updated_By'] = user_id
                record['Updated_Date'] = today
                record['Is_Deleted'] = True

                update_query = sql_rating.update(record)
                print(f'Successful debt credit ratings delete for isin {isin}')
            elif sql_debt:
                record['Created_By'] = user_id
                record['Created_Date'] = today
                record['Updated_By'] = None
                record['Updated_Date'] = None
                record['AsofDate'] = asof_date
                record['Is_Deleted'] = False
                record['ISIN'] = isin

                debt_credit_ratings = DebtCreditRating(**record)
                db_session.add(debt_credit_ratings)
                print(f"Successfully inserted credit ratings for isin - {isin}")
            else:
                lst_exception_data.append({
                    'isin': isin,
                    'dataset': 'DebtCreditRating',
                    'exception_info': f'The record with {isin} is not inserted/updated.'
                })

            db_session.commit()
        except Exception as ex:
            print(f'Exception while importing debt security credit ratings data {isin} - {ex}')
            lst_exception_data.append({
                'isin': isin,
                'dataset': 'DebtCreditRating',
                'exception_info': str(ex)
            })
            db_session.rollback()

    return lst_exception_data

import pandas as pd
import numpy as np
from sqlalchemy import func, desc
import xml.etree.ElementTree as ET
from datetime import date, datetime as dt
from typing import Union
from datetime import timedelta
from itsdangerous import json

from fin_models.masters_models import *
from fin_models.transaction_models import *
from fin_models.controller_master_models import User
from fin_models.servicemanager_models import *
from dateutil.relativedelta import relativedelta
from utils.time_func import get_next_date 
from sqlalchemy import inspect
from data.benchmark import get_benchmark_tri_returns
from data.holdings import get_fund_underlying_holdings


def get_detailed_fund_holdings(db_session, plan_id, fund_id, portfolio_date, get_full_holding=False):
    # TODO Labels of mappings and casing can be reviewed for the response on the function
    limit = 10
    hide_holding_weightage = False
    if plan_id and not fund_id:
        mf_security_id = db_session.query(Plans.MF_Security_Id).filter(Plans.Plan_Id == plan_id).scalar()
        fund_id = db_session.query(MFSecurity.Fund_Id).filter(MFSecurity.MF_Security_Id == mf_security_id).scalar()
        fund_qry = db_session.query(Fund.Fund_Id,
                                    Fund.Top_Holding_ToBeShown,
                                    Fund.HideHoldingWeightage,
                                    Fund.AIF_CATEGORY,
                                    Fund.AIF_SUB_CATEGORY)\
                            .filter(Fund.Fund_Id == fund_id).first()

        if fund_qry:
            limit = fund_qry.Top_Holding_ToBeShown if fund_qry.Top_Holding_ToBeShown else limit
            hide_holding_weightage = fund_qry.HideHoldingWeightage if fund_qry.HideHoldingWeightage else hide_holding_weightage


    if get_full_holding:
        limit = None

    df_holdings = pd.DataFrame(get_fund_underlying_holdings(db_session,
                                                            fund_id=fund_id,
                                                            portfolio_date=portfolio_date,
                                                            limit=limit))

    if fund_id and portfolio_date and fund_qry.AIF_CATEGORY == 2 and str(fund_qry.AIF_SUB_CATEGORY).upper() == "REAL ESTATE":
        columns_set_1 = ["Company_Security_Name","Purchase_Date","Location_City","Amount_Invested_Crs",
                         "Total_Receipts_Crs","Exit_Date","Exit_IRR","Exit_Multiple","LISTED_UNLISTED"]

        df_holdings = df_holdings[columns_set_1]
        df_holdings["Id"] = 1

        df_total = pd.DataFrame(columns=columns_set_1)
        df_total["Id"] = 2
        df_total["Amount_Invested_Crs"] = df_holdings["Amount_Invested_Crs"].sum()
        df_total["Total_Receipts_Crs"] = df_holdings["Total_Receipts_Crs"].sum()
        df_total["Exit_IRR"] = df_holdings["Exit_IRR"].mean()
        df_total["Exit_Multiple"] = df_holdings["Exit_Multiple"].mean()

        df_holdings = pd.concat([df_holdings, df_total])

        # rename columns to keep mapping as is for no impact downstream
        df_holdings.rename(columns={'Company_Security_Name' : 'HoldingSecurity_Name'}, inplace=True)
        df_holdings['Purchase_Date'] = df_holdings['Purchase_Date'].dt.strftime(r'%d %b %Y')

    elif fund_qry.AIF_CATEGORY == 3:
        columns_set_2 = ["MarketCap","HoldingSecurity_Type","Risk_Category","Asset_Class","StocksRank","Sector_Names",
                         "LISTED_UNLISTED","IssuerName","Purchase_Date","HoldingSecurity_Name","HoldingSecurity_Id",
                         "ISIN_Code","Portfolio_Date","Value_in_INR","Percentage_to_AUM","Equity_Style","LONG_SHORT"]

        # for long
        df_holdings = df_holdings[columns_set_2]
        df_long = df_holdings[ (df_holdings["LONG_SHORT"] == "L") or (df_holdings["LONG_SHORT"] == np.nan)]
        df_long.sort_values(["Percentage_to_AUM"], inplace=True)
        df_long.reset_index(drop=True)
        df_long["plan_Id"] = plan_id
        df_long["LISTED_UNLISTED"] = np.where(df_long["LISTED_UNLISTED"], df_long["LISTED_UNLISTED"], "Listed")
        df_long["LONG_SHORT"] = "L"

        # for short
        df_short = df_holdings[df_holdings["LONG_SHORT"] == "S"]
        df_short.sort_values(["Percentage_to_AUM"], inplace=True)
        df_short.reset_index(drop=True)
        df_short["plan_Id"] = plan_id
        df_short["LISTED_UNLISTED"] = np.where(df_short["LISTED_UNLISTED"], df_short["LISTED_UNLISTED"], "Listed")
        df_short["LONG_SHORT"] = "L"

        df_holdings = pd.concat([df_long, df_short])
        df_holdings['Purchase_Date'] = df_holdings['Purchase_Date'].dt.strftime(r'%d %b %Y')
        df_holdings['Portfolio_Date'] = df_holdings['Portfolio_Date'].dt.strftime(r'%d %b %Y')

        # column names to lower case
        df_holdings.columns = map(str.lower, df_holdings.columns)

        # rename columns to keep mapping as is for no impact downstream
        df_holdings.rename(columns={
            'holdingsecurity_type' : 'instrument_type',
            'marketcap': 'market_cap',
            'purchase_date': 'purchasedate',
            'sector_names': 'sector_name'
        }, inplace=True)


    elif fund_id and portfolio_date and fund_qry.AIF_CATEGORY == 2 and str(fund_qry.AIF_SUB_CATEGORY).upper() == "DEBT":
        columns_set_3 = ["ISIN_Code","Company_Security_Name","HoldingSecurity_Id","ISIN_Code","Portfolio_Date",
                         "Instrument","Instrument_Rating","Amount_Invested_Crs","Percentage_to_AUM","Co_Code"]

        df_holdings = df_holdings[columns_set_3]
        df_holdings["plan_id"] = plan_id
        df_holdings["Percentage_to_AUM"] = df_holdings["Percentage_to_AUM"] if hide_holding_weightage else None
        df_holdings['Portfolio_Date'] = df_holdings['Portfolio_Date'].dt.strftime(r'%d %b %Y')

        # column names to lower case
        df_holdings.columns = map(str.lower, df_holdings.columns)

        # rename columns to keep mapping as is for no impact downstream
        df_holdings.rename(columns={
            'holdingsecurity_type' : 'instrument_type',
            'isin_code': 'isin',
            'amount_invested_crs': 'amount_invested_in_cr',
            'percentage_to_aum': 'holding_weightage',
            'company_security_name': 'issuer_name',
            'instrument_rating': 'rating'
        }, inplace=True)


    else:
        columns_set_4 = ["MarketCap","HoldingSecurity_Type","Risk_Category","LISTED_UNLISTED","Purchase_Date", "Asset_Class",
                         "HoldingSecurity_Name","HoldingSecurity_Id","ISIN_Code","Portfolio_Date","Value_in_INR",
                         "Percentage_to_AUM","Equity_Style","Sector_Name","Company_Security_Name","Co_Code","DebtSecurity_Id","active"]

        df_holdings = df_holdings[columns_set_4]
        df_holdings["plan_id"] = plan_id
        df_holdings["Percentage_to_AUM"] = df_holdings["Percentage_to_AUM"] if not hide_holding_weightage else None
        df_holdings["stocksrank"] = np.where(df_holdings["HoldingSecurity_Type"] == "LISTED EQUITY", df_holdings["MarketCap"], "")
        df_holdings["LISTED_UNLISTED"] = np.where(df_holdings["LISTED_UNLISTED"], df_holdings["LISTED_UNLISTED"], "Listed")
        df_holdings['Purchase_Date'] = df_holdings['Purchase_Date'].dt.strftime(r'%d %b %Y')
        df_holdings['Portfolio_Date'] = df_holdings['Portfolio_Date'].dt.strftime(r'%d %b %Y')
        df_holdings['HoldingSecurity_Id'] = df_holdings['HoldingSecurity_Id'].astype('Int64')
        df_holdings['HoldingSecurity_Name'] = np.where((df_holdings['HoldingSecurity_Name'] == "") | (df_holdings['HoldingSecurity_Name'].isna()) | (df_holdings['HoldingSecurity_Name'].isnull()),
                                                       df_holdings['Company_Security_Name'], df_holdings['HoldingSecurity_Name'])
        df_holdings['ISIN_Code'] = np.where(df_holdings['ISIN_Code'].isna(), "", df_holdings['ISIN_Code'])

        # column names to lower case
        df_holdings.columns = map(str.lower, df_holdings.columns)

        # rename columns to keep mapping as is for no impact downstream
        df_holdings.rename(columns={
            'holdingsecurity_type' : 'instrument_type',
            'marketcap': 'market_cap',
            'purchase_date': 'purchasedate',
            'debtsecurity_id': 'debt_security_id',
            'plan_id': 'plan_Id'
        }, inplace=True)


    result = df_holdings.to_json(orient="records")
    parsed = json.loads(result)

    return parsed




# TODO: This can be re-usable in various modules, hence shift this to other lib module as it is not just relevant to investor portfolio overlap
def get_benchmark_trailing_returns_for_all_period(db_session, benchmark_id, co_code, portfolio_date):
    response = dict()
    periods = ['1m', '3m', '6m', '1y', '3y', '5y']
    
    for period in periods:
        start_date = get_next_date(portfolio_date, period, False, True)
        if co_code:
            cum_return = calculate_benchmark_tri_returns(db_session, co_code, start_date, portfolio_date, 'absolute')
            
            if cum_return:
                data = dict()
                data["BenchmarkIndices_Id"] = benchmark_id
                data["Returns_Value"] = round(cum_return, 2)
                response[period] = data
        
    return response
    
def get_benchmarkdetails(db_session, benchmark_id, tri_co_code = None):
    benchmark_details = db_session.query(BenchmarkIndices)\
                                  .filter(BenchmarkIndices.Is_Deleted != 1)
                                  
    if benchmark_id:
        benchmark_details = benchmark_details.filter(BenchmarkIndices.BenchmarkIndices_Id == benchmark_id)

    if tri_co_code:
        benchmark_details = benchmark_details.filter(BenchmarkIndices.Co_Code == tri_co_code)

    benchmark_details = benchmark_details.one_or_none()

    return benchmark_details

def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month

def calculate_benchmark_tri_returns(db_session, tri_co_code, from_date: date, to_date: date, typeof_return='cagr', get_monthly_return_df=False):
    """ typeof_return can be cagr or absolute """
    cum_return = None

    tri_returns_data = get_benchmark_tri_returns(db_session, tri_co_code, from_date, to_date)

    df = pd.DataFrame(tri_returns_data)

    if get_monthly_return_df:
        if not df.empty:
            df.rename(columns={'TRI_IndexDate': 'NAV_Date', 'Return_1Month': 'returns'}, inplace = True)
            df["month"] = df["NAV_Date"].dt.strftime('%Y-%m')
            df.drop(['TRI_IndexName', 'Co_Code', 'TRI_Co_Code', 'NAV_Date'], axis=1, inplace=True)
        return df
    else:
        if not df.empty:
            df['Return_1Month'] = df['Return_1Month']/100
            df['Cumulative_Returns_In_Perc'] = (df['Return_1Month'].add(1).cumprod() - 1 )* 100
            df['Sampled_Cumulative_Value'] = 100 * (1 + df['Return_1Month']).cumprod()
            df['Rolling_Returns'] = (1 + df['Return_1Month']).rolling(window = df.shape[0]).apply(np.prod, raw = True) - 1
            df['Cumulative_Value'] = 100 * (1 + df['Rolling_Returns'])

            cum_return = df.loc[df.index[-1], "Cumulative_Returns_In_Perc"]

            if typeof_return == 'cagr':
                # This will take care of leap year and months
                years = relativedelta(to_date, from_date).years

                current_nav = df.loc[df.index[-1], "Sampled_Cumulative_Value"]

                if years:
                    return (calculate_cagr_return(100, current_nav, years) * 100)

    return cum_return


# TODO: Check if CAGR is only applicable for years or also for months
def calculate_cagr_return(start, end, period):
    return (end/start)**(1/period)-1

def get_fund_category(db_session, plan_id):
    mf_sec_qry = db_session.query(MFSecurity.Classification_Id,
                                  MFSecurity.AssetClass_Id,
                                  PlanProductMapping.Product_Id)\
                            .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                            .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                            .filter(Plans.Is_Deleted != 1, MFSecurity.Is_Deleted != 1, MFSecurity.Status_Id == 1)\
                            .filter(Plans.Plan_Id == plan_id).first()

    classification_id = mf_sec_qry[0]
    asset_class_id = mf_sec_qry[1]
    product_id = mf_sec_qry[2]

    return (classification_id, asset_class_id, product_id)


def get_funds_in_same_category(db_session, fund_category_info):
    sql_funds = db_session.query(Fund.Fund_Id)\
                              .select_from(Plans)\
                              .join(MFSecurity, MFSecurity.MF_Security_Id == Plans.MF_Security_Id)\
                              .join(Fund, Fund.Fund_Id == MFSecurity.Fund_Id)\
                              .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                              .filter(Plans.Is_Deleted != 1, Fund.Is_Deleted != 1,
                                      MFSecurity.Is_Deleted != 1, MFSecurity.Status_Id == 1)\
                              .filter(PlanProductMapping.Product_Id == fund_category_info[2],
                                      MFSecurity.Classification_Id == fund_category_info[0],
                                      MFSecurity.AssetClass_Id == fund_category_info[1])

    sql_fund_ids = sql_funds.distinct()

    # get all fund ids in a list to use in next query
    list_fund_ids = [r for (r, ) in sql_fund_ids]

    return list_fund_ids


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
    sql_request.Request_Time = dt.now()
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


def object_to_xml(data: Union[dict, bool], root='object'):
    xml = f'<{root}>'
    if isinstance(data, dict):
        for key, value in data.items():
            xml += object_to_xml(value, key)

    elif isinstance(data, (list, tuple, set)):
        for item in data:
            xml += object_to_xml(item, 'item')

    else:
        xml += str(data)

    xml += f'</{root}>'
    return xml

def convert_sqlalchemy_object_as_dict(obj):
    return {
        c.key: getattr(obj, c.key)
        for c in inspect(obj).mapper.column_attrs
    }

def get_user_details(db_session, user_id):
    sql_user = db_session.query(User).filter(User.User_Id == user_id).one_or_none()

    if not sql_user:
        return None
    
    return sql_user

def calculate_xirr(df):
    # ref below working
    # https://support.microsoft.com/en-us/office/xirr-function-de1242ec-6477-445b-b11b-a303ad9adc9d?ui=en-us&rs=en-us&ad=us
    irr = -0.1

    if len(df) > 0:
        max_date = df['date'].max()
        count = 0
        irrprev = 0
        prev_presentvalue = None
        presentvalue = None

        prev_presentvalue = (df['value']).sum()
        presentvalue = (-1 * df['value'] / 
                                pow(1e0 - irr, (df['date'] - max_date).dt.days / 365e0)
                       ).sum()

        while abs(presentvalue) >= 0.0001:
            count = count + 1
            if count > 1000:
                irr = 0
                break

            t = irrprev
            irrprev = irr
            irr = irr + (t - irr) * presentvalue / (presentvalue - prev_presentvalue)
            prev_presentvalue = presentvalue
            presentvalue = (-1 * df['value'] / 
                                pow(1e0 + irr, (df['date'] - max_date).dt.days / 365e0)
                           ).sum()

    return (irr * 100) if np.inf != irr else None

def get_index_instrument_type(db_session, benchmark_id, from_date, to_date):
    resp = list()

    max_index_date = db_session.query(func.max(IndexWeightage.WDATE)).select_from(IndexWeightage).join(BenchmarkIndices, BenchmarkIndices.Co_Code == IndexWeightage.Index_CO_CODE).filter(IndexWeightage.Is_Deleted != 1, BenchmarkIndices.BenchmarkIndices_Id == benchmark_id, IndexWeightage.WDATE >= from_date, IndexWeightage.WDATE <= to_date).scalar()

    if max_index_date:
        sql_index_data = db_session.query(BenchmarkIndices.BenchmarkIndices_Id, IndexWeightage.WDATE.label('Portfolio_Date'), HoldingSecurity.HoldingSecurity_Type, HoldingSecurity.MarketCap, HoldingSecurity.Equity_Style, HoldingSecurity.ISIN_Code, IndexWeightage.WEIGHT_INDEX.label('Percentage_to_AUM'))\
        .select_from(BenchmarkIndices)\
        .join(IndexWeightage, IndexWeightage.Index_CO_CODE == BenchmarkIndices.Co_Code)\
        .join(HoldingSecurity, HoldingSecurity.Co_Code == IndexWeightage.CO_CODE)\
        .filter(BenchmarkIndices.BenchmarkIndices_Id == benchmark_id, IndexWeightage.Is_Deleted != 1, HoldingSecurity.ISIN_Code != None, HoldingSecurity.Is_Deleted != 1, HoldingSecurity.active != 0, IndexWeightage.WDATE == max_index_date)\
        .all()

        df = pd.DataFrame(sql_index_data)

        if not df.empty:
            investment_type_df = df.groupby(["HoldingSecurity_Type"], as_index=False)\
                                        .agg(Percentage_to_AUM = ("Percentage_to_AUM", "sum"))\
                                        .sort_values(by="Percentage_to_AUM", ascending=False)
            resp = investment_type_df.to_dict('records')

            # equity_style_df = df.groupby(["MarketCap", "Equity_Style"], as_index=False)\
            #                     .agg(Percentage_to_AUM = ("Percentage_to_AUM", "sum"))\
            #                     .sort_values(by="Percentage_to_AUM", ascending=False)
            
            # equity_style_resp = investment_type_df.to_dict('records')
    return resp

def get_fund_historic_performance(db_session, plan_id, from_date: date , to_date: date, performance_type, transaction_date: date):
    resp = list()

    if not transaction_date:
        transaction_date = get_last_transactiondate(db_session, plan_id)

    if performance_type == "CY":
        all_max_nav_dates = db_session.query(func.max(NAV.NAV_Date))\
                                      .join(Plans, Plans.Plan_Id == NAV.Plan_Id)\
                                      .filter(Plans.Plan_Id == plan_id, NAV.NAV_Type == 'P', NAV.NAV_Date <= transaction_date)\
                                      .filter(NAV.Is_Deleted != 1, Plans.Is_Deleted != 1).group_by(func.year(NAV.NAV_Date)).order_by(desc(func.max(NAV.NAV_Date))).all()

        resp = get_fund_performance_byallmaxdates(db_session, plan_id, all_max_nav_dates, transaction_date, 'CY')

    elif performance_type == "FY":
        nav_qry = db_session.query(func.max(NAV.NAV_Date).label('max_date'))\
                            .join(Plans, Plans.Plan_Id == NAV.Plan_Id)\
                            .filter(Plans.Plan_Id == plan_id, NAV.NAV_Type == 'P', NAV.NAV_Date <= transaction_date)\
                            .filter(NAV.Is_Deleted != 1, Plans.Is_Deleted != 1)\

        max_nav_dates = nav_qry.filter(func.month(NAV.NAV_Date) == 3)\
                                   .group_by(func.year(NAV.NAV_Date)).order_by(desc(func.max(NAV.NAV_Date))) #.all()

        all_max_nav_dates = nav_qry.union(max_nav_dates).order_by(desc('max_date')).all()

        resp = get_fund_performance_byallmaxdates(db_session, plan_id, all_max_nav_dates, transaction_date, 'FY')

    elif performance_type == "MOM":
        transaction_date_1yr_bfr = transaction_date - timedelta(days=365)
        transaction_date_31day_bfr = transaction_date - timedelta(days=31)

        all_max_nav_dates = db_session.query(func.max(NAV.NAV_Date)).join(Plans, Plans.Plan_Id == NAV.Plan_Id).filter(Plans.Plan_Id == plan_id).filter(NAV.NAV_Type == 'P').filter(NAV.NAV_Date <= transaction_date).filter(NAV.Is_Deleted != 1).filter(Plans.Is_Deleted != 1).filter(NAV.NAV_Date > transaction_date_1yr_bfr).group_by(func.year(NAV.NAV_Date), func.month(NAV.NAV_Date)).order_by(desc(func.max(NAV.NAV_Date))).all()

        resp = get_fund_performance_byallmaxdates(db_session, plan_id, all_max_nav_dates, transaction_date_1yr_bfr, 'MOM')

    elif performance_type == "POP":
        # from_date = from_date - timedelta(days=1)

        from_max_nav_date = get_max_navdate_tilldate(db_session, plan_id, from_date)
        to_max_nav_date = get_max_navdate_tilldate(db_session, plan_id, to_date)

        if from_max_nav_date:
            data = get_fund_performance_between_twodates(db_session, plan_id, from_max_nav_date, to_max_nav_date)
            resp.append(data)

    return resp

def get_plan_meta_info(db_session, plan_list):
    details = dict()
    for plan in plan_list:
        sql_fund1 = db_session.query(AMC.AMC_Logo, Product.Product_Code, Product.Product_Name, Plans.Plan_Name, Plans.Plan_Code, Classification.Classification_Name, AMC.AMC_Name, BenchmarkIndices.BenchmarkIndices_Name)\
            .select_from(AMC)\
            .join(MFSecurity, AMC.AMC_Id == MFSecurity.AMC_Id)\
            .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
            .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
            .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
            .join(Classification, Classification.Classification_Id == MFSecurity.Classification_Id)\
            .join(BenchmarkIndices, BenchmarkIndices.BenchmarkIndices_Id == MFSecurity.BenchmarkIndices_Id)\
            .filter(Plans.Plan_Id == plan).filter(MFSecurity.Is_Deleted != 1).filter(AMC.Is_Deleted != 1).filter(Plans.Is_Deleted != 1).filter(PlanProductMapping.Is_Deleted != 1).first()

        obj = dict()
        obj["amc_logo"] = sql_fund1[0]
        obj["product_code"] = sql_fund1[1]
        obj["product_name"] = sql_fund1[2]
        obj["plan_name"] = sql_fund1[3]
        obj["plan_code"] = sql_fund1[4]
        obj["classification"] = sql_fund1[5]
        obj["amc_name"] = sql_fund1[6]
        obj["benchmark_name"] = sql_fund1[7]
        details[str(plan)] = obj
    
    return details

def get_last_transactiondate(db_session, plan_id):        
    sql_factsheet = db_session.query(FactSheet.TransactionDate).filter(FactSheet.Plan_Id == plan_id).filter(FactSheet.Is_Deleted != 1).order_by(desc(FactSheet.TransactionDate)).first()
            
    return sql_factsheet.TransactionDate if sql_factsheet else None


def get_fund_performance_byallmaxdates(db_session, plan_id, all_max_nav_dates, min_date, performance_type):
    resp = list()

    min_nav_date = db_session.query(func.min(NAV.NAV_Date)).join(Plans, Plans.Plan_Id == NAV.Plan_Id).filter(Plans.Plan_Id == plan_id).filter(NAV.NAV_Type == 'P').filter(NAV.Is_Deleted != 1).filter(Plans.Is_Deleted != 1).scalar()

    if performance_type == "MOM":
        min_nav_date = db_session.query(func.min(NAV.NAV_Date)).join(Plans, Plans.Plan_Id == NAV.Plan_Id).filter(Plans.Plan_Id == plan_id).filter(NAV.NAV_Type == 'P').filter(NAV.Is_Deleted != 1).filter(Plans.Is_Deleted != 1).filter(NAV.NAV_Date >= min_date).scalar()

    if all_max_nav_dates:
        for index, item in enumerate(all_max_nav_dates):
            if (index + 1) < len(all_max_nav_dates):
                if item[0]:
                    data = get_fund_performance_between_twodates(db_session, plan_id, all_max_nav_dates[index + 1][0], item[0], performance_type)
                    resp.append(data)
            else:
                if item[0]:
                    data = get_fund_performance_between_twodates(db_session, plan_id, min_nav_date, item[0], performance_type)
                    resp.append(data)
    
    return resp


def get_max_navdate_tilldate(db_session, plan_id, nav_date):    
    max_nav_date = db_session.query(func.max(NAV.NAV_Date)).join(Plans, Plans.Plan_Id == NAV.Plan_Id).filter(Plans.Plan_Id == plan_id).filter(NAV.NAV_Type == 'P').filter(NAV.Is_Deleted != 1).filter(Plans.Is_Deleted != 1).filter(NAV.NAV_Date <= nav_date).scalar()

    return max_nav_date


def get_fund_performance_between_twodates(db_session, plan_id, start_date, end_date, performance_type=None):    
    res = dict()
    end_nav = get_navbydate(db_session, plan_id, end_date)   
    start_nav = get_navbydate(db_session, plan_id, start_date)

    if start_nav:
        res['performance'] = ((end_nav - start_nav) / start_nav * 100)
        start_date = start_date + timedelta(days=1)
        period = ""
        res['year'] = end_date.year 
        res['month'] = end_date.month
        today = date.today()
        is_current_month = False
        if performance_type == "MOM":
            if today.month == end_date.month and today.year == end_date.year:
                is_current_month = True
            else:
                period = end_date.strftime("%b %Y")

        elif performance_type == "CY":
            if today.year == end_date.year:
                is_current_month = True
            else:
                period = F"CY {end_date.strftime('%y')}"

        elif performance_type == "FY":
            if today.year == end_date.year and end_date.month > 3:
                is_current_month = True
            else:
                period = F"FY {end_date.strftime('%y')}"
        else:
            period = str(start_date.strftime("%B %d, %Y")) + " To " + end_date.strftime("%B %d, %Y")


        if is_current_month == False:
            res['period'] = period
        else:
            if performance_type == "MOM":
                res['period'] = "MTD"
            elif performance_type == "CY":
                res['period'] = "YTD"
            elif performance_type == "FY":
                res['period'] = "FYTD"

    return res


def get_navbydate(db_session, plan_id, nav_date, nav_type='P'):
    nav = None

    for i in range(3):
        if not nav:
            product_id = db_session.query(PlanProductMapping.Product_Id).select_from(PlanProductMapping).join(Plans, Plans.Plan_Id == PlanProductMapping.Plan_Id).filter(PlanProductMapping.Is_Deleted != 1, Plans.Plan_Id == plan_id).scalar()
    
            if product_id == 2 or product_id == 4 or product_id == 5:
                nav = db_session.query(NAV.NAV)
            elif product_id == 1:
                nav = db_session.query(NAV.RAW_NAV.label('NAV'))
            
            nav = nav.join(Plans, Plans.Plan_Id == NAV.Plan_Id).filter(Plans.Plan_Id == plan_id).filter(NAV.NAV_Type == nav_type).filter(NAV.NAV_Date == nav_date).filter(NAV.Is_Deleted != 1).filter(Plans.Is_Deleted != 1).scalar()

            nav_date = nav_date - timedelta(days=1)

    return nav

def get_investment_style_from_df(df):
    resp_dic = dict()

    largecap_blend = 0
    largecap_growth = 0
    largecap_value = 0
    midcap_blend = 0
    midcap_growth = 0
    midcap_value = 0
    smallcap_blend = 0
    smallcap_growth = 0
    smallcap_value = 0

    if not df.empty:
        data = df.to_dict(orient="records")
        
        for response in data:   
            if response["market_cap"] == "Large Cap" and response["equity_style"] == "Growth":
                largecap_growth = response["weight"]
            elif response["market_cap"] == "Large Cap" and response["equity_style"] == "Value":
                largecap_value = response["weight"]
            elif response["market_cap"] == "Large Cap" and response["equity_style"] == "Blend":
                largecap_blend = response["weight"]
            elif response["market_cap"] == "Mid Cap" and response["equity_style"] == "Growth":
                midcap_growth = response["weight"]
            elif response["market_cap"] == "Mid Cap" and response["equity_style"] == "Value":
                midcap_value = response["weight"]
            elif response["market_cap"] == "Mid Cap" and response["equity_style"] == "Blend":
                midcap_blend = response["weight"]
            elif response["market_cap"] == "Small Cap" and response["equity_style"] == "Growth":
                smallcap_growth = response["weight"]
            elif response["market_cap"] == "Small Cap" and response["equity_style"] == "Value":
                smallcap_value = response["weight"]
            elif response["market_cap"] == "Small Cap" and response["equity_style"] == "Blend":
                smallcap_blend = response["weight"]
        
    resp_dic["large_cap_blend"] = round(largecap_blend,0)
    resp_dic["large_cap_growth"] = round(largecap_growth,0)
    resp_dic["large_cap_value"] = round(largecap_value,0)

    resp_dic["mid_cap_blend"] = round(midcap_blend,0)
    resp_dic["mid_cap_growth"] = round(midcap_growth,0)
    resp_dic["mid_cap_value"] = round(midcap_value,0)

    resp_dic["small_cap_blend"] = round(smallcap_blend,0)
    resp_dic["small_cap_growth"] = round(smallcap_growth,0)
    resp_dic["small_cap_value"] = round(smallcap_value,0)
    
    return resp_dic
from datetime import timedelta
import datetime
from typing import List
from analytics.analytics import calculate_portfolio_pe
from bizlogic.holding_interface import Holding, HoldingType, InstrumentType
from fin_models.masters_models import AMC, BenchmarkIndices, HoldingSecurity, MFSecurity, Plans, Sector
from fin_models.transaction_models import IndexWeightage
from werkzeug.exceptions import BadRequest
from fin_models.transaction_models import NAV, Fundamentals, ClosingValues, UnderlyingHoldings
import pandas as pd
from sqlalchemy import func, and_, or_, cast
import sqlalchemy
from fin_models.masters_models import Plans, HoldingSecurity
from fin_models.transaction_models import NAV, ClosingValues
from utils.utils import print_query, is_valid_str
import numpy as np
from utils.finalyca_exceptions import MissingDataException
from data.fundamentals import get_portfolio_fundamental
from data.holdings import get_fund_underlying_holdings

# TODO: Add a flag for not_before or not_after. This will allow us to fill the missing value by getting last value before value_date or getting first_value after value_date.
# If it is implemented, we can optimize the data and implement caching in the data consumer.
def get_security_performance_with_fundamentals(db_session, isin_list, start_date, end_date):
    df_price_start_dt = get_portfolio_price_df(db_session, isin_list, start_date)
    df_price_start_dt.rename(columns = {'date':'start_date', 'unit_price':'start_unit_price'}, inplace=True)

    df_price_end_dt = get_portfolio_price_df(db_session, isin_list, end_date)
    df_price_end_dt.rename(columns = {'date':'end_date', 'unit_price':'end_unit_price'}, inplace=True)

    df = pd.merge(left=df_price_start_dt, right=df_price_end_dt, on=['lookup_code', "isin"], how='left')

    missing = df.loc[df["start_unit_price"].isnull() & ~df["end_unit_price"].isnull()]

    # TODO: Check differently for closing and opening prices
    lst_eq_co_codes = missing[missing['isin'].str.startswith('INE')]['lookup_code'].to_list()
    lst_mf_plan_ids = missing[missing['isin'].str.startswith('INF')]['lookup_code'].to_list()
    df_missing_prices = get_missing_security_prices(db_session, lst_eq_co_codes, start_date) if len(lst_eq_co_codes) > 0 else pd.DataFrame()
    df_missing_mf_pr = get_missing_mf_prices(db_session, lst_mf_plan_ids, start_date) if len(lst_mf_plan_ids) > 0 else pd.DataFrame()
    df_missing_prices = pd.concat([df_missing_prices, df_missing_mf_pr])

    if not df_missing_prices.empty:
        df_missing_prices.rename(columns={
            'date': 'start_date',
            'price': 'start_unit_price'
        }, inplace=True)
        df_missing_prices['lookup_code'] = df_missing_prices['lookup_code'].astype(str)
        df.set_index('lookup_code', inplace=True)
        df_missing_prices.set_index('lookup_code', inplace=True)
        df.update(df_missing_prices)
        df.reset_index(inplace=True)

    performance = df.apply(pd.to_numeric, errors='coerce').fillna(df)

    fundamental_df = get_portfolio_fundamental(db_session, isin_list, end_date)
    df = pd.merge(left=performance, right=fundamental_df, on="isin", how='left')
    df = df.fillna(np.nan).replace([np.nan], [None])
    
    return df

def get_portfolio_price_df(db_session, isin_list, performance_date):
    date_delta = 7
    final_df = pd.DataFrame()

    sec_isin_list = list() 
    mf_isin_list = list()
    for isin in isin_list:
        if isin.startswith("INF"):
            mf_isin_list.append(isin)
        else:
            sec_isin_list.append(isin)
    
    if sec_isin_list:
        sec_price = get_security_price_df(db_session, sec_isin_list, performance_date, date_delta)
        if sec_price.empty:
            raise MissingDataException(F"No security prices found for {performance_date}")
        # sec_price = sec_price.drop(['Co_Code'], axis=1)
        sec_price.rename(columns = {'Co_Code':'lookup_code'}, inplace = True)

        final_df = pd.concat([final_df, sec_price], ignore_index=True)
    
    if mf_isin_list:
        mf_price = get_mf_price_df(db_session, mf_isin_list, performance_date, date_delta)
        if mf_price.empty:
            raise MissingDataException(F"No MF prices found for {performance_date}")

        # mf_price = mf_price.drop(['Plan_Id'], axis=1)
        mf_price.rename(columns = {'Plan_Id':'lookup_code'}, inplace = True)
        final_df = pd.concat([final_df, mf_price], ignore_index=True)
    
    final_df["lookup_code"] = final_df["lookup_code"].astype(str)
    return final_df

def prepare_index_holdings_from_db(db_session, benchmark_id, asof_date):
    # get index co_code
    index_co_code = db_session.query(BenchmarkIndices.Co_Code)\
                              .filter(BenchmarkIndices.Is_Deleted != 1,
                                      BenchmarkIndices.BenchmarkIndices_Id == benchmark_id).scalar()

    if not index_co_code:
        raise Exception('The selected benchmark is not available.')

    latest_asofdate = db_session.query(func.max(IndexWeightage.WDATE).label('maxdate'))\
                                .filter(IndexWeightage.WDATE <= asof_date,
                                        IndexWeightage.Index_CO_CODE == index_co_code,
                                        IndexWeightage.Is_Deleted != 1).scalar()

    sql_index_qry = db_session.query(IndexWeightage.CO_CODE,
                                     IndexWeightage.WDATE,
                                     IndexWeightage.Index_CO_CODE,
                                     IndexWeightage.CLOSEPRICE,
                                     IndexWeightage.NOOFSHARES,
                                     IndexWeightage.FULLMCAP,
                                     IndexWeightage.FF_ADJFACTOR,
                                     IndexWeightage.FF_MCAP,
                                     IndexWeightage.WEIGHT_INDEX,
                                     IndexWeightage.Index_Type,
                                     HoldingSecurity.ISIN_Code,
                                     HoldingSecurity.HoldingSecurity_Name,
                                     Sector.Sector_Name)\
                              .select_from(IndexWeightage)\
                              .join(HoldingSecurity, HoldingSecurity.Co_Code == cast(IndexWeightage.CO_CODE, sqlalchemy.String))\
                              .join(Sector, Sector.Sector_Id == HoldingSecurity.Sector_Id)\
                              .filter(IndexWeightage.WDATE == latest_asofdate,
                                      IndexWeightage.Index_CO_CODE == index_co_code,
                                      IndexWeightage.Is_Deleted != 1,
                                      HoldingSecurity.active == 1,
                                      HoldingSecurity.Is_Deleted != 1).all()

    df_index_holdings = pd.DataFrame(sql_index_qry)

    # rename the columns to make this a std requirement for all holdings
    df_index_holdings.rename(columns={
        'NOOFSHARES': 'total_price',
        'CO_CODE': 'co_code',
        'WEIGHT_INDEX': 'weight',
        'Index_CO_CODE': 'index_co_code',
        'WDATE': 'asof_date',
        'ISIN_Code': 'isin',
        'HoldingSecurity_Name': 'name',
        'Sector_Name': 'sector'
    }, inplace=True)

    # normalize the weight to be totalled as 1 instead of 100
    df_index_holdings['weight'] = df_index_holdings['weight']/100

    return df_index_holdings[['total_price', 'co_code', 'weight', 'index_co_code', 'asof_date', 'isin', 'name', 'sector']]

def get_benchmark_pe_ratio_for_a_date(db_session, benchmark_id, asof_date):

    df_index_holdings = prepare_index_holdings_from_db(db_session, benchmark_id, asof_date)

    if df_index_holdings.shape[0] == 0:
        raise BadRequest(F"No Index found for {asof_date}")

    list_co_codes = df_index_holdings['co_code'].to_list()

    # fetch fundamental data for pe
    sql_fundamental_data = db_session.query(Fundamentals.CO_CODE.label('co_code'),
                                            Fundamentals.ISIN_Code.label('isin'),
                                            Fundamentals.PriceDate.label('price_date'),
                                            Fundamentals.PE.label('pe'))\
                                     .join(ClosingValues, ClosingValues.Co_Code == Fundamentals.CO_CODE)\
                                     .filter(Fundamentals.CO_CODE.in_(list_co_codes),
                                             Fundamentals.PriceDate == asof_date).all()

    df_fundamental_data = pd.DataFrame(sql_fundamental_data)

    df_index_holdings = pd.merge(left=df_index_holdings, right=df_fundamental_data, on='co_code')

    bmk_pe = calculate_portfolio_pe(df_index_holdings)

    return bmk_pe


def get_mf_price_df(db_session, isin_list, expected_date, date_delta):
    allowed_date = expected_date - timedelta(days=date_delta)

    # Get co_codes to ensure we handle old ISINs
    result_1 = db_session.query(Plans.Plan_Id, Plans.ISIN.label('isin'))\
                        .filter(and_(Plans.ISIN.in_(isin_list), Plans.Is_Deleted != 1)) #.all()

    result_2 = db_session.query(Plans.Plan_Id, Plans.ISIN2.label('isin'))\
                        .filter(and_(Plans.ISIN2.in_(isin_list), Plans.Is_Deleted != 1)) #.all()

    result = result_1.union(result_2).all()
    info_df= pd.DataFrame(result)

    plan_ids = list(info_df["Plan_Id"].astype(str))

    # all the securities will have trading every day. So we can get the max possible date.    
    actual_date =  db_session.query(func.max(NAV.NAV_Date))\
                             .filter(and_(NAV.Plan_Id.in_(plan_ids),NAV.NAV_Date >= allowed_date, NAV.NAV_Date <= expected_date, NAV.NAV_Type=="P", NAV.Is_Deleted!=1))\
                             .scalar()

    # Join is required as older ClosingValue data does not have ISIN information. 
    # Averaging if security is listed in NSE and BSE.
    prices = db_session.query(NAV.NAV_Date.label('date'),
                              NAV.Plan_Id,
                              NAV.RAW_NAV.label('unit_price'))\
                        .filter(and_(NAV.Plan_Id.in_(plan_ids), NAV.NAV_Date == actual_date, NAV.NAV_Type=="P", NAV.Is_Deleted!=1))\
                        .order_by(NAV.Plan_Id).all()

    value_df = pd.DataFrame(prices)

    if not value_df.empty:
        value_df = pd.merge(info_df, right=value_df, on='Plan_Id')

    return value_df


def get_security_price_df(db_session, isin_list, expected_date, date_delta):
    '''
    Get prices data for stock
    '''
    allowed_date = expected_date - timedelta(days=date_delta)

    # Get co_codes to ensure we handle old ISINs
    result = db_session.query(HoldingSecurity.Co_Code,
                              HoldingSecurity.ISIN_Code.label('isin'))\
                       .filter(and_(HoldingSecurity.ISIN_Code.in_(isin_list), HoldingSecurity.Co_Code != None, HoldingSecurity.active == 1)).all()
    info_df= pd.DataFrame(result)

    co_codes = list(info_df["Co_Code"].astype(str)) if not info_df.empty else []
    eq_co_codes = [int(x) for x in co_codes if 'BLV' not in x]

    # all the securities will have trading every day. So we can get the max possible date.
    actual_date = db_session.query(func.max(ClosingValues.Date_))\
                            .filter(and_(ClosingValues.Co_Code.in_(eq_co_codes),
                                         ClosingValues.Date_ >= allowed_date,
                                         ClosingValues.Date_ <= expected_date,
                                         ClosingValues.Is_Deleted !=1)).scalar()

    # Join is required as older ClosingValue data does not have ISIN information.
    # Averaging if security is listed in NSE and BSE.
    prices = db_session.query(ClosingValues.Date_.label('date'),
                              ClosingValues.Co_Code,
                              func.avg(ClosingValues.CLOSE).label('unit_price'))\
                       .filter(and_(ClosingValues.Co_Code.in_(eq_co_codes), ClosingValues.Date_ == actual_date, ClosingValues.Is_Deleted !=1 ))\
                       .group_by( ClosingValues.Co_Code, ClosingValues.Date_)\
                       .order_by(ClosingValues.Co_Code).all()

    value_df = pd.DataFrame(prices)
    if not value_df.empty:
        # KM: Added fix to convert the joining columns in same data type after fixed income deployment
        value_df['Co_Code'] = value_df["Co_Code"].astype(str)
        value_df['date'] = pd.to_datetime(value_df['date']).dt.date
        value_df = pd.merge(info_df, right=value_df, on='Co_Code')

    return value_df


def get_missing_security_prices(db_session, lst_co_codes, start_date):
    sql_sub_qry = db_session.query(ClosingValues.Co_Code.label('lookup_code'), func.min(ClosingValues.Date_).label('date'))\
        .filter(and_(ClosingValues.Co_Code.in_(lst_co_codes), ClosingValues.Is_Deleted != 1))\
        .filter(ClosingValues.Date_ >= start_date)\
        .group_by(ClosingValues.Co_Code).subquery()

    sql_qry = db_session.query(ClosingValues.Co_Code.label('lookup_code'), ClosingValues.Date_.label('date'), func.avg(ClosingValues.CLOSE).label('price'))\
        .join(sql_sub_qry, and_(sql_sub_qry.c.date == ClosingValues.Date_, sql_sub_qry.c.lookup_code == ClosingValues.Co_Code))\
        .filter(ClosingValues.Is_Deleted != 1)\
        .group_by(ClosingValues.Co_Code, ClosingValues.Date_).all()

    if sql_qry:
        df_sec_prices = pd.DataFrame(sql_qry)
    else:
        df_sec_prices = pd.DataFrame()

    return df_sec_prices

def get_missing_mf_prices(db_session, lst_plan_ids, start_date):
    sql_sub_qry = db_session.query(NAV.Plan_Id.label('lookup_code'),
                                   func.min(NAV.NAV_Date).label('date'))\
                            .filter(and_(NAV.Plan_Id.in_(lst_plan_ids),
                                         NAV.Is_Deleted != 1))\
                            .filter(NAV.NAV_Date >= start_date)\
                            .group_by(NAV.Plan_Id).subquery()

    sql_qry = db_session.query(NAV.Plan_Id.label('lookup_code'),
                               NAV.NAV_Date.label('date'),
                               NAV.NAV.label('price'))\
                        .join(sql_sub_qry, and_(sql_sub_qry.c.date == NAV.NAV_Date, sql_sub_qry.c.lookup_code == NAV.Plan_Id))\
                        .filter(NAV.Is_Deleted != 1).all()

    if sql_qry:
        df_mf_prices = pd.DataFrame(sql_qry)
    else:
        df_mf_prices = pd.DataFrame()

    return df_mf_prices

# TODO: Optimize the following query. Ask for all in one call
def get_security_info(db_session, isin_list: List[str]):
    holdings = dict()

    for isin in isin_list:
        h = Holding()
        isin_found = False

        if isin.startswith("INF"):
            sql_plan = db_session.query(Plans.Plan_Id,
                                        Plans.Plan_Name,
                                        Plans.AMFI_Code,
                                        MFSecurity.MF_Security_Id,
                                        AMC.AMC_Id,
                                        AMC.AMC_Name)\
                                    .join(MFSecurity, and_(MFSecurity.MF_Security_Id==Plans.MF_Security_Id, MFSecurity.Is_Deleted != 1))\
                                    .join(AMC, AMC.AMC_Id==MFSecurity.AMC_Id)\
                                    .filter(or_(Plans.ISIN == isin, Plans.ISIN2 == isin)).filter(Plans.Is_Deleted != 1).first()

            if sql_plan:
                isin_found = True
                h.isin = isin
                h.name = sql_plan.Plan_Name if is_valid_str(sql_plan.Plan_Name) else ""
                h.instrument_type = "MUTUAL FUNDS"
                h.asset_class = "MUTUAL FUNDS"
                h.issuer = sql_plan.AMC_Name if is_valid_str(sql_plan.AMC_Name) else ""
                h.sector = "Mutual Funds"
                h.sub_sector = ""
                h.market_cap = ""
                h.equity_style = ""
                h.risk_category = ""

                h.meta["plan_id"] = sql_plan.Plan_Id
                h.meta["amfi_code"] = sql_plan.AMFI_Code
                h.meta["mf_security_id"] = sql_plan.MF_Security_Id
        
        else:
            # TODO: Think if we should check for is_deleted as we may have to use some old ISIN.
            sql_holding_security = db_session.query(HoldingSecurity.HoldingSecurity_Id,
                                                    HoldingSecurity.HoldingSecurity_Name,
                                                    HoldingSecurity.ISIN_Code,
                                                    HoldingSecurity.Sub_SectorName,
                                                    HoldingSecurity.Sector_Id,
                                                    HoldingSecurity.Asset_Class,
                                                    HoldingSecurity.Instrument_Type,
                                                    HoldingSecurity.Issuer_Code,
                                                    HoldingSecurity.Issuer_Name,
                                                    HoldingSecurity.Issuer_Id,
                                                    HoldingSecurity.MarketCap,
                                                    HoldingSecurity.Equity_Style,
                                                    Sector.Sector_Name,
                                                    HoldingSecurity.HoldingSecurity_Type,
                                                    HoldingSecurity.Co_Code)\
                                    .join(Sector, and_(HoldingSecurity.Sector_Id==Sector.Sector_Id, Sector.Is_Deleted !=1))\
                                    .filter(HoldingSecurity.ISIN_Code==isin, HoldingSecurity.Is_Deleted != 1).first()

            if sql_holding_security:
                isin_found = True

                instrument_type = HoldingType.no_type.value

                if sql_holding_security.HoldingSecurity_Type:
                    instrument_type = sql_holding_security.HoldingSecurity_Type


                h.isin = sql_holding_security.ISIN_Code if is_valid_str(sql_holding_security.ISIN_Code) else ""
                h.name = sql_holding_security.HoldingSecurity_Name if is_valid_str(sql_holding_security.HoldingSecurity_Name) else ""
                h.instrument_type = instrument_type # .value
                h.asset_class = sql_holding_security.Asset_Class if is_valid_str(sql_holding_security.Asset_Class) else ""
                h.issuer = sql_holding_security.Issuer_Name if is_valid_str(sql_holding_security.Issuer_Name) else ""
                h.sector = sql_holding_security.Sector_Name if is_valid_str(sql_holding_security.Sector_Name) else ""
                h.sub_sector = sql_holding_security.Sub_SectorName if is_valid_str(sql_holding_security.Sub_SectorName) else ""
                h.market_cap = sql_holding_security.MarketCap if is_valid_str(sql_holding_security.MarketCap) else ""
                h.equity_style = sql_holding_security.Equity_Style if is_valid_str(sql_holding_security.Equity_Style) else ""
                h.risk_category = ""        
                h.meta = {'co_code': sql_holding_security.Co_Code}

        if isin_found:
            holdings[isin] = vars(h)

    return holdings


# TODO: Optimize the following query. Ask for all in one call
def get_mf_breakup(db_session, isin_list: List[str], pf_month, pf_year):
    breakups = dict()

    # Funds are updated once a month. so we should try only for the last 5 days
    d = datetime.date(pf_year, pf_month, 1)

    if pf_month == 12:
        d = d.replace(month=1)
        d = d.replace(year=pf_year + 1)
    else:
        d = d.replace(month=pf_month + 1)

    expected_date = d - datetime.timedelta(days=1)
    allowed_date = expected_date - datetime.timedelta(days=3)

    for isin in isin_list:
        underlying = list()

        sql_plan = db_session.query(Plans.Plan_Id,
                                    MFSecurity.MF_Security_Id,
                                    MFSecurity.Fund_Id,
                                    Plans.Plan_Name)\
                                .join(MFSecurity, and_(MFSecurity.MF_Security_Id==Plans.MF_Security_Id, MFSecurity.Is_Deleted != 1))\
                                .filter(or_(Plans.ISIN == isin, Plans.ISIN2 == isin)).filter(Plans.Is_Deleted != 1).first()

        if sql_plan:
            fund_id = sql_plan.Fund_Id
            plan_name = sql_plan.Plan_Name

            # Find the max allowed date
            as_on_date = db_session.query(func.max(UnderlyingHoldings.Portfolio_Date))\
                                .filter(UnderlyingHoldings.Fund_Id == fund_id, UnderlyingHoldings.Portfolio_Date <= expected_date,
                                        UnderlyingHoldings.Portfolio_Date >= allowed_date, UnderlyingHoldings.Is_Deleted != 1).scalar()

            # Find required information from UnderlyingHoldings and HoldingSecurity
            lst_holdings_data = get_fund_underlying_holdings(db_session, fund_id, as_on_date, None)

            for record in lst_holdings_data:
                h = Holding()
                instrument_type = HoldingType.no_type.value

                if record.get("HoldingSecurity_Type"):
                    instrument_type = record.get("HoldingSecurity_Type")

                h.isin = record.get("ISIN_Code") if is_valid_str(record.get("ISIN_Code")) else ""
                h.name = record.get("HoldingSecurity_Name") if is_valid_str(record.get("HoldingSecurity_Name")) else record.get("Company_Security_Name")
                h.instrument_type = instrument_type
                h.asset_class = record.get("Asset_Class") if is_valid_str(record.get("Asset_Class")) else ""
                h.issuer = record.get("Issuer_Name") if is_valid_str(record.get("Issuer_Name")) else ""
                h.sector = record.get("Sector_Name") if is_valid_str(record.get("Sector_Name")) else ""
                h.sub_sector = record.get("Sub_SectorName") if is_valid_str(record.get("Sub_SectorName")) else ""
                h.market_cap = record.get("MarketCap") if is_valid_str(record.get("MarketCap")) else ""
                h.equity_style = record.get("Equity_Style") if is_valid_str(record.get("Equity_Style")) else ""
                h.risk_category = record.get("Risk_Category") if is_valid_str(record.get("Risk_Category")) else ""
                h.coupon_rate = ""
                h.maturity = ""

                obj = vars(h)
                obj["weight"] = record.get("Percentage_to_AUM")
                obj["plan_name"] = plan_name
                underlying.append(obj)

        breakups[isin] = underlying

    return breakups


import pandas as pd
import numpy as np
import datetime
# TODO: Focus on removing this exception. Werkzeug exceptions must not be used other than api layer
from werkzeug.exceptions import BadRequest
from .portfolio_helper import analyze_portfolio_movement, get_holding_performance_with_fundamentals
from .portfolio_db import prepare_raw_holdings_from_db
from .portfolio_analysis import get_consolidated_securities

from analytics.analytics import calculate_portfolio_pe, calculate_script_xirr
from utils.finalyca_exceptions import MissingDataException
from utils.time_func import get_next_date 


def get_normalized_portfolio_holdings(db_session, account_ids, portfolio_date, detailed_analysis) -> pd.DataFrame:
    holdings = prepare_raw_holdings_from_db(db_session, account_ids, portfolio_date, detailed_analysis)  
    if holdings:
        df = get_consolidated_securities(holdings, False)
        df["portfolio_date"] = portfolio_date
        return df
    else:
        return pd.DataFrame()

def get_portfolio_movement(db_session, account_ids, start_date, end_date, detailed_analysis):
    portfolio_movement_df = None
    old_df = get_normalized_portfolio_holdings(db_session, account_ids, start_date, detailed_analysis)
    new_df = get_normalized_portfolio_holdings(db_session, account_ids, end_date, detailed_analysis)
    
    portfolio_movement_df = analyze_portfolio_movement(old_df, new_df)
    return portfolio_movement_df

def attach_date_and_convert(df, pd_date):
    df["month"], df["year"] = pd_date.month, pd_date.year
    d = df.to_dict(orient="records")
    return d

def get_portfolio_performance_analysis_by_date(db_session, holdings_df, start_date, end_date, transactions=None):
    response = dict()

    try:
        holdings_df = holdings_df.drop(["risk_category", "coupon_rate", "maturity", "account_alias", "instrument_type", "asset_class", "issuer", "sector", "sub_sector", "market_cap", "equity_style"], axis=1)
        holdings_df = holdings_df.drop(["total_price", "unit_price"], axis=1)
        
        results = get_holding_performance_with_fundamentals(db_session, holdings_df, start_date, end_date, None)
    except MissingDataException as e:
        raise BadRequest(str(e))

    response = dict()
    if results is not None:
        results["start_price"] = results["units"]*results["start_unit_price"]
        results["end_price"] = results["units"]*results["end_unit_price"]
        results["change_pr"] = ((results["end_price"] - results["start_price"])/results["start_price"])*100
        
        oldvalue = results["start_price"].sum()
        newvalue = results["end_price"].sum()
        gain = ((newvalue-oldvalue)/oldvalue)*100

        response["equity_old_value"] = oldvalue
        response["equity_new_value"] = newvalue
        response["equity_performance"] = gain
        df_pe = results.drop(['units', 'lookup_code_x', 'start_date', 'start_unit_price',
                              'lookup_code_y', 'end_date', 'end_unit_price', 'start_price',
                              'end_price', 'change_pr'], errors='ignore')
        df_pe['weight'] = df_pe['weight']/100 # normalize the weight from 100 to 1
        response["portfolio_pe"] = calculate_portfolio_pe(df_pe)

        #XIRR - Scrip level
        tran_df = pd.DataFrame(transactions)
        results["xirr"] = pd.to_numeric(None)
        if not tran_df.empty:
            results['xirr'] = results.apply(lambda x: calculate_script_xirr(x['isin'], tran_df.copy(), results.copy()), axis=1)
            response['xirr'] = calculate_script_xirr(None, tran_df.copy(), results.copy())

        results = results.replace(np.nan, None)
        response["performance"] = results.to_dict(orient="records")


    return response

def get_portfolio_performance_analysis_for_all_period(db_session, holdings_df, portfolio_date: datetime.date, lst_periods):
    response = dict()

    holdings_df = holdings_df.drop(["risk_category", "coupon_rate", "maturity", "account_alias", "instrument_type", "asset_class", "issuer", "sector", "sub_sector", "market_cap", "equity_style"], axis=1)
    holdings_df = holdings_df.drop(["total_price", "unit_price"], axis=1)

    # TODO: As we are going to ping a new API, as of now we won't be able to cache the results without making multiple API calls for the missing securities.

    # as portfolio date remains steady, we only fetch the price once for the respective portfolio date
    # isin_list = list(holdings_df["isin"].unique())
    # df_price_end_dt = get_portfolio_price_df(db_session, isin_list, portfolio_date)
    # df_price_end_dt.rename(columns = {'date':'end_date', 'unit_price':'end_unit_price'}, inplace = True)

    fund_perf_resp = dict()

    for period in lst_periods:
        # get start date based on the period
        start_date = get_next_date(portfolio_date, period, False, True)

        try:
            # not caching results as we will have to ping the portal api to get data.
            results = get_holding_performance_with_fundamentals(db_session, holdings_df, start_date, portfolio_date, None)

        except MissingDataException as e:
            raise BadRequest(str(e))

        if results is not None:
            results["start_price"] = results["units"]*results["start_unit_price"]
            results["end_price"] = results["units"]*results["end_unit_price"]
            results["change_pr"] = ((results["end_price"] - results["start_price"])/results["start_price"])*100

            response = dict()
            oldvalue = results["start_price"].sum()
            newvalue = results["end_price"].sum()
            gain = ((newvalue-oldvalue)/oldvalue)*100

            response["performance"] = results.to_dict(orient="records")
            response["equity_old_value"] = oldvalue
            response["equity_new_value"] = newvalue
            response["equity_performance"] = gain
            response["period"] = period
            fund_perf_resp[period] = response

    return fund_perf_resp


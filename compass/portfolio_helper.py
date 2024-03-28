import pandas as pd
from typing import List
from .portfolio_analysis import *

from bizlogic.holding_interface import *
from bizlogic.core_helper import get_security_performance_with_fundamentals
from utils.df_helper import parse_nested_grouped_df_to_dict_v2


def get_holding_performance_with_fundamentals(db_session, holdings_df, start_date, end_date, end_prices_df):
    """
    holdings_df must contain `isin` and `units` fields for the function to work.
    """

    performance = get_security_performance_with_fundamentals(db_session, holdings_df['isin'].to_list(), start_date, end_date)

    df = pd.merge(left=holdings_df, right=performance, on="isin", how='left')

    return df

def get_account_aggregation_report(holdings: List[Holding]):
    df_acc =  get_account_level_security_consolidation(holdings)
    
    new_df = df_acc.groupby(['instrument_type', 'name', 'account_alias'])\
                        .aggregate({'units':'sum',
                                    'weight':'sum',
                                    'total_price':'sum',
                                    'name':'first',
                                    'account_alias':'first',
                                    'instrument_type':'first',
                                    'isin':'first'})


    res = parse_nested_grouped_df_to_dict_v2(new_df)
    return res

def create_portfolio_report_only(holdings: List[Holding], only_important_field: bool, drop_unimp_cols = True):
    response = dict()
    if not holdings:
        return response

    securities = get_consolidated_securities(holdings, False)

    if drop_unimp_cols:
        security_exposure = securities.drop(columns=["issuer", "sector", "total_price", "units", "instrument_type","sub_sector", "market_cap","equity_style",
                                                     "asset_class", "risk_category", "account_alias", "unit_price", "coupon_rate","maturity"])
    else:
        security_exposure = securities.copy(deep=True)

    response["securities"] = security_exposure
    response["instrument_types"] = get_instrument_type_exposure(securities, only_important_field)
    response["issuers"] = get_issuer_exposure(securities, only_important_field)
    exposure_breakdown = get_sector_exposure(securities, only_important_field, instrument_type_break_down=1)
    exposure_total = get_sector_exposure(securities, only_important_field, instrument_type_break_down=0)
    response["geo_allocation"] = get_location_exposure(securities, only_important_field)
    response["market_cap"] = get_market_cap_exposure(securities, only_important_field)
    response["equity_style"] = get_equity_style_exposure(securities, only_important_field)

    x = exposure_breakdown.reset_index(level="sector")
    response["sectors"] = {k: g.to_dict(orient='records') for k, g in x.groupby(level=0)}
    response["sectors"]["TOTAL"] = exposure_total.to_dict(orient="records")

    return response
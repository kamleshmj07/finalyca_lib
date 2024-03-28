from fin_models.controller_transaction_models import InvestorAccount, InvestorHoldings
from utils.utils import is_valid_str

from bizlogic.holding_interface import *
from bizlogic.core_helper import get_security_info, get_mf_breakup

from sqlalchemy import func

def get_non_isin_holding_info(holding: InvestorHoldings):
    h = Holding()

    isin = None

    h.isin = isin
    h.name = holding.name
    h.instrument_type = holding.type
    h.asset_class = holding.type
    h.issuer = ""
    h.sector = ""
    h.sub_sector = ""
    h.market_cap = ""
    h.equity_style = ""
    h.risk_category = ""
    h.coupon_rate = holding.coupon_rate if is_valid_str(holding.coupon_rate) else ""
    h.maturity = holding.maturity_date if is_valid_str(holding.maturity_date) else ""

    return h

def format_investor_holding(holding: InvestorHoldings):
    h = Holding()
    holding_type = holding.type if is_valid_str(holding.type) else ""
    asset_class = ""
    instrument_type = HoldingType.no_type

    if holding_type == "Preference Shares (P)":
        instrument_type = HoldingType.equity
        asset_class = "Preference Shares"
    elif holding_type == "Corporate Bonds (C)":
        instrument_type = HoldingType.long_term_debt
        asset_class = "Corporate Bonds"
    elif holding_type == "Government Securities (G)":
        instrument_type = HoldingType.long_term_debt
        asset_class = "Government Securities"

    h.isin = holding.isin
    h.name = holding.name if is_valid_str(holding.name) else ""
    h.instrument_type = instrument_type.value if instrument_type.name != HoldingType.no_type.name else holding.type.upper()
    h.asset_class = asset_class
    h.issuer = ""
    h.sector = ""
    h.sub_sector = ""
    h.market_cap = ""
    h.equity_style = ""
    h.risk_category = ""
    h.coupon_rate = holding.coupon_rate if is_valid_str(holding.coupon_rate) else ""
    h.maturity = holding.maturity_date if is_valid_str(holding.maturity_date) else ""
    return vars(h)

# TODO: create a standard format of the following (holdings with weights, units, unit_price etc) 
def prepare_raw_holdings_from_db(db_session, account_ids, portfolio_date, merge_funds):
    isin_meta = dict()
    mf_meta = dict()
    raw_holdings = list()

    # List down all the accounts
    sql_accounts = db_session.query(InvestorAccount).filter(InvestorAccount.is_deleted != 1,
                                                            InvestorAccount.id.in_(account_ids)).all()
    accounts = dict()
    for sql_accoun in sql_accounts:
        accounts[sql_accoun.id] = sql_accoun

    # List down all available holdings
    sql_holdings = db_session.query(InvestorHoldings)\
                             .filter(InvestorHoldings.is_deleted != 1,
                                     InvestorHoldings.account_id.in_(account_ids),
                                     InvestorHoldings.as_of_date==portfolio_date).all()

    # list down ISIN and create mf_list only if merge_fund is required
    security_list = list()
    mf_breakup_list = list()
    
    for sql_holding in sql_holdings:
        if not sql_holding.total_price > 0:
            continue

        if not sql_holding.isin:
            continue

        isin_code = sql_holding.isin.strip()
        # Make a master list in case we don't have any MF holdings
        if isin_code not in security_list:
            security_list.append(isin_code)

        if merge_funds and isin_code.startswith("INF"):
            if isin_code not in mf_breakup_list:
                mf_breakup_list.append(isin_code)
            
    securities_info = get_security_info(db_session, security_list)
    mf_breakup = get_mf_breakup(db_session, mf_breakup_list, portfolio_date.month, portfolio_date.year)
    
    for sql_holding in sql_holdings:
        if not sql_holding.total_price > 0:
            continue

        sql_acc = accounts[sql_holding.account_id]
        account_label = sql_acc.label + ': ' + sql_acc.dp_name if sql_acc.dp_name else sql_acc.label

        # There are possibilities to have securities without ISIN
        if sql_holding.isin:
            # Demat Security
            isin_code = sql_holding.isin.strip()

            is_security_mf = False
            if merge_funds and isin_code.startswith("INF"):
                is_security_mf = True            

            if not is_security_mf:
                # TODO : What is the purpose of the "else" part
                if isin_code in securities_info:
                    meta = securities_info[isin_code]
                else:
                    meta = format_investor_holding(sql_holding)
                obj = meta.copy()

                obj["units"] = sql_holding.units
                obj["unit_price"] = sql_holding.unit_price
                obj["total_price"] = sql_holding.total_price
                obj["account_alias"] = account_label
                obj["as_on_date"] = portfolio_date
                raw_holdings.append(obj)
            else:
                underlyings = mf_breakup[isin_code]
                if len(underlyings) > 0:
                    for underlying in underlyings:
                        obj_2 = underlying.copy()

                        percent = obj_2["weight"]
                        del obj_2["weight"]

                        plan_name = obj_2["plan_name"]
                        del obj_2["plan_name"]

                        obj_2["units"] = 0
                        obj_2["unit_price"] = 0
                        obj_2["total_price"] = sql_holding.total_price*float(percent/100)
                        obj_2["account_alias"] = plan_name
                        obj_2["as_on_date"] = portfolio_date
                        raw_holdings.append(obj_2)
                else:
                    # TODO : Review this change by debugging for the excel updloaded data
                    # the "else" part was added because there was an ISIN added for an older mutual fund i.e. INF173K01OG3
                    if isin_code in securities_info:
                        meta = securities_info[isin_code]
                    else:
                        meta = format_investor_holding(sql_holding)                        
                    obj = meta.copy()

                    obj["units"] = sql_holding.units
                    obj["unit_price"] = sql_holding.unit_price
                    obj["total_price"] = sql_holding.total_price
                    obj["account_alias"] = account_label
                    obj["as_on_date"] = portfolio_date
                    raw_holdings.append(obj)
        else:
            meta = get_non_isin_holding_info(db_session, sql_holding)
            obj = vars(meta).copy()
            obj["units"] = sql_holding.units
            obj["unit_price"] = sql_holding.unit_price
            obj["total_price"] = sql_holding.total_price
            obj["account_alias"] = account_label
            obj["as_on_date"] = portfolio_date
            raw_holdings.append(obj)

    return raw_holdings

def get_investorid_by_account_id(db_session, account_id):
    investor_id = db_session.query(InvestorAccount.investor_id).filter(InvestorAccount.is_deleted != 1).filter(InvestorAccount.id == account_id).scalar()
    
    return investor_id

def get_investorholdings_as_of_dates_by_account_id(db_session, account_ids):
    as_of_dates = db_session.query(func.distinct(InvestorHoldings.as_of_date))\
                                        .filter(InvestorHoldings.account_id.in_(account_ids))\
                                        .order_by(InvestorHoldings.as_of_date.desc()).all()
    return as_of_dates


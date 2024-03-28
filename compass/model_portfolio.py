from datetime import datetime
from fin_models.controller_transaction_models import ModelPortfolio, ModelPortfolioHoldings, ModelPortfolioReturns
from sqlalchemy import and_

from bizlogic.core_helper import get_security_info

class DataFormatException(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info

def save_model_portfolio(db_session, name, description, as_on_date, holdings, returns, created_by_id):
    sql_obj = ModelPortfolio()
    sql_obj.name = name
    sql_obj.description = description
    sql_obj.created_by = created_by_id
    sql_obj.created_date = datetime.now()

    db_session.add(sql_obj)
    db_session.commit()

    portfolio_id = sql_obj.id

    try:
        for holding in holdings:
            sql_holding = ModelPortfolioHoldings()
            sql_holding.isin = holding["isin"]
            sql_holding.name = holding["name"]
            sql_holding.weight = float(holding["weight"])
            sql_holding.model_portfolio_id = portfolio_id
            sql_holding.as_of_date = as_on_date
            sql_holding.created_by = created_by_id
            sql_holding.created_date = datetime.now()
            db_session.add(sql_holding)

        for ret in returns:
            sql_ret = ModelPortfolioReturns()
            sql_ret.model_portfolio_id = portfolio_id
            sql_ret.as_of_date = datetime.strptime(ret["date"], '%d-%m-%Y')
            sql_ret.return_1_month = float(ret["monthly_returns"])
            # sql_ret.as_of_date = ret["date"]
            sql_ret.created_by = created_by_id
            sql_ret.created_date = datetime.now()
            db_session.add(sql_ret)

    except ValueError as ex:
        raise DataFormatException(str(ex.args))

    db_session.commit()

    # TODO: If there is an error at any moment, delete all created objects (think about database transactions and rollback)
    
    return portfolio_id

def get_model_portfolios(db_session, user_id):
    response = list()

    q = db_session.query(ModelPortfolio).filter(ModelPortfolio.is_deleted != 1)
    if user_id:
        q = q.filter(ModelPortfolio.created_by == user_id)
    sql_objs = q.all()
    for sql_obj in sql_objs:
        obj = dict()
        obj["id"] = sql_obj.id
        obj["name"] = sql_obj.name
        obj["description"] = sql_obj.description
        obj["created_by"] = sql_obj.created_by
        obj["created_date"] = sql_obj.created_date
        response.append(obj)

    return response

def get_one_model_portfolios(db_session, portfolio_id):
    response = dict()

    sql_obj = db_session.query(ModelPortfolio).filter(ModelPortfolio.id == portfolio_id).one_or_none()
    if sql_obj:
        response["name"] = sql_obj.name
        response["description"] = sql_obj.description
        response["created_by"] = sql_obj.created_by
        response["created_date"] = sql_obj.created_date

        holdings = list()
        sql_holdings = db_session.query(ModelPortfolioHoldings).filter(and_(ModelPortfolioHoldings.model_portfolio_id == portfolio_id, ModelPortfolioHoldings.is_deleted != 1)).all()
        for sql_hold in sql_holdings:
            obj = dict()
            obj["name"] = sql_hold.name
            obj["isin"] = sql_hold.isin
            obj["weight"] = sql_hold.weight
            obj["as_of_date"] = sql_hold.as_of_date
            holdings.append(obj)

        response["holdings"] = holdings
        
        returns = list()
        sql_rets = db_session.query(ModelPortfolioReturns).filter(and_(ModelPortfolioReturns.model_portfolio_id == portfolio_id, ModelPortfolioReturns.is_deleted != 1)).all()
        for sql_ret in sql_rets:
            obj = dict()
            obj["as_of_date"] = sql_ret.as_of_date
            obj["return_1_month"] = sql_ret.return_1_month
            returns.append(obj)

        response["returns"] = returns

    return response

# Used for portfolio Overlap. Currently has name, isin, weight and sector.
def get_model_portfolio_holdings(db_session, portfolio_id):
    holdings = list()

    isin_list = dict()
    sql_holdings = db_session.query(ModelPortfolioHoldings).filter(and_(ModelPortfolioHoldings.model_portfolio_id == portfolio_id, ModelPortfolioHoldings.is_deleted != 1)).all()
    for sql_hold in sql_holdings:
        obj = dict()
        obj["name"] = sql_hold.name
        obj["isin"] = sql_hold.isin
        isin_list[sql_hold.isin] = ""
        obj["weight"] = sql_hold.weight
        obj["portfolio_date"] = sql_hold.as_of_date
        holdings.append(obj)

    # TODO: Get Sector information for the given isin. different database so could not join.
    # sql_sec = db_session.query(HoldingSecurity.ISIN_Code, Sector.Sector_Name).select_from(HoldingSecurity).join(Sector, Sector.Sector_Id==HoldingSecurity.Sector_Id).filter(HoldingSecurity.ISIN_Code.in_(list(isin_list))).all()
    # for sql_obj in sql_sec:
    #     isin_list[sql_obj[0]] = sql_obj[1]

    securities_info = get_security_info(db_session, list(isin_list.keys()))       
    for obj in holdings:
        isin = obj["isin"]
        sec_obj = securities_info[isin] if isin in securities_info else None        
        obj["sector"] = sec_obj["sector"] if sec_obj else "Unknown"

    return holdings

def get_complete_model_portfolio(db_session, portfolio_id, as_of_date=None):
    holdings = list()
    q = db_session.query(ModelPortfolioHoldings).filter(and_(ModelPortfolioHoldings.model_portfolio_id == portfolio_id, ModelPortfolioHoldings.is_deleted != 1))
    if as_of_date:
        q = q.filter(ModelPortfolioHoldings.as_of_date == as_of_date)
    sql_holdings = q.all()
    
    for sql_hold in sql_holdings:
        obj = dict()
        obj["name"] = sql_hold.name
        obj["isin"] = sql_hold.isin
        obj["weight"] = sql_hold.weight
        obj["as_of_date"] = sql_hold.as_of_date
        holdings.append(obj)

    return holdings

def edit_model_portfolio(db_session, portfolio_id, description, as_on_date, holdings, returns, edited_by_id):
    sql_obj = db_session.query(ModelPortfolio).get(portfolio_id)
    sql_obj.description = description
    sql_obj.updated_by = edited_by_id
    sql_obj.updated_date = datetime.now()
    db_session.commit()

    # Remove old entries and update new ones
    db_session.query(ModelPortfolioHoldings).filter(and_(ModelPortfolioHoldings.model_portfolio_id==portfolio_id, ModelPortfolioHoldings.as_of_date == as_on_date)).delete()

    for holding in holdings:
        sql_holding = ModelPortfolioHoldings()
        sql_holding.name = holding["name"]
        sql_holding.isin = holding["isin"]
        sql_holding.weight = holding["weight"]
        sql_holding.model_portfolio_id = portfolio_id
        sql_holding.as_of_date = as_on_date
        sql_holding.created_by = edited_by_id
        sql_holding.created_date = datetime.now()
        db_session.add(sql_holding)
        db_session.commit()

    # Overwrite if not available
    for ret in returns:
        as_of_date = ret["date"]
        sql_ret = db_session.query(ModelPortfolioReturns).filter(and_(ModelPortfolioReturns.model_portfolio_id==portfolio_id, ModelPortfolioReturns.as_of_date == as_of_date)).one_or_none()
        if sql_ret:
            sql_ret.as_of_date = ret["date"]
            sql_ret.return_1_month = ret["monthly_returns"]
            sql_ret.updated_by = edited_by_id
            sql_ret.updated_date = datetime.now()
            db_session.add(sql_ret)
            db_session.commit()
        else:
            sql_ret = ModelPortfolioReturns()
            sql_ret.model_portfolio_id = portfolio_id
            sql_ret.as_of_date = ret["date"]
            sql_ret.return_1_month = ret["monthly_returns"]
            sql_ret.created_by = edited_by_id
            sql_ret.created_date = datetime.now()
            db_session.add(sql_ret)
            db_session.commit()

    return portfolio_id

def delete_model_portfolio(db_session, portfolio_id):
    # Mark the portfolio for deletion -> this will ensure the portfolio is not going to be read even if other functions crash.
    sql_obj = db_session.query(ModelPortfolio).get(portfolio_id)    
    sql_obj.is_deleted = 1
    db_session.commit()

    # Remove holdings and returns
    db_session.query(ModelPortfolioHoldings).filter(ModelPortfolioHoldings.model_portfolio_id==portfolio_id).delete()

    db_session.query(ModelPortfolioReturns).filter(ModelPortfolioReturns.model_portfolio_id==portfolio_id).delete()

    # Remove the portfolio
    db_session.query(ModelPortfolio).filter(ModelPortfolio.id==portfolio_id).delete()
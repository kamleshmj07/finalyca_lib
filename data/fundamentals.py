import pandas as pd
import numpy as np
import sqlalchemy
from datetime import timedelta
from sqlalchemy import func, desc
from operator import and_
from sqlalchemy.sql.expression import cast

from fin_models.masters_models import Plans, HoldingSecurity, Sector
from fin_models.transaction_models import Fundamentals, FactSheet

def get_portfolio_fundamental(db_session, list_isins, asof_date):
    '''
        Get fundamental data for the portfolio like pe, pb, div_yld
    '''
    # get fundamental data for stocks
    fundamental_cols = ['isin', 'pe', 'pbv', 'div_yld']
    
    list_of_eq_isins = [x for x in list_isins if x.startswith('INE')]
    list_of_mf_isins = [x for x in list_isins if x.startswith('INF')]

    df_fundamental = get_equity_fundamentals(db_session, list_of_eq_isins, asof_date)
    df_fundamental = df_fundamental[fundamental_cols] if not df_fundamental.empty else pd.DataFrame()

    # get fundamental data for funds/plans
    df_factsheet = get_mf_fundamentals(db_session, list_of_mf_isins, asof_date)
    if df_factsheet.shape[0] > 0:
        # df_factsheet = pd.merge(left=df_factsheet, right=pd.DataFrame(sql_plans), on='Plan_Id')
        df_factsheet.rename(columns={'ISIN':'isin'}, inplace=True)
        df_factsheet.drop(['Plan_Id'], axis=1, inplace=True)
        df_factsheet = df_factsheet[fundamental_cols]

    df_main = pd.concat([df_fundamental, df_factsheet], ignore_index=True, sort=False) if not (df_fundamental.empty > 0 and df_factsheet.empty) else None

    # TODO Review the below logic to get non-none dataframe
    if df_main.empty:
        df_main = df_fundamental if not df_fundamental.empty else df_factsheet

    df_main = df_main.fillna(np.nan).replace([np.nan], [None])
    
    return df_main


def get_equity_fundamentals(db_session, list_of_isins, asof_date):
    '''
        Get the fundamental data of a list of stock isins.
        Dataframe columns: isin, pe, eps, pbv, div_yld, market_cap_cr, pricedate, security_name, sector, market_cap
        Delta for backdated data is 3 days.
    '''
    delta = 3
    backdated_date = asof_date - timedelta(days=delta)
    sql_fundamentals_sq = db_session.query(func.max(Fundamentals.PriceDate).label('PriceDate'),
                                           HoldingSecurity.ISIN_Code,
                                           HoldingSecurity.Co_Code)\
                                    .join(HoldingSecurity, HoldingSecurity.Co_Code == cast(Fundamentals.CO_CODE, sqlalchemy.String))\
                                    .filter(HoldingSecurity.ISIN_Code.in_(list_of_isins),
                                            Fundamentals.PriceDate <= asof_date,
                                            Fundamentals.PriceDate >= backdated_date,
                                            Fundamentals.Is_Deleted != 1,
                                            HoldingSecurity.Is_Deleted != 1,
                                            HoldingSecurity.active == 1)\
                                    .group_by(HoldingSecurity.ISIN_Code, HoldingSecurity.Co_Code).subquery()

    sql_fundamental_data = db_session.query(HoldingSecurity.ISIN_Code.label('isin'),
                                            Fundamentals.PE.label('pe'),
                                            Fundamentals.EPS.label('eps'),
                                            Fundamentals.PBV.label('pbv'),
                                            Fundamentals.DivYield.label('div_yld'),
                                            Fundamentals.mcap.label('market_cap_cr'),
                                            Fundamentals.PriceDate.label('pricedate'),
                                            HoldingSecurity.HoldingSecurity_Name.label('security_name'),
                                            Sector.Sector_Name.label('sector'),
                                            HoldingSecurity.MarketCap.label('market_cap'))\
                                     .join(sql_fundamentals_sq, and_(sql_fundamentals_sq.c.Co_Code == cast(Fundamentals.CO_CODE, sqlalchemy.String),
                                                                     sql_fundamentals_sq.c.PriceDate == Fundamentals.PriceDate))\
                                     .join(HoldingSecurity, HoldingSecurity.ISIN_Code == sql_fundamentals_sq.c.ISIN_Code)\
                                     .join(Sector, Sector.Sector_Id == HoldingSecurity.Sector_Id)\
                                     .filter(Fundamentals.Is_Deleted != 1, HoldingSecurity.active == 1).all()


    df_fundamental = pd.DataFrame(sql_fundamental_data)

    return df_fundamental


def get_mf_fundamentals(db_session, list_of_isins, asof_date):
    '''
        Get the portfolio characteristics data for a list of fund/plan isins/plan_ids.
        Dataframe columns: total_stocks, pe, pbv, div_yld, avg_mcap_cr, macaulay_duration_yrs, avg_maturity_yrs, modified_duration_yrs, avg_credit_rating, ytm
        Delta for backdated data is 3 days.
    '''
    df_factsheet = pd.DataFrame(None)
    
    if list_of_isins:
        delta = 3
        backdated_date = asof_date - timedelta(days=delta)
        
        # Get planids
        # TODO Improvise on the union if possible to get the plan_ids for the isins,
        # but we need the isins from eCas to join the final data frame
        sql_plan_1 = db_session.query(Plans.Plan_Id,
                                    Plans.ISIN)\
                            .filter(Plans.ISIN.in_(list_of_isins), Plans.Is_Deleted != 1)

        sql_plan_2 = db_session.query(Plans.Plan_Id,
                                    Plans.ISIN2.label('ISIN'))\
                            .filter(Plans.ISIN2.in_(list_of_isins), Plans.Is_Deleted != 1)
        
        sql_plans = sql_plan_1.union(sql_plan_2)
        list_plan_ids = [plan.Plan_Id for plan in sql_plans]

        sql_factsheet_sq = db_session.query(func.max(FactSheet.TransactionDate).label('TransactionDate'),
                                            FactSheet.Plan_Id)\
                                    .filter(FactSheet.Plan_Id.in_(list_plan_ids),
                                            FactSheet.TransactionDate <= asof_date,
                                            FactSheet.TransactionDate >= backdated_date,
                                            FactSheet.Is_Deleted != 1)\
                                    .group_by(FactSheet.Plan_Id).subquery()

        sql_factsheet_data = db_session.query(Plans.Plan_Id,
                                            FactSheet.PortfolioP_ERatio.label('pe'),
                                            FactSheet.Portfolio_Dividend_Yield.label('div_yld'),                                          
                                            FactSheet.PortfolioP_BRatio.label('pbv'))\
                                        .join(sql_factsheet_sq, and_(sql_factsheet_sq.c.Plan_Id == FactSheet.Plan_Id,
                                                                        sql_factsheet_sq.c.TransactionDate == FactSheet.TransactionDate))\
                                        .join(Plans, Plans.Plan_Id == sql_factsheet_sq.c.Plan_Id)\
                                        .filter(FactSheet.Is_Deleted != 1,
                                                Plans.Is_Deleted != 1)\
                                        .order_by(desc(FactSheet.TransactionDate)).all()

        df_factsheet = pd.merge(left=pd.DataFrame(sql_factsheet_data), right=pd.DataFrame(sql_plans), on='Plan_Id')
                                                                    
    return df_factsheet

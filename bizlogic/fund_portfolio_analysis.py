import datetime
from bizlogic.importer_helper import get_rolling_returns
import pandas as pd
from sqlalchemy import func, case, and_, desc
from utils.utils import print_query
from fin_models.transaction_models import *
from fin_models.masters_models import *
from dateutil.relativedelta import relativedelta
import numpy as np
from sqlalchemy import func, desc, select, and_
from utils.utils import truncate_table, calculate_age
from typing import Dict
import logging
from datetime import timedelta, datetime as dt
from bizlogic.importer_helper import get_marketcap_composition
from data.holdings import get_fund_underlying_holdings



def save_portfolio_sectors(db_session, plan_id, transaction_date, sectors: Dict, user_id, forced_write):
    # check in database if it exists
    sql_objs = db_session.query(PortfolioSectors).filter(PortfolioSectors.Plan_Id == plan_id).\
    filter(PortfolioSectors.Portfolio_Date == transaction_date).filter(PortfolioSectors.Is_Deleted != 1).all()
    
    insert_op = True
    if sql_objs:
        if forced_write:
            delete_portfolio_sectors(db_session, plan_id, transaction_date, user_id)
        else:
            insert_op = False
    
    if insert_op:
        for obj in sectors:
            id = save_portfolio_sector(db_session, plan_id, transaction_date, obj["Sector_Code"], obj["Sector_Name"], obj["Sub_SectorName"], obj["Percentage_to_AUM"], obj["LONG_SHORT"], user_id)
        
        #get cash from factsheet
        if sectors:
            cash_perc = db_session.query(FactSheet.Cash).filter(FactSheet.Plan_Id == plan_id)\
                                                        .filter(FactSheet.TransactionDate == transaction_date)\
                                                        .filter(FactSheet.Is_Deleted != 1).scalar()
            
            id = save_portfolio_sector(db_session, plan_id, transaction_date, 'Cash_01', 'Cash', '', cash_perc if cash_perc else 0, 'L', user_id)
    

def delete_portfolio_sectors(db_session, plan_id, portfolio_date, user_id):
    db_session.query(PortfolioSectors).filter(PortfolioSectors.Plan_Id == plan_id).filter(PortfolioSectors.Portfolio_Date == portfolio_date)\
    .filter(PortfolioSectors.Is_Deleted != 1).update(
        {
            PortfolioSectors.Is_Deleted : 1,
            PortfolioSectors.Updated_By : user_id,
            PortfolioSectors.Updated_Date : datetime.datetime.today()
        }
    )
    db_session.commit()
        

def save_portfolio_sector(db_session, plan_id, portfolio_date, sector_code, sector_name, sub_sector_name, percentage_to_aum, long_short, user_id):
    sql_portfoliosectors = PortfolioSectors()
    sql_portfoliosectors.Plan_Id = plan_id
    sql_portfoliosectors.Portfolio_Date = portfolio_date
    sql_portfoliosectors.Sector_Code = sector_code
    sql_portfoliosectors.Sector_Name = sector_name
    sql_portfoliosectors.Sub_Sector_Name = sub_sector_name
    sql_portfoliosectors.Percentage_To_AUM = percentage_to_aum
    sql_portfoliosectors.Is_Deleted = 0
    sql_portfoliosectors.Created_By = user_id
    sql_portfoliosectors.Created_Date = datetime.datetime.today()
    sql_portfoliosectors.LONG_SHORT = long_short
    db_session.add(sql_portfoliosectors)
    db_session.commit()
    return sql_portfoliosectors.Portfolio_Sectors_Id


def save_portfolio_analysis(db_session, plan_id, transaction_date, portfolio_analysis: Dict, user_id, force_write):
    # check in database if it exists
    sql_objs = db_session.query(PortfolioAnalysis).filter(PortfolioAnalysis.Plan_Id == plan_id).\
    filter(PortfolioAnalysis.Portfolio_Date == transaction_date).filter(PortfolioAnalysis.Is_Deleted != 1).all()
    
    insert_op = True
    if sql_objs:
        if force_write:
            delete_portfolio_analysis(db_session, plan_id, transaction_date, user_id)
        else:
            insert_op = False

    if insert_op:
        for obj in portfolio_analysis:
            id = save_portfolio_analysis_entry(db_session, plan_id, transaction_date, obj["Attribute_Type"], obj["Attribute_Text"], obj["Attribute_Sub_Text"], 
                                  obj["Attribute_Value"], user_id)
    

def delete_portfolio_analysis(db_session, plan_id, portfolio_date, user_id):
    db_session.query(PortfolioAnalysis).filter(PortfolioAnalysis.Plan_Id == plan_id).filter(PortfolioAnalysis.Portfolio_Date == portfolio_date)\
    .filter(PortfolioAnalysis.Is_Deleted != 1).update(
        {
            PortfolioAnalysis.Is_Deleted : 1,
            PortfolioAnalysis.Updated_By : user_id,
            PortfolioAnalysis.Updated_Date : datetime.datetime.today()
        }
    )
    db_session.commit()


def update_portfolio_date(db_session, factsheet_id, portfolio_date):
    db_session.query(FactSheet).filter(FactSheet.FactSheet_Id == factsheet_id).update(
        {
            FactSheet.Portfolio_Date : portfolio_date
        }
    )
    db_session.commit()
    

def save_portfolio_analysis_entry(db_session, plan_id, portfolio_date, Attribute_Type, Attribute_Text, Attribute_Sub_Text, Attribute_Value, user_id):
    sql_obj = PortfolioAnalysis()
    sql_obj.Plan_Id = plan_id
    sql_obj.Portfolio_Date = portfolio_date
    sql_obj.Attribute_Type = Attribute_Type
    sql_obj.Attribute_Text = Attribute_Text
    sql_obj.Attribute_Sub_Text = Attribute_Sub_Text
    sql_obj.Attribute_Value = Attribute_Value
    sql_obj.Is_Deleted = 0
    sql_obj.Created_By = user_id
    sql_obj.Created_Date = datetime.datetime.today()
    db_session.add(sql_obj)
    db_session.commit()
    return sql_obj.Porfolio_Analysis_Id


def save_portfolio_movement(db_session, plan_id, transaction_date, portfolio_movement: Dict, user_id, forced_write):
    # check in database if it exists
    sql_objs = db_session.query(PortfolioHoldings).filter(PortfolioHoldings.Plan_Id == plan_id).\
    filter(PortfolioHoldings.Portfolio_Date == transaction_date).filter(PortfolioHoldings.Is_Deleted != 1).all()
    
    insert_op = True
    if sql_objs:
        if forced_write:
            delete_portfolio_movement(db_session, plan_id, transaction_date, user_id)
        else:
            insert_op = False

    if insert_op:
        for obj in portfolio_movement:
            id = save_portfolio_movement_entry(db_session, plan_id, transaction_date, obj["Holding_Type"], obj["ISIN_Code"], obj["Security_Name"], 
                                  obj["Current_Weight"], obj["Difference_Weight"], user_id)
    

def delete_portfolio_movement(db_session, plan_id, portfolio_date, user_id):
    db_session.query(PortfolioHoldings).filter(PortfolioHoldings.Plan_Id == plan_id).filter(PortfolioHoldings.Portfolio_Date == portfolio_date)\
    .filter(PortfolioHoldings.Is_Deleted != 1).update(
        {
            PortfolioHoldings.Is_Deleted : 1,
            PortfolioHoldings.Updated_By : user_id,
            PortfolioHoldings.Updated_Date : datetime.datetime.today()
        }
    )
    db_session.commit()


def save_portfolio_movement_entry(db_session, plan_id, portfolio_date, Holding_Type, ISIN_Code, Security_Name, Current_Weight, Difference_Weight, user_id):
    sql_obj = PortfolioHoldings()
    sql_obj.Plan_Id = plan_id
    sql_obj.Portfolio_Date = portfolio_date
    sql_obj.Holding_Type = Holding_Type
    sql_obj.ISIN_Code = ISIN_Code
    sql_obj.Security_Name = Security_Name
    sql_obj.Current_Weight = Current_Weight
    sql_obj.Difference_Weight = Difference_Weight
    sql_obj.Is_Deleted = 0
    sql_obj.Created_By = user_id
    sql_obj.Created_Date = datetime.datetime.today()
    db_session.add(sql_obj)
    db_session.commit()
    return sql_obj.Portfololio_Holdings_Id


def generate_portfolio_analysis(db_session, factsheet_id, user_id, dry_run, force_write):
    sql_obj = db_session.query(FactSheet.FactSheet_Id, FactSheet.Plan_Id, FactSheet.TransactionDate, Fund.Fund_Id, Fund.HidePortfolioHoldingChanges)\
                        .select_from(FactSheet)\
                        .join(Plans, Plans.Plan_Id==FactSheet.Plan_Id)\
                        .join(PlanProductMapping, Plans.Plan_Id==PlanProductMapping.Plan_Id)\
                        .join(MFSecurity, MFSecurity.MF_Security_Id==Plans.MF_Security_Id)\
                        .join(Fund, Fund.Fund_Id==MFSecurity.Fund_Id)\
                        .filter(FactSheet.FactSheet_Id==factsheet_id).one_or_none()

    fund_id = sql_obj.Fund_Id
    plan_id = sql_obj.Plan_Id
    transaction_date = sql_obj.TransactionDate
    hide_portfolio_holding_changes = sql_obj.HidePortfolioHoldingChanges
    portfolio_analysis_df = pd.DataFrame()
    sector_df = pd.DataFrame()
    portfolio_movement_df = pd.DataFrame()
    final = pd.DataFrame()
    old_df = pd.DataFrame()

    p_portfolio_date = None
    portfolio_date = None

    holding_date = db_session.query(func.distinct(UnderlyingHoldings.Portfolio_Date))\
                             .filter(UnderlyingHoldings.Fund_Id == fund_id, UnderlyingHoldings.Portfolio_Date <= transaction_date)\
                             .filter(UnderlyingHoldings.Is_Deleted!=1, UnderlyingHoldings.Percentage_to_AUM > 0)\
                             .order_by(desc(UnderlyingHoldings.Portfolio_Date)).all()

    if holding_date:
        portfolio_date = holding_date[0][0]

        if len(holding_date)>1:
            p_portfolio_date = holding_date[1][0]
    
        Portfolio_Analysis = db_session.query(func.max(PortfolioAnalysis.Portfolio_Date))\
                                       .filter(PortfolioAnalysis.Plan_Id==plan_id, PortfolioAnalysis.Portfolio_Date==portfolio_date)\
                                       .filter(PortfolioAnalysis.Is_Deleted!=1).scalar()

        Portfolio_Sectors = db_session.query(func.max(PortfolioSectors.Portfolio_Date))\
                                      .filter(PortfolioSectors.Plan_Id==plan_id, PortfolioSectors.Portfolio_Date==portfolio_date)\
                                      .filter(PortfolioSectors.Is_Deleted!=1).scalar()

        Portfolio_Holding = db_session.query(func.max(PortfolioHoldings.Portfolio_Date))\
                                      .filter(PortfolioHoldings.Plan_Id==plan_id, PortfolioHoldings.Portfolio_Date==portfolio_date)\
                                      .filter(PortfolioHoldings.Is_Deleted!=1).scalar()

        df = pd.DataFrame(get_fund_underlying_holdings(db_session, fund_id, portfolio_date, None))
        columns_used = ["Portfolio_Date","Underlying_Holdings_Id","HoldingSecurity_Id","Co_Code","ISIN_Code","HoldingSecurity_Type",
                        "Percentage_to_AUM","Value_in_INR","Asset_Class","Risk_Category","Instrument_Type","MarketCap",
                        "HoldingSecurity_Name","Equity_Style","Sector_Code","Sector_Name","Sub_SectorName", "LONG_SHORT"]   

        if not df.empty:            
            df = df[columns_used]
            df['StocksRank'] = df['MarketCap']
            df['LONG_SHORT'] = np.where((df['LONG_SHORT'].isna()), 'L', df['LONG_SHORT'])
            
            stocks_rank_df = df.groupby(["StocksRank"], as_index=False).agg(Percentage_to_AUM = ("Percentage_to_AUM", "sum"))\
                                                                       .sort_values(by="Percentage_to_AUM", ascending=False)

            stocks_rank_df.insert(loc=0, column='Plan_Id', value=plan_id)
            stocks_rank_df.insert(loc=1, column='Portfolio_Date', value=transaction_date)
            stocks_rank_df.insert(loc=2, column='Attribute_Type', value="Market_Cap")
            stocks_rank_df.rename(columns = {'StocksRank':'Attribute_Text'}, inplace = True)
            stocks_rank_df.insert(loc=4, column='Attribute_Sub_Text', value="")
            stocks_rank_df.rename(columns = {'Percentage_to_AUM':'Attribute_Value'}, inplace = True)

            risk_category_df = df.groupby(["Risk_Category"], as_index=False)\
                                 .agg(Percentage_to_AUM = ("Percentage_to_AUM", "sum"))\
                                 .sort_values(by="Percentage_to_AUM", ascending=False)
            risk_category_df.insert(loc=0, column='Plan_Id', value=plan_id)
            risk_category_df.insert(loc=1, column='Portfolio_Date', value=transaction_date)
            risk_category_df.insert(loc=2, column='Attribute_Type', value="Risk_Ratings")
            risk_category_df.rename(columns = {'Risk_Category':'Attribute_Text'}, inplace = True)
            risk_category_df.insert(loc=4, column='Attribute_Sub_Text', value="")
            risk_category_df.rename(columns = {'Percentage_to_AUM':'Attribute_Value'}, inplace = True)

            # Instrument_Type >> HoldingSecurity_Type
            investment_type_df = df.groupby(["HoldingSecurity_Type"], as_index=False)\
                                   .agg(Percentage_to_AUM = ("Percentage_to_AUM", "sum"))\
                                   .sort_values(by="Percentage_to_AUM", ascending=False)
            investment_type_df.insert(loc=0, column='Plan_Id', value=plan_id)
            investment_type_df.insert(loc=1, column='Portfolio_Date', value=transaction_date)
            investment_type_df.insert(loc=2, column='Attribute_Type', value="Instrument_Type")
            investment_type_df.rename(columns = {'HoldingSecurity_Type':'Attribute_Text'}, inplace = True)
            investment_type_df.insert(loc=4, column='Attribute_Sub_Text', value="")
            investment_type_df.rename(columns = {'Percentage_to_AUM':'Attribute_Value'}, inplace = True)

            equity_style_df = df.groupby(["StocksRank", "Equity_Style"], as_index=False)\
                                .agg(Percentage_to_AUM = ("Percentage_to_AUM", "sum"))\
                                .sort_values(by="Percentage_to_AUM", ascending=False)
            equity_style_df.insert(loc=0, column='Plan_Id', value=plan_id)
            equity_style_df.insert(loc=1, column='Portfolio_Date', value=transaction_date)
            equity_style_df.insert(loc=2, column='Attribute_Type', value="Stocks_Rank")
            equity_style_df.rename(columns = {'StocksRank':'Attribute_Text'}, inplace = True)
            equity_style_df.rename(columns = {'Equity_Style':'Attribute_Sub_Text'}, inplace = True)
            equity_style_df.rename(columns = {'Percentage_to_AUM':'Attribute_Value'}, inplace = True)

            portfolio_analysis_df = pd.concat([stocks_rank_df, risk_category_df, investment_type_df, equity_style_df])

            sector_df = df.groupby(["Sector_Code","Sector_Name", "Sub_SectorName", "LONG_SHORT"], dropna=False, as_index=False)\
                          .agg(Percentage_to_AUM = ("Percentage_to_AUM", "sum"))\
                          .sort_values(by="Percentage_to_AUM", ascending=False)

            # Handling empty sub sector names
            sector_df = sector_df.fillna("")

        if p_portfolio_date:
            old_df = pd.DataFrame(get_fund_underlying_holdings(db_session, fund_id, p_portfolio_date, None))
            if not old_df.empty:
                old_df = old_df[columns_used]
                old_df['StocksRank'] = old_df['MarketCap']
                old_df['LONG_SHORT'] = np.where((old_df['LONG_SHORT'].isna()), 'L', old_df['LONG_SHORT'])


        drop_cols = ["Underlying_Holdings_Id", "HoldingSecurity_Type", "Asset_Class", "Risk_Category", "Instrument_Type",
                     "MarketCap", "StocksRank", "Equity_Style", "Sector_Code", "Sector_Name", "Sub_SectorName", "Value_in_INR"]
        if not df.empty:            
            df = df.drop(drop_cols, axis=1)

        if not old_df.empty:            
            old_df = old_df.drop(drop_cols, axis=1)
        
        if not df.empty or not old_df.empty:
            # df = df.fillna(0)
            # old_df = old_df.fillna(0)
            if not df.empty and not old_df.empty:
                try:
                    final = pd.merge(df, old_df, how="outer", on=["Co_Code"], suffixes=('', '_P'),)
                except:
                    #do nothing
                    final = pd.DataFrame()
            
            if not final.empty:
                final = final.loc[ :, ["Portfolio_Date", "HoldingSecurity_Id", "Co_Code", "ISIN_Code", "HoldingSecurity_Name", "Percentage_to_AUM", "Portfolio_Date_P", "Percentage_to_AUM_P",]]
                final.insert(loc=0, column='Plan_Id', value=plan_id)
                final["Portfolio_Date"] = final["Portfolio_Date"].fillna(value=final["Portfolio_Date"].unique()[0])
                final["Portfolio_Date_P"] = final["Portfolio_Date_P"].fillna(value=final["Portfolio_Date_P"].unique()[0])
                final = final.fillna(0)
                final["Diff_Percentage_to_AUM"] = final["Percentage_to_AUM"].fillna(0) - final["Percentage_to_AUM_P"].fillna(0)
                final.rename(columns = {'HoldingSecurity_Name':'Security_Name'}, inplace = True)
                final.rename(columns = {'Percentage_to_AUM':'Current_Weight'}, inplace = True)
                final.rename(columns = {'Percentage_to_AUM_P':'Old_Weight'}, inplace = True)
                final.rename(columns = {'Diff_Percentage_to_AUM':'Difference_Weight'}, inplace = True)
                
                increase_df = final.loc[(final["Current_Weight"] > 0) & (final["Old_Weight"] > 0) & (final["Difference_Weight"] > 0)]
                increase_df.insert(loc=2, column='Holding_Type', value="Increase_Exposure")
                
                decrease_df = final.loc[(final["Current_Weight"] > 0) & (final["Old_Weight"] > 0) & (final["Difference_Weight"] < 0)]
                decrease_df.insert(loc=2, column='Holding_Type', value="Decrease_Exposure")
                
                entry_df = final.loc[(final["Current_Weight"] > 0 ) & (final["Old_Weight"] == 0) & (final["Difference_Weight"] > 0)]
                entry_df.insert(loc=2, column='Holding_Type', value="New_Entrants")
                
                exit_df = final.loc[(final["Current_Weight"] == 0) & (final["Old_Weight"] > 0) & (final["Difference_Weight"] < 0)]
                exit_df.insert(loc=2, column='Holding_Type', value="Complete_Exit")

                if not final.empty:    
                    portfolio_movement_df = pd.concat([increase_df, decrease_df, entry_df, exit_df])

        if not dry_run:
            print("Storing data in database")
            #update portfolio date
            if portfolio_date:
                update_portfolio_date(db_session, factsheet_id, portfolio_date)

                if not sector_df.empty:
                    sectors = sector_df.to_dict(orient="records")
                    save_portfolio_sectors(db_session, plan_id, portfolio_date, sectors, user_id, force_write)

                if not portfolio_analysis_df.empty:
                    portfolio_analysis = portfolio_analysis_df.to_dict(orient="records")
                    save_portfolio_analysis(db_session, plan_id, portfolio_date, portfolio_analysis, user_id, force_write)

                if not portfolio_movement_df.empty:
                    if not hide_portfolio_holding_changes:
                        portfolio_movement = portfolio_movement_df.to_dict(orient="records")
                        save_portfolio_movement(db_session, plan_id, portfolio_date, portfolio_movement, user_id, force_write)
                else:
                    delete_portfolio_movement(db_session, plan_id, portfolio_date, user_id)
        else:
            print("NOT Storing data in database")
    
    return portfolio_analysis_df, sector_df, portfolio_movement_df


'''
# TODO: KM: This function was left to be refactored in the underlying holdings refactor phase 1, needs review.
The data aggregation/manipulations inside this function can be implemented on a dataframe that will receive underlyingholdings from our core function in holdings.py
'''
def get_holding_details(db_session, fund_id, portfolio_date):
    # TODO: Do outer join with Holding Security if Cash and Cash Equivalents are required in output
    sql_subquery = db_session.query(func.max(HoldingSecurity.HoldingSecurity_Id).label('max_holdingsecurity_id'), 
                                    HoldingSecurity.Co_Code).filter(HoldingSecurity.Is_Deleted != 1,
                                                                    HoldingSecurity.Co_Code != None,
                                                                    HoldingSecurity.Co_Code != '',
                                                                    HoldingSecurity.active != 0).group_by(HoldingSecurity.Co_Code).subquery()

    sql_subquery_securityname = db_session.query(HoldingSecurity.HoldingSecurity_Id,
                                                 HoldingSecurity.HoldingSecurity_Name,
                                                 HoldingSecurity.HoldingSecurity_Type,
                                                 HoldingSecurity.Instrument_Type,
                                                 HoldingSecurity.MarketCap,
                                                 HoldingSecurity.Equity_Style,
                                                 HoldingSecurity.Co_Code,
                                                 HoldingSecurity.Sector_Id,
                                                 HoldingSecurity.Issuer_Id).filter(HoldingSecurity.Is_Deleted != 1,
                                                                                   HoldingSecurity.active != 0).subquery()

    sql_holdings = db_session.query(
        UnderlyingHoldings.Portfolio_Date,         
        func.coalesce(sql_subquery.c.max_holdingsecurity_id, HoldingSecurity.HoldingSecurity_Id).label('HoldingSecurity_Id'),       
        func.coalesce(sql_subquery_securityname.c.HoldingSecurity_Name, func.coalesce(HoldingSecurity.HoldingSecurity_Name, UnderlyingHoldings.Company_Security_Name)).label("HoldingSecurity_Name"),
        func.coalesce(sql_subquery_securityname.c.Co_Code, HoldingSecurity.Co_Code).label('Co_Code'),
        UnderlyingHoldings.ISIN_Code,
        HoldingSecurity.HoldingSecurity_Type, 
        UnderlyingHoldings.Percentage_to_AUM, 
        UnderlyingHoldings.Value_in_INR, 
        func.upper(func.coalesce(UnderlyingHoldings.Asset_Class, func.coalesce(sql_subquery_securityname.c.HoldingSecurity_Type, HoldingSecurity.HoldingSecurity_Type))).label("Asset_Class"),
        UnderlyingHoldings.Risk_Category,
        func.upper(func.coalesce(sql_subquery_securityname.c.Instrument_Type, HoldingSecurity.Instrument_Type)).label('Instrument_Type'),
        # UnderlyingHoldings.MarketCap,
        func.coalesce(sql_subquery_securityname.c.MarketCap, HoldingSecurity.MarketCap).label('MarketCap'),
        func.coalesce(sql_subquery_securityname.c.Equity_Style, HoldingSecurity.Equity_Style).label('Equity_Style'),  
        Issuer.Issuer_Id,
        Issuer.Issuer_Name,
        Issuer.Issuer_Code,
        Sector.Sector_Id,
        Sector.Sector_Code,
        Sector.Sector_Name,
        func.coalesce(UnderlyingHoldings.LONG_SHORT, 'L').label("Sector_Long_Short"), 
        HoldingSecurity.Sub_SectorName,
        UnderlyingHoldings.Purchase_Date
    ).join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id==UnderlyingHoldings.HoldingSecurity_Id)\
    .join(sql_subquery, sql_subquery.c.Co_Code == HoldingSecurity.Co_Code, isouter = True)\
    .join(sql_subquery_securityname, sql_subquery_securityname.c.HoldingSecurity_Id ==\
    func.coalesce(sql_subquery.c.max_holdingsecurity_id, HoldingSecurity.HoldingSecurity_Id), isouter = True)\
    .join(Sector, Sector.Sector_Id == func.coalesce(sql_subquery_securityname.c.Sector_Id, HoldingSecurity.Sector_Id))\
    .join(Issuer, Issuer.Issuer_Id == func.coalesce(sql_subquery_securityname.c.Issuer_Id, HoldingSecurity.Issuer_Id))\
    .filter(UnderlyingHoldings.Fund_Id == fund_id)\
    .filter(UnderlyingHoldings.Portfolio_Date == portfolio_date)\
    .filter(UnderlyingHoldings.Is_Deleted != 1).filter(HoldingSecurity.Is_Deleted != 1)\
    .filter(HoldingSecurity.active != 0)\
    .filter(UnderlyingHoldings.Percentage_to_AUM > 0).all()

    df = pd.DataFrame(sql_holdings)
    
    return df

def get_fund_stocks(db_session, fund_id, portfolio_date, prev_portfolio_date):
    # print(F"Reading stocks from {fund_id} for {portfolio_date} and for old {prev_portfolio_date}")
    final = pd.DataFrame()
    logging.warning(F"prepare_fund_stocks_table inside get_fund_stocks for fund_id-{fund_id}")
    last_df = get_holding_details(db_session, fund_id, portfolio_date)
    logging.warning(F"prepare_fund_stocks_table inside get_fund_stocks after last_df for fund_id-{fund_id}")
    prev_df = get_holding_details(db_session, fund_id, prev_portfolio_date)
    logging.warning(F"prepare_fund_stocks_table inside get_fund_stocks after prev_df for fund_id-{fund_id}")

    if last_df.empty and not prev_df.empty:        
        # Latest portfolio is not available. maybe having 100% Cash or Cash Equivalents
        final = prev_df
        final[['is_increased', 'is_decreased', 'is_entry', 'is_exit']] = 0
                
        final["Portfolio_Date"] = portfolio_date
        final["Portfolio_Date_P"] = final["Portfolio_Date"].fillna(value=final["Portfolio_Date"].unique()[0])
        final["Percentage_to_AUM"] = 0
        final["Percentage_to_AUM_P"] = final["Percentage_to_AUM"].fillna(0)
        final["Value_in_INR"] = 0
        final["Value_in_INR_P"] = final["Value_in_INR"].fillna(0)
        final["Diff_Percentage_to_AUM"] = final["Percentage_to_AUM"] - final["Percentage_to_AUM_P"]
        
        final.rename(columns = {'HoldingSecurity_Name':'Security_Name'}, inplace = True)
        final.rename(columns = {'Percentage_to_AUM':'Current_Weight'}, inplace = True)
        final.rename(columns = {'Percentage_to_AUM_P':'Old_Weight'}, inplace = True)
        final.rename(columns = {'Diff_Percentage_to_AUM':'Difference_Weight'}, inplace = True) 
        
        final['is_exit'] = 1       
        
    elif prev_df.empty and not last_df.empty:
        # Previous portfolio is not available. Maybe having 100% Cash or Cash Equivalents.
        final = last_df
        final[['is_increased', 'is_decreased', 'is_entry', 'is_exit']] = 0
                
        final["Portfolio_Date"] = final["Portfolio_Date"].fillna(value=final["Portfolio_Date"].unique()[0])
        final["Portfolio_Date_P"] = prev_portfolio_date
        final["Percentage_to_AUM"] = final["Percentage_to_AUM"].fillna(0)
        final["Percentage_to_AUM_P"] = 0
        final["Value_in_INR"] = final["Value_in_INR"].fillna(0)
        final["Value_in_INR_P"] = 0
        final["Diff_Percentage_to_AUM"] = final["Percentage_to_AUM"] - final["Percentage_to_AUM_P"]
        
        final.rename(columns = {'HoldingSecurity_Name':'Security_Name'}, inplace = True)
        final.rename(columns = {'Percentage_to_AUM':'Current_Weight'}, inplace = True)
        final.rename(columns = {'Percentage_to_AUM_P':'Old_Weight'}, inplace = True)
        final.rename(columns = {'Diff_Percentage_to_AUM':'Difference_Weight'}, inplace = True) 
        
        final['is_entry'] = 1       
    
    elif not prev_df.empty and not last_df.empty:   
        # prev_df.to_csv('prev_df.csv')
        # last_df.to_csv('last_df.csv')
        final = pd.merge(last_df, prev_df, how="outer", on=[
            # "Co_Code", # We need to merge securities with Co Code but there could be some mismatch
            "HoldingSecurity_Id", "HoldingSecurity_Name", "HoldingSecurity_Type", "Asset_Class", 
            # "Risk_Category", -- uncomment this if required
            "Instrument_Type", 
            "MarketCap", "Equity_Style", "Issuer_Id", "Issuer_Name", "Issuer_Code", "Sector_Id", "Sector_Code", "Sector_Name", "Sub_SectorName",
            "Sector_Long_Short"
            # , "Purchase_Date"
            ], suffixes=('', '_P') )
        
        # Do not keep exited securities
        # final = final.loc[final["Percentage_to_AUM"] > 0 ]

        # remove the other isin code (It may happen that ISIN has changed since last month, so only keep the latest one)
        final = final.drop(["ISIN_Code_P", "Purchase_Date_P"], axis=1)

        final[['is_increased', 'is_decreased', 'is_entry', 'is_exit']] = 0

        final["Portfolio_Date"] = final["Portfolio_Date"].fillna(value=final["Portfolio_Date"].unique()[0])
        final["Portfolio_Date_P"] = final["Portfolio_Date_P"].fillna(value=final["Portfolio_Date_P"].unique()[0])
        final["Percentage_to_AUM"] = final["Percentage_to_AUM"].fillna(0)
        final["Percentage_to_AUM_P"] = final["Percentage_to_AUM_P"].fillna(0)
        final["Value_in_INR"] = final["Value_in_INR"].fillna(0)
        final["Value_in_INR_P"] = final["Value_in_INR_P"].fillna(0)
        final["Diff_Percentage_to_AUM"] = final["Percentage_to_AUM"] - final["Percentage_to_AUM_P"]

        final.rename(columns = {'HoldingSecurity_Name':'Security_Name'}, inplace = True)
        final.rename(columns = {'Percentage_to_AUM':'Current_Weight'}, inplace = True)
        final.rename(columns = {'Percentage_to_AUM_P':'Old_Weight'}, inplace = True)
        final.rename(columns = {'Diff_Percentage_to_AUM':'Difference_Weight'}, inplace = True)   
        # final.to_csv('final1.csv')
        final.loc[(final["Current_Weight"] > 0) & (final["Old_Weight"] > 0) & (final["Difference_Weight"] > 0), 'is_increased'] = 1
        final.loc[(final["Current_Weight"] > 0) & (final["Old_Weight"] > 0) & (final["Difference_Weight"] < 0), 'is_decreased'] = 1
        final.loc[(final["Current_Weight"] > 0 ) & (final["Old_Weight"] == 0) & (final["Difference_Weight"] > 0), 'is_entry'] = 1
        final.loc[(final["Current_Weight"] == 0) & (final["Old_Weight"] > 0) & (final["Difference_Weight"] < 0), 'is_exit'] = 1
        # final.to_csv('final2.csv')
    logging.warning(F"prepare_fund_stocks_table inside get_fund_stocks before sql_obj line 447 for fund_id-{fund_id}")
    sql_obj = db_session.query(
        AMC.AMC_Id.label('AMC_Id'), AMC.AMC_Name.label('AMC_Name'), AMC.AMC_Logo.label('AMC_Logo'),
        Product.Product_Id.label('Product_Id'), Product.Product_Name.label('Product_Name'), Product.Product_Code.label('Product_Code'), 
        Fund.Fund_Id.label('Fund_Id'), Fund.Fund_Name.label('Fund_Name'), 
        Classification.Classification_Id.label('Classification_Id'), Classification.Classification_Name.label('Classification_Name'),
        Plans.Plan_Id.label('Plan_Id'), Plans.Plan_Name.label('Plan_Name')
        )\
    .join(MFSecurity, and_(MFSecurity.Fund_Id==Fund.Fund_Id, MFSecurity.Is_Deleted != 1))\
    .join(Plans, and_(MFSecurity.MF_Security_Id==Plans.MF_Security_Id, Plans.Is_Deleted != 1))\
    .join(Classification, Classification.Classification_Id==MFSecurity.Classification_Id)\
    .join(AMC, MFSecurity.AMC_Id==AMC.AMC_Id)\
    .join(Product, Product.Product_Id==AMC.Product_Id)\
    .filter(and_(Fund.Fund_Id==fund_id, Fund.Is_Deleted != 1, Plans.PlanType_Id == 1, Plans.Option_Id == 2))\
    .first()    # MFSecurity and Fund needs cleanup
    # .one_or_none()

    if not sql_obj:
        return pd.DataFrame()
    
    final.insert(0, "AMC_Id", sql_obj.AMC_Id)
    final.insert(1, "AMC_Name", sql_obj.AMC_Name)
    final.insert(2, "AMC_Logo", sql_obj.AMC_Logo)
    
    final.insert(3, "Product_Id", sql_obj.Product_Id)
    final.insert(4, "Product_Name", sql_obj.Product_Name)
    final.insert(5, "Product_Code", sql_obj.Product_Code)

    final.insert(6, "Fund_Id", sql_obj.Fund_Id)
    final.insert(7, "Fund_Name", sql_obj.Fund_Name)

    final.insert(8, "Classification_Id", sql_obj.Classification_Id)
    final.insert(9, "Classification_Name", sql_obj.Classification_Name)

    final.insert(8, "Plan_Id", sql_obj.Plan_Id)
    final.insert(9, "Plan_Name", sql_obj.Plan_Name)
    
    return final

def update_fund_stocks(db_session, fund_id, portfolio_date, prev_portfolio_date):
    # print(F"Generating Fund Stocks for {fund_id} for {portfolio_date} and {prev_portfolio_date}")
    logging.warning(F"prepare_fund_stocks_table inside update_fund_stocks for fund_id-{fund_id}")
    final = get_fund_stocks(db_session, fund_id, portfolio_date, prev_portfolio_date)
    logging.warning(F"prepare_fund_stocks_table inside update_fund_stocks after final for fund_id-{fund_id}")
    if not final.empty:
        final = final.fillna(0)
        # final.to_csv('final.csv')
        final_data = final.to_dict(orient="records")

        for item in final_data:
            logging.warning(F"prepare_fund_stocks_table inside update_fund_stocks for fund_id-{fund_id} prepare insert")
            sql_obj = FundStocks()
            sql_obj.AMC_Id = item["AMC_Id"]
            sql_obj.AMC_Name = item["AMC_Name"]
            sql_obj.AMC_Logo = item["AMC_Logo"]
            sql_obj.Product_Id = item["Product_Id"]
            sql_obj.Product_Code = item["Product_Code"]
            sql_obj.Product_Name = item["Product_Name"]
            sql_obj.Fund_Id = item["Fund_Id"]
            sql_obj.Fund_Name = item["Fund_Name"]
            sql_obj.Classification_Id = item["Classification_Id"]
            sql_obj.Classification_Name = item["Classification_Name"]
            sql_obj.Plan_Id = item["Plan_Id"]
            sql_obj.Plan_Name = item["Plan_Name"]
            sql_obj.HoldingSecurity_Id = item["HoldingSecurity_Id"]
            sql_obj.Portfolio_Date = pd.to_datetime(item["Portfolio_Date"])
            sql_obj.P_Portfolio_Date = pd.to_datetime(item["Portfolio_Date_P"])
            sql_obj.Percentage_to_AUM = item["Current_Weight"]
            sql_obj.P_Percentage_to_AUM = item["Old_Weight"]
            sql_obj.Diff_Percentage_to_AUM = item["Difference_Weight"]
            sql_obj.Value_In_Inr = item["Value_in_INR"]
            sql_obj.P_Value_In_Inr = item["Value_in_INR_P"]
            sql_obj.IncreaseExposure = item["is_increased"]
            sql_obj.DecreaseExposure = item["is_decreased"]
            sql_obj.NewStockForFund = item["is_entry"]
            sql_obj.ExitStockForFund = item["is_exit"]
            sql_obj.HoldingSecurity_Name = item["Security_Name"]
            sql_obj.InstrumentType = item["Instrument_Type"]
            sql_obj.Equity_Style = item["Equity_Style"] if item["Equity_Style"] and item["Equity_Style"] != 0 else ''
            sql_obj.ISIN_Code = item["ISIN_Code"] if item["ISIN_Code"] and item["ISIN_Code"] != 0 else ''
            sql_obj.Issuer_Id = item["Issuer_Id"]
            sql_obj.IssuerName = item["Issuer_Name"]
            sql_obj.Sector_Id = item["Sector_Id"]
            sql_obj.Sector_Code = item["Sector_Code"]
            sql_obj.Sector_Names = item["Sector_Name"]
            sql_obj.Asset_Class = item["Asset_Class"]
            sql_obj.Risk_Category = item["Risk_Category"]
            sql_obj.MarketCap = item["MarketCap"] if item["MarketCap"] and item["MarketCap"] != 0 else ''
            sql_obj.Purchase_Date = pd.to_datetime(item["Purchase_Date"])
            logging.warning(F"prepare_fund_stocks_table inside update_fund_stocks for fund_id-{fund_id} prepare insert before add")
            db_session.add(sql_obj)
            logging.warning(F"prepare_fund_stocks_table inside update_fund_stocks for fund_id-{fund_id} prepare insert before commit")
            db_session.commit()

def prepare_fund_stocks_table(db_session):
    logging.warning("prepare_fund_stocks_table started")
    # TODO : Monitor the impact of below changes, 
    # if not impactful then we need to take below session refresh off
    # References : 
    # https://stackoverflow.com/questions/19143345/about-refreshing-objects-in-sqlalchemy-session
    # http://docs.sqlalchemy.org/en/rel_0_8/orm/session.html#refreshing-expiring

    today = dt.today()
    today = today.replace(day=1)
    last_month = today - timedelta(days=1)
    last_6month = last_month - relativedelta(months=6)

    db_session.expire_all()
    logging.warning("prepare_fund_stocks_table before truncate_table")
    truncate_table('PMS_Base.Transactions.FundStocks')
    logging.warning("prepare_fund_stocks_table after truncate_table")
    users_cte = select(UnderlyingHoldings.Fund_Id.label("fund_id"), 
                       func.max(UnderlyingHoldings.Portfolio_Date).label("portfolio_date"),
                       func.lag(UnderlyingHoldings.Portfolio_Date).over(partition_by=(UnderlyingHoldings.Fund_Id),
                                                                        order_by=UnderlyingHoldings.Portfolio_Date).label('prev_portfolio_date'),)\
                .join(Fund, and_(UnderlyingHoldings.Fund_Id==Fund.Fund_Id, Fund.AutoPopulate == 1, Fund.Is_Deleted != 1))\
                .join(MFSecurity, and_(MFSecurity.Fund_Id==Fund.Fund_Id, MFSecurity.Status_Id == 1, MFSecurity.Is_Deleted != 1))\
                .where(and_(UnderlyingHoldings.Is_Deleted != 1, UnderlyingHoldings.Percentage_to_AUM > 0, 
                UnderlyingHoldings.ISIN_Code != None, UnderlyingHoldings.Portfolio_Date != None, UnderlyingHoldings.Portfolio_Date >= last_6month))\
                .group_by(UnderlyingHoldings.Fund_Id, UnderlyingHoldings.Portfolio_Date)\
                .cte()
    logging.warning("prepare_fund_stocks_table after user_cte")
    stmt = select(users_cte.c.fund_id, func.max(users_cte.c.portfolio_date), func.max(users_cte.c.prev_portfolio_date))\
            .group_by(users_cte.c.fund_id).order_by(users_cte.c.fund_id)

    # res = db_session.query(stmt.subquery()).all()
    # print(stmt.compile(compile_kwargs={"literal_binds": True}))
    res = db_session.execute(stmt).fetchall()
    logging.warning("prepare_fund_stocks_table before for loop")

    for record in res:        
        logging.warning("prepare_fund_stocks_table inside for loop")
        fund_id = record[0]
        portfolio_date = record[1]
        prev_portfolio_date = record[2]
        logging.warning(F"prepare_fund_stocks_table loop for fund_id-{fund_id}")
        update_fund_stocks(db_session, fund_id, portfolio_date, prev_portfolio_date)

def set_fund_screener_rolling_returns(db_session, sql_obj : FundScreener, plan_id, benchmark_id, transaction_date):
    scheme_result = get_rolling_returns(db_session, plan_id, False, 1, transaction_date, True)
    sql_obj.scheme_rolling_min_1_yr = scheme_result["min_returns"] if scheme_result else None  
    sql_obj.scheme_rolling_max_1_yr = scheme_result["max_returns"] if scheme_result else None  
    sql_obj.scheme_rolling_avg_1_yr = scheme_result["average_returns"] if scheme_result else None  
    sql_obj.scheme_rolling_median_1_yr = scheme_result["median_returns"] if scheme_result else None  
    sql_obj.scheme_rolling_positive_1_yr = scheme_result["positive_observation_perc"] if scheme_result else None  
    sql_obj.scheme_rolling_neutral_1_yr = scheme_result["neutral_observation_perc"] if scheme_result else None  
    sql_obj.scheme_rolling_negative_1_yr = scheme_result["negative_observation_perc"] if scheme_result else None  

    index_result = get_rolling_returns(db_session, benchmark_id, True, 1, transaction_date, True)
    sql_obj.bm_rolling_min_1_yr = index_result["min_returns"] if index_result else None  
    sql_obj.bm_rolling_max_1_yr = index_result["max_returns"] if index_result else None  
    sql_obj.bm_rolling_avg_1_yr = index_result["average_returns"] if index_result else None  
    sql_obj.bm_rolling_median_1_yr = index_result["median_returns"] if index_result else None  
    sql_obj.bm_rolling_positive_1_yr = index_result["positive_observation_perc"] if index_result else None  
    sql_obj.bm_rolling_neutral_1_yr = index_result["neutral_observation_perc"] if index_result else None  
    sql_obj.bm_rolling_negative_1_yr = index_result["negative_observation_perc"] if index_result else None  

    scheme_result = get_rolling_returns(db_session, plan_id, False, 3, transaction_date, True)
    sql_obj.scheme_rolling_min_3_yr = scheme_result["min_returns"] if scheme_result else None  
    sql_obj.scheme_rolling_max_3_yr = scheme_result["max_returns"] if scheme_result else None  
    sql_obj.scheme_rolling_avg_3_yr = scheme_result["average_returns"] if scheme_result else None  
    sql_obj.scheme_rolling_median_3_yr = scheme_result["median_returns"] if scheme_result else None  
    sql_obj.scheme_rolling_positive_3_yr = scheme_result["positive_observation_perc"] if scheme_result else None  
    sql_obj.scheme_rolling_neutral_3_yr = scheme_result["neutral_observation_perc"] if scheme_result else None  
    sql_obj.scheme_rolling_negative_3_yr = scheme_result["negative_observation_perc"] if scheme_result else None  

    index_result = get_rolling_returns(db_session, benchmark_id, True, 3, transaction_date, True)
    sql_obj.bm_rolling_min_3_yr = index_result["min_returns"] if index_result else None  
    sql_obj.bm_rolling_max_3_yr = index_result["max_returns"] if index_result else None  
    sql_obj.bm_rolling_avg_3_yr = index_result["average_returns"] if index_result else None  
    sql_obj.bm_rolling_median_3_yr = index_result["median_returns"] if index_result else None  
    sql_obj.bm_rolling_positive_3_yr = index_result["positive_observation_perc"] if index_result else None  
    sql_obj.bm_rolling_neutral_3_yr = index_result["neutral_observation_perc"] if index_result else None  
    sql_obj.bm_rolling_negative_3_yr = index_result["negative_observation_perc"] if index_result else None  
    
    scheme_result = get_rolling_returns(db_session, plan_id, False, 5, transaction_date, True)
    sql_obj.scheme_rolling_min_5_yr = scheme_result["min_returns"] if scheme_result else None  
    sql_obj.scheme_rolling_max_5_yr = scheme_result["max_returns"] if scheme_result else None  
    sql_obj.scheme_rolling_avg_5_yr = scheme_result["average_returns"] if scheme_result else None  
    sql_obj.scheme_rolling_median_5_yr = scheme_result["median_returns"] if scheme_result else None  
    sql_obj.scheme_rolling_positive_5_yr = scheme_result["positive_observation_perc"] if scheme_result else None  
    sql_obj.scheme_rolling_neutral_5_yr = scheme_result["neutral_observation_perc"] if scheme_result else None  
    sql_obj.scheme_rolling_negative_5_yr = scheme_result["negative_observation_perc"] if scheme_result else None  

    index_result = get_rolling_returns(db_session, benchmark_id, True, 5, transaction_date, True)
    sql_obj.bm_rolling_min_5_yr = index_result["min_returns"] if index_result else None  
    sql_obj.bm_rolling_max_5_yr = index_result["max_returns"] if index_result else None  
    sql_obj.bm_rolling_avg_5_yr = index_result["average_returns"] if index_result else None  
    sql_obj.bm_rolling_median_5_yr = index_result["median_returns"] if index_result else None  
    sql_obj.bm_rolling_positive_5_yr = index_result["positive_observation_perc"] if index_result else None  
    sql_obj.bm_rolling_neutral_5_yr = index_result["neutral_observation_perc"] if index_result else None  
    sql_obj.bm_rolling_negative_5_yr = index_result["negative_observation_perc"] if index_result else None  


def update_fund_screener(db_session, plan_id, transaction_date):
    sql_obj = db_session.query(FactSheet.TransactionDate.label('transaction_date'),
                               Product.Product_Name.label('product'),
                               Fund.Fund_Id.label('fund_id'),
                               FactSheet.Plan_Id.label('plan_id'),
                               Plans.Plan_Name.label('plan_name'),
                               Fund.Fund_Name.label('fund'),
                               AMC.AMC_Name.label('amc'),
                               Classification.Classification_Name.label('classification_name'),
                               Plans.PlanType_Id.label('plan_type'),
                               Plans.Option_Id.label('option_id'),
                               AssetClass.AssetClass_Name.label('asset_class'),
                               FactSheet.ExpenseRatio.label('expense_ratio'),
                               FactSheet.TotalStocks.label('total_stocks'),
                               FactSheet.NetAssets_Rs_Cr.label('aum'),
                               FactSheet.Equity.label('equity'), FactSheet.Debt.label('debt'), FactSheet.Cash.label('cash'),
                               FactSheet.AvgMktCap_Rs_Cr.label('avg_market_cap_in_cr'),
                               FactSheet.PortfolioP_BRatio.label('pb_ratio'),
                               FactSheet.PortfolioP_ERatio.label('pe_ratio'),
                               FactSheet.AvgMaturity_Yrs.label('avg_maturity_years'),
                               FactSheet.ModifiedDuration_yrs.label('modified_duration_years'),
                               FactSheet.Portfolio_Dividend_Yield.label('portfolio_dividend_yield'),
                               FactSheet.Churning_Ratio.label('churning_ratio'),
                               FactSheet.SCHEME_RETURNS_1MONTH.label('returns_1_month'),
                               FactSheet.SCHEME_RETURNS_3MONTH.label('returns_3_months'),
                               FactSheet.SCHEME_RETURNS_6MONTH.label('returns_6_months'),
                               FactSheet.SCHEME_RETURNS_1YEAR.label('returns_1_yr'),
                               FactSheet.SCHEME_RETURNS_2YEAR.label('returns_2_yr'), 
                               FactSheet.SCHEME_RETURNS_3YEAR.label('returns_3_yr'), 
                               FactSheet.SCHEME_RETURNS_5YEAR.label('returns_5_yr'), 
                               FactSheet.SCHEME_RETURNS_10YEAR.label('returns_10_yr'), 
                               FactSheet.SCHEME_RETURNS_since_inception.label('returns_since_inception'),
                               FactSheet.StandardDeviation_1Yr.label('std_1_yr'), 
                               FactSheet.SharpeRatio_1Yr.label('sharpe_ratio_1_yr'), 
                               FactSheet.Beta_1Yr.label('beta_1_yr'), 
                               FactSheet.R_Squared_1Yr.label('r_squared_1_yr'), 
                               FactSheet.Alpha_1Yr.label('alpha_1_yr'), 
                               FactSheet.Mean_1Yr.label('mean_1_yr'), 
                               FactSheet.Sortino_1Yr.label('sortino_1_yr'),
                               FactSheet.StandardDeviation.label('std_3_yr'), 
                               FactSheet.SharpeRatio.label('sharpe_ratio_3_yr'), 
                               FactSheet.Beta.label('beta_3_yr'), 
                               FactSheet.R_Squared.label('r_squared_3_yr'), 
                               FactSheet.Alpha.label('alpha_3_yr'), 
                               FactSheet.Mean.label('mean_3_yr'), 
                               FactSheet.Sortino.label('sortino_3_yr'),
                               MFSecurity.MF_Security_OpenDate.label('inception_date'),
                               MFSecurity.BenchmarkIndices_Id.label('benchmark_id'),
                               BenchmarkIndices.BenchmarkIndices_Name.label('benchmarkindices_name')
                              )\
                        .select_from(FactSheet)\
                        .join(Plans, and_(Plans.Plan_Id == FactSheet.Plan_Id, Plans.Is_Deleted != 1))\
                        .join(PlanProductMapping, and_(Plans.Plan_Id == PlanProductMapping.Plan_Id, PlanProductMapping.Is_Deleted != 1))\
                        .join(Product, and_(Product.Product_Id == PlanProductMapping.Product_Id, Product.Product_Code == FactSheet.SourceFlag))\
                        .join(MFSecurity, and_(Plans.MF_Security_Id == MFSecurity.MF_Security_Id, MFSecurity.Status_Id == 1, MFSecurity.Is_Deleted != 1))\
                        .join(BenchmarkIndices, BenchmarkIndices.BenchmarkIndices_Id == MFSecurity.BenchmarkIndices_Id)\
                        .join(AssetClass, and_(AssetClass.AssetClass_Id == MFSecurity.AssetClass_Id))\
                        .join(Classification, and_(Classification.Classification_Id == MFSecurity.Classification_Id))\
                        .join(Fund, and_(Fund.Fund_Id == MFSecurity.Fund_Id, Fund.Is_Deleted != 1))\
                        .join(AMC, and_(AMC.AMC_Id == MFSecurity.AMC_Id, AMC.Is_Deleted != 1))\
                        .filter(and_(FactSheet.Plan_Id == plan_id, FactSheet.TransactionDate == transaction_date, FactSheet.Is_Deleted != 1, PlanProductMapping.Is_Deleted != 1))

    sql_obj = sql_obj.one_or_none()

    # TODO : Sadly have to add this if condition to skip if sql_obj is NONE, 
    # our logic needs to be processing best effort basis !!! URGENT !!!
    # and reporting the funds for which the screener was not generated via email to development team, 
    # to take appropriate action further
    today = dt.today()
    year_ago = today - relativedelta(months=12)
    three_year_ago = today - relativedelta(months=36)

    if sql_obj:
        large_cap = None
        mid_cap = None
        small_cap = None

        fund_age_in_months = calculate_age(sql_obj.inception_date, sql_obj.transaction_date, in_months=True)

        mcap_comp_resp = get_marketcap_composition(db_session, plan_id, transaction_date, composition_for='fund_level')
        if mcap_comp_resp:
            large_cap = mcap_comp_resp[0]['large_cap'] if mcap_comp_resp[0]['large_cap'] != 'NA' else None
            mid_cap = mcap_comp_resp[0]['mid_cap'] if mcap_comp_resp[0]['mid_cap'] != 'NA' else None
            small_cap = mcap_comp_resp[0]['small_cap'] if mcap_comp_resp[0]['small_cap'] != 'NA' else None

        sql_fund_screener = FundScreener()
        sql_fund_screener.transaction_date = sql_obj.transaction_date
        sql_fund_screener.product = sql_obj.product
        sql_fund_screener.fund_id = sql_obj.fund_id
        sql_fund_screener.plan_id = sql_obj.plan_id
        sql_fund_screener.plan_name = sql_obj.plan_name
        sql_fund_screener.fund = sql_obj.fund
        sql_fund_screener.amc = sql_obj.amc
        sql_fund_screener.classification_name = sql_obj.classification_name
        sql_fund_screener.plan_type = sql_obj.plan_type
        sql_fund_screener.option_id = sql_obj.option_id
        sql_fund_screener.asset_class = sql_obj.asset_class
        sql_fund_screener.expense_ratio = sql_obj.expense_ratio
        sql_fund_screener.total_stocks = sql_obj.total_stocks
        sql_fund_screener.aum = sql_obj.aum
        sql_fund_screener.equity = sql_obj.equity
        sql_fund_screener.debt = sql_obj.debt
        sql_fund_screener.cash = sql_obj.cash
        sql_fund_screener.avg_market_cap_in_cr = sql_obj.avg_market_cap_in_cr
        sql_fund_screener.pb_ratio = sql_obj.pb_ratio
        sql_fund_screener.pe_ratio = sql_obj.pe_ratio
        sql_fund_screener.avg_maturity_years = sql_obj.avg_maturity_years
        sql_fund_screener.modified_duration_years = sql_obj.modified_duration_years
        sql_fund_screener.portfolio_dividend_yield = sql_obj.portfolio_dividend_yield
        sql_fund_screener.churning_ratio = sql_obj.churning_ratio
        sql_fund_screener.returns_1_month = sql_obj.returns_1_month
        sql_fund_screener.returns_3_months = sql_obj.returns_3_months
        sql_fund_screener.returns_6_months = sql_obj.returns_6_months
        sql_fund_screener.returns_1_yr = sql_obj.returns_1_yr
        sql_fund_screener.returns_2_yr = sql_obj.returns_2_yr
        sql_fund_screener.returns_3_yr = sql_obj.returns_3_yr
        sql_fund_screener.returns_5_yr = sql_obj.returns_5_yr
        sql_fund_screener.returns_10_yr = sql_obj.returns_10_yr
        sql_fund_screener.returns_since_inception = sql_obj.returns_since_inception
        # TODO CMOTS shares the information for 1 yr and 3 yr ratios even if the fund's age is not equivalent to the period for which the ratio is calculated
        # We need to migrate to our calculations for ratios and therefore we need to remove the below logic as well once the migration is completed
        sql_fund_screener.std_1_yr = sql_obj.std_1_yr if sql_obj.inception_date and sql_obj.inception_date < year_ago else None
        sql_fund_screener.sharpe_ratio_1_yr = sql_obj.sharpe_ratio_1_yr if sql_obj.inception_date and sql_obj.inception_date < year_ago else None
        sql_fund_screener.beta_1_yr = sql_obj.beta_1_yr if sql_obj.inception_date and sql_obj.inception_date < year_ago else None
        sql_fund_screener.r_squared_1_yr = sql_obj.r_squared_1_yr if sql_obj.inception_date and sql_obj.inception_date < year_ago else None
        sql_fund_screener.alpha_1_yr = sql_obj.alpha_1_yr if sql_obj.inception_date and sql_obj.inception_date < year_ago else None
        sql_fund_screener.mean_1_yr = sql_obj.mean_1_yr if sql_obj.inception_date and sql_obj.inception_date < year_ago else None
        sql_fund_screener.sortino_1_yr = sql_obj.sortino_1_yr if sql_obj.inception_date and sql_obj.inception_date < year_ago else None
        sql_fund_screener.std_3_yr = sql_obj.std_3_yr if sql_obj.inception_date and sql_obj.inception_date < three_year_ago else None
        sql_fund_screener.sharpe_ratio_3_yr = sql_obj.sharpe_ratio_3_yr if sql_obj.inception_date and sql_obj.inception_date < three_year_ago else None
        sql_fund_screener.beta_3_yr = sql_obj.beta_3_yr if sql_obj.inception_date and sql_obj.inception_date < three_year_ago else None
        sql_fund_screener.r_squared_3_yr = sql_obj.r_squared_3_yr if sql_obj.inception_date and sql_obj.inception_date < three_year_ago else None
        sql_fund_screener.alpha_3_yr = sql_obj.alpha_3_yr if sql_obj.inception_date and sql_obj.inception_date < three_year_ago else None
        sql_fund_screener.mean_3_yr = sql_obj.mean_3_yr if sql_obj.inception_date and sql_obj.inception_date < three_year_ago else None
        sql_fund_screener.sortino_3_yr = sql_obj.sortino_3_yr if sql_obj.inception_date and sql_obj.inception_date < three_year_ago else None
        sql_fund_screener.inception_date = sql_obj.inception_date
        sql_fund_screener.benchmark_name = sql_obj.benchmarkindices_name
        sql_fund_screener.large_cap = large_cap
        sql_fund_screener.mid_cap = mid_cap
        sql_fund_screener.small_cap = small_cap
        sql_fund_screener.fund_age_in_months = fund_age_in_months

        benchmark_id = sql_obj.benchmark_id
        set_fund_screener_rolling_returns(db_session, sql_fund_screener, plan_id, benchmark_id, sql_fund_screener.transaction_date)

        db_session.add(sql_fund_screener)
        db_session.commit()
    
def prepare_fund_screener(db_session):

    # TODO : Monitor the impact of below changes, 
    # if not impactful then we need to take below session refresh off
    # References : 
    # https://stackoverflow.com/questions/19143345/about-refreshing-objects-in-sqlalchemy-session
    # http://docs.sqlalchemy.org/en/rel_0_8/orm/session.html#refreshing-expiring
    db_session.expire_all()

    truncate_table('PMS_Base.Transactions.FundScreener')

    #get max factsheet date - PMS
    max_pms_factsheet_date = db_session.query(func.max(FactSheet.TransactionDate).label('TransactionDate'))\
                      .join(Plans, and_(Plans.Plan_Id == FactSheet.Plan_Id, Plans.PlanType_Id == 1, Plans.Is_Deleted != 1))\
                      .join(PlanProductMapping, and_(Plans.Plan_Id == PlanProductMapping.Plan_Id, PlanProductMapping.Is_Deleted != 1, PlanProductMapping.Product_Id == 4))\
                      .join(MFSecurity, and_(Plans.MF_Security_Id == MFSecurity.MF_Security_Id, MFSecurity.Status_Id == 1, MFSecurity.Is_Deleted != 1))\
                      .join(Fund, and_(Fund.Fund_Id == MFSecurity.Fund_Id, Fund.Is_Deleted != 1))\
                      .join(Options, and_(Options.Option_Id == Plans.Option_Id, Options.Option_Name.like('%G%')))\
                      .filter(FactSheet.TransactionDate.isnot(None))\
                      .filter(FactSheet.Is_Deleted != 1).scalar()
    
    # TODO : Re-confirm on how to proceed with the below added filter, also implement a best effort basis processing of the following logic
    # The "TransactionDate != None" filter is added for all the plans which donot 
    # recieve any TransactionDate for factsheets in any of the month from CMOTs
    # Mostly debt FMP plans, but still few Equity funds are impacted.
    plans = db_session.query(FactSheet.Plan_Id, Fund.Fund_Id, func.max(FactSheet.TransactionDate).label('TransactionDate'))\
                      .join(Plans, and_(Plans.Plan_Id == FactSheet.Plan_Id, Plans.PlanType_Id == 1, Plans.Is_Deleted != 1))\
                      .join(MFSecurity, and_(Plans.MF_Security_Id == MFSecurity.MF_Security_Id, MFSecurity.Status_Id == 1, MFSecurity.Is_Deleted != 1))\
                      .join(Fund, and_(Fund.Fund_Id == MFSecurity.Fund_Id, Fund.Is_Deleted != 1))\
                      .join(Options, and_(Options.Option_Id == Plans.Option_Id, Options.Option_Name.like('%G%')))\
                      .filter(FactSheet.TransactionDate.isnot(None))\
                      .filter(FactSheet.Is_Deleted != 1)\
                      .filter(FactSheet.TransactionDate <= max_pms_factsheet_date)\
                      .group_by(FactSheet.Plan_Id, Fund.Fund_Id).all()

    for obj in plans:
        plan_id = obj.Plan_Id
        fund_id = obj.Fund_Id
        transaction_date = obj.TransactionDate
        try:
            update_fund_screener(db_session, plan_id, transaction_date)
        except Exception as ex:            
            db_session.rollback()
            continue
        

def get_equity_analysis_overview(db_session, product_id, classification_id, sector_id, market_cap):
    fundstock_query = db_session.query(
        HoldingSecurity.ISIN_Code.label('isin_code'), 
        FundStocks.HoldingSecurity_Name.label('holdingsecurity_name'), 
        FundStocks.HoldingSecurity_Id.label('holdingsecurity_id'), 
        FundStocks.MarketCap.label('marketcap'),  
        FundStocks.Equity_Style.label('investmentstyle'),
        # FundStocks.Sector_Code.label('Sector_Code'),
        # FundStocks.Sector_Code.label('Sector_Names'),
        # FundStocks.Classification_Name.label('Classification_Name'),

        (func.sum(case((FundStocks.Product_Id == 1, 1), else_=0)) - func.sum(case((and_(FundStocks.Product_Id == 1, FundStocks.ExitStockForFund == 1), 1), else_=0))).label('mf'),
        (func.sum(case((FundStocks.Product_Id == 2, 1), else_=0))- func.sum(case((and_(FundStocks.Product_Id == 2, FundStocks.ExitStockForFund == 1), 1), else_=0))).label('ulip'), 
        (func.sum(case((FundStocks.Product_Id == 4, 1), else_=0))- func.sum(case((and_(FundStocks.Product_Id == 4, FundStocks.ExitStockForFund == 1), 1), else_=0))).label('pms'), 
        (func.sum(case((FundStocks.Product_Id == 5, 1), else_=0))- func.sum(case((and_(FundStocks.Product_Id == 5, FundStocks.ExitStockForFund == 1), 1), else_=0))).label('aif'), 
        (func.count(FundStocks.Product_Id) - func.sum(case((FundStocks.ExitStockForFund == 1, 1), else_=0))).label('total') ,

        func.sum(case((and_(FundStocks.Product_Id == 1, FundStocks.IncreaseExposure == 1), 1), else_=0)).label('increaseexposure_mf'), 
        func.sum(case((and_(FundStocks.Product_Id == 2, FundStocks.IncreaseExposure == 1), 1), else_=0)).label('increaseexposure_ulip'), 
        func.sum(case((and_(FundStocks.Product_Id == 4, FundStocks.IncreaseExposure == 1), 1), else_=0)).label('increaseexposure_pms'), 
        func.sum(case((and_(FundStocks.Product_Id == 5, FundStocks.IncreaseExposure == 1), 1), else_=0)).label('increaseexposure_aif'), 
        func.sum(case((FundStocks.IncreaseExposure == 1, 1), else_=0)).label('increaseexposure_total'),

        func.sum(case((and_(FundStocks.Product_Id == 1, FundStocks.DecreaseExposure == 1), 1), else_=0)).label('decreaseexposure_mf'),
        func.sum(case((and_(FundStocks.Product_Id == 2, FundStocks.DecreaseExposure == 1), 1), else_=0)).label('decreaseexposure_ulip'), 
        func.sum(case((and_(FundStocks.Product_Id == 4, FundStocks.DecreaseExposure == 1), 1), else_=0)).label('decreaseexposure_pms'), 
        func.sum(case((and_(FundStocks.Product_Id == 5, FundStocks.DecreaseExposure == 1), 1), else_=0)).label('decreaseexposure_aif'), 
        func.sum(case((FundStocks.DecreaseExposure == 1, 1), else_=0)).label('decreaseexposure_total'),

        func.sum(case((and_(FundStocks.Product_Id == 1, FundStocks.NewStockForFund == 1), 1), else_=0)).label('newstockforfund_mf'), 
        func.sum(case((and_(FundStocks.Product_Id == 2, FundStocks.NewStockForFund == 1), 1), else_=0)).label('newstockforfund_ulip'), 
        func.sum(case((and_(FundStocks.Product_Id == 4, FundStocks.NewStockForFund == 1), 1), else_=0)).label('newstockforfund_pms'), 
        func.sum(case((and_(FundStocks.Product_Id == 5, FundStocks.NewStockForFund == 1), 1), else_=0)).label('newstockforfund_aif'), 
        func.sum(case((FundStocks.NewStockForFund == 1, 1), else_=0)).label('newstockforfund_total'),

        func.sum(case((and_(FundStocks.Product_Id == 1, FundStocks.ExitStockForFund == 1), 1), else_=0)).label('exitstockforfund_mf'), 
        func.sum(case((and_(FundStocks.Product_Id == 2, FundStocks.ExitStockForFund == 1), 1), else_=0)).label('exitstockforfund_ulip'), 
        func.sum(case((and_(FundStocks.Product_Id == 4, FundStocks.ExitStockForFund == 1), 1), else_=0)).label('exitstockforfund_pms'), 
        func.sum(case((and_(FundStocks.Product_Id == 5, FundStocks.ExitStockForFund == 1), 1), else_=0)).label('exitstockforfund_aif'), 
        func.sum(case((FundStocks.ExitStockForFund == 1, 1), else_=0)).label('exitstockforfund_total')
    ).join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == FundStocks.HoldingSecurity_Id)\
    .group_by(
        HoldingSecurity.ISIN_Code, 
        FundStocks.HoldingSecurity_Name, 
        FundStocks.HoldingSecurity_Id,
        FundStocks.MarketCap,
        FundStocks.Equity_Style
    )\
    .filter(HoldingSecurity.ISIN_Code.like("INE%")).filter(FundStocks.InstrumentType == 'Equity')
    if product_id:
        fundstock_query = fundstock_query.filter(FundStocks.Product_Id == product_id)
    if classification_id:
        fundstock_query = fundstock_query.filter(FundStocks.Classification_Id == classification_id)
    if sector_id:
        fundstock_query = fundstock_query.filter(HoldingSecurity.Sector_Id == sector_id)
    if market_cap:
        fundstock_query = fundstock_query.filter(FundStocks.MarketCap == market_cap)
    fundstock_dt = fundstock_query.order_by(desc('total'))
    # print_query(fundstock_dt)
    equities = fundstock_dt.all()
    
    return equities

def get_equity_exposure(db_session, isin_code, product_id):
    sql_favoritestockfunds = db_session.query(
        FundStocks.Fund_Id.label("fund_id"), FundStocks.Plan_Id.label("plan_id"), FundStocks.Plan_Name.label("plan_name"), FundStocks.HoldingSecurity_Name.label("security_name"),
        FundStocks.Product_Name.label("product_name"), FundStocks.Product_Id.label("product_id"), FundStocks.Product_Code.label("product_code"), 
        FundStocks.Classification_Name.label("classification_name"), 
        FundStocks.Classification_Id.label("classification_id"), 
        FundStocks.Percentage_to_AUM.label("percentage_to_aum"), FundStocks.Diff_Percentage_to_AUM.label("diff_percentage_to_aum"), 
        FundStocks.Purchase_Date.label("purchase_date"), 
        FundStocks.IncreaseExposure.label("increaseexposure"), FundStocks.DecreaseExposure.label("decreaseexposure"), FundStocks.NewStockForFund.label("new_stocks_for_fund")
    ).filter(FundStocks.ISIN_Code == isin_code)

    if product_id:
        sql_favoritestockfunds = sql_favoritestockfunds.filter(FundStocks.Product_Id == product_id)

    sql_favoritestockfunds = sql_favoritestockfunds.order_by(desc(FundStocks.Percentage_to_AUM)).all()
    
    sql_favoritestockfunds[0]._mapping

    favoritestockfunds_list = list()
    for favoritestockfunds in sql_favoritestockfunds:
        data = favoritestockfunds._asdict()
        favoritestockfunds_list.append(data)
    
    return favoritestockfunds_list
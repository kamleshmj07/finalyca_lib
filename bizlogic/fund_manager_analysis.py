from fin_models.masters_models import *
from fin_models.transaction_models import *
from sqlalchemy import func, or_, desc
from utils.utils import print_query
import pandas as pd
from bizlogic.importer_helper import get_fund_change
from datetime import datetime as dt

def generate_fundmanager_details(db_session):
    
    sql_factsheet_subquery = db_session.query(PlanProductMapping.Product_Id, MFSecurity.AMC_Id, FundManager.Fund_Id, FundManager.FundManager_Code, func.max(FactSheet.FactSheet_Id).label('max_factsheet_id'))\
    .select_from(FundManager)\
    .join(Fund, FundManager.Fund_Id == Fund.Fund_Id)\
    .join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id)\
    .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
    .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
    .join(FactSheet, FactSheet.Plan_Id == Plans.Plan_Id)\
    .filter(FundManager.Is_Deleted != 1).filter(FactSheet.Is_Deleted != 1).filter(Plans.PlanType_Id == 1).filter(MFSecurity.Status_Id == 1).filter(FundManager.DateTo == None)\
    .group_by(PlanProductMapping.Product_Id, MFSecurity.AMC_Id, FundManager.Fund_Id, FundManager.FundManager_Code).distinct().subquery()

    sql_factsheet = db_session.query(FactSheet.NetAssets_Rs_Cr.label('aum'),  sql_factsheet_subquery.c.Product_Id, sql_factsheet_subquery.c.AMC_Id, sql_factsheet_subquery.c.Fund_Id, sql_factsheet_subquery.c.FundManager_Code)\
    .select_from(FactSheet)\
    .join(sql_factsheet_subquery, sql_factsheet_subquery.c.max_factsheet_id == FactSheet.FactSheet_Id)\
    .filter(FactSheet.Is_Deleted != 1).all()

    if sql_factsheet:
        df = pd.DataFrame(sql_factsheet)
        #get unique fundmanager_code
        unique_fm_code = df.FundManager_Code.unique()
        
        for fm_code in unique_fm_code:
            fm_code_df = df.loc[df['FundManager_Code'] == fm_code]
            data = fm_code_df.iloc[0]
            
            amc_id = int(data['AMC_Id'])
            product_id = int(data['Product_Id'])
            num_of_fund = len(fm_code_df.index)
            sum_of_aum = fm_code_df.sum(axis=0, skipna=True)['aum']
            
            # update             
            update_values = {
                    FundManager.AMC_Id : amc_id,
                    FundManager.Funds_Managed : num_of_fund,
                    FundManager.AUM : sum_of_aum,
                    FundManager.Product_Id : product_id
                }
                
            db_session.query(FundManager).filter(FundManager.FundManager_Code == fm_code).filter(FundManager.Is_Deleted != 1).update(update_values)
            db_session.commit()


def fund_manager_fundactivity_by_exposure(db_session, exposure_type, fundmanager_code):
    today = dt.today()

    sql_fund = db_session.query(Plans.Plan_Id,
                                Plans.Plan_Name,
                                Fund.Fund_Id,
                                Fund.Fund_Name,
                                FundManager.Funds_Managed,
                                FundManager.AUM)\
                            .select_from(FundManager)\
                            .join(Fund, Fund.Fund_Id == FundManager.Fund_Id)\
                            .join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id)\
                            .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                            .filter(Plans.Is_Deleted != 1, MFSecurity.Is_Deleted != 1, MFSecurity.Status_Id == 1, Plans.PlanType_Id == 1)\
                            .filter(or_(FundManager.DateTo >= today, FundManager.DateTo == None))\
                            .filter(FundManager.FundManager_Code == fundmanager_code).filter(FundManager.Is_Deleted != 1).all()

    unique_fund_id = list()
    fund_changes_list = list()
    response_data = list()

    if sql_fund:
        for fund in sql_fund:
            if not fund.Fund_Id in unique_fund_id:
                hide_portfolio_changes = None
                portfolio_date = None
                aum = None

                sql_factsheet_count =  db_session.query(func.count(FactSheet.Plan_Id))\
                                                .filter(FactSheet.Plan_Id == fund.Plan_Id, FactSheet.Is_Deleted != 1).scalar()

                if sql_factsheet_count < 2:
                    hide_portfolio_changes = True

                if hide_portfolio_changes:
                    pass

                sql_funddata = db_session.query(Fund.HideHoldingWeightage,
                                                Fund.HidePortfolioHoldingChanges,
                                                Fund.Fund_Id)\
                                            .select_from(Fund)\
                                            .join(MFSecurity, Fund.Fund_Id == MFSecurity.Fund_Id)\
                                            .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                            .filter(MFSecurity.Status_Id == 1, Plans.Plan_Id == fund.Plan_Id, Plans.Is_Deleted != 1, MFSecurity.Is_Deleted != 1).all()

                hide_holding_weightage = sql_funddata[0].HideHoldingWeightage
                hide_portfolio_changes = sql_funddata[0].HidePortfolioHoldingChanges
                scheme_id = sql_funddata[0].Fund_Id

                #get last transactiondate and Portfolio_Date
                factsheet_query = db_session.query(FactSheet)\
                                            .filter(FactSheet.Plan_Id == fund.Plan_Id, FactSheet.Is_Deleted != 1)\
                                            .order_by(desc(FactSheet.TransactionDate)).first()

                if factsheet_query:
                    portfolio_date = factsheet_query.Portfolio_Date
                    aum = factsheet_query.NetAssets_Rs_Cr

                fund_changes = get_fund_change(db_session, fund.Plan_Id, portfolio_date, exposure_type, scheme_id, hide_holding_weightage)
                
                for fund_change in fund_changes:
                    data = dict()
                    change_percent = fund_change["weight_difference"]
                    if exposure_type == "New_Entrants" or exposure_type == "Complete_Exit" :
                        change_percent = fund_change["security_new_weight"]

                    change_in_inr = (change_percent * aum / 100) * 10000000

                    data["change_in_inr"] = change_in_inr
                    data["isin_code"] = fund_change["security_isin"]
                    data["plan_name"] = fund.Plan_Name
                    data["plan_id"] = fund.Plan_Id
                    data["fund_name"] = fund.Fund_Name
                    data["security_new_weight"] = fund_change["security_new_weight"]
                    data["weight_difference"] = fund_change["weight_difference"]
                    data["security_name"] = fund_change["security_name"]

                    fund_changes_list.append(data)
                
                unique_fund_id.append(fund.Fund_Id)

        if exposure_type == "Decrease_Exposure":
            fund_changes_list = sorted(fund_changes_list, key=lambda d: d['change_in_inr']) 
        else:
            fund_changes_list = sorted(fund_changes_list, key=lambda d: d['change_in_inr'], reverse=True) 

        top_unique_isincode = list()
        
        for isindata in fund_changes_list:
            if len(top_unique_isincode) < 5:
                if isindata["isin_code"] not in top_unique_isincode:                
                    top_unique_isincode.append(isindata["isin_code"])                
            else:
                break
        
        for isincode in top_unique_isincode:
            isindata_list_dict = dict()
            isindata_list = list()
            security_name = None
            no_of_funds = 0

            for fund_changes in fund_changes_list:
                if isincode == fund_changes["isin_code"]:
                    security_name = fund_changes["security_name"]
                    no_of_funds = no_of_funds + 1

                    data = dict()
                    data["security_new_weight"] = fund_changes["security_new_weight"]
                    data["security_name"] = fund_changes["security_name"]
                    data["weight_difference"] = fund_changes["weight_difference"]
                    data["isin_code"] = fund_changes["isin_code"]
                    data["fund_name"] = fund_changes["fund_name"]
                    data["plan_name"] = fund_changes["plan_name"]
                    data["plan_id"] = fund_changes["plan_id"]

                    isindata_list.append(data)
            
            isindata_list_dict["security_name"] = security_name
            isindata_list_dict["no_of_funds"] = no_of_funds
            isindata_list_dict["data"] = isindata_list
            response_data.append(isindata_list_dict)
    
    return response_data



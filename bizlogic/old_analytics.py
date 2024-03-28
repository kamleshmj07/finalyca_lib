from fin_models.transaction_models import FundStocks
from fin_models.masters_models import HoldingSecurity, FundManager
from sqlalchemy import func, case, and_, desc, or_
from datetime import datetime, date

from bizlogic.importer_helper import get_aumandfundcountbyproduct

def get_equity_analysis_overview_old(db_session, product_id, classification_id, sector_id, market_cap):
    resp = list()

    fundstock_query = db_session.query(func.sum(case((FundStocks.Product_Id == 1, 1), else_=0)).label('MF'),func.sum(case((FundStocks.Product_Id == 2, 1), else_=0)).label('ULIP'), func.sum(case((FundStocks.Product_Id == 4, 1), else_=0)).label('PMS'), func.sum(case((FundStocks.Product_Id == 5, 1), else_=0)).label('AIF'), func.count(FundStocks.Product_Id).label('Total') ,

    func.sum(case((and_(FundStocks.Product_Id == 1, FundStocks.IncreaseExposure == 1), 1), else_=0)).label('IncreaseExposure_MF'), func.sum(case((and_(FundStocks.Product_Id == 2, FundStocks.IncreaseExposure == 1), 1), else_=0)).label('IncreaseExposure_ULIP'), func.sum(case((and_(FundStocks.Product_Id == 4, FundStocks.IncreaseExposure == 1), 1), else_=0)).label('IncreaseExposure_PMS'), func.sum(case((and_(FundStocks.Product_Id == 5, FundStocks.IncreaseExposure == 1), 1), else_=0)).label('IncreaseExposure_AIF'), func.sum(case((FundStocks.IncreaseExposure == 1, 1), else_=0)).label('IncreaseExposure_Total'),

    func.sum(case((and_(FundStocks.Product_Id == 1, FundStocks.DecreaseExposure == 1), 1), else_=0)).label('DecreaseExposure_MF'), func.sum(case((and_(FundStocks.Product_Id == 2, FundStocks.DecreaseExposure == 1), 1), else_=0)).label('DecreaseExposure_ULIP'), func.sum(case((and_(FundStocks.Product_Id == 4, FundStocks.DecreaseExposure == 1), 1), else_=0)).label('DecreaseExposure_PMS'), func.sum(case((and_(FundStocks.Product_Id == 5, FundStocks.DecreaseExposure == 1), 1), else_=0)).label('DecreaseExposure_AIF'), func.sum(case((FundStocks.DecreaseExposure == 1, 1), else_=0)).label('DecreaseExposure_Total'),

    func.sum(case((and_(FundStocks.Product_Id == 1, FundStocks.NewStockForFund == 1), 1), else_=0)).label('NewStockForFund_MF'), func.sum(case((and_(FundStocks.Product_Id == 2, FundStocks.NewStockForFund == 1), 1), else_=0)).label('NewStockForFund_ULIP'), func.sum(case((and_(FundStocks.Product_Id == 4, FundStocks.NewStockForFund == 1), 1), else_=0)).label('NewStockForFund_PMS'), func.sum(case((and_(FundStocks.Product_Id == 5, FundStocks.NewStockForFund == 1), 1), else_=0)).label('NewStockForFund_AIF'), func.sum(case((FundStocks.NewStockForFund == 1, 1), else_=0)).label('NewStockForFund_Total'),

    func.sum(case((and_(FundStocks.Product_Id == 1, FundStocks.ExitStockForFund == 1), 1), else_=0)).label('ExitStockForFund_MF'), func.sum(case((and_(FundStocks.Product_Id == 2, FundStocks.ExitStockForFund == 1), 1), else_=0)).label('ExitStockForFund_ULIP'), func.sum(case((and_(FundStocks.Product_Id == 4, FundStocks.ExitStockForFund == 1), 1), else_=0)).label('ExitStockForFund_PMS'), func.sum(case((and_(FundStocks.Product_Id == 5, FundStocks.ExitStockForFund == 1), 1), else_=0)).label('ExitStockForFund_AIF'), func.sum(case((FundStocks.ExitStockForFund == 1, 1), else_=0)).label('ExitStockForFund_Total'), 
    
    FundStocks.ISIN_Code, FundStocks.HoldingSecurity_Name, FundStocks.MarketCap,  FundStocks.Sector_Code).join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == FundStocks.HoldingSecurity_Id).group_by( FundStocks.ISIN_Code, FundStocks.HoldingSecurity_Name, FundStocks.MarketCap, FundStocks.Sector_Code).filter(FundStocks.ISIN_Code.like("INE%")).filter(FundStocks.InstrumentType == 'Equity')
    # .filter(FundStocks.HoldingSecurity_Id == 813)
   
    if product_id:
        fundstock_query = fundstock_query.filter(FundStocks.Product_Id == product_id)
    if classification_id:
        fundstock_query = fundstock_query.filter(FundStocks.Classification_Id == classification_id)
    if sector_id:
        fundstock_query = fundstock_query.filter(HoldingSecurity.Sector_Id == sector_id)
    if market_cap:
        fundstock_query = fundstock_query.filter(FundStocks.MarketCap == market_cap)

    fundstock_dt = fundstock_query.order_by(desc('Total')).all()

    if fundstock_dt:
        for fundstock in fundstock_dt:
            securityname = fundstock.HoldingSecurity_Name
            date = datetime.today().date()
            if len(securityname) > 10:
                dates = str(securityname).replace("/","-").strip()[-10:]
            try:
                dates = datetime.strptime(dates, '%d-%m-%Y').date()
            except:
                dates = datetime.today().date()
                
            if dates >= datetime.today().date():
                mf_count = 0
                ulip_count = 0
                pms_count = 0
                aif_count = 0

                fundstock_q = db_session.query(HoldingSecurity).filter(HoldingSecurity.ISIN_Code == fundstock.ISIN_Code).filter(HoldingSecurity.Is_Deleted != 1).first()
                holdingsecurity_id = None
                investmentstyle = None
                if fundstock_q:
                    holdingsecurity_id = fundstock_q.HoldingSecurity_Id
                    investmentstyle = fundstock_q.Equity_Style

                fundstk = dict()
                fundstk["holdingsecurity_id"] = holdingsecurity_id
                fundstk["isin_code"] = fundstock.ISIN_Code
                fundstk["holdingsecurity_name"] = fundstock.HoldingSecurity_Name
                fundstk["marketcap"] = fundstock.MarketCap
                fundstk["investmentstyle"] = investmentstyle
                
                mf_data = get_aumandfundcountbyproduct(db_session, fundstock.ISIN_Code, 1)

                if mf_data:
                    mf_count = mf_data["count"]
                    mf_aum = mf_data["aum"]

                ulip_data = get_aumandfundcountbyproduct(db_session, fundstock.ISIN_Code, 2)

                if ulip_data:
                    ulip_count = ulip_data["count"]
                    ulip_aum = ulip_data["aum"]

                pms_data = get_aumandfundcountbyproduct(db_session, fundstock.ISIN_Code, 4)

                if pms_data:
                    pms_count = pms_data["count"]
                    pms_aum = pms_data["aum"]

                aif_data = get_aumandfundcountbyproduct(db_session, fundstock.ISIN_Code, 5)

                if aif_data:
                    aif_count = aif_data["count"]
                    aif_aum = aif_data["aum"]

                fundstk["mf"] = mf_count
                fundstk["ulip"] = ulip_count
                fundstk["pms"] = pms_count
                fundstk["aif"] = aif_count
                fundstk["total"] = mf_count + ulip_count + aif_count + pms_count

                fundstk["increaseexposure_mf"] = fundstock[5]
                fundstk["increaseexposure_ulip"] = fundstock[6]
                fundstk["increaseexposure_pms"] = fundstock[7]
                fundstk["increaseexposure_aif"] = fundstock[8]
                fundstk["increaseexposure_total"] = fundstock[9]

                fundstk["decreaseexposure_mf"] = fundstock[10]
                fundstk["decreaseexposure_ulip"] = fundstock[11]
                fundstk["decreaseexposure_pms"] = fundstock[12]
                fundstk["decreaseexposure_aif"] = fundstock[13]
                fundstk["decreaseexposure_total"] = fundstock[14]

                fundstk["newstockforfund_mf"] = fundstock[15]
                fundstk["newstockforfund_ulip"] = fundstock[16]
                fundstk["newstockforfund_pms"] = fundstock[17]
                fundstk["newstockforfund_aif"] = fundstock[18]
                fundstk["newstockforfund_total"] = fundstock[19]

                fundstk["exitstockforfund_mf"] = fundstock[20]
                fundstk["exitstockforfund_ulip"] = fundstock[21]
                fundstk["exitstockforfund_pms"] = fundstock[22]
                fundstk["exitstockforfund_aif"] = fundstock[23]
                fundstk["exitstockforfund_total"] = fundstock[24]
                
                resp.append(fundstk)

    return resp

def get_equity_exposure_old(db_session, isin_code, product_id):
    sql_favoritestockfunds = db_session.query(
        FundStocks.Fund_Id, FundStocks.Plan_Id, FundStocks.Plan_Name, FundStocks.Product_Name, FundStocks.Product_Id, FundStocks.Product_Code, 
        FundStocks.Percentage_to_AUM, FundStocks.Diff_Percentage_to_AUM, FundStocks.Purchase_Date, 
        FundStocks.IncreaseExposure, FundStocks.DecreaseExposure, FundStocks.NewStockForFund
    ).filter(FundStocks.ISIN_Code == isin_code)

    if product_id:
        sql_favoritestockfunds = sql_favoritestockfunds.filter(FundStocks.Product_Id == product_id)

    sql_favoritestockfunds = sql_favoritestockfunds.order_by(desc(FundStocks.Percentage_to_AUM)).all()

    favoritestockfunds_list = list()
    for favoritestockfunds in sql_favoritestockfunds:
        data = dict()
        data["plan_id"] = favoritestockfunds.Plan_Id
        data["plan_name"] = favoritestockfunds.Plan_Name
        data["product_name"] = favoritestockfunds.Product_Name
        data["product_id"] = favoritestockfunds.Product_Id
        data["product_code"] = favoritestockfunds.Product_Code
        data["percentage_to_aum"] = favoritestockfunds.Percentage_to_AUM
        data["diff_percentage_to_aum"] = favoritestockfunds.Diff_Percentage_to_AUM
        data["purchase_date"] = favoritestockfunds.Purchase_Date
        data["increaseexposure"] = favoritestockfunds.IncreaseExposure
        data["decreaseexposure"] = favoritestockfunds.DecreaseExposure
        data["new_stocks_for_fund"] = favoritestockfunds.NewStockForFund

        sql_fundmanagers = db_session.query(FundManager.FundManager_Name).filter(FundManager.Fund_Id == favoritestockfunds.Fund_Id)\
        .filter(FundManager.Is_Deleted != 1).filter(or_(FundManager.DateTo >= date.today(), FundManager.DateTo == None)).all()
        fundmanager_names = list()
        for sql_fundmanager in sql_fundmanagers:
            fundmanager = dict()
            fundmanager["fundmanager_name"] = sql_fundmanager.FundManager_Name

            fundmanager_names.append(fundmanager)
        data["fund_manager"] = fundmanager_names
        
        favoritestockfunds_list.append(data)
    
    return favoritestockfunds_list
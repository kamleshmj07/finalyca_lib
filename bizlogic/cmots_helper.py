import os
import csv
from utils.utils import print_query
from fin_models.transaction_models import NAV, ClosingValues, TRIReturns, IndexWeightage, Fundamentals
from fin_models.masters_models import BenchmarkIndices, HoldingSecurity
from sqlalchemy import desc, or_


def write_csv_v2(filepath, header, items, sep= ','):
    base_dir = os.path.dirname(filepath)
    os.makedirs(base_dir, exist_ok=True)
    with open(filepath, 'w') as f:
        csvwriter = csv.writer(f, delimiter=sep, lineterminator="\n")
        if header:
            csvwriter.writerow(header)
        csvwriter.writerows(items)


def cmots_save_bm_values(db_session, plan_id, nav_date, nav, user_id, dt):
    NAV_TYPE = "I"
    remark = ""

    sql_obj = db_session.query(NAV).filter(NAV.Plan_Id == plan_id).filter(NAV.NAV_Date==nav_date).filter(NAV.NAV_Type==NAV_TYPE).filter(NAV.Is_Deleted != 1).one_or_none()
    if sql_obj:
        if sql_obj.is_locked != 1:
            sql_obj.NAV = nav
            sql_obj.Updated_By = user_id
            sql_obj.Updated_Date = dt
            db_session.commit()
            remark = "Success: UPDATED in NAV."
        else:            
            remark = F"Fail: NAV for {nav_date.strftime('%d-%m-%Y')} is locked."
    else:
        sql_obj = NAV()
        sql_obj.Plan_Id = plan_id
        sql_obj.NAV_Date = nav_date
        sql_obj.NAV_PortfolioReturn = None
        sql_obj.NAV_Type = NAV_TYPE
        sql_obj.NAV = nav
        sql_obj.Created_By = user_id
        sql_obj.Created_Date = dt
        sql_obj.Is_Deleted = 0
        db_session.add(sql_obj)
        db_session.commit()

        remark = "Success: INSERTED in NAV."

    return remark


def cmots_delete_bm_values(db_session, plan_id, nav_date, user_id, dt):
    NAV_TYPE = "I"
    remark = ""

    sql_obj = db_session.query(NAV).filter(NAV.Plan_Id == plan_id).filter(NAV.NAV_Date==nav_date).filter(NAV.NAV_Type==NAV_TYPE).filter(NAV.Is_Deleted != 1).one_or_none()

    if sql_obj:
        sql_obj.Updated_By = user_id
        sql_obj.Updated_Date = dt
        sql_obj.Is_Deleted = 1
        db_session.commit()
        remark = 'Success: DELETED in ClosingValues.'

    return remark


def cmots_save_security_values(db_session, bse_code, isin_code, date, st_exchng, co_code, high, low, open, close, tdcloindi, volumne, no_trades, net_turnov, user_id, dt):
    remark = ""
    
    sql_obj = db_session.query(ClosingValues).filter(ClosingValues.Co_Code == co_code).filter(ClosingValues.Date_ == date).filter(ClosingValues.ST_EXCHNG == st_exchng).filter(ClosingValues.Is_Deleted != 1).one_or_none()

    if sql_obj:
        sql_obj.BSE_Code = bse_code
        sql_obj.ISIN_Code = isin_code

        sql_obj.HIGH = high
        sql_obj.LOW = low
        sql_obj.OPEN = open
        sql_obj.CLOSE = close
        sql_obj.TDCLOINDI = tdcloindi
        sql_obj.VOLUME = volumne
        sql_obj.NO_TRADES = no_trades
        sql_obj.NET_TURNOV = net_turnov
        sql_obj.Updated_By = user_id
        sql_obj.Updated_Date = dt        
        remark = 'Success: UPDATED in ClosingValues.'
    else:
        sql_obj = ClosingValues()
        sql_obj.BSE_Code = bse_code
        sql_obj.ISIN_Code = isin_code

        sql_obj.Date_ = date
        sql_obj.ST_EXCHNG = st_exchng
        sql_obj.Co_Code = co_code
        sql_obj.HIGH = high
        sql_obj.LOW = low
        sql_obj.OPEN = open
        sql_obj.CLOSE = close
        sql_obj.TDCLOINDI = tdcloindi
        sql_obj.VOLUME = volumne
        sql_obj.NO_TRADES = no_trades
        sql_obj.NET_TURNOV = net_turnov
        sql_obj.Created_By = user_id
        sql_obj.Created_Date = dt
        sql_obj.Is_Deleted = 0
        db_session.add(sql_obj)
        remark = 'Success: INSERTED in ClosingValues.'

    db_session.commit()
    return remark


def cmots_delete_security_values(db_session, date, st_exchng, co_code, user_id, dt):
    remark = ""
    
    sql_obj = db_session.query(ClosingValues).filter(ClosingValues.Co_Code == co_code).filter(ClosingValues.Date_ == date).filter(ClosingValues.ST_EXCHNG == st_exchng).filter(ClosingValues.Is_Deleted != 1).one_or_none()

    if sql_obj:
        sql_obj.Updated_By = user_id
        sql_obj.Updated_Date = dt 
        sql_obj.Is_Deleted = 1
        db_session.commit()
        remark = 'Success: DELETED in ClosingValues.'

    return remark


def cmots_save_security(db_session, isin, co_code, short_name, long_name, bse_code, nse_symbol, bse_groupname, sub_sectorname, user_id, dt):
    '''
    Use only in cmas master upload.

    New isin
    old co_code
    new isin and old co_code
    '''
    TODAY = dt.today()
    holdingsecurity_type = "Equity"

    remark = "No Actions."

    if not isin:
        return "ISIN cannot be blank."
    

    # First check if we already have the ISIN in the system created by CM or other fund holdings
    sql_securities = db_session.query(HoldingSecurity)\
                            .filter(or_(HoldingSecurity.ISIN_Code == isin, HoldingSecurity.Co_Code == co_code))\
                            .filter(HoldingSecurity.Is_Deleted != 1).filter(HoldingSecurity.active == 1).order_by(desc(HoldingSecurity.HoldingSecurity_Id)).all()
    
    # check if anyone has the same ISIN
    is_create = True
    for sql_obj in sql_securities:
        # Case 1: Check if existing co code is the same but ISIN is diff -> Corporate movement
        if sql_obj.Co_Code == co_code:
            if sql_obj.ISIN_Code.upper() == isin.upper():
                is_create = False

        # Case 2: Check if ISIN is same, but co code is None -> row created by other importers
        if sql_obj.ISIN_Code == isin:
            is_create = False
            if sql_obj.Co_Code != co_code and not sql_obj.Co_Code:
                sql_obj.BSE_Code = bse_code
                sql_obj.Co_Code = co_code
                sql_obj.BSE_GroupName = bse_groupname
                sql_obj.NSE_Symbol = nse_symbol
                sql_obj.short_name = short_name
                # sql_obj.Sub_SectorName = sub_sectorname  # We will now be using the Sebi defined industry field from Industry_Classification tbl
                sql_obj.Updated_By = user_id
                sql_obj.Updated_Date = dt
                remark = "Success: UPDATED in HoldingSecurity."

    if is_create:
        #mark old isin inactive
        if co_code:
            update_values = {
                        HoldingSecurity.active : 0,
                        HoldingSecurity.Updated_By : user_id,
                        HoldingSecurity.Updated_Date : TODAY,
                        }
            
            sql_securities = db_session.query(HoldingSecurity)\
                                .filter(HoldingSecurity.Co_Code == co_code)\
                                .filter(HoldingSecurity.Is_Deleted != 1).filter(HoldingSecurity.active == 1).update(update_values)
        #create new 
        sql_sec = HoldingSecurity()
        sql_sec.ISIN_Code = isin
        sql_sec.Co_Code = co_code
        sql_sec.Instrument_Type = holdingsecurity_type
        sql_sec.Asset_Class = holdingsecurity_type
        sql_sec.BSE_Code = bse_code
        sql_sec.BSE_GroupName = bse_groupname
        sql_sec.NSE_Symbol = nse_symbol
        sql_sec.HoldingSecurity_Name = long_name
        sql_sec.short_name = short_name
        # sql_sec.Sub_SectorName = sub_sectorname   # We will now be using the Sebi defined industry field from Industry_Classification tbl
        sql_sec.Created_By = user_id
        sql_sec.Created_Date = dt
        sql_sec.Is_Deleted = 0
        sql_sec.active = 1

        db_session.add(sql_sec)
        remark = "Success: INSERTED in HoldingSecurity."

    db_session.commit()
    
    return remark


def cmots_save_bm(db_session, co_code, short_name, long_name, bse_code, nse_symbol, bse_groupname, user_id, dt):
    sql_bm = db_session.query(BenchmarkIndices).filter(BenchmarkIndices.Co_Code == co_code).filter(BenchmarkIndices.Is_Deleted != 1).one_or_none()

    if sql_bm:
        sql_bm.Updated_By = user_id
        sql_bm.Updated_Date = dt

        sql_bm.Short_Name = short_name
        sql_bm.Long_Name = long_name
        sql_bm.BSE_Code = bse_code
        sql_bm.NSE_Symbol = nse_symbol
        sql_bm.BSE_GroupName = bse_groupname

        remark = "Success: UPDATED in BenchmarkIndices."
    else:
        sql_bm = BenchmarkIndices()
        sql_bm.Created_By = user_id
        sql_bm.Created_Date = dt
        sql_bm.Co_Code = co_code
        sql_bm.BenchmarkIndices_Name = short_name
        sql_bm.BenchmarkIndices_Description = long_name

        sql_bm.Short_Name = short_name
        sql_bm.Long_Name = long_name
        sql_bm.BSE_Code = bse_code
        sql_bm.NSE_Symbol = nse_symbol
        sql_bm.BSE_GroupName = bse_groupname
        sql_bm.Is_Deleted = 0
        remark = "Success: INSERTED in BenchmarkIndices."
        db_session.add(sql_bm)

    db_session.commit()
    
    return remark


def cmots_save_fundamentals(db_session, CO_CODE, price_date, PE, EPS, dividend_yield, PBV, market_cap, user_id, dt, pe_cons, eps_cons, pbv_cons):

    sql_obj = db_session.query(Fundamentals).filter(Fundamentals.CO_CODE == CO_CODE).filter(Fundamentals.PriceDate == price_date).filter(Fundamentals.Is_Deleted != 1).one_or_none()
    
    remark = None
    if sql_obj:
        sql_obj.PE = PE
        sql_obj.EPS = EPS
        sql_obj.DivYield = dividend_yield
        sql_obj.PBV = PBV
        sql_obj.mcap = market_cap
        sql_obj.PE_CONS = pe_cons
        sql_obj.EPS_CONS = eps_cons
        sql_obj.PBV_CONS = pbv_cons
        sql_obj.Updated_By = user_id
        sql_obj.Updated_Date = dt
        remark = "Success: UPDATED in Fundamentals."
        db_session.commit()

    else:
        sql_obj = Fundamentals()
        sql_obj.CO_CODE = CO_CODE
        sql_obj.PriceDate = price_date
        sql_obj.PE = PE
        sql_obj.EPS = EPS
        sql_obj.DivYield = dividend_yield
        sql_obj.PBV = PBV
        sql_obj.mcap = market_cap
        sql_obj.PE_CONS = pe_cons
        sql_obj.EPS_CONS = eps_cons
        sql_obj.PBV_CONS = pbv_cons
        sql_obj.Is_Deleted = 0
        sql_obj.Created_By = user_id
        sql_obj.Created_Date = dt
        remark = "Success: INSERTED in Fundamentals."
        
        db_session.add(sql_obj)
        db_session.commit()

    return remark


def cmots_save_index_weight(db_session, security_co_code, wdate, index_co_code, close_price, no_of_shares, full_market_cap, free_float_value, free_float_market_cap, index_weight, index_type, user_id, dt):
    remark = ""

    sql_obj = db_session.query(IndexWeightage).filter(IndexWeightage.CO_CODE == security_co_code).filter(IndexWeightage.WDATE == wdate).filter(IndexWeightage.Index_CO_CODE == index_co_code).filter(IndexWeightage.Is_Deleted != 1).one_or_none()

    if not sql_obj:
        sql_obj = IndexWeightage()
        sql_obj.CO_CODE = security_co_code
        sql_obj.WDATE = wdate
        sql_obj.Index_CO_CODE = index_co_code
        sql_obj.CLOSEPRICE = close_price
        sql_obj.NOOFSHARES = no_of_shares
        sql_obj.FULLMCAP = full_market_cap
        sql_obj.FF_ADJFACTOR = free_float_value
        sql_obj.FF_MCAP = free_float_market_cap
        sql_obj.WEIGHT_INDEX = index_weight
        sql_obj.Index_Type = index_type
        sql_obj.Created_By = user_id
        sql_obj.Created_Date = dt
        sql_obj.Is_Deleted = 0
        sql_obj.Updated_By = None
        sql_obj.Updated_Date = None
        
        db_session.add(sql_obj)
        db_session.commit()
        remark = F"SUCCESS: INSERTED IN IndexWeightage"

    else:                
        sql_obj.CLOSEPRICE = close_price
        sql_obj.NOOFSHARES = no_of_shares
        sql_obj.FULLMCAP = full_market_cap
        sql_obj.FF_ADJFACTOR = free_float_value
        sql_obj.FF_MCAP = free_float_market_cap
        sql_obj.WEIGHT_INDEX = index_weight
        sql_obj.Index_Type = index_type
        sql_obj.Updated_By = user_id
        sql_obj.Updated_Date = dt

        db_session.commit()
        remark = F"SUCCESS: UPDATED IN IndexWeightage"

    return remark


def cmots_delete_index_weight(db_session, security_co_code, wdate, index_co_code, user_id, dt):
    remark = ""
    sql_obj = db_session.query(IndexWeightage).filter(IndexWeightage.CO_CODE == security_co_code).filter(IndexWeightage.WDATE == wdate).filter(IndexWeightage.Index_CO_CODE == index_co_code).filter(IndexWeightage.Is_Deleted != 1).one_or_none()

    if sql_obj:
        sql_obj = IndexWeightage()
        sql_obj.Is_Deleted = 1
        sql_obj.Updated_By = user_id
        sql_obj.Updated_Date = dt
        db_session.commit()
        remark = F"SUCCESS: DELETED IN IndexWeightage"

    return remark


def cmots_save_tri_returns(db_session, exchange, tri_index_code, tri_index_name, tri_index_date, ret_1w, ret_1m, ret_3m, ret_6m, ret_1y, ret_3y, base_index_code, user_id, dt):
    remark = ""
    sql_obj = db_session.query(TRIReturns).filter(TRIReturns.TRI_Co_Code == tri_index_code).filter(TRIReturns.TRI_IndexDate == tri_index_date).filter(TRIReturns.Is_Deleted != 1).one_or_none()
    if not sql_obj:
        sql_obj = TRIReturns()
        sql_obj.Exchange = exchange
        sql_obj.TRI_Co_Code = tri_index_code
        sql_obj.TRI_IndexName = tri_index_name
        sql_obj.TRI_IndexDate = tri_index_date
        sql_obj.Return_1Week = ret_1w
        sql_obj.Return_1Month = ret_1m
        sql_obj.Return_3Month = ret_3m
        sql_obj.Return_6Month = ret_6m
        sql_obj.Return_1Year = ret_1y
        sql_obj.Return_3Year = ret_3y
        sql_obj.Co_Code = base_index_code
        sql_obj.Created_By = user_id
        sql_obj.Crated_Date = dt
        sql_obj.Is_Deleted = 0
        db_session.add(sql_obj)
        db_session.commit()
        remark = F"SUCCESS: INSERTED IN TRIReturns"

    else:            
        sql_obj.Exchange = exchange 
        sql_obj.TRI_IndexName = tri_index_name
        sql_obj.Return_1Week = ret_1w
        sql_obj.Return_1Month = ret_1m
        sql_obj.Return_3Month = ret_3m
        sql_obj.Return_6Month = ret_6m
        sql_obj.Return_1Year = ret_1y
        sql_obj.Return_3Year = ret_3y
        sql_obj.Co_Code = base_index_code
        sql_obj.Updated_By = user_id
        sql_obj.Updated_Date = dt
        
        db_session.commit()
        remark = F"SUCCESS: UPDATED IN TRIReturns"

    return remark


def get_holding_security_type_mapping():
    """
    Maintain a mapping for MF/ULIP Asset Class reported in Holdings file to Finalyca Holding Security Types
    """
    return {
        'Cash & Bank Balance & Bank Deposits': 'Cash and Equivalents', # Not able to find any security in Underlying_Holdings table
        'Certificate of Deposits': 'Debt',
        'Commercial Paper': 'Debt',
        'Corporate Debts': 'Debt',
        'Equity': 'Listed Equity',
        'Floating Rate Instruments': 'Debt',
        'Govt. Securities': 'Debt',
        'Indian Mutual Funds': 'Mutual Funds',
        'Infrastructure Investment Trust (InvITs)': 'InvIt/REITS',
        'Net CA & Others': 'Cash and Equivalents',
        'PTC': 'Debt',
        'Reverse Repo': 'Debt',
        'Warrants': 'Cash and Equivalents', # Need to confirm with Vijay
        'Real Estate Investment Trust (REIT)': 'InvIt/REITS',
        'Corporate Debts -Others': 'Debt',
        'Cash & Cash Equivalent': 'Cash and Equivalents',
        'NCD': 'Debt',
        'Bonds - Others': 'Debt',
        'Fixed Deposits': 'Other Debts',
        'Mutual Fund Units': 'Mutual Funds',
        'T Bills': 'Debt',
        'MMI - Others': 'Debts',
        'Partly paid Share(PPS)': 'Listed Equity',
        'Rights': 'Listed Equity', # Need to confirm with Vijay
        'Foreign Equity': 'LISTED EQUITY',
        'Derivatives': 'OTHER FUNDS',
        'Derivatives - Stock Future': 'OTHER FUNDS',
        'Derivatives - Index Put Option': 'OTHER FUNDS',
        'Derivatives - Stock Call Option': 'OTHER FUNDS',
        'Derivatives - Index Future': 'OTHER FUNDS',
        'PSU & PFI Bonds - DEBT': 'DEBT',
        'Silver': 'COMMODITY',
        'Gold': 'COMMODITY',
        'ZCB': 'DEBT',
        'Preference Shares': 'LISTED EQUITY',
        'Foreign Mutual Funds (Equity Fund)': 'Mutual Funds',
        'Alternative Investment Fund (AIF)': 'OTHER FUNDS',
        'Debt - Other': 'Debt',
        'PSU & PFI Bonds': 'DEBT',
        'Derivatives-Commodity' : 'OTHER FUNDS',
        'Derivatives - Index Call Option' : 'OTHER FUNDS',
        'Derivatives - Stock Put Option': 'OTHER FUNDS',
    }


def save_equity_in_holding_master(db_session, security_name, bse_code, nse_code, sector_id, isin_demat, industry_name, market_cap, issuer_name, issuer_code, issuer_id, user_id, dt):

    sql_obj = db_session.query(HoldingSecurity)\
                        .filter(HoldingSecurity.ISIN_Code == isin_demat, HoldingSecurity.Is_Deleted != 1).one_or_none()
    holding_security_type_mapping = get_holding_security_type_mapping()

    if sql_obj:

        """
        # TODO The following exceptions to be reported in a email report to the BDE team
        # TODO The following checks are causing breaks to update the new sector and issuer information during CMOTS migration
        #      Review these checks post first month of CMOT migration
        if sql_obj.Sector_Id != sector_id:
            raise Exception("Sector Id is different compared to database.")

        if sql_obj.Issuer_Id != issuer_id:
            raise Exception("Issuer Id is different compared to database.")

        if sql_obj.Issuer_Code != issuer_code:
            raise Exception("Issuer code is different compared to database.")
        """

        # Prefer VR name over CMOTS name
        sql_obj.HoldingSecurity_Name = security_name
        sql_obj.Issuer_Name = issuer_name
        sql_obj.MarketCap = market_cap
        sql_obj.BSE_Code = bse_code
        sql_obj.NSE_Symbol = nse_code

        sql_obj.Issuer_Id = issuer_id
        sql_obj.Issuer_Code = issuer_code
        
        sql_obj.Updated_By = user_id
        sql_obj.Updated_Date = dt

        db_session.commit()
        remark = "Success: UPDATED in HoldingSecurity."
    else:
        sql_obj = HoldingSecurity()
        sql_obj.HoldingSecurity_Name = security_name
        sql_obj.Short_CompanyName = security_name
        sql_obj.ISIN_Code = isin_demat
        sql_obj.Sector_Id = sector_id
        sql_obj.Sub_SectorName = industry_name
        sql_obj.Asset_Class = "Equity"
        sql_obj.Instrument_Type = "Equity"
        sql_obj.Issuer_Id = issuer_id
        sql_obj.Issuer_Code = issuer_code
        sql_obj.Issuer_Name = issuer_name
        sql_obj.Is_Deleted = 0
        sql_obj.active = 1
        sql_obj.Created_By = user_id
        sql_obj.Created_Date = dt
        sql_obj.MarketCap = market_cap
        sql_obj.HoldingSecurity_Type = holding_security_type_mapping["Equity"].upper()
        sql_obj.BSE_Code = bse_code
        sql_obj.NSE_Symbol = nse_code

        db_session.add(sql_obj)
        db_session.commit()
        remark = "Success: CREATED in HoldingSecurity."

    return remark


def save_mf_in_holding_master(db_session, security_name, bse_code, nse_code, isin_demat, issuer_name, issuer_code, script_status, user_id, dt):

    sql_obj = db_session.query(HoldingSecurity)\
                        .filter(HoldingSecurity.ISIN_Code == isin_demat, HoldingSecurity.Is_Deleted != 1).one_or_none()

    holding_security_type_mapping = get_holding_security_type_mapping()

    if sql_obj:

        """
        # TODO The following exceptions to be reported in a email report to the BDE team
        # TODO The following checks are causing breaks to update the new issuer information during CMOTS migration
        #      Review these checks post first month of CMOT migration

        # If something is changing, we need an alert.
        if sql_obj.HoldingSecurity_Name != security_name:
            raise Exception("Security name does not match database record.")

        if sql_obj.Issuer_Name != issuer_name:
            raise Exception("Issuer name does not match database record.")

        if sql_obj.Issuer_Code != issuer_code:
            raise Exception("Issuer code does not match database record.")
        """

        sql_obj.HoldingSecurity_Type = holding_security_type_mapping['Indian Mutual Funds'].upper()
        sql_obj.Asset_Class = "Mutual Fund"
        sql_obj.Instrument_Type = "Mutual Funds"
        sql_obj.BSE_Code = bse_code
        sql_obj.NSE_Symbol = nse_code

        # TODO Added the following logic to update the HoldingSecurity with 
        #      new issuer information from CMOTs than previous one
        #      Need to review this post first month of CMOT migration
        sql_obj.Issuer_Code = issuer_code
        sql_obj.Issuer_Name = issuer_name

        sql_obj.Updated_By = user_id
        sql_obj.Updated_Date = dt

        if script_status == "Inactive":
            sql_obj.Is_Deleted = 1
            remark = "Success: DELETED in HoldingSecurity."
        else:
            sql_obj.Is_Deleted = 0
            remark = "Success: UPDATED in HoldingSecurity."

        db_session.commit()
    else:
        sql_obj = HoldingSecurity()
        sql_obj.HoldingSecurity_Name = security_name
        sql_obj.Short_CompanyName = security_name
        sql_obj.ISIN_Code = isin_demat
        sql_obj.Issuer_Code = issuer_code
        sql_obj.Issuer_Name = issuer_name

        sql_obj.HoldingSecurity_Type = holding_security_type_mapping['Indian Mutual Funds'].upper()
        sql_obj.Asset_Class = "Mutual Fund"
        sql_obj.Instrument_Type = "Mutual Funds"
        sql_obj.BSE_Code = bse_code
        sql_obj.NSE_Symbol = nse_code
        sql_obj.Created_By = user_id
        sql_obj.Created_Date = dt

        if script_status == "Inactive":
            sql_obj.Is_Deleted = 1
            remark = "Success: INSERTED in HoldingSecurity."
        else:
            sql_obj.Is_Deleted = 0
            remark = "Success: INSERTED in HoldingSecurity."

        db_session.add(sql_obj)
        db_session.commit()

    return remark
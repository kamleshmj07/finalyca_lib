from sqlalchemy import func, extract
from fin_models.transaction_models import TRIReturns, NAV
from datetime import date

def get_benchmark_tri_returns(db_session, tri_co_code, from_date: date, to_date: date):
    """
    For given tri_co_code, finds monthly returns between from_date and to_date. includes both dates in results.
    Return Parameters: TRI_IndexName, Co_Code, TRI_Co_Code, TRI_IndexDate, Return_1Month
    """
    # get all month end dates based on period
    sql_month_end_dates = db_session.query(
        func.max(TRIReturns.TRI_IndexDate).label('TRI_IndexDate'), 
        extract('year', TRIReturns.TRI_IndexDate).label('year'),
        extract('month', TRIReturns.TRI_IndexDate).label('month')
        )\
    .filter(TRIReturns.TRI_Co_Code == tri_co_code, TRIReturns.Is_Deleted != 1)\
    .filter(TRIReturns.TRI_IndexDate >= from_date, TRIReturns.TRI_IndexDate <= to_date)\
    .group_by(extract('year', TRIReturns.TRI_IndexDate), extract('month', TRIReturns.TRI_IndexDate)).subquery()

    tri_returns_data = db_session.query(
        TRIReturns.TRI_IndexName,
        TRIReturns.Co_Code,
        TRIReturns.TRI_Co_Code,
        TRIReturns.TRI_IndexDate,
        TRIReturns.Return_1Month
    )\
    .select_from(TRIReturns)\
    .join(sql_month_end_dates, sql_month_end_dates.c.TRI_IndexDate == TRIReturns.TRI_IndexDate)\
    .filter(TRIReturns.TRI_Co_Code == tri_co_code, TRIReturns.Is_Deleted != 1)\
    .order_by(TRIReturns.TRI_IndexDate).all()

    return tri_returns_data

def get_monthly_nav(db_session, plan_id, is_benchmark, from_date, to_date):
    """
    For given tri_co_code, finds monthly returns between from_date and to_date. includes both dates in results.
    Return Parameters: Plan_Id, NAV_Date, NAV
    """
    # get all month end dates based on period
    sq = db_session.query(
        func.max(NAV.NAV_Date).label('NAV_Date'), 
        extract('year', NAV.NAV_Date).label('year'),
        extract('month', NAV.NAV_Date).label('month')
        )\
    .filter(NAV.Plan_Id == plan_id, NAV.Is_Deleted != 1)
    if is_benchmark:
        sq = sq.filter(NAV.NAV_Type == 'I')
    else:
        sq = sq.filter(NAV.NAV_Type == 'P')
    sq = sq.filter(NAV.NAV_Date >= from_date, NAV.NAV_Date <= to_date)\
    .group_by(extract('year', NAV.NAV_Date), extract('month', NAV.NAV_Date)).subquery()

    tri_returns_data = db_session.query(
        NAV.Plan_Id,
        NAV.NAV_Date,
        NAV.NAV
    )\
    .select_from(NAV)\
    .join(sq, sq.c.NAV_Date == NAV.NAV_Date)
    
    if is_benchmark:
        tri_returns_data = tri_returns_data.filter(NAV.Plan_Id == plan_id, NAV.Is_Deleted != 1, NAV.NAV_Type == 'I')
    else:
        tri_returns_data = tri_returns_data.filter(NAV.Plan_Id == plan_id, NAV.Is_Deleted != 1, NAV.NAV_Type == 'P')
    
    tri_returns_data = tri_returns_data.order_by(NAV.NAV_Date).all()

    return tri_returns_data
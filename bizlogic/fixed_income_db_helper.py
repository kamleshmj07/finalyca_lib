import pandas as pd
from datetime import date as dt, datetime as dtt, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import func, and_, desc, asc

from fin_models.masters_models import *
from fin_models.transaction_models import UnderlyingHoldings, PlanProductMapping
from fin_models.transaction_models import DebtPrice
from bizlogic.common_helper import convert_sqlalchemy_object_as_dict
from utils.utils import remove_stop_words
from utils.time_func import last_date_of_month


def get_fi_securities_summary(db_session, page, limit, filters=None, sorting=None):

    sql_fisec = db_session.query(DebtSecurity.DebtSecurity_Id,
                                 DebtSecurity.Security_Name,
                                 DebtSecurity.ISIN,
                                 DebtSecurity.Exchange_1,
                                 DebtSecurity.Exchange_1_Local_Code,
                                 DebtSecurity.Exchange_2,
                                 DebtSecurity.Exchange_2_Local_Code,
                                 DebtSecurity.Exchange_3,
                                 DebtSecurity.Exchange_3_Local_Code,
                                 DebtSecurity.Security_Type,
                                 DebtSecurity.Bond_Type,
                                 DebtSecurity.Country,
                                 DebtSecurity.Issuer,
                                 DebtSecurity.Maturity_Price,
                                 DebtSecurity.Maturity_Based_On,
                                 DebtSecurity.Maturity_Benchmark_Index,
                                 DebtSecurity.Is_Perpetual,
                                 DebtSecurity.On_Tap_Indicator,
                                 DebtSecurity.Coupon_Type,
                                 DebtSecurity.Interest_Payment_Frequency,
                                 DebtSecurity.Is_Cumulative,
                                 DebtSecurity.Compounding_Frequency,
                                 DebtSecurity.Min_Investment_Amount,
                                 DebtSecurity.FRN_Index_Benchmark,
                                 DebtSecurity.Issuer_Type,
                                 DebtSecurity.Issue_Size,
                                 DebtSecurity.Yield_At_Issue,
                                 DebtSecurity.Maturity_Structure,
                                 DebtSecurity.Convention_Method,
                                 DebtSecurity.Interest_Commencement_Date,
                                 DebtSecurity.FRN_Type,
                                 DebtSecurity.Markup,
                                 DebtSecurity.Minimum_Interest_Rate,
                                 DebtSecurity.Maximum_Interest_Rate,
                                 DebtSecurity.Is_Guaranteed,
                                 DebtSecurity.Is_Secured,
                                 DebtSecurity.Security_Charge,
                                 DebtSecurity.Security_Collateral,
                                 DebtSecurity.Tier,
                                 DebtSecurity.Is_Upper,
                                 DebtSecurity.Is_Sub_Ordinate,
                                 DebtSecurity.Is_Senior,
                                 DebtSecurity.Is_Callable,
                                 DebtSecurity.Is_Puttable,
                                 DebtSecurity.Strip,
                                 DebtSecurity.Is_Taxable,
                                 DebtSecurity.Latest_Applied_INTPY_Annual_Coupon_Rate,
                                 DebtSecurity.Bilav_Internal_Issuer_Id,
                                 DebtSecurity.Bilav_Code,
                                 DebtSecurity.LEI,
                                 DebtSecurity.CIN,
                                 DebtSecurity.Issue_Price,
                                 DebtSecurity.Issue_Date,
                                 DebtSecurity.Interest_Payout_1,
                                 DebtSecurity.Current_Yield,
                                 HoldingSecurity.Interest_Rate,
                                 HoldingSecurity.Maturity_Date,
                                 HoldingSecurity.Currency,
                                 HoldingSecurity.Face_Value,
                                 HoldingSecurity.Paid_Up_Value,
                                 func.coalesce(DebtSecurity.Sect_Code, Sector.Sector_Code).label('Sector_Code'),
                                 func.coalesce(DebtSecurity.Sector, Sector.Sector_Name).label('Sector_Name'))\
                            .join(HoldingSecurity, HoldingSecurity.Co_Code == func.concat('BLV_', DebtSecurity.Bilav_Code))\
                            .join(Sector, Sector.Sector_Id == HoldingSecurity.Sector_Id, isouter=True)\
                            .filter(HoldingSecurity.Maturity_Date >= dt.today(),
                                    HoldingSecurity.Is_Deleted != 1,
                                    HoldingSecurity.active == 1)


    # filter the data here based on the filter criteria
    if filters:
        for f in filters:
            column_name = f.get('column')
            column_value = f.get('value')
            column_value_from = f.get('value_from')
            column_value_to = f.get('value_to')

            if column_name == 'Interest_Payment_Frequency':
                if len(column_value) > 0:
                    sql_fisec = sql_fisec.filter(DebtSecurity.Interest_Payment_Frequency.in_(column_value))

            if column_name == 'Is_Secured':
                if not column_value == "":
                    sql_fisec = sql_fisec.filter(DebtSecurity.Is_Secured == column_value)

            if column_name == 'Security_Name':
                search = "%{}%".format(column_value)
                sql_fisec = sql_fisec.filter(DebtSecurity.Security_Name.like(search))

            if column_name == 'Face_Value':
                sql_fisec = sql_fisec.filter(and_(HoldingSecurity.Face_Value >= column_value_from, HoldingSecurity.Face_Value <= column_value_to))

            if column_name == 'Tenure_Remaining':
                from_date = dtt.now() + relativedelta(years=int(column_value_from))
                to_date = dtt.now() + relativedelta(years=int(column_value_to))
                sql_fisec = sql_fisec.filter(and_(HoldingSecurity.Maturity_Date >= from_date, HoldingSecurity.Maturity_Date <= to_date))
            
            if column_name == 'Bilav_Internal_Issuer_Id':
                sql_fisec = sql_fisec.filter(DebtSecurity.Bilav_Internal_Issuer_Id == column_value)


    # sort the data here based on the sorting criteria
    if not sorting:
        sorting = [{'column':'Security_Name', 'direction':'asc'}]

    for s in sorting:
        sort_column = s.get('column')
        sort_direction = s.get('direction')
        sort = asc(sort_column) if sort_direction == 'asc' else desc(sort_column) 
        sql_fisec = sql_fisec.order_by(sort)

    total_records = sql_fisec.count()
    if page >= 0 and limit:
        offset = page*limit
        sql_fisec = sql_fisec.offset(offset)
        sql_fisec = sql_fisec.limit(limit)

    df_fisec = pd.DataFrame(sql_fisec.all())

    # return None for both result and total_records if there is nothing to return
    if df_fisec.empty:
        return (None, None)

    df_fisec['Maturity_Date'] = pd.to_datetime(df_fisec['Maturity_Date'], format=r'%Y-%m-%d')
    df_fisec['Issue_Date'] = pd.to_datetime(df_fisec['Issue_Date'], format=r'%Y-%m-%d')
    df_fisec['Tenure_Remaining'] = df_fisec['Maturity_Date'].sub(pd.Timestamp('today')).dt.days
    df_fisec['Maturity_Date'] = df_fisec['Maturity_Date'].dt.strftime(r'%d %b %Y')
    df_fisec['Issue_Date'] = df_fisec['Issue_Date'].dt.strftime(r'%d %b %Y')

    return df_fisec.to_dict(orient="records"), total_records


def get_fi_detailed_security_info(db_session, filters=None):
    # TODO: Rename Sector Code and Sector Name fields as per standard nomenclature
    sql_debt = db_session.query(DebtSecurity, HoldingSecurity)\
                         .join(HoldingSecurity, HoldingSecurity.Co_Code == func.concat('BLV_', DebtSecurity.Bilav_Code))\
                         .filter(DebtSecurity.Is_Deleted != 1,
                                 HoldingSecurity.Is_Deleted != 1)

    if filters:
        for f in filters:
            column_name = f.get('column')
            column_value = f.get('value')

            if column_name == 'DebtSecurity_Id':
                sql_debt = sql_debt.filter(DebtSecurity.DebtSecurity_Id == column_value).one_or_none()

            if column_name == 'ISIN':
                sql_debt = sql_debt.filter(DebtSecurity.ISIN == column_value).one_or_none()

    if not sql_debt:
        raise Exception(F'Security not found.')

    list_credit_ratings = get_credit_ratings_for_security(db_session, filters=[{'column' : 'DebtSecurity_Id' , 'value' : sql_debt[0].DebtSecurity_Id}])

    dict_fi_security = convert_sqlalchemy_object_as_dict(sql_debt[0])
    dict_holding_security = convert_sqlalchemy_object_as_dict(sql_debt[1])
    dict_final = {**dict_fi_security, **dict_holding_security}

    df_fisec = pd.DataFrame(dict_final, index=[0])
    df_fisec['Maturity_Date'] = pd.to_datetime(df_fisec['Maturity_Date'], format=r'%Y-%m-%d')
    df_fisec['Issue_Date'] = pd.to_datetime(df_fisec['Issue_Date'], format=r'%Y-%m-%d')
    df_fisec['Interest_Payout_1'] = pd.to_datetime(df_fisec['Interest_Payout_1'], format=r'%Y-%m-%d', errors='coerce')
    df_fisec['Interest_Commencement_Date'] = pd.to_datetime(df_fisec['Interest_Commencement_Date'], format=r'%Y-%m-%d', errors='coerce')
    df_fisec['Tenure_Remaining'] = df_fisec['Maturity_Date'].sub(pd.Timestamp('today')).dt.days
    df_fisec['Maturity_Date'] = df_fisec['Maturity_Date'].dt.strftime(r'%d %b %Y')
    df_fisec['Issue_Date'] = df_fisec['Issue_Date'].dt.strftime(r'%d %b %Y')
    df_fisec['Interest_Payout_1'] = df_fisec['Interest_Payout_1'].dt.strftime(r'%d %b %Y')
    df_fisec['Interest_Commencement_Date'] = df_fisec['Interest_Commencement_Date'].dt.strftime(r'%d %b %Y')
    df_fisec = df_fisec.replace({float("nan"): None})

    list_fi_security = df_fisec.to_dict(orient='records')
    list_fi_security[0]["FI_Credit_Ratings"] = list_credit_ratings

    return list_fi_security


def get_fi_call_put_for_id(db_session, security_id, bond_option):
    if bond_option == 'call':
        sql_debt_call_puts = db_session.query(DebtCallOption).filter(DebtCallOption.DebtSecurity_Id == security_id)
    else:
        sql_debt_call_puts = db_session.query(DebtPutOption).filter(DebtPutOption.DebtSecurity_Id == security_id)

    sql_debt_call_puts = sql_debt_call_puts.all()

    call_put_list = list()
    for sql_debt in sql_debt_call_puts:
        call_put_list.append(convert_sqlalchemy_object_as_dict(sql_debt))

    return call_put_list


def get_fi_redemption_for_id(db_session, security_id):
    sql_debt_redemption = db_session.query(DebtRedemption).filter(DebtRedemption.DebtSecurity_Id == security_id).all()
    
    redemption = list()
    for sql_debt in sql_debt_redemption:
        redemption.append(convert_sqlalchemy_object_as_dict(sql_debt))

    return redemption


def get_credit_ratings_for_security(db_session, filters=None):

    # rank the credit ratings by AsofDate for all ISINs
    sql_credit_ratings_sub = db_session.query(DebtCreditRating.DebtSecurity_Id,
                                              DebtCreditRating.DebtCreditRating_Id,
                                              DebtCreditRating.ISIN,
                                              DebtCreditRating.Rating_Agency,
                                              DebtCreditRating.Rating_Date,
                                              DebtCreditRating.Rating_Direction,
                                              DebtCreditRating.Rating_Direction_Code,
                                              DebtCreditRating.Rating_Symbol,
                                              func.replace(func.replace(DebtCreditRating.Rating_Symbol, "+", ""), "-", "").label("Rating_Symbol_Without_Signs"),
                                              DebtCreditRating.Watch_Flag,
                                              DebtCreditRating.Watch_Flag_Code,
                                              DebtCreditRating.Watch_Flag_Reason,
                                              DebtCreditRating.Watch_Flag_Reason_Code,
                                              DebtCreditRating.AsofDate,
                                              func.rank().over(
                                                  partition_by=DebtCreditRating.DebtCreditRating_Id,
                                                  order_by=DebtCreditRating.AsofDate.desc()).label('Rank'))\
                                        .filter(DebtCreditRating.Is_Deleted != 1).subquery()

    # filter top ranked
    sql_credit_ratings = db_session.query(sql_credit_ratings_sub).filter(sql_credit_ratings_sub.c.Rank == 1)

    if filters:
        for f in filters:
            column_name = f.get('column')
            column_value = f.get('value')
            column_value_from = f.get('value_from')
            column_value_to = f.get('value_to')

            if column_name == 'DebtSecurity_Id':
                sql_credit_ratings = sql_credit_ratings.filter(sql_credit_ratings_sub.c.DebtSecurity_Id == column_value)

            if column_name == 'Rating_Symbol' and len(column_value) > 0:
                sql_credit_ratings = sql_credit_ratings.filter(sql_credit_ratings_sub.c.Rating_Symbol_Without_Signs.in_(column_value))


    df_credit_ratings = pd.DataFrame(sql_credit_ratings)

    # cleaning data for the Rating_Agency information
    list_credit_ratings = []
    if df_credit_ratings.empty:
        return list_credit_ratings

    df_credit_ratings["Rating_Agency"] = df_credit_ratings["Rating_Agency"].str.upper()
    df_credit_ratings["Rating_Agency"] = df_credit_ratings["Rating_Agency"].str.replace('[^a-zA-Z0-9\s]', '')

    stop_words  = ['RATINGS', 'LTD', 'RESEARCH']
    if not df_credit_ratings.empty:
        df_credit_ratings["Rating_Agency"] = df_credit_ratings.apply(lambda x: remove_stop_words(x["Rating_Agency"], stop_words), axis=1)
        df_credit_ratings["Rating_Agency"] = df_credit_ratings["Rating_Agency"].str.strip()
        list_credit_ratings = df_credit_ratings.to_dict(orient='records')

    return list_credit_ratings


def get_fi_securities_for_name(db_session, fi_security_name):
    sql_fi_securities = db_session.query(DebtSecurity.DebtSecurity_Id,
                                         DebtSecurity.Security_Name)\
                                  .filter(DebtSecurity.Is_Deleted != 1,
                                          DebtSecurity.Security_Name.like('%' + fi_security_name + '%')).all()

    resp = list()
    for fi_security in sql_fi_securities:
        data = dict()
        data["key"] = fi_security.DebtSecurity_Id
        data["label"] = fi_security.Security_Name

        resp.append(data)

    return resp


def get_issuer_types(db_session):
    sql_obj = db_session.query(DebtSecurity.Issuer_Type,
                               DebtSecurity.Issuer_Type_Code).distinct()    
    df = pd.DataFrame(sql_obj)

    return df


def get_latest_available_price_of_securities(db_session, filters=None):
    price_subq = db_session.query(DebtPrice.DebtPrice_Id,
                                  func.max(DebtPrice.Trading_Date).label('Last_Trading_Date'))\
                           .filter(DebtPrice.Is_Deleted != 1)\
                           .group_by(DebtPrice.DebtPrice_Id).subquery()
    
    price_qry = db_session.query(DebtPrice.DebtPrice_Id,
                                 DebtPrice.DebtSecurity_Id,
                                 DebtPrice.ISIN,
                                 DebtPrice.Trading_Date,
                                 DebtPrice.Exchange_Code,
                                 DebtPrice.Exchange,
                                 DebtPrice.Segment_Code,
                                 DebtPrice.Segment,
                                 DebtPrice.Local_Code,
                                 DebtPrice.No_Of_Trades,
                                 DebtPrice.Traded_Qty,
                                 DebtPrice.Traded_Value,
                                 DebtPrice.Open,
                                 DebtPrice.High,
                                 DebtPrice.Low,
                                 DebtPrice.Close,
                                 DebtPrice.Weighted_Avg_Price,
                                 DebtPrice.FaceValuePrice,
                                 DebtPrice.Currency,
                                 DebtPrice.WYTM,
                                 DebtPrice.TT_Status,
                                 DebtPrice.Trade_Type,
                                 DebtPrice.Settlement_Type,
                                 DebtPrice.Residual_Maturity_Date,
                                 DebtPrice.Residual_Maturity_Derived_From,
                                 DebtPrice.Clean_Dirty_Indicator,
                                 DebtPrice.Dirty_Price,
                                 DebtPrice.AsofDate)\
                          .join(price_subq, price_subq.c.DebtPrice_Id == DebtPrice.DebtPrice_Id)

    if filters:
        for f in filters:
            column_name = f.get('column')
            column_value = f.get('value')
            column_value_from = f.get('value_from')
            column_value_to = f.get('value_to')

            if column_name == 'DebtSecurity_Id':
                price_qry = price_qry.filter(DebtPrice.DebtSecurity_Id == column_value)

    return price_qry


def get_raw_data_for_securities_exposure(db_session, lst_hsecurity_id : list[int]):

    # set portfolio date to last monthend or previous to last monthend
    today = dt.today()
    prev_month = today - timedelta(weeks=4)
    prev2_prev_month = today - timedelta(weeks=8)
    portfolio_date = last_date_of_month(prev_month.year, prev_month.month) if today.day > 15 else last_date_of_month(prev2_prev_month.year, prev2_prev_month.month)

    sql_underlyingsec = db_session.query(UnderlyingHoldings.Fund_Id,
                                         Plans.Plan_Id,
                                         Fund.Fund_Name,
                                         Product.Product_Name,
                                         UnderlyingHoldings.Value_in_INR,
                                         UnderlyingHoldings.Percentage_to_AUM,
                                         UnderlyingHoldings.Purchase_Date,
                                         UnderlyingHoldings.Portfolio_Date)\
                         .join(Fund, Fund.Fund_Id == UnderlyingHoldings.Fund_Id)\
                         .join(MFSecurity, MFSecurity.Fund_Id == UnderlyingHoldings.Fund_Id)\
                         .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                         .join(PlanProductMapping, PlanProductMapping.Plan_Id == Plans.Plan_Id)\
                         .join(Product, Product.Product_Id == PlanProductMapping.Product_Id)\
                         .join(PlanType, PlanType.PlanType_Id == Plans.PlanType_Id)\
                         .join(Options, Options.Option_Id == Plans.Option_Id)\
                         .filter(UnderlyingHoldings.Is_Deleted != 1,
                                 UnderlyingHoldings.HoldingSecurity_Id.in_(lst_hsecurity_id),
                                 UnderlyingHoldings.Portfolio_Date == portfolio_date,
                                 MFSecurity.Status_Id == 1,
                                 Plans.Is_Deleted != 1,
                                 PlanType.PlanType_Id == 1,
                                 Options.Option_Name.like(r"%G%"))

    total_records = sql_underlyingsec.count()
    sql_underlyingsec = sql_underlyingsec.order_by(UnderlyingHoldings.Value_in_INR.desc())
    resp = [x._asdict() for x in sql_underlyingsec]

    return resp, total_records



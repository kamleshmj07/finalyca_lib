from sqlalchemy import func, desc, asc
from typing import List
from datetime import date as dt, datetime as dtt
import pandas as pd
import numpy as np
import utils

from fin_models.masters_models import DebtSecurity, HoldingSecurity
from fin_models.transaction_models import DebtScreener
from bizlogic.fixed_income_db_helper import get_fi_securities_summary, get_credit_ratings_for_security

def get_issuers_detail(db_session, page, limit, filters=None, sorting=None):

    sql_issuers = db_session.query(DebtSecurity.Issuer,
                                   DebtSecurity.Bilav_Internal_Issuer_Id,
                                   DebtSecurity.Issuer_Type,
                                   DebtSecurity.Issuer_Type_Code)\
                            .filter(DebtSecurity.Is_Deleted != 1)\
                            .distinct()

    # filter the data here based on the filter criteria
    if filters:
        for f in filters:
            column_name = f.get('column')
            column_value = f.get('value')

            if column_name == 'Issuer_Type' and len(column_value) > 0:
                sql_issuers = sql_issuers.filter(DebtSecurity.Issuer_Type.in_(column_value))

            if column_name == 'Issuer' and len(column_value) > 0:
                search = "%{}%".format(column_value)
                sql_issuers = sql_issuers.filter(func.lower(DebtSecurity.Issuer).contains(func.lower(search)))

            if column_name == 'Bilav_Internal_Issuer_Id' and len(column_value) > 0:
                sql_issuers = sql_issuers.filter(DebtSecurity.Bilav_Internal_Issuer_Id.in_(column_value))

    # sort the data here based on the sorting criteria
    if sorting:
        for s in sorting:
            sort_column = s.get('column')
            sort_direction = s.get('direction')
            sort = asc(sort_column) if sort_direction == 'asc' else desc(sort_column) 
            sql_issuers = sql_issuers.order_by(sort)

    total_records = sql_issuers.count()

    if page >= 0 and limit:
        offset = page*limit
        sql_issuers = sql_issuers.offset(offset)
        sql_issuers = sql_issuers.limit(limit)

    # logic for getting characteristics of issuer using a dataframe
    df_issuer = pd.DataFrame(sql_issuers)

    # return None for both result and total_records if there is nothing to return
    if df_issuer.empty:
        return (None, None)
    
    # creating filter for the function
    issuer_id_list = df_issuer["Bilav_Internal_Issuer_Id"].tolist()
    filters = [{'column' : 'Bilav_Internal_Issuer_Id', 'value' : issuer_id_list}]

    df_issuer_details = get_fi_issuer_characteristic(db_session, filters=filters)
    df_issuer_details.drop(['Issuer'], inplace=True, axis=1)
    df_issuer = pd.merge(df_issuer, df_issuer_details, on='Bilav_Internal_Issuer_Id')

    return df_issuer.to_dict(orient="records") , total_records


def get_fi_issuer_characteristic(db_session, filters=None):

    sql_issuers = db_session.query(DebtSecurity.Bilav_Internal_Issuer_Id,
                                   DebtSecurity.Issuer,
                                   DebtSecurity.Issuer_Type,
                                   DebtSecurity.Issue_Size,
                                   DebtSecurity.Is_Secured,
                                   HoldingSecurity.Is_Listed,
                                   DebtSecurity.Issue_Date,
                                   DebtSecurity.Bilav_Code)\
                            .join(HoldingSecurity, HoldingSecurity.Co_Code == func.concat('BLV_', DebtSecurity.Bilav_Code))\
                            .filter(DebtSecurity.Is_Deleted != 1,
                                    HoldingSecurity.Maturity_Date >= dt.today())

    # filter the data here based on the filter criteria
    if filters:
        for f in filters:
            column_name = f.get('column')
            column_value = f.get('value')

            if column_name == 'Bilav_Internal_Issuer_Id' and len(column_value) > 0:
                sql_issuers = sql_issuers.filter(DebtSecurity.Bilav_Internal_Issuer_Id.in_(column_value))

            if column_name == 'Issue_Date' and (not column_value == ""):
                    sql_issuers = sql_issuers.filter(DebtSecurity.Issue_Date > column_value)

    df_issuer = pd.DataFrame(sql_issuers)
    if df_issuer.empty:
        return pd.DataFrame()

    df_issuer.loc[df_issuer['Is_Secured'] == True, 'Secured'] = 1
    df_issuer.loc[df_issuer['Is_Secured'] == False, 'UnSecured'] = 1
    df_issuer.loc[df_issuer['Is_Listed'] == True, 'Listed'] = 1
    df_issuer.loc[df_issuer['Is_Listed'] == False, 'UnListed'] = 1

    df_grp = df_issuer.groupby('Bilav_Internal_Issuer_Id').agg({
        'Bilav_Code': 'count',
        'Issue_Size': 'sum',
        'Secured': 'sum',
        'UnSecured': 'sum',
        'Listed': 'sum',
        'UnListed': 'sum',
        'Issue_Date': 'max',
        'Issuer': 'max'
    })

    df_grp.reset_index(inplace=True)
    df_grp.rename(columns={'Bilav_Code':'Securities'}, inplace=True)
    df_grp.rename(columns={'Issue_Date':'Last_Issuance'}, inplace=True)
    df_grp.rename(columns={'Issue_Size':'Total_Issue_Size'}, inplace=True)
    df_grp['Last_Issuance'] = pd.to_datetime(df_grp['Last_Issuance'], format=r'%Y-%m-%d')
    df_grp['Last_Issuance'] = df_grp['Last_Issuance'].dt.strftime(r'%d %b %Y') # this is required for formatting while converting to json

    return df_grp


def get_fi_cashflows(face_value, coupon_rate, maturity_date, lst_payout_dates, list_redemption_data):

    # prepare the cashflow dataframe for calculation
    df_cf = pd.DataFrame()
    df_cf['Payout_Date'] = lst_payout_dates
    df_cf['Coupon_Rate'] = coupon_rate
    df_cf['Face_Value'] = np.NaN
    df_cf['Is_Redemption'] = False
    df_cf['Redemption_Price'] = np.NaN

    # sort the dataframe by payout dates
    df_cf.sort_values(by=['Payout_Date'], inplace=True)
    df_cf.reset_index(drop=True, inplace=True)
    df_cf['Face_Value'][0] = face_value
    df_cf['Redemption_Price'][0] = np.float64(0.0)


    for record in list_redemption_data:
        if record['Is_Mandatory_Redemption']:
            redemption_date = dtt.fromordinal(record['Redemption_Date'].toordinal())
            closest_date = df_cf.loc[(pd.to_datetime(df_cf['Payout_Date']) - pd.to_datetime(redemption_date)).abs().idxmin(), 'Payout_Date']
            row_index = df_cf.loc[df_cf['Payout_Date'] == closest_date].index

            # if match not found then raise exception
            if len(row_index) == 0:
                raise Exception(f'There was not match for redemption date for the record {record}')

            df_cf.loc[row_index, 'Is_Redemption'] = True
            df_cf.loc[row_index, 'Redemption_Price'] = np.float64(record['Redemption_Price'])
            df_cf.loc[row_index, 'Face_Value'] = np.float64(face_value)
            # calculate new face value and apply to next payment date
            face_value -= np.float64(record['Redemption_Price'])
            next_index = row_index[0]+1
            if df_cf.loc[next_index, 'Face_Value']:
                df_cf.loc[next_index, 'Face_Value'] = np.float64(face_value)

    # forward fill the redemption price and facevalue
    df_cf['Redemption_Price'].ffill(inplace=True)
    df_cf['Face_Value'].ffill(inplace=True)
    df_cf['Redemption_Price'] = pd.to_numeric(df_cf['Redemption_Price'])
    df_cf['Coupon_Rate'] = pd.to_numeric(df_cf['Coupon_Rate'])
    df_cf['Coupon_Value'] = df_cf['Coupon_Rate'] * df_cf['Face_Value'] / 100

    # maturity date redempted price
    row_index = df_cf.loc[df_cf['Payout_Date'] == maturity_date].index
    if len(row_index) != 0:
        df_cf.loc[row_index, 'Is_Redemption'] = True
        df_cf.loc[row_index, 'Redemption_Price'] = df_cf.loc[row_index, 'Face_Value']
    else:
        raise Exception('Maturity Date did not match with any Payout Date from redemptions. Need to review the payout date calculation logic.')

    # add final maturity redemption + coupon value record at end
    df_cf['Payout_Date'] = df_cf['Payout_Date'].dt.strftime(r'%d %b %Y')

    return df_cf


def get_fi_yield_to_maturity(face_value, single_coupon_value, coupon_frequency, total_compounding_periods, curr_mkt_price_of_bond):
    '''
    The YTM here is implemented based on the following references: https://www.wallstreetprep.com/knowledge/yield-to-maturity-ytm/
    '''
    yield_to_maturity = (single_coupon_value + ((face_value - curr_mkt_price_of_bond) / total_compounding_periods)) / ((face_value + curr_mkt_price_of_bond) / 2)

    if coupon_frequency == 'Monthly':
        multiplier = 12
    elif coupon_frequency == 'Quarterly':
        multiplier = 3
    elif coupon_frequency == 'Semi-Annual':
        multiplier = 2
    elif coupon_frequency == 'Annual':
        multiplier = 1

    return yield_to_maturity * multiplier


def get_fi_macaulay_duration(cashflows, effective_coupon_rate):
    '''
    References: https://www.investopedia.com/terms/m/modifiedduration.asp#toc-formula-and-calculation-of-modified-duration
    '''
    df = pd.DataFrame()
    df['Cashflow'] = cashflows
    df['Coupon_Rate'] = effective_coupon_rate
    df.reset_index(inplace=True)
    df.rename(columns={'index' : 'Period'}, inplace=True)
    df['Period'] = df['Period'] + 1
    df['Denominator1'] = (1 + (df['Coupon_Rate']/100))**df['Period']
    df['PVn'] = df['Cashflow']/df['Denominator1']
    market_price = df['PVn'].sum()
    df['Duration_n'] = df['PVn'] * df['Period'] / market_price
    macaulay_duration = df['Duration_n'].sum()

    return macaulay_duration


def get_fi_coupon_and_pay_dates(lst_payout_dates: List[str], maturity_date: dtt, coupon_rate, coupon_frequency):

    effective_coupon_rate, compounding_periods = get_fi_effective_coupon_rate_with_compounding_periods(coupon_rate, coupon_frequency)

    next_coupon_date = dtt.today()
    year = dt.today().year
    lst_coupon_dates = []

    if coupon_frequency != 'Blank':
        while next_coupon_date < maturity_date:
            for i in range(compounding_periods):
                next_coupon_date = lst_payout_dates[i]
                month = int(next_coupon_date[-2:])
                day = int(next_coupon_date[:-2])
                next_coupon_date = dtt(year, month, day)
                lst_coupon_dates.append(next_coupon_date)

            year += 1

    return (lst_coupon_dates, effective_coupon_rate, compounding_periods)


def get_fi_effective_coupon_rate_with_compounding_periods(coupon_rate, coupon_frequency):
    compounding_periods = 0

    if coupon_frequency == 'Monthly':
        compounding_periods = 12
        coupon_rate /= 12
    elif coupon_frequency == 'Quarterly':
        compounding_periods = 3
        coupon_rate /= 4
    elif coupon_frequency == 'Semi-Annual':
        compounding_periods = 2
        coupon_rate /= 2
    elif coupon_frequency == 'Annual':
        compounding_periods = 1
    else:
        coupon_rate = 0

    return coupon_rate, compounding_periods


def prepare_fixed_income_screener(db_session):

    try:
        # is_production = False
        utils.truncate_table(table_name='PMS_Base.Transactions.DebtScreener')

        result, total_records = get_fi_securities_summary(db_session, page=-1, limit=None)
        df = pd.DataFrame(result)
        
        columns = df.columns
        df.columns = map(str.lower, columns)
        drop_columns = ['debtsecurity_id', 'exchange_1', 'exchange_1_local_code', 'exchange_2','exchange_2_local_code',
                        'exchange_3', 'exchange_3_local_code',  'bilav_internal_issuer_id', 'bilav_code', 'lei', 'cin',
                        'issue_price', 'issue_date', 'interest_payout_1', 'current_yield', 'sector_code', 'tenure_remaining']
        df.drop(labels=drop_columns, axis=1, inplace=True)
        df["data_date"] = dtt.today()
        df['is_perpetual'] = np.where(df['is_perpetual'].isnull(), False, df['is_perpetual'])    # TODO: logic incomplete here, need to update. 
        df['is_senior'] = np.where(df['is_senior'].isnull(), False, np.where(df['is_senior'] == 'Senior', True, df['is_senior']))
        df['interest_commencement_date'] = np.where(df['interest_commencement_date'] == dt(year=1, month=1, day=1), None, df['interest_commencement_date'])
        df['interest_payment_frequency'] = np.where(df['coupon_type'] == 'Zero Coupon', 'Zero', df['interest_payment_frequency'])


        df_ratings = pd.DataFrame(get_credit_ratings_for_security(db_session))[['ISIN', 'Rating_Agency', 'Rating_Symbol']]
        df_ratings = df_ratings.pivot(index='ISIN', columns='Rating_Agency', values='Rating_Symbol').rename_axis(columns='').reset_index()
        columns = df_ratings.columns
        df_ratings.columns = map(str.lower, columns)

        # replace NaN with N/A for ratings
        df_ratings = df_ratings.replace({np.nan: 'N/A'})
        df_ = pd.merge(df, df_ratings, left_on='isin', right_on='isin', how='left')
        columns = list(map(str.lower, columns))
        df_[columns] = df_[columns].fillna('N/A')

        # replace NaN with N/A for sectors
        df_['sector_name'] = df_['sector_name'].fillna('N/A')

        # update the DebtScreener
        numeric_columns = ['maturity_price','min_investment_amount','issue_size','yield_at_issue','markup',
                           'minimum_interest_rate','maximum_interest_rate','latest_applied_intpy_annual_coupon_rate',
                           'interest_rate','face_value','paid_up_value']
        df_[numeric_columns].fillna(value=0.00, inplace=True)
        df_ = df_.loc[:, ~df_.columns.duplicated()]
        df_[numeric_columns] = df_[numeric_columns].apply(pd.to_numeric, errors='coerce')
        df_ = df_.replace({np.nan: None})

        update_debt_screener(db_session, df_.to_dict(orient="records"))

    except Exception as ex:
        raise Exception(f"Error: {ex}")


def update_debt_screener(db_session, lst_data):

    for record in lst_data:
        try:
            debt_screener_obj = DebtScreener(**record)
            db_session.add(debt_screener_obj)
        except Exception as ex:
            print(f"Exception while importing debt security credit ratings data {record['isin']} - {ex}")
            continue

    db_session.commit()


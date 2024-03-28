from data.benchmark import get_monthly_nav
import pandas as pd
import numpy as np
from utils.utils import print_query
from dateutil.relativedelta import relativedelta
from bizlogic.common_helper import get_benchmarkdetails, calculate_benchmark_tri_returns, calculate_xirr
from analytics.analytics import calculate_risk_ratios
from datetime import datetime

def get_monthly_returns(db_session, plan_id, is_benchmark, end_date, last_month_count):
    start_date = end_date - relativedelta(months=last_month_count)
    tri_co_code = None

    if is_benchmark:
        benchmark_details = get_benchmarkdetails(db_session, plan_id)
        if benchmark_details:
            if benchmark_details.TRI_Co_Code:
                tri_co_code = benchmark_details.TRI_Co_Code
            else:
                tri_co_code = benchmark_details.Co_Code #if it is TRI

    if tri_co_code:
        start_date = end_date - relativedelta(months=last_month_count-1)
        df = calculate_benchmark_tri_returns(db_session, tri_co_code, start_date, end_date, get_monthly_return_df=True)
        return df  
    else:
        query = get_monthly_nav(db_session, plan_id, is_benchmark, start_date, end_date)

        df = pd.DataFrame(query)
        if not df.empty:
            df["NAV_Date"] = df["NAV_Date"].apply(pd.to_datetime)
            df["old_NAV"] = df.shift(periods=1)["NAV"]
            df["month"] = df["NAV_Date"].dt.strftime('%Y-%m')            
            df["returns"] = ((df["NAV"] - df["old_NAV"])/ df["old_NAV"])*100
            df = df.drop(["NAV_Date", "NAV", "old_NAV"], axis=1)
            # First row will be all Null
            df = df.iloc[1:]

        return df    

def get_risk_ratios(db_session, plan_id, benchmark_id, transaction_date, time_period_in_months, risk_free_index_rate):    
    plans_df = get_monthly_returns(db_session, plan_id, False, transaction_date, time_period_in_months)
    index_df = get_monthly_returns(db_session, benchmark_id, True, transaction_date, time_period_in_months)
    if not plans_df.empty and not index_df.empty:
        mean = plans_df["returns"].mean()
        plans_df["fund_var"] = plans_df["returns"] - mean
        plans_df["fund_var_sqaure"] = plans_df["fund_var"].pow(2)
    
        mean = index_df["returns"].mean()
        index_df["fund_var"] = index_df["returns"] - mean
        index_df["fund_var_sqaure"] = index_df["fund_var"].pow(2)

        return calculate_risk_ratios(plans_df, index_df, risk_free_index_rate)
    else:
        return None


def calculate_investment_style_for_stocks(df_f):

    df_f = df_f.loc[df_f.groupby('CO_CODE').PriceDate.idxmax(),:]   # grab the latest available information from last 5 days for each co_code
    df_f.sort_values(by=['mcap'], ascending=False, inplace=True) # sort the df by mcap descending
    df_f.reset_index(drop=True, inplace=True)   # drop & reset index

    # Step 1: Set Market Cap based classification by SEBI
    # https://www.etmoney.com/learn/mutual-funds/sebi-issues-new-rules-for-multi-cap-funds-what-should-the-investors-do/
    df_f["MCap_Classification"] = ""
    classification = {
        0: "Large-Cap",
        100: "Mid-Cap",
        250: "Small-Cap",
    }

    # Assign the respective index with the values and then forward fill them
    df_f["MCap_Classification"] = df_f.index.map(classification).values
    df_f["MCap_Classification"] = df_f["MCap_Classification"].fillna(method="ffill")

    # Step 1.1: Calculate the market cap totals based on the Step 1 classification
    # total_mcap = df_f['mcap'].sum()
    total_mcap_lrgcap = df_f[df_f['MCap_Classification'] == classification[0]]['mcap'].sum()
    total_mcap_midcap = df_f[df_f['MCap_Classification'] == classification[100]]['mcap'].sum()
    total_mcap_smlcap = df_f[df_f['MCap_Classification'] == classification[250]]['mcap'].sum()

    # Step 2: Calculate the valuation score
    df_f['Valuation_Score'] = 0.0
    df_f.loc[df_f['MCap_Classification'] == classification[0], 'Valuation_Score'] = ((df_f['PE'] + df_f['PBV']) * df_f['mcap'])/total_mcap_lrgcap
    df_f.loc[df_f['MCap_Classification'] == classification[100], 'Valuation_Score'] = ((df_f['PE'] + df_f['PBV']) * df_f['mcap'])/total_mcap_midcap
    df_f.loc[df_f['MCap_Classification'] == classification[250], 'Valuation_Score'] = ((df_f['PE'] + df_f['PBV']) * df_f['mcap'])/total_mcap_smlcap

    # Step 2.1: Calculate the median valuation score as per classification
    df_f['Median_Valuation_Score'] = np.NAN
    df_f.loc[0, 'Median_Valuation_Score'] = df_f[df_f['MCap_Classification'] == classification[0]]['Valuation_Score'].median()
    df_f.loc[100, 'Median_Valuation_Score'] = df_f[df_f['MCap_Classification'] == classification[100]]['Valuation_Score'].median()
    df_f.loc[250, 'Median_Valuation_Score'] = df_f[df_f['MCap_Classification'] == classification[250]]['Valuation_Score'].median()
    df_f["Median_Valuation_Score"] = df_f["Median_Valuation_Score"].fillna(method="ffill")

    # Step 3: Calculate the Investment Style
    df_f['Equity_Style'] = ''   # naming according to holdingsecurity table
    df_f.loc[df_f['Valuation_Score'] >= 1.15*df_f['Median_Valuation_Score'] , 'Equity_Style'] = 'Growth'
    df_f.loc[(df_f['Valuation_Score'] < 1.15*df_f['Median_Valuation_Score']) & (df_f['Valuation_Score'] > 0.85*df_f['Median_Valuation_Score']), 'Equity_Style'] = 'Blend'
    df_f.loc[df_f['Valuation_Score'] <= 0.85*df_f['Median_Valuation_Score'], 'Equity_Style'] = 'Value'

    # df_f.to_excel(r'C:\dev\backend\tasks\read\cmots\investmentstyle.xlsx')
    return df_f


def calculate_portfolio_pe(df_holdings, is_index=False):
    '''
    Expected column names are: 
        >> total_price, pe, isin, name, instrument_type, issuer
    is_index: Flag to be used if benchmark pe is calculated using different methodology
    # TODO : Implement normalization for any category basis for pe calculation
    '''
    column_names = df_holdings.columns

    if not is_index:
        if not ('weight' in column_names):
            df_holdings['total_price_new'] = np.where((df_holdings['pe'] > 0), df_holdings['total_price'], 0)
            total_price_sum = df_holdings['total_price_new'].sum()

            df_holdings['weight'] = df_holdings['total_price_new']/total_price_sum

        df_holdings['weight'] = df_holdings['weight'].fillna(0).astype(float)
        df_holdings['pe'] = df_holdings['pe'].fillna(0).astype(float)

        df_holdings['weighted_pe'] = df_holdings['pe'] * df_holdings['weight']

        sum_weighted_pe = df_holdings['weighted_pe'].sum()
    else:
        # TODO: Can use this space to calculate the benchmark pe as per the market standard 
        # Total Mcap / Total of (EPS * Total Shares) i.e. Total Earnings
        pass


    return sum_weighted_pe


def generate_portfolio_characteristics(df_holdings):
    '''
    Expected column names are:
        >> name, isin, weight, div_yld, eps, pe, pbv, mcap
    # TODO 1. We will also have to implement the ratios required for a debt fund perspective under this function
    # TODO 2. We will have to create separate functions for calculating individual ratios
    '''
    result = {}

    # total stocks
    total_stocks = df_holdings.shape[0]
    result['total_stocks'] = total_stocks

    sum_wts = df_holdings['weight'].sum()
    df_holdings['weights_rebased'] = ((df_holdings['weight'].astype(float) / float(sum_wts)) * 100) if sum_wts else 0

    # pe calculation
        # step 1: pick weights only for pe greater than zero i.e. positive earnings
    df_pe = df_holdings[df_holdings['pe'] > 0]
    sum_wts_pe = df_pe['weight'].sum()
    df_pe['weights_pe_rebased'] = ((df_pe['weight'].astype(float) / float(sum_wts_pe)) * 100)

    df_pe['PE_Per'] = (1 / df_pe['pe'])
    df_pe['PE_Ratio'] = (df_pe['weights_pe_rebased']/100) * (df_pe['PE_Per'])
    sum_pe = df_pe['PE_Ratio'].sum()
    portfolio_pe = 1/sum_pe if sum_pe != 0 else None
    result['p_e'] = round(portfolio_pe,2) if portfolio_pe else None

    # pbv calculation
    df_holdings['PB_Per'] = (1 / df_holdings['pbv'])
    df_holdings['PB_Ratio'] = (df_holdings['weights_rebased']/100) * (df_holdings['PB_Per'])
    sum_pbv = df_holdings['PB_Ratio'].sum()
    portfolio_pbv = 1/sum_pbv if sum_pbv != 0 else None
    result['p_b'] = round(portfolio_pbv,2) if portfolio_pbv else None

    # div yield calculation
    df_dy = df_holdings[df_holdings['div_yld'] > 0]
    sum_wts_dy = df_dy['weight'].sum()
    df_dy['weights_dy_rebased'] = ((df_dy['weight'].astype(float) / float(sum_wts_dy)) * 100)

    df_dy["DY_Per"] =  (1 / df_dy['div_yld'])
    df_dy["DY_Ratio"] = (df_dy['weights_dy_rebased']/100) * (df_dy['DY_Per'])
    sum_dy = df_dy['DY_Ratio'].sum()
    portfolio_div_yld = 1/sum_dy if sum_dy != 0 else None
    result['dividend_yield'] = round(portfolio_div_yld,2) if portfolio_div_yld else None

    # average market cap
    avg_mcap = df_holdings['mcap'].mean()
    result['avg_mkt_cap'] = round(avg_mcap,2) if avg_mcap else None

    # median market cap
    med_mcap = df_holdings['mcap'].median()
    result['median_mkt_cap'] = round(med_mcap,2) if med_mcap else None

    return result

def calculate_script_xirr(isin, df_transactions, df_holdings=pd.DataFrame(None)):
    '''
    Expected column names are: 
        >> isin, total_price, tran_date
    '''
    df = df_transactions.loc[df_transactions['isin'] == isin] if isin else df_transactions
    df_holding = df_holdings.loc[df_holdings['isin'] == isin] if isin else df_holdings

    if not df.empty:
        df.loc[df['tran_type'] == 'B', 'total_price'] = -df['total_price']

        df_xirr = df[['tran_date', 'total_price']]
        df_xirr.rename(columns = {'tran_date':'date', 'total_price':'value'}, inplace = True)
        
        #add holdings as transaction - sell
        for index, row in df_holding.iterrows():
            # check if df_holding isin is available in transaction in case of xirr for full portfolio
            df_tran_available = df_transactions.loc[df_transactions['isin'] == row['isin']]

            if not df_tran_available.empty:
                dt = datetime.combine(row['end_date'], datetime.min.time()) if row['end_date'] else None
                price = row['end_price'] if row['end_price'] else None
                
                if dt and price:
                    df_xirr.loc[len(df_xirr.index)] = [dt, price] 
        
        df_xirr.sort_values(by=['date'], ascending=True, inplace=True) # sort the df by mcap descending
        df_xirr.reset_index(drop=True, inplace=True)   # drop & reset index
        
        return calculate_xirr(df_xirr)
    
    return None

from operator import itemgetter
import logging
import locale
from datetime import datetime, date
# HACK: TODO: This must not be here. Must be validated in the frontend.
from werkzeug.exceptions import BadRequest
from copy import deepcopy
from compass.investor_portfolio import get_portfolio_performance_analysis_by_date, get_normalized_portfolio_holdings
from compass.portfolio_helper import create_portfolio_report_only, get_account_aggregation_report
from compass.portfolio_analysis import get_investment_style_exposure, get_instrument_type_exposure,\
                                        get_issuer_exposure, get_sector_exposure
from compass.investor_portfolio import get_portfolio_performance_analysis_for_all_period, attach_date_and_convert
from compass.portfolio_db import get_investorholdings_as_of_dates_by_account_id
from bizlogic.common_helper import get_benchmark_trailing_returns_for_all_period, get_fund_historic_performance,\
                                    get_plan_meta_info, get_investment_style_from_df, get_benchmarkdetails,\
                                    calculate_benchmark_tri_returns, get_index_instrument_type
from reports.visuals import get_geo_location_chart, get_trand_analysis_stacked_chart, get_pie_chart,\
                            get_barchart, get_line_chart
from reports.utils import get_table_from_html_template, prepare_pdf_from_html, get_html_from_dataframe, to_decimal_str, to_money_str, to_percentage_str, to_integer_str
import math
from compass.portfolio_db import prepare_raw_holdings_from_db, get_investorid_by_account_id
from compass.portfolio_crud import get_investor_dashboard_info
# from utils.utils import get_image_path, get_whitelabel_path, get_config_by_key
from bizlogic.importer_helper import get_organizationid_by_userid, get_organization_whitelabel
import pandas as pd
from reports.df_to_html_interface import df_to_html_column_type, currency_type, ReportTableColumnInfo




color_list = ['#153a6d', '#FF971A', '#990F02', '#bcd83f', '#046d1fad', '#6ebd32', '#CFBCF9', '#6C9686',
    '#ECA19D','#F0F97E','#EB9D7D','#BDC920','#B48E36','#ffeb44','#FFA87F','#ECD096','#88DBE1' ]

def prepare_portfolio_report(db_session, user_id, account_ids, portfolio_date, from_date, to_date, plan_id, benchmark_id, is_detailed, show_trend_analysis, show_performance, show_stock_details, page_break_required, image_path, whitelabel_dir, generatereport_dir, time_period_type):
    try:
        template_vars = dict()
        
        template_vars['portfolio_date_label'] = datetime.strftime(portfolio_date,'%d %b %Y')
        template_vars['from_date_label'] = datetime.strftime(from_date,'%d %b %Y')
        template_vars['to_date_label'] = datetime.strftime(to_date,'%d %b %Y')

        template_vars["page_break_class"] = "breakpage" if page_break_required else "donotbreak"
        template_vars["hide_trend_analysis"] = True if not show_trend_analysis else False
        template_vars["hide_performance"] = True if not show_performance else False
        template_vars["hide_stock_details"] = True if not show_stock_details else False

        template_vars['image_path'] = image_path
        template_vars['whitelabel_dir'] = whitelabel_dir
        template_vars['generatereport_dir'] = generatereport_dir
        
        template_vars1 = get_portfolio_report_data(db_session, account_ids, user_id, portfolio_date, from_date, to_date, time_period_type, plan_id, benchmark_id, not show_stock_details, not show_trend_analysis, is_detailed)

        #Merge both dict
        template_vars.update(template_vars1)
        
        return prepare_pdf_from_html(template_vars, 'portfolio_report_1_template.html', template_vars['generatereport_dir'])
    except Exception as ex:
        logging.warning(F"Delivery request - portfolio pdf generation - {ex}" )

def prepare_nested_holding_response(data):
    parent_data = list()
    for parent_key in data:
        parent_dict = dict()
        parent_dict["label"] = parent_key
        amount = 0
        no_0f_accounts = 0
        weights = 0

        a = 1
        child1_data = list()
        for child1_key in data[parent_key]:
            child1_dict = dict()
            child1_dict["label"] = child1_key
            child1_amount = 0
            child1_no_0f_accounts = 0
            child1_weights = 0

            child2_data = list()
            b = data[parent_key][child1_key]
            for child2_key in data[parent_key][child1_key]:
                child2 = data[parent_key][child1_key][child2_key]

                child1_amount = child1_amount + (child2['total_price'] if child2['total_price'] else 0)
                child1_no_0f_accounts = child1_no_0f_accounts + 1
                child1_weights = child1_weights + (child2['weight'] if child2['weight'] else 0)

                child2_dict = dict()
                child2_dict["total_price"] = F"₹&nbsp;{format( int( round(child2['total_price'], 2) ) , ',') if round(child2['total_price'], 2) else 0}"
                child2_dict["weight"] = round(child2['weight'], 2)
                child2_dict["units"] = F"₹&nbsp;{format(child2['units'], ',')}" if child2['units'] else 0
                child2_dict["account_alias"] = child2['account_alias']
                child2_dict["label"] = child2_key
                child2_data.append(child2_dict)
            
            child1_dict['total_price'] = F"₹&nbsp;{format(int(round((child1_amount if child1_amount else 0), 2)), ',') if round((child1_amount if child1_amount else 0), 2) else 0}"
            
            child1_dict['no_0f_accounts'] = format(child1_no_0f_accounts, ',') if child1_no_0f_accounts else 0
            child1_dict['weight'] = round((child1_weights if child1_weights else 0), 2)
            child1_dict['child_data'] = child2_data
            child1_data.append(child1_dict)

            amount = amount + (child1_amount if child1_amount else 0)
            no_0f_accounts = no_0f_accounts + child1_no_0f_accounts
            weights = weights + (child1_weights if child1_weights else 0)
        
        parent_dict['total_price'] = F"₹&nbsp;{format(int(round(amount, 2)), ',') if round(amount, 2) else 0}"
        parent_dict['no_0f_accounts'] = format(no_0f_accounts, ',') if no_0f_accounts else 0
        parent_dict['weight'] = round(weights, 2)
        parent_dict['child_data'] = child1_data
        parent_data.append(parent_dict)
        

    return parent_data


def get_portfolio_report_data(db_session, account_ids, user_id, portfolio_date, from_date, to_date, time_period_type, plan_id, benchmark_id, hide_stock_details, hide_trend_analysis, is_detailed):
    # locale.setlocale(locale.LC_MONETARY, 'en_IN.UTF-8')
    final_resp = dict()
    final_resp['report_date'] = datetime.strftime(date.today(),'%d %b %Y')

    investor_id = get_investorid_by_account_id(db_session, account_ids[0])
    final_resp['investor_details'] = get_investor_dashboard_info(db_session, investor_id, user_id)  
    final_resp['organization_whitelabel_data'] = get_organization_whitelabel(db_session, get_organizationid_by_userid(db_session, user_id))

    fund_info = get_plan_meta_info(db_session,[plan_id])
    final_resp['fund_info'] = fund_info[str(plan_id)]

    holdings_df = get_normalized_portfolio_holdings(db_session, account_ids, portfolio_date, is_detailed)
    if holdings_df.empty:
        pf_date = datetime.strftime(portfolio_date, '%Y-%m-%d')
        raise BadRequest(F"No Portfolio found for {pf_date}")

    performance_holdings_df = get_normalized_portfolio_holdings(db_session, account_ids, portfolio_date, False)
    portfolio_performance_analysis = get_portfolio_performance_analysis_by_date(db_session, performance_holdings_df, from_date, to_date)
    raw_holdings = prepare_raw_holdings_from_db(db_session, account_ids, portfolio_date, is_detailed)
    results = create_portfolio_report_only(raw_holdings, False, False)
    response = dict()

    for key, exposure in results.items():
        if key != 'sectors':
            response[key] = exposure.to_dict(orient="records")  
        else:
            #TODO we need to work on below. as of now we have ony considered LISTED EQUITY.
            response[key] = results['sectors']['LISTED EQUITY'] if 'LISTED EQUITY' in results['sectors'] else []

    #prepare overview - start
    overview_dict = dict()    
    overview_dict['equity_new_value'] = to_money_str(portfolio_performance_analysis['equity_new_value']) 
    overview_dict['equity_old_value'] = to_money_str(portfolio_performance_analysis['equity_old_value']) 
    overview_dict['equity_performance'] = round(portfolio_performance_analysis['equity_performance'],2) if portfolio_performance_analysis['equity_performance'] else 0
    overview_dict['portfolio_pe'] = round(portfolio_performance_analysis['portfolio_pe'],2) if portfolio_performance_analysis['portfolio_pe'] else 0

    #get ETF performance
    overview_dict['etf_performance'] = ''
    overview_dict['etf_name'] = ''  
    etf_perf = get_fund_historic_performance(db_session, plan_id, from_date, to_date, time_period_type, None)
    if etf_perf:
        meta_info = get_plan_meta_info(db_session, [plan_id])
        overview_dict['etf_name'] = meta_info[str(plan_id)]['plan_name']          
        overview_dict['etf_performance'] = float(etf_perf[0]['performance'])
    
    #get benchmark return
    non_tri_benchmark_id = None
    benchmark_details = get_benchmarkdetails(db_session, benchmark_id)
    co_code = benchmark_details.Co_Code
    tri_co_code = benchmark_details.TRI_Co_Code if benchmark_details.TRI_Co_Code else benchmark_details.Co_Code
    
    non_tri_benchmarkindices = get_benchmarkdetails(db_session, None, tri_co_code)

    if non_tri_benchmarkindices:
        non_tri_benchmark_id = non_tri_benchmarkindices.BenchmarkIndices_Id

    if co_code:        
        cum_return = calculate_benchmark_tri_returns(db_session, co_code, from_date, to_date, typeof_return='absolute')

        overview_dict['benchmark_performance'] = cum_return
        overview_dict['benchmark_name'] = benchmark_details.BenchmarkIndices_Name

    final_resp['overview_html'] = prepare_pdf_from_html(overview_dict, 'overview.html', '', True)
    #prepare overview - End


    
    final_resp['equity_new_value'] = overview_dict['equity_new_value']
    final_resp['benchmark_name'] = overview_dict['benchmark_name']


    df_performance = pd.DataFrame(portfolio_performance_analysis['performance'])
    # df.replace(r'\s+', np.nan, regex=True)
    report_column_config = list()

    report_column_config.append(ReportTableColumnInfo(name='name', label='Company', type=df_to_html_column_type.String, width_in_percent=30))
    report_column_config.append(ReportTableColumnInfo(name='units', label='Units', type=df_to_html_column_type.Decimal, width_in_percent=10))
    report_column_config.append(ReportTableColumnInfo(name='start_unit_price', label=F'Unit Price as on: {datetime.strftime(from_date,"%d %b %Y")}', type=df_to_html_column_type.Money, width_in_percent=10, currency_type=currency_type.INR))    
    report_column_config.append(ReportTableColumnInfo(name='start_price', label=F'Value as on: {datetime.strftime(from_date,"%d %b %Y")}', type=df_to_html_column_type.Money, width_in_percent=10, currency_type=currency_type.INR))
    report_column_config.append(ReportTableColumnInfo(name='end_unit_price', label=F'Unit Price as on: {datetime.strftime(to_date,"%d %b %Y")}', type=df_to_html_column_type.Money, width_in_percent=14,currency_type=currency_type.INR))
    report_column_config.append(ReportTableColumnInfo(name='end_price', label=F'Value as on: {datetime.strftime(to_date,"%d %b %Y")}', type=df_to_html_column_type.Money, width_in_percent=10, currency_type=currency_type.INR))
    report_column_config.append(ReportTableColumnInfo(name='weight', label='Weight', type=df_to_html_column_type.Percent, width_in_percent=8))
    report_column_config.append(ReportTableColumnInfo(name='change_pr', label='Absolute', type=df_to_html_column_type.Percent, width_in_percent=8))
    
    final_resp['portfolio_performance_analysis'] = get_html_from_dataframe(df=df_performance, columns_config=report_column_config,  header_font_size_in_pt=6, body_font_size_in_pt=6)
     
    
    #Equity style box - Start
    equity_style_box_template_dict = dict()
    investment_style_data = get_investment_style_exposure(holdings_df, False)
    equity_style_box_template_dict['investment_style_data'] = get_investment_style_from_df(investment_style_data)
    equity_style_box_template_dict['investment_style_total_securities'] = len(holdings_df)
    equity_style_box_template_dict['investment_style_not_classified'] = len(holdings_df) - (investment_style_data["security_count"].sum())
    final_resp['equity_style_box_html'] = prepare_pdf_from_html(equity_style_box_template_dict, 'equity_style_box.html', '', True)
    #Equity style box - End    



    #Asset allocation - start
    instrument_type_template_dict = dict()
    index_instrument_type = get_index_instrument_type(db_session, non_tri_benchmark_id, from_date, to_date)    
    if response:        
        data_label = []
        instrument_type_data = []
        i = 0;
        for dt in response["instrument_types"]:
            instrumenttype_dict = dict()
            instrumenttype_dict = {
                'title':dt['instrument_type'],
                'values':round(float(dt["weight"]),2) if dt["weight"] else 0
            }
            data_label.append(instrumenttype_dict)

            instrumenttype_dict['value'] = str(instrumenttype_dict['values'])
            instrumenttype_dict['color_code'] = color_list[i] if i<len(color_list) else None
            instrumenttype_dict['index_value'] = '0.0'

            #check instrument type for index
            if index_instrument_type:
                idx_instr = list(filter(lambda instr_type: instr_type['HoldingSecurity_Type'] == dt['instrument_type'], index_instrument_type))
                if idx_instr:
                    instrumenttype_dict['index_value'] = round(float(idx_instr[0]['Percentage_to_AUM']),2)

            instrument_type_data.append(instrumenttype_dict)

            i = i+1

        instrument_type_template_dict["instrument_type_portfolio_value"] = instrument_type_data
        instrument_type_template_dict["instrument_type_chart_html"] = get_pie_chart(data_label, '', False, 10, False, color_list, True, False, 300, 900)
        final_resp['instrument_type_html'] =  prepare_pdf_from_html(instrument_type_template_dict, 'asset_allocation.html', '', True)
        #Asset allocation - End



        #Geo allocation - Start
        geo_allocation_dict = dict()
        geo_allocation_dict['geo_allocation_data_html'] = get_geo_location_chart(response['geo_allocation'], 12, 220, 330, ('#537db7', '#245396', '#cfc95a', '#bdb50c'))
        final_resp["geo_allocation_html"] = prepare_pdf_from_html(geo_allocation_dict, 'geo_allocation.html', '', True)
        #Geo allocation - End

        

        #Sectors - start
        sector_dict = prepare_donut_chart(response["sectors"], 'sector', 'Sector Weight')
        final_resp['sector_weight_value'] = sector_dict['data']
        final_resp['sector_weight_html'] = prepare_pdf_from_html(sector_dict, 'donut_chart.html', '', True)        
        #Sectors - End



        #Issuers - start
        issuer_dict = prepare_donut_chart(response["issuers"], 'issuer', 'Issuer Weight')
        final_resp['instrument_type_portfolio_value'] = issuer_dict['data']
        final_resp['issuer_weight_html'] = prepare_pdf_from_html(issuer_dict, 'donut_chart.html', '', True)        
        #Issuers - End



        #equity_style - start
        equity_style_dict = prepare_donut_chart(response["equity_style"], 'equity_style', 'Equity Style')
        final_resp['equity_style_html'] = prepare_pdf_from_html(equity_style_dict, 'donut_chart.html', '', True)        
        #equity_style - End



        #market_cap - start
        market_cap_dict = prepare_donut_chart(response["market_cap"], 'market_cap', 'Market Cap')
        final_resp['market_cap_html'] = prepare_pdf_from_html(market_cap_dict, 'donut_chart.html', '', True)        
        #market_cap - End



        df_holding = pd.DataFrame(response["securities"]).copy()
        holding_header = F'Top {10 if len(response["securities"])>=10 else len(response["securities"])} holdings out of {len(response["securities"])}'
                
        hol_report_column_config = list()
     
        hol_report_column_config.append(ReportTableColumnInfo(name='name', label=holding_header, type=df_to_html_column_type.String, width_in_percent=40))
        hol_report_column_config.append(ReportTableColumnInfo(name='isin', label='Symbol', type=df_to_html_column_type.String, width_in_percent=15))
        hol_report_column_config.append(ReportTableColumnInfo(name='asset_class', label='Asset Class', type=df_to_html_column_type.String, width_in_percent=20))
        hol_report_column_config.append(ReportTableColumnInfo(name='total_price', label='Holding Value', type=df_to_html_column_type.Money, width_in_percent=15, currency_type=currency_type.INR))
        hol_report_column_config.append(ReportTableColumnInfo(name='weight', label='Assets', type=df_to_html_column_type.Percent, width_in_percent=10))

        final_resp['custom_holding'] = get_html_from_dataframe(df=df_holding.head(10), columns_config=hol_report_column_config, header_font_size_in_pt=6, body_font_size_in_pt=6)

        #Investment Performance graph - start
        performance_dict = dict()
        res_fact = dict()
        periods = ['1m', '3m', '6m', '1y', '3y', '5y']

        fund_trailing_returns = get_portfolio_performance_analysis_for_all_period(db_session, performance_holdings_df, portfolio_date, periods)

        if not co_code:
            raise BadRequest('Data Error: No benchmark found.')

        benchmark_trailing_returns = get_benchmark_trailing_returns_for_all_period(db_session, benchmark_id, co_code, portfolio_date)

        for period in periods:
            if fund_trailing_returns and benchmark_trailing_returns:
                res_fact[F"scheme_ret_{period}"] =round(fund_trailing_returns[period]['equity_performance'],2) if fund_trailing_returns[period] and not math.isnan(fund_trailing_returns[period]['equity_performance']) else None

                res_fact[F"bm_ret_{period}"] = round(benchmark_trailing_returns[period]['Returns_Value'],0) if benchmark_trailing_returns[period]  and not math.isnan(benchmark_trailing_returns[period]['Returns_Value']) else None

        # active returns
        if res_fact:
            res_fact["active_1m"] = round((res_fact["scheme_ret_1m"] if res_fact["scheme_ret_1m"] else 0) - (res_fact["bm_ret_1m"] if res_fact["scheme_ret_1m"] else 0),2)
            res_fact["active_3m"] = round((res_fact["scheme_ret_3m"] if res_fact["scheme_ret_3m"] else 0) - (res_fact["bm_ret_3m"] if res_fact["bm_ret_3m"] else 0),2)
            res_fact["active_6m"] = round((res_fact["scheme_ret_6m"] if res_fact["scheme_ret_6m"] else 0) - (res_fact["bm_ret_6m"] if res_fact["bm_ret_6m"] else 0),2)
            res_fact["active_1y"] = round((res_fact["scheme_ret_1y"] if res_fact["scheme_ret_1y"] else 0) - (res_fact["bm_ret_1y"] if res_fact["bm_ret_1y"] else 0),2)
            res_fact["active_3y"] = round((res_fact["scheme_ret_3y"] if res_fact["scheme_ret_3y"] else 0) - (res_fact["bm_ret_3y"] if res_fact["bm_ret_3y"] else 0),2)
            res_fact["active_5y"] = round((res_fact["scheme_ret_5y"] if res_fact["scheme_ret_5y"] else 0) - (res_fact["bm_ret_5y"] if res_fact["bm_ret_5y"] else 0),2)

        #Fund Performance data
        final_resp["performance_data"] = res_fact
        if res_fact:
            x_labels = ['1 Month','3 Month','6 Month','1 Year','3 Years','5 Years']
            data_label = []
            # portfolio
            fund_perf = []
            fund_perf.append(res_fact["scheme_ret_1m"]) 
            fund_perf.append(res_fact["scheme_ret_3m"]) 
            fund_perf.append(res_fact["scheme_ret_6m"]) 
            fund_perf.append(res_fact["scheme_ret_1y"]) 
            fund_perf.append(res_fact["scheme_ret_3y"])
            fund_perf.append(res_fact["scheme_ret_5y"])
            fund_dict = {
                'title':'Portfolio Returns',
                'values':fund_perf
            }
            data_label.append(fund_dict)

            # benchmarkmark
            bm_perf = []
            bm_perf.append(res_fact["bm_ret_1m"])
            bm_perf.append(res_fact["bm_ret_3m"])
            bm_perf.append(res_fact["bm_ret_6m"])
            bm_perf.append(res_fact["bm_ret_1y"])
            bm_perf.append(res_fact["bm_ret_3y"])
            bm_perf.append(res_fact["bm_ret_5y"])
            bm_dict = {
                'title':'Benchmark Returns',
                'values':bm_perf
            }
            data_label.append(bm_dict)

            encoded_string = get_barchart(x_labels,data_label, False, "", 11, 350, False, False, False)
            performance_dict["performance_chart_src"] = encoded_string
            performance_dict["performance_values"] = res_fact
        else:
            performance_dict["performance_chart_src"] = None
            performance_dict["performance_values"] = None
        
        final_resp['investment_performance_html'] = prepare_pdf_from_html(performance_dict, 'performance.html', '', True)
        #Investment Performance graph - end

        
        
        if not hide_stock_details:
            #create holdings tab response
            nested_holding_dict = dict()
            nested_holding_response = get_account_aggregation_report(raw_holdings)
            nested_holding_dict["nested_holding_data"] = prepare_nested_holding_response(nested_holding_response)
            final_resp['stock_nested_holding_data'] = prepare_pdf_from_html(nested_holding_dict, 'stock_details.html', '', True)

        
        if not hide_trend_analysis:
            # Trend analysis
            trend_data = get_investor_portfolio_history(db_session, account_ids, portfolio_date, 0)
            final_resp["trend_count"] = len(trend_data['aum'])

            
            data_label = []
            i = 0;
            for dt in trend_data["aum"]:
                final_resp["trend_aum_date"] = datetime.strftime(datetime.fromtimestamp(dt[0]), '%Y-%b-%d') if dt[0] else '-'
                final_resp["trend_aum_value"] = str(float(round(dt[1], 2)) if round(dt[1], 2) else 0) if dt[1] else 'Not Available'

            #sector weight - historical
            final_resp["trend_analysis_sectorweights_historical_data_html"] = get_trand_analysis_stacked_chart(trend_data['sectors'],'sector',8, False, 'weight', False, 400, 1200, 4, 4)

            #instrument type - historical
            final_resp["trend_analysis_instrumenttype_historical_data_html"] = get_trand_analysis_stacked_chart(trend_data['instrument_types'],'instrument_type', 8, False, 'weight', False, 400, 1200, 4, 4)

            final_resp["trend_analysis_aum_data_html"] = get_line_chart(trend_data['aum'], fontsize=8, in_miliseconds= False, add_range=True, height=400, width=1200, show_legend=False, show_label=True )
            
            
    return final_resp

def prepare_donut_chart(data, title_field, header_title):
    res_dict = dict()
    data_label = []
    weight_data = []
    i = 0;
    for dt in data:
        weight_dict = dict()
        weight_dict = {
            'title':dt[title_field],
            'values':round(float(dt["weight"]),2) if dt["weight"] else 0
        }
        data_label.append(weight_dict)

        weight_dict['value'] = str(weight_dict['values'])
        weight_dict['color_code'] = color_list[i] if i<len(color_list) else None

        weight_data.append(weight_dict)
        if i == 9:
            break;
        
        i = i+1

    res_dict["data"] = weight_data
    res_dict["title"] = header_title
    res_dict["donut_chart_html"] = get_pie_chart(data_label, '', False, 10, False, color_list, True, True, 300, 900)
    return res_dict

def get_performance_html(data, per_type, transaction_date, columnname):
    resp = dict()
    data_list = list()
    chart_x_label_list = list()
    dict_chartdata = list()

    #prepare table data
    column_list = [columnname, '']
    data = sorted(data[per_type], key=itemgetter('year', 'month'))
    
    for dt in data:
        dict_data = dict()
        dict_data[columnname] = dt["period"] 
        dict_data[""] = round(dt["performance"],2) if dt["performance"] else 'NA'
        data_list.append(dict_data)

        #chart data 
        dict_chartdata.append(round(dt["performance"],2))
        chart_x_label_list.append(dt["period"])

    temp = dict()
    temp["title"] = columnname
    temp["values"] = dict_chartdata

    resp["title"] = columnname
    resp["performance_table_html_data"] = get_table_from_html_template(data_list, column_list, 20)
    resp["performance_chart_html_data"] = get_barchart(chart_x_label_list, [temp], True, columnname, 22, 350, False, False, False)

    return resp

def get_investor_portfolio_history(db_session, account_ids, as_of_dates, detailed_analysis):
    response = dict()
    response["aum"]= list()
    response["instrument_types"] = list()
    response["issuers"] = list()
    response["sectors"] = list()

    as_of_dates = get_investorholdings_as_of_dates_by_account_id(db_session, account_ids)
    
    for pd_date in as_of_dates:
        actual_date = pd_date[0]
        securities = get_normalized_portfolio_holdings(db_session, account_ids, actual_date, detailed_analysis)
        
        aum = securities["total_price"].sum()
        d = datetime(actual_date.year, actual_date.month, actual_date.day, 0, 0, 0)
        response["aum"].append([d.timestamp(), round(aum, 2)])

        instruments = get_instrument_type_exposure(securities, True)
        response["instrument_types"].extend(attach_date_and_convert(instruments, actual_date))

        issuers = get_issuer_exposure(securities, True)
        response["issuers"].extend(attach_date_and_convert(issuers, actual_date))

        sectors = get_sector_exposure(securities, True, 0)
        response["sectors"].extend(attach_date_and_convert(sectors, actual_date))
    return response

def get_portfoliooverlappdf(db_session, generatereport_dir, data, is_model_portfolio=False):
    template_vars = dict()

    template_vars = beautify_portfolio_overlap_data(template_vars, data, is_model_portfolio)

    file_name = prepare_pdf_from_html(template_vars, 'portfoliooverlaphtml_template.html', generatereport_dir)

    return file_name


def beautify_portfolio_overlap_data(template_vars, data, is_model_portfolio=False):

    if data:
        template_vars['current_date'] = datetime.strftime(date.today(),'%d %b %Y')
        template_vars['portfolio_date'] = data["portfolio_date"]
        template_vars['fund_portfolio_date'] = data["fund_portfolio_date"]
        template_vars["is_model_overlap"] = is_model_portfolio
        template_vars["investor_name"] = data["investor_details"]["label"]
        template_vars["pan_no"] = data["investor_details"]["pan_no"]
        if is_model_portfolio:
            template_vars["fund_a_name"] = data["model_portfolio_name"]
        else:
            template_vars["fund_a_name"] = data["fund_info"]["plan_name"]
        
        template_vars["common_stocks"] = data['securities_info']['common_securities']   
        template_vars["fund_a_total_securities"] = data["securities_info"]['left_total_securities']
        template_vars["fund_a_uncommon_securities"] = data["securities_info"]['left_total_securities'] - data['securities_info']['common_securities']
        template_vars["fund_b_total_securities"] = data["securities_info"]['right_total_securities']
        template_vars["fund_b_uncommon_securities"] = data["securities_info"]['right_total_securities'] - data['securities_info']['common_securities']

        template_vars["fund_a_common_weight"] = int(data["securities_info"]['left_common_weight']) if data["securities_info"]['left_common_weight'] else 0
        template_vars["fund_a_unique_weight"] = int(data["securities_info"]['left_unique_weight']) if data["securities_info"]['left_unique_weight'] else 0
        template_vars["fund_b_common_weight"] = int(data["securities_info"]['right_total_securities']) if data["securities_info"]['right_total_securities'] else 0
        template_vars["fund_b_unique_weight"] = int(data["securities_info"]['right_unique_weight']) if data["securities_info"]['right_unique_weight'] else 0
        
        if not is_model_portfolio:
            #build trailing returns
            fund_perf_data = data["trailing_returns"][0]['returns']
            portfolio_trailing_returns = data["portfolio_trailing_returns"]

            periods = ['1m', '3m', '6m', '1y', '3y', '5y']
            period_label = {
                '1m':'1-Month',
                '3m':'3-Months',
                '6m':'6-Months',
                '1y':'1-Year',
                '3y':'3-Years',
                '5y':'5-Years'
            }

            perf_list = list()

            for period in periods:
                dt = dict()                
                dt['portfolio_return'] = round(float(portfolio_trailing_returns['trailing_returns'][period]['equity_performance']),2) if portfolio_trailing_returns['trailing_returns'][period]['equity_performance'] else 'NA'
                dt['portfolio_bm_return'] = round(float(portfolio_trailing_returns['benchmark_returns'][period]['Returns_Value']),2) if portfolio_trailing_returns['benchmark_returns'][period]['Returns_Value'] else 'NA'
                dt['fund_return'] = round(float(fund_perf_data[F'scheme_ret_{period}']),2) if fund_perf_data[F'scheme_ret_{period}'] else 'NA'
                dt['fund_bm_return'] = round(float(fund_perf_data[F'bm_ret_{period}']),2) if fund_perf_data[F'bm_ret_{period}'] else 'NA'
                dt['period_label'] = period_label[period]
                perf_list.append(dt)

            template_vars['performance_data'] = perf_list
            template_vars['benchmark_name'] = data["portfolio_trailing_returns"]['benchmark_name']

        
        overlap = data["securities_overlap"]

        x_labels = list()
        fund_a = []
        fund_b = []
        data_label = []
        for overlap_data in overlap:
            
            x_labels.append(overlap_data["name"])
            fund_a.append(overlap_data["weight_a"])
            fund_b.append(overlap_data["weight_b"])

        a_dict = {
            'title':"",
            'values':fund_a
        }
        
        b_dict = {
            'title':"",
            'values':fund_b
        }
        data_label.append(a_dict)
        data_label.append(b_dict)

        overlap_len = len(overlap)
        height = 450
        if overlap_len < 3:
            height = 100
        elif overlap_len < 5:
            height = 300
        elif overlap_len < 8:
            height = 450
        elif overlap_len < 12:
            height = 600
        else:
            height = 800

        template_vars["security_overlap_html"] = get_barchart(x_labels,data_label, False, "", 9, height, True, True, False)


        #sector overlap
        overlap = data["sector_overlap"]  
        x_labels = list()
        fund_a = []
        fund_b = []
        data_label = []
        for overlap_data in overlap:
            
            x_labels.append(overlap_data["name"])
            fund_a.append(overlap_data["weight_a"])
            fund_b.append(overlap_data["weight_b"])

        a_dict = {
            'title':"",
            'values':fund_a
        }
        
        b_dict = {
            'title':"",
            'values':fund_b
        }
        data_label.append(a_dict)
        data_label.append(b_dict)

        overlap_len = len(overlap)
        height = 450
        if overlap_len < 3:
            height = 100
        elif overlap_len < 5:
            height = 300
        elif overlap_len < 8:
            height = 450
        elif overlap_len < 12:
            height = 600
        else:
            height = 800
        
        template_vars["sector_overlap_html"] = get_barchart(x_labels,data_label, False, "", 9, height, True, True, False)
        
    return template_vars

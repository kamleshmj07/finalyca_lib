import pandas as pd
import numpy as np
import pygal
from pygal.style import Style
from utils.time_func import last_date_of_month
from datetime import datetime


def get_linechart(x_labels, data):
    custom_style = Style(colors=('#228FCF', '#364EB9', '#36B9A5'),
                         background='transparent', plot_background='transparent')

    date_chart = pygal.Line(x_label_rotation=1, show_minor_x_labels=False, height=300, width=850, style=custom_style,
                            max_scale=6, show_legend=False, show_only_major_dots=True, x_value_formatter=lambda dt: dt.strftime('%b %Y'))

    x_labels = []
    nav = []
    for data in data:
        x_labels.append(datetime.strftime(data['nav_date'], '%Y %b %d'))
        nav.append(data['nav'])

    date_chart.x_labels = map(str, x_labels)
    if x_labels:
        N = round(len(x_labels)/3)  # we will plot only 4 date from date_list
        a = x_labels[::N] if N > 4 else x_labels
        a.append(x_labels[len(x_labels) - 1])  # adding last date
        date_chart.x_labels_major = a

    date_chart.add("", nav)

    encoded_string = date_chart.render_data_uri()
    return encoded_string


def get_line_chart(data_list, fontsize: int, in_miliseconds: bool, add_range: bool, height: int, width: int, show_legend: bool, show_label: bool):
    chart_config = {
        "human_readable": True,
        "pretty_print": True,
        "truncate_legend": -1,
        "show_legend": show_legend,
        "show_label": show_label,
    }

    custom_style = Style(colors=('#228FCF', '#303030', '#6C63FF', '#D4A656', '#E4813B', '#CF22A7', '#9ACF22', '#9F9F0D', '#50D0C6', '#D9042D', '3D0886', '#E390DD', '#769AAF'), background='transparent',
                         plot_background='transparent', value_font_size=fontsize, label_font_size=fontsize, title_font_size=fontsize, legend_font_size=fontsize, value_label_font_size=fontsize)

    line_chart = pygal.Line(height=height, width=width, Style=custom_style,
                            max_scale=6, legend_at_bottom=show_legend,  **chart_config)
    max_value = 0
    min_value = None
    list_aum_data = list()
    list_aum_date_data = list()
    for dt in data_list:
        if not min_value:
            min_value = dt[1]
        max_value = dt[1] if dt[1] > max_value else max_value
        min_value = dt[1] if dt[1] < min_value else min_value
        list_aum_data.append(dt[1])
        if in_miliseconds:
            list_aum_date_data.append(datetime.strftime(datetime.fromtimestamp(dt[0]/1000.0), '%Y-%b'))
        else:
            list_aum_date_data.append(datetime.strftime(datetime.fromtimestamp(dt[0]), '%Y-%b'))

    if add_range:
        max_value = max_value + (max_value/10)
        min_value = min_value - (min_value/20)
        line_chart.range = (min_value, max_value)

    line_chart.x_labels = map(str, list_aum_date_data)
    line_chart.x_label_rotation = 90
    line_chart.add('AUM', list_aum_data)

    return line_chart.render_data_uri()


def get_geo_location_chart(data, fontsize: int, height: int, width: int, colors: list()):
    custom_style = Style(colors=colors, background='transparent',  plot_background='transparent', value_font_size=fontsize,
                         label_font_size=fontsize, title_font_size=fontsize, legend_font_size=fontsize, value_label_font_size=fontsize)

    supra = pygal.maps.world.SupranationalWorld(height=height, width=width, legend_at_bottom=True, 
                                                legend_at_bottom_columns=4, style=custom_style)

    list_0_25 = list()
    list_26_50 = list()
    list_51_75 = list()
    list_76_100 = list()

    for dt in data:
        if dt['weight'] < 26:
            list_0_25.append((str(dt['continent']).lower(), dt['weight']))
        elif dt['weight'] < 51:
            list_26_50.append((str(dt['continent']).lower(), dt['weight']))
        elif dt['weight'] < 76:
            list_51_75.append((str(dt['continent']).lower(), dt['weight']))
        elif dt['weight'] <= 100:
            dict({'in': 100})
            list_76_100.append((str(dt['continent']).lower(), dt['weight']))

    supra.add('0-25', list_0_25)
    supra.add('26-50', list_26_50)
    supra.add('51-75', list_51_75)
    supra.add('76-100', list_76_100)
    return supra.render_data_uri()


def get_trand_analysis_stacked_chart(data_list, field_name: str, fontsize: int, is_linechart: bool, value_key_name: str, fill_gap: bool, height: int, width: int, max_scale: int, legend_at_bottom_columns: int):
    min_date = None
    max_date = None
    unique_list = []
    dates_list = []
    dates_list_name = []
    all_dates = []

    for sec_li in data_list:
        enddate = last_date_of_month(sec_li['year'], sec_li['month'])

        # dont fill gap
        if enddate not in all_dates:
            all_dates.append(enddate)

        sec_li['date'] = enddate

        if not min_date:
            min_date = enddate
        else:
            min_date = enddate if min_date > enddate else min_date

        if not max_date:
            max_date = enddate
        else:
            max_date = enddate if max_date < enddate else max_date

        if sec_li[field_name] not in unique_list:
            unique_list.append(sec_li[field_name])

    # get all dates - fill gap
    if fill_gap:
        if min_date and max_date:
            dates_list = pd.date_range((min_date.replace(day=1)),max_date, freq='MS').strftime("%Y-%m").tolist()
            dates_list_name = pd.date_range((min_date.replace(day=1)),max_date, freq='MS').strftime("%Y-%b").tolist()
    else:
        all_dates.sort()
        for dt in all_dates:
            dates_list.append(dt.strftime("%Y-%m"))
            dates_list_name.append(dt.strftime("%Y-%b"))

    chart_config = {
        "human_readable": True,
        "pretty_print": True,
        "truncate_legend": -1,
        "show_legend": True,
    }

    custom_style = Style(colors=('#228FCF', '#303030', '#6C63FF', '#D4A656', '#E4813B', '#CF22A7', '#9ACF22', '#9F9F0D', '#50D0C6', '#D9042D', '3D0886', '#E390DD', '#769AAF'), background='transparent',
                         plot_background='transparent', value_font_size=fontsize, label_font_size=fontsize, title_font_size=fontsize, legend_font_size=fontsize, value_label_font_size=fontsize)

    if is_linechart:
        line_chart = pygal.Line(fill=True, height=height, width=width, Style=custom_style, max_scale=max_scale,
                                legend_at_bottom=True, legend_at_bottom_columns=legend_at_bottom_columns, **chart_config)
    else:
        line_chart = pygal.StackedLine(fill=True, height=height, width=width, Style=custom_style, max_scale=max_scale,
                                       legend_at_bottom=True, legend_at_bottom_columns=legend_at_bottom_columns, **chart_config)
    # line_chart.title = 'Sector Weights'
    line_chart.x_label_rotation = 90
    line_chart.x_labels = map(str, dates_list_name)
    for unique_lst in unique_list:
        value_lst = []
        for dates in dates_list:
            dt_res = next((sub for sub in data_list if sub[field_name] == unique_lst and sub['month'] == int(dates.split('-')[1]) and sub['year'] == int(dates.split('-')[0])), None)
            if dt_res:
                value_lst.append(dt_res[value_key_name])
            else:
                value_lst.append(None)

        line_chart.add(unique_lst, value_lst)

    return line_chart.render_data_uri()


def get_barchart(x_labels, data, apply_custome_css: bool, title: str, fontsize: int, chart_height: int, for_overlap: bool, for_portfolio_overlap: bool, show_legend: bool):
    chart_config = {
        "human_readable": True,
        "pretty_print": True,
        "truncate_legend": -1,
        "print_values": True,
        "show_legend": show_legend,
        "print_values_position": "top",
        "print_labels": True,
        # "value_formatter": lambda x: "{0: .2f}".format(x),
    }

    custom_style = Style(colors=('#228FCF', '#364EB9', '#36B9A5'), background='transparent',  plot_background='transparent',
                         value_font_size=fontsize, label_font_size=fontsize, title_font_size=fontsize, legend_font_size=fontsize, value_label_font_size=fontsize)
    # pie_chart = pygal.Pie(inner_radius=1, style=custom_style)
    bar_chart = pygal.Bar(height=chart_height, width=850, style=custom_style, max_scale=6,
                          legend_at_bottom=True, legend_at_bottom_columns=2,  **chart_config)

    if for_overlap:
        if for_portfolio_overlap:
            custom_style = Style(colors=('#364EB9', '#E4813B'), background='transparent', plot_background='transparent', value_font_size=fontsize,
                                 label_font_size=fontsize, title_font_size=fontsize, legend_font_size=fontsize, value_label_font_size=fontsize)
        else:
            custom_style = Style(colors=('#e85d5f', '#364EB9'), background='transparent', plot_background='transparent', value_font_size=fontsize,
                                 label_font_size=fontsize, title_font_size=fontsize, legend_font_size=fontsize, value_label_font_size=fontsize)

        bar_chart = pygal.HorizontalBar(height=chart_height, width=400, style=custom_style, max_scale=3, legend_at_bottom=True,
                                        legend_at_bottom_columns=0, show_x_labels=False, show_y_guides=False, title=title, **chart_config)

    if apply_custome_css:
        bar_chart.x_label_rotation = 90
        bar_chart.height = 400
        bar_chart.title = title
        bar_chart.show_legend = False

    bar_chart.x_labels = map(str, x_labels)

    for data in data:
        bar_chart.add(data['title'], data['values'])

    encoded_string = bar_chart.render_data_uri()
    return encoded_string


def get_pie_chart(data, title: str, half_pie: bool, font_size: int, is_debtfactsheet_pie: bool, color_list: list(), hide_legends: bool, is_donut: bool, height: int, width: int):
    chart_config = {
        "human_readable": True,
        "pretty_print": True,
        "truncate_legend": -1
    }

    custom_style = Style(colors=(color_list), title_font_family='sans-serif', background='transparent', value_font_size=font_size, label_font_size=font_size,
                         no_data_font_size=12, title_font_size=font_size if is_debtfactsheet_pie else font_size, legend_font_size=font_size, value_label_font_size=font_size)

    pie_chart = pygal.Pie(height=height, width=width, style=custom_style, print_values=True,
                          legend_at_bottom=True, legend_at_bottom_columns=2, **chart_config)

    if is_donut:
        pie_chart.inner_radius = .4
    if half_pie:
        pie_chart.half_pie = True
        pie_chart.width = 400
    else:
        pie_chart.width = 260

    if hide_legends:
        pie_chart.show_legend = False
        pie_chart.print_values = False

    pie_chart.title = title

    for data in data:
        pie_chart.add(data['title'], data['values'])

    encoded_string = pie_chart.render_data_uri()
    return encoded_string

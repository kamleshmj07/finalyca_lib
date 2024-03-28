import uuid
import locale
import pandas as pd
import numpy as np
from weasyprint import HTML
from jinja2 import Template
from pathlib import Path
from reports.df_to_html_interface import df_to_html_column_type, currency_type, ReportTableColumnInfo


def get_table_from_html_template(data_dict, header_sequence_list, width):
    base_url = str(Path(__file__).parent.parent)
    # environment = Environment(loader=FileSystemLoader('./reports/templates'), keep_trailing_newline=False, trim_blocks=True, lstrip_blocks=True)

    path = F"{base_url}/reports/templates/table_component.html"

    with open(path) as file_:
        template = Template(file_.read())

    data = dict()
    data["data_dict"] = data_dict
    data["header_list"] = header_sequence_list
    data["table_width"] = round((width * 720 / 100), 0)

    return template.render(data)


def prepare_pdf_from_html(template_vars, template_name, report_generation_path, get_html=False):
    base_url = str(Path(__file__).parent.parent)
    file_path = F"{report_generation_path}/{str(uuid.uuid4())}.pdf"
    path = F"{base_url}/reports/templates/"

    with open(F'{path}{template_name}') as file_:
        template = Template(file_.read())

    html_out = template.render(template_vars)

    if get_html:
        return html_out

    # f1=codecs.open("chart2.html", 'r')
    # html_out = f1.read()

    # #uncomment below to render html in file
    # with open("chart1.html", "w", encoding="utf-8") as image_file:
    #     image_file.write(html_out)

    # create pdf
    HTML(string=html_out, base_url=base_url).write_pdf(file_path, stylesheets=[F'{path}css/report.css'])

    return file_path


def get_html_from_dataframe(df, columns_config=[ReportTableColumnInfo], header_font_size_in_pt=6, body_font_size_in_pt=6):
    column_list = list()

    for col in columns_config:
        df.rename(columns = {col.name: col.label}, inplace = True)
        column_list.append(col.label)
        
        #TODO Work on locale currently it is not supporting
        # if col.currency_type == currency_type.INR:
        #     locale.setlocale(locale.LC_MONETARY, 'en_IN.UTF-8')
        # elif col.currency_type == currency_type.USD:
        #     locale.setlocale(locale.LC_MONETARY, 'en_US.UTF-8')
        # elif col.currency_type == currency_type.EUR:
        #     locale.setlocale(locale.LC_MONETARY, 'nl_NL.UTF-8')

        #Format column
        if col.type == df_to_html_column_type.Money:
            df[col.label] = df[col.label].apply(lambda x: to_money_str(x))  
        if col.type == df_to_html_column_type.Decimal:
            df[col.label] = df[col.label].apply(lambda x: to_decimal_str(x))  
        elif col.type == df_to_html_column_type.Percent:
            df[col.label] = df[col.label].apply(lambda x: to_percentage_str(x))
        elif col.type == df_to_html_column_type.Number:
            df[col.label] = df[col.label].apply(lambda x: to_integer_str(x))
        
    df = df[column_list]
    column_style = get_table_styles_for_columns(columns_config, header_font_size_in_pt, body_font_size_in_pt)

    b = df.style.set_table_styles(column_style, overwrite=True).set_table_attributes('class="custom_html_from_dataframe"')
   
    return b.hide_index().to_html()

def get_table_styles_for_columns(columns_config = [ReportTableColumnInfo], header_font_size_in_pt=6, body_font_size_in_pt=6):
    column_style = dict()

    for col in columns_config:
        text_align = 'left' if col.type == df_to_html_column_type.String else 'right'

        props = [{'selector': 'th', 'props': F'font-weight: bold;\
                                                text-align: {text_align};\
                                                width: {str(col.width_in_percent)}% !important;\
                                                font-size: {str(header_font_size_in_pt)}pt;'},
                {'selector': 'td', 'props': F'font-size: {str(body_font_size_in_pt)}pt;\
                                                width: {str(col.width_in_percent)}% !important;\
                                                text-align: {text_align}'}]
        column_style[col.label] = props

    return column_style

def convert_to_money_str(val):
    return F'₹ {format(round(val, 2), ",")}' if val else val 

def to_money_str(val):
    # s = locale.currency(val, symbol=True, grouping=True) if not pd.isnull(val) else ''    
    s = convert_to_money_str(val)
    return s


def to_decimal_str(val):
    v = round(val, 2) if not pd.isnull(val) else np.nan
    s = locale.format_string("%d", v, grouping=True) if not pd.isnull(val) else ''
    return s


def to_percentage_str(val):
    v = round(val, 2) if not pd.isnull(val) else ''
    s = F'{v}%' if v else ''
    return s


def to_integer_str(val):
    s = round(val, 0) if not pd.isnull(val) else ''
    return s

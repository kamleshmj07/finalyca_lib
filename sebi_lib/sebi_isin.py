from datetime import date
import time
import os
from typing import final
from sqlalchemy import func
from dateutil import relativedelta
from sebi_lib import *
import logging

# TODO: implement cool down feature. Try to make a standard function for scrapping.
def get_data(url):
    header = list()
    items = list()

    resp = requests.get(url)
    content = resp.content
    html = BeautifulSoup(content, "html.parser" )
    table = html.table.table
    rows = table.findAll(['tr'])
    for row in rows:
        if row.attrs:
            r = list()
            for val in row.findAll(['td']):
                r.append(val.text.strip())
            items.append(r)        
        else:
            for val in row.find_all('th'):
                header.append(val.text.strip())

    return header, items

def scrap_sebi_isin(db_session, verbose: bool):

    last_tried = db_session.query(func.max(SebiISINScrapperStatus.last_tried_on)).scalar()
    if last_tried and not last_tried.date() < datetime.today().date():
        # No need to scan again
        return

    sql_status = SebiISINScrapperStatus()

    try:
        headers = list()
        items = list()
        url_extensions = ['othr', 'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
        for url_ext in url_extensions:
            _header, _items = get_data(F"https://www.sebi.gov.in/isin/isin_{url_ext}.html")
            if not headers:
                headers = _header
            else:
                if len(headers) != len(_header):
                    raise Exception("header size does not match.")
            items.extend(_items)

        for row in items:
            issuer_name = row[headers.index('Issuer Name')]
            issuer_type = row[headers.index('Issuer Type')]
            security_type = row[headers.index('Security Type')]
            security_sub_type = row[headers.index('Security Sub Type')]
            isin = row[headers.index('ISIN')]
            status = row[headers.index('Status')]
            demat_isin = row[headers.index('Demat ISIN')]

            sql_isin = db_session.query(SebiISIN).filter(SebiISIN.issuer_name == issuer_name).filter(SebiISIN.issuer_type == issuer_type).filter(SebiISIN.security_type == security_type).filter(SebiISIN.security_sub_type == security_sub_type).filter(SebiISIN.isin == isin).filter(SebiISIN.status == status).filter(SebiISIN.demat_isin == demat_isin).one_or_none()
            if not sql_isin:
                sql_isin = SebiISIN()
                sql_isin.issuer_name = issuer_name
                sql_isin.issuer_type = issuer_type
                sql_isin.security_type = security_type
                sql_isin.security_sub_type = security_sub_type
                sql_isin.isin = isin
                sql_isin.status = status
                sql_isin.demat_isin = demat_isin
                db_session.add(sql_isin)
                db_session.commit()

        sql_status.has_data = True
        sql_status.is_successful = True
        
    except Exception as e:
        sql_status.has_data = False
        sql_status.is_successful = True
        sql_status.error_description = str(e)

    finally:
        sql_status.last_tried_on = datetime.now()
        db_session.add(sql_status)
        db_session.commit()


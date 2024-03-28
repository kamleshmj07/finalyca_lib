import datetime
import calendar
from dateutil import relativedelta

def get_next_date(current_date: datetime.date, time_period: str, period_in_future: bool, use_month_end: bool):
    '''
    time_period can be in months or years. Use it like 1M or 1m or 3Y or 3y. extension is case-insensitive.
    If period_in_future is True, it will add time_period else it will be subtracted
    If use_month_end is True, it will return the last date of the final month
    '''
    _ , last_date = calendar.monthrange(current_date.year, current_date.month)
    d = current_date.replace(day=last_date) if use_month_end else current_date
    
    c = None
    is_year = False 
    if "m" in time_period.lower():
        c, _  = time_period.lower().split('m')
    
    if "y" in time_period.lower():
        is_year = True
        c, _  = time_period.lower().split('y')

    count = int(c) if period_in_future else -int(c)

    new_date = d + (
        relativedelta.relativedelta(years=count) if is_year else relativedelta.relativedelta(months=count)
    )
    
    return new_date

def last_date_of_month(year: int, month: int) -> datetime.date:
    '''
    handles leap years automatically.
    '''
    _ , last_date = calendar.monthrange(year, month)
    return datetime.date(year, month, last_date)


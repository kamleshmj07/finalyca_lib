from datetime import date, datetime
from fin_models.controller_master_models import NewsLetterSubscribers
from sqlalchemy import and_

def create_newsletter(db_session, form):
    name = form.get("name")
    email = form.get("email")
    mobile = form.get("mobile")

    newsletter_query = db_session.query(NewsLetterSubscribers).filter(NewsLetterSubscribers.Email_Address == email)
    sql_query = newsletter_query.one_or_none()

    if sql_query:
        raise Exception("Same email id is already registered, please use different email id.")
    
    newsletter_query = db_session.query(NewsLetterSubscribers).filter(NewsLetterSubscribers.Contact_Number == mobile)
    sql_query = newsletter_query.one_or_none() if mobile else None

    if sql_query:
        raise Exception("Same contact no is already registered, please use different contact no.")
    
    sql_obj = NewsLetterSubscribers()
    sql_obj.Name = name
    sql_obj.Email_Address = email
    sql_obj.Contact_Number = mobile
    sql_obj.Is_Active = True
    sql_obj.Created_Date = datetime.now()
    sql_obj.Created_By = 1

    db_session.add(sql_obj)
    db_session.commit()

    return sql_obj.Subscriber_Id

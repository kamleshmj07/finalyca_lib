from fin_models.controller_master_models import RecentlyViewed
from datetime import datetime
from sqlalchemy import func, desc, or_
import json

def get_recently_viewed(db_session, user_id):
    sql_recently_viewed = db_session.query(RecentlyViewed).filter(RecentlyViewed.Created_By == user_id).order_by(RecentlyViewed.Created_Date.desc()).limit(5).all()

    recently_viewed_data = list()

    for recently_viewed in sql_recently_viewed:
        recently_viewed_dict = dict()
        recently_viewed_dict["id"] = recently_viewed.Id
        recently_viewed_dict["created_on"] = recently_viewed.Created_Date
        recently_viewed_dict["name"] = recently_viewed.Display_Name
        recently_viewed_dict["table_name"] = recently_viewed.Table_Name
        recently_viewed_dict["table_row_id"] = recently_viewed.Table_Row_Id
        recently_viewed_dict["table_row_code"] = recently_viewed.Table_Row_Code
        recently_viewed_dict["table_obj"] = json.dumps(recently_viewed.Table_Obj) if recently_viewed.Table_Obj else None 
        recently_viewed_data.append(recently_viewed_dict)

    return recently_viewed_data


def save_recently_viewed(db_session, data, user_id):

    sql_recently_viewed_query = db_session.query(RecentlyViewed).filter(RecentlyViewed.Created_By == user_id).filter(RecentlyViewed.Table_Name == data["table_name"])

    if data.get("row_id"):
        sql_recently_viewed_query = sql_recently_viewed_query.filter(RecentlyViewed.Table_Row_Id == data["row_id"])

    if data.get("row_code"):
        sql_recently_viewed_query = sql_recently_viewed_query.filter(RecentlyViewed.Table_Row_Code == data["row_code"])

    sql_recently_viewed = sql_recently_viewed_query.one_or_none()

    if sql_recently_viewed:
        sql_recently_viewed_query.delete()
        db_session.commit()

    sql_new_recently_viewed = RecentlyViewed()
    sql_new_recently_viewed.Created_By = user_id
    sql_new_recently_viewed.Created_Date = datetime.now()
    sql_new_recently_viewed.Display_Name = data["name"]
    sql_new_recently_viewed.Table_Name = data['table_name']
    sql_new_recently_viewed.Table_Row_Id = data["row_id"]
    sql_new_recently_viewed.Table_Row_Code = data["row_code"] if data.get("row_code") else ''
    if data.get("row_obj"):
        sql_new_recently_viewed.Table_Obj = data["row_obj"] 

    db_session.add(sql_new_recently_viewed)
    db_session.commit()

    return sql_new_recently_viewed.Id


def get_top_searched_amc_and_funds(db_session):
    try:
        sql_top_fund_results = db_session.query(
            RecentlyViewed.Table_Row_Id, 
            RecentlyViewed.Table_Name,
            RecentlyViewed.Display_Name,
            func.count(RecentlyViewed.Table_Row_Id).label('count')
            ).filter(
                or_(RecentlyViewed.Table_Name == 'Fund', RecentlyViewed.Table_Name == 'AMC')
            ).group_by(
                RecentlyViewed.Table_Row_Id, 
                RecentlyViewed.Table_Name,
                RecentlyViewed.Display_Name,
            ).order_by(
                desc('count')
            )
        
        top_searched_results = list()
        for sql_top_result in sql_top_fund_results:
            top_result = dict()
            top_result["table_name"] = sql_top_result.Table_Name
            top_result["table_row_id"] = sql_top_result.Table_Row_Id
            top_result["name"] = sql_top_result.Display_Name
            top_searched_results.append(top_result)
        
    except Exception as exe:
        raise Exception(exe)

    return top_searched_results

from fin_models.controller_master_models import Favorites
from datetime import datetime
from fin_resource.exceptions import NotUniqueValueException
import json

def get_favorites(db_session, user_id):
    sql_favorites = db_session.query(Favorites).filter(Favorites.Created_By == user_id).all()

    favorites_data = list()

    for sql_favorite in sql_favorites:
        favorite = dict()
        favorite["id"] = sql_favorite.Id
        favorite["created_on"] = sql_favorite.Created_Date
        favorite["name"] = sql_favorite.Display_Name
        favorite["table_name"] = sql_favorite.Table_Name
        favorite["table_row_id"] = sql_favorite.Table_Row_Id
        favorite["table_row_code"] = sql_favorite.Table_Row_Code
        favorite["table_obj"] = json.dumps(sql_favorite.Table_Obj) if sql_favorite.Table_Obj else None 
        favorites_data.append(favorite)

    return favorites_data

def save_favorite(db_session, data, user_id):

    sql_favorite = db_session.query(Favorites).filter(Favorites.Created_By == user_id).filter(Favorites.Table_Name == data["table_name"])

    if data.get("row_id"):
        sql_favorite = sql_favorite.filter(Favorites.Table_Row_Id == data["row_id"])

    if data.get("row_code"):
        sql_favorite = sql_favorite.filter(Favorites.Table_Row_Code == data["row_code"])

    sql_favorite = sql_favorite.one_or_none()

    if sql_favorite:
        raise NotUniqueValueException("Record already exists into favorites.")
    
    sql_favorite = Favorites()
    sql_favorite.Created_By = user_id
    sql_favorite.Created_Date = datetime.now()
    sql_favorite.Display_Name = data["favorite_name"]
    sql_favorite.Table_Name = data['table_name']
    sql_favorite.Table_Row_Id = data["row_id"] if data.get("row_id") else ''
    sql_favorite.Table_Row_Code = data["row_code"] if data.get("row_code") else ''
    if data.get("row_obj"):
        sql_favorite.Table_Obj = data["row_obj"] 

    db_session.add(sql_favorite)
    db_session.commit()

    return sql_favorite.Id

def delete_favorite(db_session, favorite_id, user_id):
    sql_favorite = db_session.query(Favorites).filter(Favorites.Created_By == user_id).filter(Favorites.Id == favorite_id)

    if not sql_favorite.one_or_none():
        resp = "Record not found"
        
    sql_favorite.delete()

    resp = "Record deleted successfully"

    return resp

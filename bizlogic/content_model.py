from datetime import date, datetime
from fin_models.masters_models import Content, ContentCategory, ContentType, ContentUploadType, AMC, Product, Fund
from sqlalchemy import and_
from sqlalchemy import desc, extract, func, or_
from utils.utils import print_query

def get_model_content(db_session, content_id):
    response = list()

    sql_objs_query = db_session.query(Content.Content_Id,
                                    Content.Content_Category_Id, 
                                    ContentCategory.Content_Category_Name,
                                    ContentType.Content_Type_Name,
                                    Content.Content_Type_Id, 
                                    Content.Content_Header, 
                                    Content.Content_SubHeader,
                                    Content.Content_Detail,
                                    Content.Content_DateTime,
                                    Content.Images_URL,
                                    Content.Content_Name,
                                    Content.Is_Front_Dashboard,
                                    Content.AMC_Id,
                                    Content.Product_Id,
                                    Content.Fund_Id,
                                    Content.Created_By,
                                    Content.Created_Date,
                                    Content.Updated_By,
                                    Content.Updated_Date, AMC.AMC_Name, Product.Product_Name, Fund.Fund_Name)\
        .select_from(Content)\
        .join(ContentCategory, ContentCategory.Content_Category_Id == Content.Content_Category_Id, isouter=True)\
        .join(ContentType, ContentType.Content_Type_Id == Content.Content_Type_Id, isouter=True)\
        .join(AMC, AMC.AMC_Id == Content.AMC_Id, isouter=True)\
        .join(Product, Product.Product_Id == Content.Product_Id, isouter=True)\
        .join(Fund, Fund.Fund_Id == Content.Fund_Id, isouter=True)\
        .filter(Content.Is_Deleted != 1, AMC.Is_Deleted != 1)

    if content_id:
        sql_objs_query.filter(Content.Content_Id == content_id)

    sql_objs = sql_objs_query.order_by(desc(Content.Created_Date)).all()
    
    for sql_obj in sql_objs:
        obj = dict()
        obj["content_id"] = sql_obj.Content_Id
        obj["content_category_id"] = sql_obj.Content_Category_Id
        obj["content_category_name"] = sql_obj.Content_Category_Name
        obj["content_type_name"] = sql_obj.Content_Type_Name
        obj["content_type_id"] = sql_obj.Content_Type_Id
        obj["content_header"] = sql_obj.Content_Header
        obj["content_sub_header"] = sql_obj.Content_SubHeader
        obj["content_details"] = sql_obj.Content_Detail
        obj["content_datetime"] = sql_obj.Content_DateTime
        obj["image_url"] = sql_obj.Images_URL
        obj["content_name"] = sql_obj.Content_Name
        obj["is_frontend"] = sql_obj.Is_Front_Dashboard
        obj["amc_id"] = sql_obj.AMC_Id
        obj["product_id"] = sql_obj.Product_Id
        obj["fund_id"] = sql_obj.Fund_Id
        obj["created_by"] = sql_obj.Created_By
        obj["created_date"] = sql_obj.Created_Date
        obj["updated_by"] = sql_obj.Updated_By
        obj["updated_date"] = sql_obj.Updated_Date
        obj["amc_name"] = sql_obj.AMC_Name
        obj["product_name"] = sql_obj.Product_Name
        obj["fund_name"] = sql_obj.Fund_Name
        response.append(obj)

    return response

def save_model_content(db_session, request_data):
    sql_content = Content()
    sql_content.Content_Category_Id = request_data["content_category_id"]
    sql_content.Content_Type_Id = request_data["content_type_id"]
    sql_content.Content_Header = request_data["content_header"]
    sql_content.Content_SubHeader = request_data["content_sub_header"]
    sql_content.Content_Source = request_data["content_source"]
    sql_content.Images_URL = request_data["image_url"]
    sql_content.Content_DateTime = request_data["content_datetime"]
    sql_content.Content_Name = request_data["content_name"]
    sql_content.Is_Front_Dashboard = request_data["is_frontend"]
    sql_content.AMC_Id = request_data["amc_id"]
    sql_content.Product_Id = request_data["product_id"]
    sql_content.Fund_Id = request_data["fund_id"]
    sql_content.Content_Detail = request_data["content_details"]
    sql_content.Created_By = request_data["user_id"]
    sql_content.Created_Date = datetime.now()
    sql_content.Is_Deleted = False

    db_session.add(sql_content)
    db_session.commit()

    return sql_content.Content_Id

def get_data_by_request(form_request, user_id):
    content_request = dict()
    content_request["content_category_id"] = form_request["content_category_id"]
    content_request["content_type_id"] = form_request["content_type_id"]
    content_request["content_header"] = form_request["content_header"]
    content_request["content_sub_header"] = form_request["content_sub_header"]
    content_request["content_source"] = form_request["content_source"]
    content_request["image_url"] = form_request["image_url"]
    content_request["content_datetime"] = form_request["content_datetime"]
    content_request["content_name"] = form_request["content_name"]
    content_request["is_frontend"] = False if form_request["is_frontend"] == 'false' or form_request['is_frontend'] == '0' or form_request['is_frontend'] == '' else True
    content_request["amc_id"] = form_request["amc_id"]
    content_request["product_id"] = form_request["product_id"]
    content_request["fund_id"] = form_request["fund_id"]
    content_request["content_details"] = form_request["content_details"]
    content_request["user_id"] = user_id

    return content_request



def edit_model_content(db_session, request_data, content_id):
    sql_content = db_session.query(Content).get(content_id)
  
    sql_content.Content_Category_Id = request_data["content_category_id"]
    sql_content.Content_Type_Id = request_data["content_type_id"]
    sql_content.Content_Header = request_data["content_header"]
    sql_content.Content_SubHeader = request_data["content_sub_header"]
    sql_content.Content_Source = request_data["content_source"]
    sql_content.Images_URL = request_data["image_url"]
    sql_content.Content_DateTime = request_data["content_datetime"]
    sql_content.Content_Name = request_data["content_name"]
    sql_content.Is_Front_Dashboard = request_data["is_frontend"]
    sql_content.AMC_Id = request_data["amc_id"]
    sql_content.Product_Id = request_data["product_id"]
    sql_content.Fund_Id = request_data["fund_id"]
    sql_content.Content_Detail = request_data["content_details"]

    db_session.commit()

    return sql_content.Content_Id


def delete_model_content(db_session, content_id):
    sql_content = db_session.query(Content).get(content_id)
    sql_content.Is_Deleted = 1

    db_session.commit()

    return sql_content.Content_Id

    
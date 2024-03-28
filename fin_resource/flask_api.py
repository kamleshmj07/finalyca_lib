from fin_resource.json_helper import schema_to_json
from .flask_bl import prepare_filter, prepare_create, prepare_patch, prepare_edit, prepare_read
from .common_enum import FieldType, ResourceType, ReadResponseType
from .query_formatter import  QueryFormatter
from .validators import _prepare_FieldChoices
from flask import Blueprint, request, jsonify, current_app
from werkzeug.exceptions import NotFound
from werkzeug.utils import secure_filename
from uuid import uuid4
import os

admin_bp = Blueprint('admin_bp', __name__)

def __get_model(data_store, model_name):
    db_table = data_store.get(model_name)
    if not db_table:
        raise NotFound(description=F"{model_name} resource was not found.")

    return db_table

def get_only_choices(resource, data_store= None):
    choices = dict()

    for key_name, field in resource.field_dict.items():
        if field.type == FieldType.REF and resource.type == ResourceType.master:
            current_choices = _prepare_FieldChoices(field.options, data_store)
            choices[key_name] = current_choices

    return choices
    
# Following function is used in Resource Schema library for admin interface
# TODO: following function should store files in S3 bucket
def save_image_file(root_path, dir, req_file):
    filename = secure_filename(req_file.filename)
    # get an uuid for the file
    unique_file_name = str(uuid4())

    file_path = os.path.join(dir, unique_file_name)

    #extension
    split_tup = os.path.splitext(filename)
    if split_tup:
        file_path = file_path + split_tup[len(split_tup)-1]
    
    total_path = os.path.join(root_path, file_path)

    # save the file -> expecting a flask request file object.
    req_file.save(total_path)
    
    return file_path

def remove_image_file(complete_path):
    os.remove(complete_path)


@admin_bp.route("/admin/<model_name>/schema", methods=["GET"])
def api_admin_schema(model_name):
    PAGINATION_LIMIT = 500
    is_pagination = False

    db_table = __get_model(current_app.store, model_name)
    schema = db_table.get_schema()

    s = schema_to_json(schema, current_app.store)
    record_count = db_table.get_count(current_app.store.db, {}, True)

    if record_count > PAGINATION_LIMIT:
        is_pagination = True

    res = {"data": s, "meta" : {"is_pagination": int(is_pagination), "total_count": record_count}}

    return jsonify(res)

@admin_bp.route("/admin/<model_name>", methods=["GET", "POST", "DELETE", "PATCH"])
def api_admin(model_name):
    PAGINATION_LIMIT = 500
    PAGE_SIZE = 50

    db_table = __get_model(current_app.store, model_name)
    schema = db_table.get_schema()

    if request.method == "GET":
        formatter = QueryFormatter(request)
        args_filter = prepare_filter(schema, request, current_app.store)

        res = None
               
        if formatter.resp_category == ReadResponseType.sel:
            res = list()
            sql_objs = db_table.get_results(current_app.store.db, args_filter, formatter, True)
            for sql_obj in sql_objs:
                o = {
                    "key": db_table._get_id(sql_obj),
                    "label": str(sql_obj),
                    }
                res.append(o)  
        elif formatter.resp_category == ReadResponseType.rec:
            res = dict()
            is_pagination = False
            # Check if pagination is required. if yes, do the pagination
            record_count = db_table.get_count(current_app.store.db, args_filter, True)
            if record_count > PAGINATION_LIMIT:
                # Check if API has asked for pagination, if not do it anyways
                if not formatter.page_size > 0:
                    is_pagination = True
                    formatter.page_size = PAGE_SIZE
                if not formatter.page_nr > 0:
                    formatter.page_nr = 1

            schema_choices = get_only_choices(schema, current_app.store)
            sql_objs = db_table.get_results(current_app.store.db, args_filter, formatter, True)

            data = list()
            for sql_obj in sql_objs:
                o = prepare_read(schema, sql_obj, formatter, None, schema_choices)
                data.append(o)    

            res = {"data": data, "meta" : {"is_pagination": int(is_pagination), "page_size": formatter.page_size, "page_nr": formatter.page_nr, "total_count": record_count}}

        return jsonify(res)

    elif request.method == "POST":
        form = prepare_create(schema, request, current_app.store, current_app.config['SECRET_KEY'])
        
        #  save files
        for name, field in schema.field_dict.items():
            if field.is_input and field.type == FieldType.FILE:
                if form[name]:
                    final_value = None
                    ip_file = form[name]
                    final_value = save_image_file(current_app.config['DOC_ROOT_PATH'], current_app.config['IMAGES_DIR'], ip_file)

                    form[name] = final_value

        id = db_table.create(current_app.store.db, form)

        return jsonify({"id": id})

    elif request.method == "DELETE":
        args_filter = prepare_filter(schema, request, current_app.store)
        d = db_table.remove_many(current_app.store.db, args_filter)
        return jsonify({"msg": F"{model_name} records has been deleted"})

    elif request.method == "PATCH":
        args_filter = prepare_filter(schema, request, current_app.store)
        patch_data = prepare_patch(schema, request, current_app.store, current_app.config['SECRET_KEY'])
        d = db_table.update_many(current_app.store.db, patch_data, args_filter)
        return jsonify({"msg": F"{model_name} records has been partially updated"})

@admin_bp.route("/admin/<model_name>/<int:id>", methods=["GET", "PUT", "DELETE", "PATCH"])
def api_admin_single(model_name, id):
    db_table = __get_model(current_app.store, model_name)
    schema = db_table.get_schema()

    if request.method == "GET":
        formatter = QueryFormatter(request)

        obj = dict()
        sql_obj = db_table.get(current_app.store.db, id)
        if sql_obj:
            obj = prepare_read(schema, sql_obj, formatter, store=current_app.store)
            return jsonify(obj)
        else:
            raise NotFound(description=F"{model_name} does not have resource with {id}")

    elif request.method == "PUT":
        sql_obj = db_table.get(current_app.store.db, id)
        if sql_obj:
            obj = prepare_read(schema, sql_obj, None, store=current_app.store)
            form = prepare_edit(schema, request, obj, current_app.store, current_app.config['SECRET_KEY'])

            for name, field in schema.field_dict.items():
                if field.is_input and field.type == FieldType.FILE:
                    new_file = form.get(name)
                    if new_file is not None:
                        # TODO: save the case for file type is list
                        # remove old file
                        old_file_path = obj[name]
                        if old_file_path:
                            remove_image_file(os.path.join(current_app.config['DOC_ROOT_PATH'],  old_file_path))

                        # save the new file
                        ip_file = form[name]
                        file_path = save_image_file(current_app.config['DOC_ROOT_PATH'], current_app.config['IMAGES_DIR'], ip_file)

                        form[name] = file_path
            
            db_table.edit(current_app.store.db,form, id)
            return jsonify(msg="Object has been edited.")
        else:
            raise NotFound(description=F"{model_name} does not have resource with {id}")

    elif request.method == "DELETE":
        db_table.remove(current_app.store.db, id)
        return jsonify(msg="Object has been deleted.")

    elif request.method == "PATCH":
        patch_data = prepare_patch(schema, request, current_app.store, current_app.config['SECRET_KEY'])
        db_table.update(current_app.store.db, patch_data, id)
        return jsonify(msg="Object has been edited.")

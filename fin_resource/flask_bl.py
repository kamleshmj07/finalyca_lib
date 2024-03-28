from typing import Dict
from .common_enum import ReadRefType, ResourceType, FieldType
from .query_formatter import QueryFormatter
from .utils import get_current_ts
from .validators import validate_basic_fields
from .common_enum import FieldType
from .exceptions import FieldMutationException, MissingFieldException, UnknownFieldException
from .resource_interface import DataSchema
from flask import Request
from .filters import RangeFilter, TextFilter, ExactFilter, ChoiceFilter
from utils.utils import get_user_id
from .validators import _prepare_FieldChoices

def prepare_filter(schema: DataSchema, req: Request, global_data_store):
    args_filter = dict()

    for key, values in req.args.lists():
        if key.startswith("_"):
            continue

        if key not in schema.field_dict:
            raise UnknownFieldException(F"{key} is not part of {schema.name}")

        field = schema.field_dict[key]

        if field.type == FieldType.TEXT:
            # Always take first value
            args_filter[key] = TextFilter(
                validate_basic_fields(field, values[0], global_data_store)
                )

        if field.type == FieldType.TS or field.type == FieldType.INT or field.type == FieldType.DECIMAL or field.type == FieldType.CURRENT_TS:
            # Always take first value or first 2 values
            val_0 = validate_basic_fields(field, values[0], global_data_store)
            if len(values) == 1:
                args_filter[key] = ExactFilter(val_0)
            else:
                val_1 = validate_basic_fields(field, values[1], global_data_store)
                args_filter[key] = RangeFilter(val_0, val_1)

        if field.type == FieldType.BOOL:
            # Always take first value
            args_filter[key] = ExactFilter(
                validate_basic_fields(field, values[0], global_data_store)
            )

        if field.type == FieldType.REF:
            new_values = [validate_basic_fields(field, val, global_data_store) for val in values]
            args_filter[key]= ChoiceFilter(new_values)
        
    return args_filter

# TODO: this will have 2 steps. first will be to validate and second will be to provide defaults. -> provide defauls could become a empty object (dict) generated from schema.
def prepare_create(schema: DataSchema, req: Request, global_data_store= None, secret_key=None):
    output = dict()

    files = req.files
    incoming_data = req.json if req.json else req.form
    db_session = global_data_store.db
    
    for name, field in schema.field_dict.items():
        if field.is_input:
            final_value = None

            if name in incoming_data:
                tmp_value = incoming_data.get(name)
                final_value = validate_basic_fields(field, tmp_value, global_data_store)
            else:
                if field.is_required:
                    raise MissingFieldException(F"'{name}' field is missing from the incoming data.")
                else:
                    final_value = field.default            
            
            # Files are not part of request.json or request.form
            if field.type == FieldType.FILE:
                final_value = files.get(name)

            output[name] =  final_value
        else:
            if field.type == FieldType.CURRENT_TS:
                final_value = get_current_ts()
                output[name] =  final_value

            elif field.type == FieldType.CURRENT_USER:
                final_value = get_user_id(req, db_session, secret_key)
                output[name] =  final_value
            
    return output
    
# This expects custom dict which will be filled by schema and global data store 
def prepare_read(schema: DataSchema, obj:Dict, formatter: QueryFormatter, store= None, cached_choices= None):
    packet = dict()

    for key_name, field in schema.field_dict.items():
        col_value = getattr(obj, key_name)

        if field.type == FieldType.REF and col_value:
            if formatter and formatter.ref_mode == ReadRefType.ext:
                if schema.type == ResourceType.master:
                    current_choices = cached_choices[key_name] if cached_choices else _prepare_FieldChoices(field.options, store)
                    packet[field.name] = current_choices[col_value]
                else:
                    packet[key_name] = col_value 
            else:
                packet[key_name] = col_value            

        elif field.type == FieldType.JSON and col_value:
            if formatter and formatter.ref_mode == ReadRefType.ext:
                packet.update(col_value)
            else:
                packet[key_name] = col_value

        elif field.type == FieldType.CURRENT_USER or field.type == FieldType.CURRENT_TS:
            if formatter and formatter.ref_mode == ReadRefType.ext:
                continue
            else:
                packet[key_name] = col_value
        else:
            packet[key_name] = col_value 

    return packet

# It may happen that we get the existing properties back from the client.
# so we need to check if the props have been changed with existing obj. E.g. updating a ticket.
def prepare_edit(schema: DataSchema, req: Request, current_obj: any, global_data_store= None, secret_key=None):
    output = dict()

    files = req.files
    incoming_data = req.json if req.json else req.form
    db_session = global_data_store.db
    
    for name, field in schema.field_dict.items():
        if field.is_input:
            final_value = None

            if name in incoming_data:
                tmp_value = incoming_data.get(name)
                final_value = validate_basic_fields(field, tmp_value, global_data_store)
            
                if not field.is_mutable:
                    current_value = current_obj[name]

                    if current_value != final_value:
                        raise FieldMutationException(F"'{name}' field is not allowed to be edited. {current_value} with type {type(current_value)} is requested to be changed from {final_value} with type {type(final_value)}.")
                            
            elif field.type == FieldType.FILE:
                final_value = files.get(name)   
                
            else:
                final_value =  current_obj[name]
            
            if field.type == FieldType.FILE:
                if final_value:
                    output[name] = final_value
            else:
                output[name] =  final_value

        else:
            if field.type == FieldType.CURRENT_TS and field.is_mutable:
                final_value = get_current_ts()
                output[name] =  final_value

            elif field.type == FieldType.CURRENT_USER and field.is_mutable:
                final_value = get_user_id(req, db_session, secret_key)
                output[name] =  final_value

    return output

# We expect that user is updating certain field for many objects. User may not want to attach same file for multiple object therefore that use case is not handled. Also it does not expect existing data to be coming again. So it does not check if non-mutable field is really updated or not. e.g. acknowledging an alert. 
# Also we do not check for all the fields. we just check for incoming data.
def prepare_patch(schema: DataSchema, req: Request, global_data_store= None, secret_key=None):
    output = dict()

    incoming_data = req.json if req.json else req.form
    db_session = global_data_store.db

    for name, value in incoming_data.lists():
        if name not in schema.field_dict:
            raise Exception(F"{name} field is not available in {schema.name}")
        field = schema.field_dict[name]

        final_value = None

        final_value = validate_basic_fields(field, value[0], global_data_store)

        if not field.is_mutable:
            raise FieldMutationException(F"Patch operation does not allow non-mutable fields to be updated. Therefore '{name}' field is not allowed to be edited. ")
        
        output[name] =  final_value

    for name, field in schema.field_dict.items():
        if field.type == FieldType.CURRENT_TS and field.is_mutable:
            output[name] = get_current_ts()
        elif field.type == FieldType.CURRENT_USER and field.is_mutable:
            output[name] = get_user_id(req, db_session, secret_key)


    return output
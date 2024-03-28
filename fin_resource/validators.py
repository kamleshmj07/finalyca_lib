from datetime import datetime
import json
from .exceptions import FieldIntegrityException
from .resource_interface import FieldChoices, DataField
from .common_enum import FieldType

def validate_basic_fields(field: DataField, value: str, global_data_store = None):
    rv = None
    if field.type == FieldType.BOOL:
        # Check if value is in bool format.
        if type(value) == bool:
            rv = value
        # the value could be 1 or 0
        elif type(value) == int:
            rv = bool(value)
        elif value.isdigit():
            v = int(value)
            rv = bool(v)
        else:
            if value.lower() == "false":
                rv = False
            elif value.lower() == "true":
                rv = True
            # HACK: for ant design. must be fixed in frontend.
            elif value == "":
                rv = False
            else:
                raise Exception(F"Unexpected {value} for Boolean field {field.name}.")
    
    elif field.type == FieldType.TEXT:
        rv = value
    
    elif field.type == FieldType.INT:
        rv = int(value) if value else 0
    
    elif field.type == FieldType.DECIMAL:
        rv = float(value) if value else 0.
    
    elif field.type == FieldType.JSON:
        rv = json.loads(value) if type(value) == str else value
    
    elif field.type == FieldType.DATE:
        rv = _parse_date(field, value)
    
    elif field.type == FieldType.TS:
        rv = _parse_timestamp(field, value)
    
    elif field.type == FieldType.REF:
        rv = _validate_reference(field, value, global_data_store)

    else:
        raise Exception(F"{field.type} cannot be parsed by BasicFieldParser.")

    return rv

def _parse_date(field: DataField, value):
    try:
        if type(value) == str:
            # value could be in html5 datetime input value
            return datetime.fromisoformat(value)
    except ValueError as err:
        raise FieldIntegrityException(F"'{field.name}' field must be a HTML5 datetime format.")
    
def _parse_timestamp(field: DataField, value):
    return _parse_date(field, value)

def _validate_reference(field, value, global_data_store = None):    
    # Only check if value is not empty.
    final_value = None
    if value:
        current_choices = _prepare_FieldChoices(field.options, global_data_store)
            
        final_value = _check_choices(value, current_choices)
    
    return final_value

def _prepare_FieldChoices(options: FieldChoices, global_data_store = None, self_model_name = None):
    current_choices = dict()

    if options.choices:
        current_choices = options.choices            
    
    elif options.model_name:
        target_model = global_data_store.get(options.model_name)
        current_choices = target_model.get_choices(global_data_store.db)
    
    elif options.column_name:
        self_model = global_data_store.get(self_model_name)
        current_choices = self_model.get_distinct_refs(global_data_store.db, options.column_name)        

    else:
        raise Exception("Currently this option is not supported.")

    return current_choices

def _check_choices(value, current_choices):
    valid_value = None

    # if incoming value type and key types are the same
    if value in current_choices:
        valid_value = value
    
    # if incoming value is not of the same type.
    key_type = type(list(current_choices.keys())[0])
    if not valid_value:
        if key_type == str and type(value) == int:
            if str(value) in current_choices:
                valid_value = str(value)
        elif key_type == int and type(value) == str:
            if int(value) in current_choices:
                valid_value = int(value)

    return valid_value
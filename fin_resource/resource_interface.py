from .common_enum import ResourceType, FieldType, FieldChoicesType
from .utils import prettify
from typing import List, Dict

class DataField:
    def __init__(
        self, 
        name: str, 
        type: FieldType,
        default: any = None, 
        label: str = None, 
        is_required: bool = False, 
        is_input: bool=True,
        is_mutable: bool = True, 
        options: Dict = None
    ):
        self.name = name
        self.type = type
        self.default = default
        self.label = label if label else prettify(name)
        self.is_required = is_required if is_required is not None else False
        self.is_input = is_input if is_input is not None else True
        self.is_mutable = is_mutable if is_mutable is not None else True
        self.options = options

        if type == FieldType.ID or type == FieldType.CURRENT_TS or type == FieldType.CURRENT_USER:
            self.is_input = False
        
        if type == FieldType.REF:
            if options is None:
                raise Exception(F"Options must be set for {self.type.value} type fields.")


class FieldChoices:
    def __init__(self):
        self.type = FieldChoicesType

        # key/value or key/object choices
        self.choices = dict()
        # name of the model
        self.model_name = str()
        # name of the column in sql model -> this will result in a list converted to a dictionary. e.g. { "LABEL" : "LABEL" } -> this will have same values for key and value.
        self.column_name = str()
    
    @classmethod
    def from_dict(cls, dict_choices):
        if type(dict_choices) == type(dict()):
            obj = cls()
            obj.type = FieldChoicesType.static
            obj.choices = dict_choices
            return obj
        else:
            raise Exception("Function expects dictionary input for choices")

    @classmethod
    def from_model(cls, model_name):
        if type(model_name) == str:
            obj = cls()
            obj.type = FieldChoicesType.master_ref
            obj.model_name = model_name
            return obj
        else:
            raise Exception(F"Function expects model name in string. Currently it is in {type(model_name)}")

    @classmethod
    def from_view_column(cls, column_name):
        if type(column_name) == str:
            obj = cls()
            obj.type = FieldChoicesType.view_category
            obj.column_name = column_name
            return obj
        else:
            raise Exception(F"Function expects column name in string. Currently it is in {type(column_name)}")

class DataSchema:
    def __init__(self, name: str, fields: List[DataField], label: str = None, description: str = None, type: ResourceType = ResourceType.master):
        self.name = name
        self.label = label if label else prettify(name)
        self.description = description
        self.type = type
        self.field_dict : Dict[str, DataField] = dict()
        for f in fields:
            self.field_dict[f.name] = f

class DataStore:
    def __init__(self, db_session):
        self.db = db_session
        self.models = dict()        

    def add(self, name, model):
        if name not in self.models:
            self.models[name] = model
        else:
            raise Exception(F"{name} is already added to DataStore instance")

    def get(self, model_name):
        return self.models.get(model_name, None)



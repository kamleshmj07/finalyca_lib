from .common_enum import ResourceType, FieldType , FieldChoicesType
from .validators import _prepare_FieldChoices

def schema_to_json(self, store = None):
    d = dict()
    d["name"] = self.name
    d["label"] = self.label
    d["description"] = self.description
    d["type"] = self.type.name

    fields = dict()
    for name, f in self.field_dict.items():
        if f.type == FieldType.REF:
            if self.type == ResourceType.master:
                f.options.choices = _prepare_FieldChoices(f.options, store)
            elif self.type == ResourceType.view:
                f.options.choices = _prepare_FieldChoices(f.options, store, self.name)

        f_json = field_to_json(f)
        fields[name] = f_json
    d["fields"] = fields

    return d

def field_to_json(self):
    d = dict()
    d["name"] = self.name
    d["type"] = self.type.value
    d["label"] = self.label
    d["default"] = self.default
    d["is_input"] = self.is_input
    d["is_required"] = self.is_required
    d["is_mutable"] = self.is_mutable
    if self.type == FieldType.REF:
        d["options"] = options_to_json(self.options) 

    return d

def options_to_json(self):
    choices = self.choices
    source = None

    if self.type == FieldChoicesType.master_ref:
        source = self.model_name
    
    elif self.type == FieldChoicesType.view_category:
        source = self.column_name

    obj = dict()
    obj["type"] = self.type.name
    if choices:
        obj["data"] = choices
    else:
        obj["source"] = source

    return obj
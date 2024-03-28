from .common_enum import ResourceType
from .resource_interface import DataStore
from .data_table import DataTable


def add_sqlalchemy_model(store: DataStore, model, fields_override, resource_type: ResourceType = ResourceType.master):
    store.add(
        model.__tablename__ , 
        DataTable(model, fields_override, resource_type)
    )
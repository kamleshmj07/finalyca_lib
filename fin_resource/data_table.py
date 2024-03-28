from .query_formatter import QueryFormatter
from .common_enum import FieldType, FilterType, ReadResponseType, ResourceType
from .utils import prettify
from .resource_interface import DataSchema, DataField
import sqlalchemy
import logging
from typing import Dict

class DataTable:
    def __init__(self, sqlalchemy_model, fields_override, resource_type):
        self.model = sqlalchemy_model
        self.admin_resource = self.from_sqlalchemy_model(self.model, fields_override, resource_type)

        self.pk_list = list()
        for column in self.model.__mapper__.primary_key:
            self.pk_list.append(column.name)
    
    def from_sqlalchemy_model(self, model, fields_override: Dict = {}, type: ResourceType = ResourceType.master):        
        model_name = model.__table__.name
        fields = _fields_from_model(model, fields_override)     
        return DataSchema(model_name, fields, prettify(model_name), type=type)

    def get_schema(self):
        return self.admin_resource
   
    # return int if single primary key is there.
    # else returns None.
    def _get_id(self, obj):
        pkey = self.pk_list
        if len(pkey) > 1:
            return None
        elif len(pkey) == 1:
            return getattr(obj, pkey[0] )
        else:
            raise "No Primary Key Found!"

    def _get_object(self, db, id):
        return db.query(self.model).filter(self.model.__table__.c[self.pk_list[0]]==id).one()
   
    def create(self, db, form_data):
        obj = self.model()

        for name, value in form_data.items():
            if name not in self.pk_list:
                setattr(obj, name, value)
        
        db.add(obj)
        db.commit()
        
        return self._get_id(obj)

    def edit(self, db, form_data, id):
        obj = self._get_object(db, id)

        for name, value in form_data.items():
            if name not in self.pk_list:
                setattr(obj, name, value)

        db.commit()
        
    def update(self, db, partial_form, id):
        obj = self._get_object(db, id)

        for name, value in partial_form.items():
            if name not in self.pk_list:
                setattr(obj, name, value)
        
        db.commit()

    def remove(self, db, id):
        sql_obj = db.query(self.model).get(id)
        db.delete(sql_obj)
        db.commit()

    def update_many(self, db, partial_form, filter_dict):
        update_values = dict()
        for name, value in partial_form.items():
            update_values[name]= value

        partial_q = self._prepare_query(db, self.model, filter_dict)
        partial_q.update(update_values, synchronize_session='fetch')
        db.commit()

    def remove_many(self, db, filter_dict):
        partial_q = self._prepare_query(db, self.model, filter_dict)
        partial_q.delete()
        db.commit()

    def get_many(self, db, filter_dict, formatter, verbose):
        partial_q = self._prepare_query(db, self.model, filter_dict, formatter, verbose)
        sql_objs = partial_q.all()
        return sql_objs

    def get_distinct_refs(self, db, field_name):
        choices = dict()

        col_class = getattr(self.model, field_name)
        sql_objs = db.query(sqlalchemy.func.distinct(col_class)).all()

        for sql_obj in sql_objs:
            choices[sql_obj[0]] = sql_obj[0]

        return choices

    def get_count(self, db, filter_dict, verbose):
        pk_col = getattr(self.model, self.pk_list[0])
        q = db.query(sqlalchemy.func.count(pk_col))

        if filter_dict:       
            for name, filter in filter_dict.items():
                col_class = getattr(self.model, name)

                if filter.type == FilterType.exact:
                    q = q.filter(col_class==filter.value)
                elif filter.type == FilterType.partial:
                    q = q.filter(col_class.like(F"%{filter.search}%"))
                elif filter.type == FilterType.range:
                    q = q.filter(col_class.between(filter.min, filter.max))
                elif filter.type == FilterType.choice:
                    q = q.filter(col_class.in_(tuple(filter.choices)))
                else:
                    raise Exception("Unsupported filter type")

        count = q.scalar()

        return count

    def get_results(self, db, filter_dict, formatter: QueryFormatter, verbose):
        partial_q = self._prepare_query(db, self.model, filter_dict, formatter, verbose)
        sql_objs = partial_q.all()
        return sql_objs 

    def get(self, db, id):
        sql_obj = db.query(self.model).get(id)
        return sql_obj

    def get_choices(self, db, filter_dict=None):
        frmter = QueryFormatter(None)
        frmter.resp_category = ReadResponseType.sel
        partial_q = self._prepare_query(db, self.model, filter_dict, frmter, False)
        sql_objects = partial_q.all()

        selection = dict()
        for sql_obj in sql_objects:
            selection[self._get_id(sql_obj)] = str(sql_obj)
        
        return selection

    def _prepare_query(self, db_session, model, filter_dict, formatter: QueryFormatter = None, verbose: bool = False):
        q = db_session.query(model)

        if filter_dict:       
            for name, filter in filter_dict.items():
                col_class = getattr(model, name)

                if filter.type == FilterType.exact:
                    q = q.filter(col_class==filter.value)
                elif filter.type == FilterType.partial:
                    q = q.filter(col_class.like(F"%{filter.search}%"))
                elif filter.type == FilterType.range:
                    q = q.filter(col_class.between(filter.min, filter.max))
                elif filter.type == FilterType.choice:
                    q = q.filter(col_class.in_(tuple(filter.choices)))
                else:
                    raise Exception("Unsupported filter type")

        if formatter.sort:
            sort_col = getattr(model, formatter.sort)
            q = q.order_by(sort_col)

        if formatter.page_nr:
            offset = formatter.page_nr * (formatter.page_size - 1)
            if not formatter.sort:
                # For MSSQL, it is not possible to offset query without specifying order by column. so when user does not provide the sorting option, it will use primary key for the sorting. 
                sort_col = model.__mapper__.primary_key[0]
                q = q.order_by(sort_col)
            if offset > 0:
                q = q.offset(offset)
            
        if formatter.page_size:
            q = q.limit(formatter.page_size)

        if verbose:
            q_str = str(q.statement.compile(compile_kwargs={"literal_binds": True}
            # , dialect=q.session.bind.dialect
            ))
            logging.info(q_str)
        return q

def _fields_from_model(model, fields_override):
    model_name = model.__table__.name
    columns = model.__mapper__.attrs.keys()

    for override_col in fields_override.keys():
        if override_col not in columns:
            raise Exception(F"{override_col} is not part of the {model_name}.")

    pk_list = list()
    for column in model.__mapper__.primary_key:
        pk_list.append(column.name)

    fields = list()
    for column in model.__table__.c:
        col_name = column.name
        col_type = _convert_type(type(column.type).__name__)
        col_label = ""
        col_default = column.default.arg if column.default else None
        col_is_required = False if column.nullable else True
        col_options = None
        col_is_input = True
        col_is_mutable = True

        if col_name in pk_list:
            # primary keys are generated by database therefore they are neither required or part of the input
            col_type = FieldType.ID
            col_is_required = False
            col_is_input = False
            col_is_mutable = False

        if col_name in fields_override:
            custom_override = fields_override[col_name]
            if "type" in custom_override:
                col_type = custom_override["type"]
            if "label" in custom_override:
                col_label = custom_override["label"]
            if "default" in custom_override:
                col_default = custom_override["default"]
            if "is_input" in custom_override:
                col_is_input = custom_override["is_input"]
            if "is_required" in custom_override:
                col_is_required = custom_override["is_required"]
            if "is_mutable" in custom_override:
                col_is_mutable = custom_override["is_mutable"]
            if "options" in custom_override:
                col_options = custom_override["options"]
                
        fields.append(DataField(name=col_name, type=col_type, default=col_default, label=col_label,  is_required=col_is_required, is_input=col_is_input, is_mutable=col_is_mutable, options=col_options))        

    return fields

def _convert_type(sqlalchemy_column_type):
    column_type = None
    if sqlalchemy_column_type == 'Integer':
        column_type = FieldType.INT
    elif sqlalchemy_column_type == 'BIGINT':
        column_type = FieldType.INT
    elif sqlalchemy_column_type == 'BigInteger':
        column_type = FieldType.INT
    elif sqlalchemy_column_type == 'Float':
        column_type = FieldType.DECIMAL
    elif sqlalchemy_column_type == 'Numeric':
        column_type = FieldType.DECIMAL
    elif sqlalchemy_column_type == 'String':
        column_type = FieldType.TEXT
    elif sqlalchemy_column_type == 'Unicode':
        column_type = FieldType.TEXT 
    elif sqlalchemy_column_type == 'Text':
        column_type = FieldType.TEXT
    elif sqlalchemy_column_type == 'Boolean':
        column_type = FieldType.BOOL
    elif sqlalchemy_column_type == 'JSON':
        column_type = FieldType.JSON
    elif sqlalchemy_column_type == 'DateTime':
        column_type = FieldType.TS
    elif sqlalchemy_column_type == 'Date':
        column_type = FieldType.DATE
    else:
        raise Exception(F"Unknown sqlalchmey type {sqlalchemy_column_type} is found.")

    return column_type
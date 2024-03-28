from typing import Dict
from flask import current_app
from sqlalchemy import and_, desc, func, or_
from werkzeug.exceptions import BadRequest
from sqlalchemy.orm import Session

from .flask_bl import prepare_read
from .common_enum import AggregationEnum, ComparisonEnum
from .query_formatter import QueryFormatter
from .resource_interface import DataSchema
from .query_validation import can_compare, can_aggregate, can_compare_with_aggregate
from .exceptions import NotSupportedException

# SQL Operator
# =	Equal	
# >	Greater than	
# <	Less than	
# >=	Greater than or equal	
# <=	Less than or equal	
# <>	Not equal. Note: In some versions of SQL this operator may be written as !=	
# LIKE	Search for a pattern	
# IN	To specify multiple possible values for a column
# BETWEEN	Between a certain range	

def get_aggregate_value(db_session, model, aggregate_field, aggregate_func):
    col_class = getattr(model, aggregate_field)
    if aggregate_func== AggregationEnum.avg.name:
        q = db_session.query(func.avg(col_class))
    elif aggregate_func == AggregationEnum.std.name:
        # TODO: This function does not work with Sqlite
        q = db_session.query(func.std(col_class))
    elif aggregate_func == AggregationEnum.cnt.name:
        q = db_session.query(func.count(col_class))
    elif aggregate_func == AggregationEnum.max.name:
        q = db_session.query(func.max(col_class))
    elif aggregate_func == AggregationEnum.min.name:
        q = db_session.query(func.min(col_class))
    elif aggregate_func == AggregationEnum.sum.name:
        q = db_session.query(func.sum(col_class))

    return q.scalar()

def setup_rule(model, field: str, operator: str, value: any):
    sql_rule = None
    
    col_class = getattr(model, field)

    if operator == "=":
        sql_rule = (col_class == value)
    elif operator == "!=":
        sql_rule = (col_class != value)
    elif operator == "<":
        if value is None:
            sql_rule = None
        else:
            sql_rule = (col_class < value)
    elif operator == "<=":
        if value is None:
            sql_rule = None
        else:
            sql_rule = (col_class <= value)
    elif operator == ">":
        if value is None:
            sql_rule = None
        else:
            sql_rule = (col_class > value)
    elif operator == ">=":
        if value is None:
            sql_rule = None
        else:
            sql_rule = (col_class >= value)
    elif operator == "contains":
        sql_rule = (col_class.like(F"%{value}%"))
    elif operator == "in":
        # Only use in variable if there are any values.
        if len(value):
            sql_rule = (col_class.in_(value))
        else:
            sql_rule = None
    else:
        raise Exception(F"Unknown operator {operator} has been used")

    return sql_rule

def process_rule(db_session, resource_schema:DataSchema, model, rule: Dict):
    field = rule["field"]
    ope = rule["operator"]
    value_type = rule["value_type"]

    # check if the operator is supported by the field
    resource_field = resource_schema.field_dict[field]
    if not can_compare(resource_field.type, ComparisonEnum(ope)):
        raise NotSupportedException(F"{field} does not support {ope}")

    if value_type == "value":
        value = rule["value"]
        return setup_rule(model, field, ope, value)

    elif value_type == "field":
        value = rule["value"]
        value_class = getattr(model, value)
        return setup_rule(model, field, ope, value_class)

    elif value_type == "aggregate":
        #  Could be multiple values
        value = rule["value"]
        aggregate_field = value["field"]
        aggregate_func = value["func"]
        
        f_agg = resource_schema.field_dict[aggregate_field]
        if not can_aggregate(f_agg.type, AggregationEnum[aggregate_func]):
            raise NotSupportedException(F"{field} does not support {ope}")

        agg_esource_field = resource_schema.field_dict[field]
        if not can_compare_with_aggregate(agg_esource_field.type):
            raise NotSupportedException(F"{field} does not support aggregation comparison")
        
        abs_value = get_aggregate_value(db_session, model, aggregate_field, aggregate_func)
        return setup_rule(model, field, ope, abs_value)

def process_rule_group(db_session, resource_schema:DataSchema, model, rules, combinator, is_not):
    r = list()
    for rule in rules:
        if "combinator" in rule:
            # found a rule group
            r.append(process_rule_group(db_session, resource_schema, model, rule["rules"], rule["combinator"], rule["not"]))
        else:
            rule = process_rule(db_session, resource_schema, model, rule)
            if rule is not None:
                r.append(rule)
    
    if len(r) > 1:
        if combinator == "and":
            res = and_(*r)
        elif combinator == "or":
            res = or_(*r)
    elif len(r) == 1:
        res = r
    
    return res

def process_query_builder(db_session: Session, model_name: str, d: Dict):
    schema_model = current_app.store.get(model_name)
    # schema_model = __get_model(current_app.store, model_name)
    schema = schema_model.get_schema()
    model = schema_model.model

    q = db_session.query(model)    
    res = process_rule_group(db_session, schema, model, d["rules"], d["combinator"], d["not"])    
    q = q.filter(*res)
    
    # q_str = str(q.statement.compile(compile_kwargs={"literal_binds": True}, dialect=q.session.bind.dialect))
    # print(q_str)

    sql_objects = q.all()

    res = list()
    for sql_obj in sql_objects:
        o = prepare_read(schema, sql_obj, QueryFormatter(None), store=current_app.store)
        res.append(o)

    return res

def process_query_builder_for_entity(db_session: Session, model_name: str, entity:str, d: Dict):
    schema_model = current_app.store.get(model_name)
    resource_schema = schema_model.get_schema()
    model = schema_model.model
    entity_class = getattr(model, entity)
    q = db_session.query(entity_class)    
    res = process_rule_group(db_session, resource_schema, model, d["rules"], d["combinator"], d["not"])    
    q = q.filter(*res)

    q_str = str(q.statement.compile(compile_kwargs={"literal_binds": True}, 
    # dialect= model.bind.dialect
    ))
    print(q_str)

    sql_objects = q.all()

    res = list()
    for sql_obj in sql_objects:
        o = sql_obj[0]
        res.append(o)

    return res

from .common_enum import FieldType, AggregationEnum, ComparisonEnum

def can_compare(field_type: FieldType, operator: ComparisonEnum):
    result = False

    if field_type == FieldType.BOOL:
        if operator == ComparisonEnum._eq or operator == ComparisonEnum._neq:
            result = True
    
    elif field_type == FieldType.TEXT:
        if operator == ComparisonEnum._eq or operator == ComparisonEnum._neq or operator == ComparisonEnum._has or operator == ComparisonEnum._in:
            result = True
    
    elif field_type == FieldType.REF:
        if operator == ComparisonEnum._eq or operator == ComparisonEnum._neq or operator == ComparisonEnum._in:
            result = True

    elif field_type == FieldType.INT or field_type == FieldType.DECIMAL or field_type == FieldType.TS or field_type == FieldType.DATE:
        if operator == ComparisonEnum._eq or operator == ComparisonEnum._neq or operator == ComparisonEnum._lt or operator == ComparisonEnum._lte or operator == ComparisonEnum._gt or operator == ComparisonEnum._gte or operator == ComparisonEnum._in or operator == ComparisonEnum._between:
            result = True

    return result

def can_aggregate(field_type: FieldType, agg_func: AggregationEnum):
    result = False

    if field_type == FieldType.BOOL or field_type == FieldType.TEXT or field_type == FieldType.REF or field_type == FieldType.TS or field_type == FieldType.DATE:
        if agg_func == AggregationEnum.cnt:
            result = True

    elif field_type == FieldType.INT or field_type == FieldType.DECIMAL:
        if agg_func == AggregationEnum.cnt or agg_func == AggregationEnum.sum or agg_func == AggregationEnum.max or agg_func == AggregationEnum.min or agg_func == AggregationEnum.avg or agg_func == AggregationEnum.std:
            result = True
    
    return result

def can_compare_with_aggregate(field_type: FieldType):
    result = False

    if field_type == FieldType.INT or field_type == FieldType.DECIMAL:
        result = True
    
    return result
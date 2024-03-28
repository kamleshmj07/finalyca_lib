import enum

class ResourceType(enum.Enum):
    # master data entities. Normalized data that depends on other data sources to have complete picture. 
    master = "Master Models"
    # tables that do not belong to other data sources. Report or view table -> csv files.
    # could have REF but are stored as text. has to be assigned at the data layer to ensure multi and single select.
    # Also single_table does not expect CRUD operations. It will mainly have screener. De-normalized tables -> subset of original transactional tables. PK is not important.
    view = "View Models"

class FieldType(enum.Enum):
    ID = "ID"
    BOOL = "BOOL"
    TEXT = "TEXT"
    REF = "REF"
    # Number Types
    INT = "INT"
    DECIMAL = "DECIMAL"
    # Date time types
    TS = "TS"   # Timestamp: datetime
    DATE = "DATE"
    # More complex types
    FILE = "FILE"
    JSON = "JSON"
    # only for backend. never input
    CURRENT_USER = "CURRENT_USER"
    CURRENT_TS = "CURRENT_TS"

class FieldChoicesType(enum.Enum):
    # Useful for static choices e.g. enums or dictionaries -> will not check for new values after initilization.
    static = "Static"
    # returns pk, label from the another table in same database. There has to be a source model.
    master_ref = "Master Reference"
    # text column will become a REF field. Good for CSV for categories. It may or may not have a source model and field.
    view_category = "View Category"

class FilterType(enum.Enum):
    exact = "Exact Filter"
    partial = "Partial Filter"
    range = "Range Filter"
    choice = "Choices Filter"
    
class AggregationEnum(enum.Enum):
    sum = "Total"
    max = "Maximum"
    min = "Minimum"
    avg = "Average"
    cnt = "Count"
    std = "Standard Deviation"

class ComparisonEnum(enum.Enum):
    _eq = "="
    _neq = "!="
    _gt = ">"
    _gte = ">="
    _lt = "<"
    _lte = "<="
    _has = "contains"
    _in = "in"

class ReadResponseType(enum.Enum):
    sel = "Selection"
    rec = "Records"

class ReadRefType(enum.Enum):
    ext = "External"
    adm = "Admin"
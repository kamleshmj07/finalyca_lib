from flask import Request
from .common_enum import *

class QueryFormatter:
    """
    All response formatter parameters will have underscore to differentiate with schema fields.
    - _rsp : enum (response: expected response type for GET requests)
        - rec: default option, will send list of records (list of complete objects)
        - sel: will send list of choices for the dropdown (optimized for ANT design UI)
        - agg: will send aggregation value for the field. (Max of cash in the table)
    - _rf_m : enum (reference_mode: only relavent if _rsp = rec (records) is selected)
        - ext -> external : it will normalize foreign keys to the string representation
        - adm -> admin : it will return keys showing reference to the backend data storage. useful for admin interface. could be used with frontend caching too.
    - _pg_size : int (limit, it will limit the results to the number provided)
    - _pg_nr : int (offset, records to be sent will start after this count (if offset is 100 and result has 150, the response will have records from 101-150))
    - _sort : str (sort : field name based on which results will be sorted)
    - _asc : int (ascending)
        - 1 for ascending and 0 for descending

    usage: 
    /admin/AMC?_hmn=1&_smpl=1 will show asset types with labels instead of keys and references normalized to keys
    """
    def __init__(self, req: Request):
        self.resp_category =  ReadResponseType.rec
        self.ref_mode = ReadRefType.ext
        self.page_size = 0
        self.page_nr = 0
        self.sort = None
        self.ascending = 0

        self.filter = dict()

        if req:
            rsp = req.args.get("_rsp", type=str, default=ReadResponseType.rec.name)
            self.resp_category =  ReadResponseType[rsp]

            rfm = req.args.get("_rf_m", type=str, default=ReadRefType.ext.name)
            self.ref_mode = ReadRefType[rfm]
            
            self.page_size = req.args.get("_pg_size", type=int, default=0)
            self.page_nr = req.args.get("_pg_nr", type=int, default=0)  

            self.sort = req.args.get("_sort", type=str, default=None)
            self.ascending = req.args.get("_asc", type=int, default=0)  

            self.filter = dict()
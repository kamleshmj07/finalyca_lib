from typing import List
from locale import atof
from .market_interface import *
from .common_func import *

def is_str_valid(content : str):
    is_valid = True
    if content == "--":
        is_valid = False
    return is_valid
    
def parse_cdsl_holdings(rows, holdings):
    total_found = False

    if check_for_empty_table(rows):
        return total_found

    for row in rows:
        if row[0] and row[0].startswith("ISIN"):
            # Header found. skip it                
            continue

        if "Portfolio Value" in row[0]:
            total_found = True
            continue
        
        new_row = list()
        for cell in row:
            new_cell = " ".join(cell.split("\n")) if cell else ""
            new_row.append(new_cell)

        obj = Holding()
        obj.isin = new_row[0]
        obj.name = new_row[1]
        obj.total_units = atof(new_row[2]) if is_str_valid(new_row[2]) else 0
        obj.unit_value = atof(new_row[7]) if is_str_valid(new_row[2]) else 0
        obj.total_value = atof(new_row[8]) if is_str_valid(new_row[2]) else 0
        holdings.append(obj)
        
    return total_found

def parse_nsdl_holdings(rows, holdings):
    total_found = False

    if check_for_empty_table(rows):
        return total_found

    for row in rows:
        # if row[0] and row[0].startswith("ISIN"):
        if row[0] and 'ISIN' in row[0]:
            # Header found. skip it                
            continue
        
        # if row[2] and row[2].startswith("b) Pledge"):
        if row[2] and "Pledge" in row[2]:
            # Header found. skip it                
            continue
        
        if "Portfolio Value" in row[0]:
            total_found = True
            continue

        new_row = list()
        for cell_idx, cell in enumerate(row):
            if cell_idx > 1 and cell_idx < 7:
                new_cells = cell.split("\n")
                for nc in new_cells:
                    if not is_str_valid(nc):
                        nc = None
                    new_row.append(nc)
            else:
                new_cell = " ".join(cell.split("\n")) if cell else ""
                if not is_str_valid(new_cell):
                    new_cell = None
                new_row.append(new_cell)
        
        obj = Holding()
        obj.isin = new_row[0]
        obj.name = new_row[1]
        obj.total_units = atof(new_row[2]) if new_row[2] else 0.0
        obj.unit_value = atof(new_row[13]) if new_row[13] else 0.0
        obj.total_value = atof(new_row[14]) if new_row[14] else 0.0

        holdings.append(obj)

    return total_found

def parse_mf_holdings(rows, folio_list : List[FolioAccount]):
    total_found = False
    header = list()

    col_mapping_v1 = {"name": 0, "isin": 1, "folio": 2, "units": 3, "nav": 4, "total_investment": 5, "total_value": 6}
    col_mapping_v2 = {"name": 0, "isin": 1, "folio": 2, "arn": 3, "units": 4, "nav": 5, "total_investment": 6, "total_value": 7, "regular_expesne": 8, "direct_expense":9, "commission": 10}

    active_mapping = None

    for row in rows:
        if not row[0]:
            continue
        
        if row[0] == "Scheme Name":
            # skip the header
            if not header:
                header = row
                if len(header) == 7:
                    active_mapping = col_mapping_v1
                elif len(header) == 11:
                    active_mapping = col_mapping_v2
            continue

        if row[0] == "Grand Total":
            total_found = True
            continue

        new_row = list()
        for cell in row:
            new_cell = " ".join(cell.split("\n")) if cell else ""
            new_row.append(new_cell)

        name = new_row[active_mapping["name"]]
        isin = new_row[active_mapping["isin"]]
        folio_no = new_row[active_mapping["folio"]]
        units = new_row[active_mapping["units"]]
        nav = new_row[active_mapping["nav"]]
        total_value = new_row[active_mapping["total_value"]]

        wip_folio = None
        for folio in folio_list:
            if folio.folio_no == folio_no:
                wip_folio = folio
                break
        
        if not wip_folio:
            wip_folio = FolioAccount()
            wip_folio.folio_no = folio_no
            folio_list.append(wip_folio)

        obj = Holding()
        obj.name = name
        obj.isin = isin
        obj.total_units = atof(units) if units else  0.0
        obj.unit_value = atof(nav) if nav else 0.0
        obj.total_value = atof(total_value) if total_value else 0.0
        wip_folio.holdings.append(obj)

    return total_found



from typing import List
from .market_interface import *
from .common_func import *

def merge_cell(cell):
    name = " ".join(cell.split("\n")) if cell else ""
    name = "-".join(name.split(",")) if name else ""
    name = name.strip()
    return name

def get_nsdl_holdings(lines):
    holdings = list()
        
    isin_type = ISINType.NONE
    for line in lines:
        if isinstance(line, str):
            new_isin_type, sub_type = check_isin(line)
            if new_isin_type != ISINType.NONE:
                isin_type = new_isin_type

        elif isinstance(line, list):
            if line[0] and line[0].startswith("ISIN"):
                # Header found. skip it                
                continue
                
            if line[0] and line[0] == "Sub Total":
                # Footer found. skip it                
                continue
                
            if line[0] and line[0] == "Total":
                # Footer found. skip it                
                continue
            
            if isin_type == ISINType.EQUITIES:
                parse_nsdl_row_equity(line, isin_type, holdings)          

            elif isin_type == ISINType.PREFERENCE_SHARES:
                parse_nsdl_row_preference_shares(line, isin_type, holdings)
            
            elif isin_type == ISINType.CORPORATE_BONDS:
                parse_nsdl_row_bond(line, isin_type, holdings, sub_type)

            elif isin_type == ISINType.GOVERNMENT_SECURITIES:
                parse_nsdl_row_bond(line, isin_type, holdings, sub_type)

            elif isin_type == ISINType.MUTUAL_FUNDS:
                parse_nsdl_row_mutual_fund(line, isin_type, holdings)

    return holdings

def get_cdsl_holdings(lines):
    holdings = list()
        
    isin_type = ISINType.NONE
    for line in lines:
        if isinstance(line, str):
            isin_type, sub_type = check_isin(line)
        elif isinstance(line, list):
            if line[0] and line[0].startswith("ISIN"):
                # Header found. skip it                
                continue
                
            if line[0] and line[0] == "Sub Total":
                # Footer found. skip it                
                continue
                
            if line[0] and line[0] == "Total":
                # Footer found. skip it                
                continue
            
            isin = line[0].strip()
            name = " ".join(line[1].split("\n"))
            name = "-".join(name.split(","))
            cur_balance = line[2].split("\n")[0]
            market_price = line[5].split("\n")[0]
            total_value = line[6].split("\n")[0]

            obj = Holding()
            obj.isin = isin
            obj.type = isin_type
            obj.name = name.strip()
            obj.total_units = to_valid_number(cur_balance) if cur_balance else 0.0
            obj.unit_value = to_valid_number(market_price) if market_price else 0.0
            obj.total_value = to_valid_number(total_value) if total_value else 0.0

            holdings.append(obj)
    
    return holdings

def get_folio_holdings(rows):
    folio_list : List[FolioAccount] = []
    
    total_found = False

    if check_for_empty_table(rows):
        return total_found

    for row_ in rows:
        if not isinstance(row_, list):
            continue
            
        if not row_:
            continue

        if row_[0] == "Total":
            total_found = True
            continue

        if "ISIN" in row_[0]:
            continue

        if 'Mutual Fund Folios (F)' in row_[0]:
            continue
        
        isin = ucc = None 
        sl = row_[0].split("\n")
        if len(sl) == 2:
            isin = sl[0]
            ucc = sl[1]
        elif len(sl) == 1:
            if sl[0].startswith("IN"):
                isin = sl[0]
            else:
                ucc = sl[0]
        
        scheme_name = merge_cell(row_[1])
        folio_no = row_[2]

        is_data_missing = False
        if not isin:
            is_data_missing = True
        # if not ucc:
        #     is_data_missing = True
        if not scheme_name:
            is_data_missing = True

        current_folio = None
        for old_folio in folio_list:
            if old_folio.folio_no == folio_no:
                current_folio = old_folio
                break
        
        if not current_folio:
            if not folio_no or is_data_missing:
                current_folio = folio_list[-1]
            else:
                current_folio = FolioAccount()
                current_folio.folio_no = folio_no
                folio_list.append(current_folio)

        if is_data_missing:
            if folio_list:
                # folio = folio_list[-1]
                if len(current_folio.holdings):
                    holding = current_folio.holdings[-1]
                else:
                    holding = Holding()
                    holding.type = ISINType.MUTUAL_FUNDS
                    current_folio.holdings.append(holding)
            else:
                # folio = FolioAccount()
                # folio_list.append(folio)
                holding = Holding()
                holding.type = ISINType.MUTUAL_FUNDS
                current_folio.holdings.append(holding)

            # folio.folio_no = row_[2] if row_[2] else folio.folio_no
            current_folio.ucc = ucc if ucc else current_folio.ucc
            holding.isin = isin if isin else holding.isin
            holding.name = " ".join([holding.name, scheme_name])
            holding.total_units = to_valid_number(row_[3]) if row_[3] else holding.total_units
            # holding.total_investment = atof(row_[5]) if row_[5] else 0.0
            holding.unit_value = to_valid_number(row_[6]) if row_[6] else holding.unit_value
            holding.total_value = to_valid_number(row_[7]) if row_[7] else holding.total_value
            # folio_list.append(obj)
        else:
            # folio = FolioAccount()
            # folio.folio_no = row_[2]
            current_folio.ucc = ucc
            holding = Holding()
            holding.type = ISINType.MUTUAL_FUNDS
            holding.isin = isin
            holding.name = scheme_name
            row_[3] = ''.join(map(str, row_[3].split("\n")))    # Added this line to handle line breaks for units in the eCas statement
            holding.total_units = to_valid_number(row_[3]) if row_[3] else 0.0
            holding.unit_value = to_valid_number(row_[6]) if row_[6] else 0.0
            holding.total_value = to_valid_number(row_[7]) if row_[7] else 0.0
            current_folio.holdings.append(holding)
            # folio_list.append(folio)

    return folio_list

def parse_nsdl_row_preference_shares(row_, isin_type, holdings):
    # Here, we only have Face Value and do not have unit value.
    isin = row_[0].split("\n")[0].strip() if row_[0] else ""
    name = merge_cell(row_[1])
    units = row_[3]
    
    is_data_missing = False
    if not isin:
        is_data_missing = True
    if not name:
        is_data_missing = True
    if not units:
        is_data_missing = True

    if is_data_missing:
        if holdings:
            obj = holdings[-1]
        else:
            obj = Holding()
            holdings.append(obj)
        # obj.isin = " ".join([obj.isin, isin])
        # Stick to previous row ISIN
        obj.isin = obj.isin if obj.isin else isin
        obj.name = " ".join([obj.name, name])
        obj.unit_value = to_valid_number(row_[2]) if row_[2] else obj.unit_value
        obj.total_units = to_valid_number(row_[3]) if row_[3] else obj.total_units
        obj.total_value = to_valid_number(row_[4]) if row_[4] else obj.total_value
    else:
        obj = Holding()
        obj.type = isin_type
        obj.isin = isin
        obj.name = name
        obj.unit_value = to_valid_number(row_[2]) if row_[2] else 0.0
        obj.total_units = to_valid_number(row_[3]) if row_[3] else 0.0
        obj.total_value = to_valid_number(row_[4]) if row_[4] else 0.0
        holdings.append(obj)  

def parse_nsdl_row_equity(row_, isin_type, holdings):
    isin = row_[0].split("\n")[0].strip() if row_[0] else ""
    name = merge_cell(row_[1])
    units = row_[3]
    
    is_data_missing = False
    if not isin:
        is_data_missing = True
    if not name:
        is_data_missing = True
    if not units:
        is_data_missing = True

    if is_data_missing:
        if holdings:
            obj = holdings[-1]
        else:
            obj = Holding()
            holdings.append(obj)
        # obj.isin = " ".join([obj.isin, isin])
        # Stick to previous row ISIN
        obj.isin = obj.isin if obj.isin else isin
        obj.name = " ".join([obj.name, name])
        obj.total_units = to_valid_number(row_[3]) if row_[3] else obj.total_units
        obj.unit_value = to_valid_number(row_[4]) if row_[4] else obj.unit_value
        obj.total_value = to_valid_number(row_[5]) if row_[5] else obj.total_value
    else:
        obj = Holding()
        obj.type = isin_type
        obj.isin = isin
        obj.name = name
        obj.total_units = to_valid_number(row_[3]) if row_[3] else 0.0
        obj.unit_value = to_valid_number(row_[4]) if row_[4] else 0.0
        obj.total_value = to_valid_number(row_[5]) if row_[5] else 0.0
        holdings.append(obj)  

def parse_nsdl_row_bond(row_, isin_type, holdings, sub_type =""):
    isin = row_[0].split("\n")[0].strip() if row_[0] else ""
    name = merge_cell(row_[1])

    is_data_missing = False
    if not isin:
        is_data_missing = True
    if not name:
        is_data_missing = True

    if is_data_missing:
        if holdings:
            obj = holdings[-1]
        else:
            obj = Holding()
            holdings.append(obj)
        obj.isin = " ".join([obj.isin, isin])
        obj.name = " ".join([obj.name, name])
        obj.coupon_rate = merge_cell(row_[2]) if row_[2] else obj.coupon_rate
        obj.maturity_date = merge_cell(row_[3]) if row_[3] else obj.maturity_date
        obj.total_units = to_valid_number(row_[4]) if row_[4] else obj.total_units
        obj.unit_value = to_valid_number(row_[5]) if row_[5] else obj.unit_value
        obj.total_value = to_valid_number(row_[6]) if row_[6] else obj.total_value
    else:
        obj = Holding()
        obj.type = isin_type
        obj.isin = isin
        obj.name = F"{name} {sub_type}" if sub_type else name
        obj.coupon_rate = merge_cell(row_[2])
        obj.maturity_date = merge_cell(row_[3])
        obj.total_units = to_valid_number(row_[4]) if row_[4] else 0.0
        obj.unit_value = to_valid_number(row_[5]) if row_[5] else 0.0
        obj.total_value = to_valid_number(row_[6]) if row_[6] else 0.0
        holdings.append(obj)

def parse_nsdl_row_mutual_fund(row_, isin_type, holdings):
    isin = row_[0].split("\n")[0].strip() if row_[0] else ""
    name = merge_cell(row_[1])

    is_data_missing = False
    if not isin:
        is_data_missing = True
    if not name:
        is_data_missing = True

    if is_data_missing:
        if holdings:
            obj = holdings[-1]
        else:
            obj = Holding()
            holdings.append(obj)

        obj.isin = " ".join([obj.isin, isin])
        obj.name = " ".join([obj.name, name])
        obj.total_units = to_valid_number(row_[2]) if row_[2] else obj.total_count
        obj.unit_value = to_valid_number(row_[3]) if row_[3] else obj.unit_value
        obj.total_value = to_valid_number(row_[4]) if row_[4] else obj.total_value
    else:
        obj = Holding()
        obj.type = isin_type
        obj.isin = isin
        obj.name = name
        obj.total_units = to_valid_number(row_[2]) if row_[2] else 0.0
        obj.unit_value = to_valid_number(row_[3]) if row_[3] else 0.0
        obj.total_value = to_valid_number(row_[4]) if row_[4] else 0.0
        holdings.append(obj)

def check_isin(cell):
    isin_type = ISINType.NONE
    sub_type = ""

    if cell == "Equities (E)":
        isin_type = ISINType.EQUITIES
        
    elif cell == "Equity Shares":
        isin_type = ISINType.EQUITIES

    elif cell == "Preference Shares (P)":
        isin_type = ISINType.PREFERENCE_SHARES

    elif cell == "Mutual Funds (M)":
        isin_type = ISINType.MUTUAL_FUNDS

    elif cell == "Corporate Bonds (C)":
        isin_type = ISINType.CORPORATE_BONDS

    elif cell == "Money Market Instruments (I)":
        isin_type = ISINType.MONEY_MARKET_INSTRUMENTS

    elif cell == "Securitised Instruments (S)":
        isin_type = ISINType.SECURITISED_INSTRUMENTS

    elif cell == "Government Securities (G)":
        isin_type = ISINType.GOVERNMENT_SECURITIES

    elif cell == "Postal Saving Scheme (O)":
        isin_type = ISINType.POSTAL_SAVING_SCHEME

    elif cell == "Mutual Fund Folios (F)":
        isin_type = ISINType.MUTUAL_FUND_FOLIOS

    # HACK: Fixed for split lines for CORPORATE_BONDS
    elif cell == "Fixed Interest Bonds":
        sub_type = "Fixed Interest Bonds"
        isin_type = ISINType.CORPORATE_BONDS

    # HACK: Fixed for split lines for CORPORATE_BONDS
    elif cell == 'Variable Interest\nBonds':
        sub_type = "Variable Interest Bonds"
        isin_type = ISINType.CORPORATE_BONDS

    # HACK: Fixed for split lines for CORPORATE_BONDS
    elif cell == 'Variable Interest - \nIndex Linked Bonds':
        sub_type = "Variable Interest - Index Linked Bonds"
        isin_type = ISINType.CORPORATE_BONDS

    return isin_type, sub_type
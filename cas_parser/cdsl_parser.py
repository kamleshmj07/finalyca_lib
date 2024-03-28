from .BaseParser import BaseParser
from typing import Dict
from .cdsl_helper import  parse_cdsl_holdings, parse_mf_holdings, is_str_valid, parse_nsdl_holdings
from .common_func import make_clean_segments, clean_pdf_data, ReaderStatus, ReaderSubStatus, get_portfolio_account_summary
from cas_parser.market_interface import *
from cas_parser.common_func import *

class CDSLParser(BaseParser):
    def __init__(self) -> None:
        BaseParser.__init__(self)

        self.summary = list()

        self.wip_owner : Owner = None
        self.wip_account : DematAccount = None
        self.wip_folio : FolioAccount = None

        self.skip_lines = [
            "Central Depository Services (India) Limited",
            "A Wing, 25th Floor, Marathon Futurex, Mafatlal Mills Compounds, N M Joshi Marg,",
            "Lower Parel (E), Mumbai - 400013.| (CIN : L67120MH1997PLC112443)",
            "No Transaction during the period",
            "CONSOLIDATED ACCOUNT STATEMENT (CAS) FOR SECURITIES HELD IN DEMAT FORM",
            'AND INVESTMENTS IN MUTUAL FUNDS', 
            'Summary of Investments CDSL Demat Account Details NSDL Demat Account Details MF Details Notes About CDSL',
        ]

    def process(self, raw_content: Dict):
        content = clean_pdf_data(raw_content, self.skip_lines, "Summary of Investments")
        
        overview = list()
        performance = list()
        
        this_status = ReaderStatus.Is_Finding
        this_sub_status = ReaderSubStatus.Is_Waiting
        
        current_account = None
        current_owner = None
        
        info = list()
        holdings = list()
        transactions = list()

        is_mf_holding_already_found = False
        
        for line in content:
            if isinstance(line, str):
                if "YOUR CONSOLIDATED PORTFOLIO VALUE" in line:
                    this_status = ReaderStatus.Reading_Overview
                    continue
                    
                if line == "Consolidated Portfolio Valuation for Year":
                    this_status = ReaderStatus.Reading_Performance
                    continue
                    
                if line == "Consolidated Portfolio for Accounts for the Month":
                    this_status = ReaderStatus.Is_Finding
                    continue
                    
                if 'DEMAT ACCOUNTS HELD WITH CDSL' in line:
                    this_status = ReaderStatus.Reading_CDSL_Demat
                    continue
                    
                if 'DEMAT ACCOUNTS HELD WITH NSDL' in line:
                    this_status = ReaderStatus.Reading_NSDL_Demat
                    continue
                
                if "Statement for the period from" in line:
                    sl = line.split('from')
                    sl = sl.pop(-1)
                    sl = line.split('to')
                    as_on_date = sl.pop(-1).strip()
                    # as_on_date = find_date_using_regex(sl.pop(-1).strip())
                    self.statement.as_on_date = as_on_date

                # if line.startswith("DP Name :"):
                #     # Check if we are reading fresh account or reading the existing account                   
                #     info = list()
                #     holdings = list()
                #     transactions = list()
                    
                #     this_sub_status = ReaderSubStatus.Reading_Account_Info
                #     uid = None
                #     if this_status == ReaderStatus.Reading_CDSL_Demat:
                #         [dp_id, bo_id] = make_clean_segments(line, ["DP Name", "BO ID"])
                #         uid = bo_id
                        
                #     elif this_status == ReaderStatus.Reading_NSDL_Demat:
                #         [dp_name, dp_id, client_id] = make_clean_segments(line, ["DP Name", "DPID", "CLIENT ID"])
                #         uid =  F"{dp_id}{client_id}"
                        
                #     for owner in self.statement.owners:
                #         acc = owner.get_account(uid)
                #         if acc:
                #             current_account = self.wip_account = acc
                #             current_owner = self.wip_owner = owner
                #             break

                #     info.append(line)

                if "BO ID" in line or "DPID" in line:
                    # Check if we are reading fresh account or reading the existing account                   
                    info = list()
                    holdings = list()
                    transactions = list()
                    
                    this_sub_status = ReaderSubStatus.Reading_Account_Info
                    uid = None
                    if this_status == ReaderStatus.Reading_CDSL_Demat:
                        # [dp_id, bo_id] = make_clean_segments(line, ["DP Name", "BO ID"])
                        # uid = bo_id
                        line = "".join(e for e in line if e.isalnum() or e.isspace() )
                        sl = line.split("BO ID")
                        uid = sl[1].strip()
                        
                    elif this_status == ReaderStatus.Reading_NSDL_Demat:
                        # [dp_name, dp_id, client_id] = make_clean_segments(line, ["DP Name", "DPID", "CLIENT ID"])
                        # uid =  F"{dp_id}{client_id}"
                        if "CLIENT ID" in line:
                            [dp_name, dp_id, client_id] = make_clean_segments(line, ["DP Name", "DPID", "CLIENT ID"])
                            uid =  F"{dp_id}{client_id}"        
                        else:
                            line = "".join(e for e in line if e.isalnum() or e.isspace() )
                            sl = line.split("DPID")
                            uid = sl[1].strip()
                        
                    # print(uid)
                    for owner in self.statement.owners:
                        acc = owner.get_account(uid)
                        if acc:
                            current_account = self.wip_account = acc
                            current_owner = self.wip_owner = owner
                            break

                    info.append(line)
                    
                if line == "MUTUAL FUND UNITS HELD WITH MF/RTA":
                    this_status = ReaderStatus.Reading_Folio
                    this_sub_status = ReaderSubStatus.Reading_Account_Info
                    info = list()
                    holdings = list()
                    transactions = list()
                    continue
                
                if line.startswith("MUTUAL FUND UNITS HELD") and "WITH MF/RTA" not in line:
                    this_sub_status = ReaderSubStatus.Reading_Holdings
                    continue
                    
                if line.startswith("STATEMENT OF TRANSACTIONS FOR THE PERIOD"):
                    this_sub_status = ReaderSubStatus.Reading_Transactions
                    continue
                    
                if line.startswith("HOLDING STATEMENT AS ON"):
                    this_sub_status = ReaderSubStatus.Reading_Holdings
                    continue
                    
                if line.startswith('Portfolio Value'):
                    if this_status == ReaderStatus.Reading_CDSL_Demat:
                        raw_holdings = parse_cdsl_holdings(holdings, current_account.holdings)
                    elif this_status == ReaderStatus.Reading_NSDL_Demat:
                        raw_holdings = parse_nsdl_holdings(holdings, current_account.holdings)

                    info = list()
                    holdings = list()
                    transactions = list()
                    continue

            elif isinstance(line, list):
                # line = [remove_non_ascii_from_string(x) for x in line]

                # HACK: Fixing multi-lingual problem
                for single_line in line:
                    if single_line:
                        if "Scheme Name" in single_line:
                            print(line)
                            this_status = ReaderStatus.Reading_Folio
                            this_sub_status = ReaderSubStatus.Reading_Holdings
                            if not is_mf_holding_already_found:
                                is_mf_holding_already_found = True
                                info = list()
                                holdings = list()
                                transactions = list()

                if this_status == ReaderStatus.Reading_Performance:
                    performance.append(line)   

                if this_sub_status == ReaderSubStatus.Reading_Holdings:
                    holdings.append(line)

                if this_sub_status == ReaderSubStatus.Reading_Transactions:
                    transactions.append(line)             
                    
                if 'Grand Total' in line and this_status == ReaderStatus.Reading_Overview:
                    self.statement.owners = get_portfolio_account_summary(overview)
                    this_status = ReaderStatus.Is_Finding
                    continue
                    
                if 'Grand Total' in line and this_status == ReaderStatus.Reading_Folio:
                    if this_status == ReaderStatus.Reading_Folio:
                        raw_holdings = parse_mf_holdings(holdings, current_owner.folio_accounts)

                    info = list()
                    holdings = list()
                    transactions = list()
                    continue

            else:
                raise Exception("Unknown type found.")
            
            if this_status == ReaderStatus.Reading_Overview:
                overview.append(line)
            
            if this_sub_status == ReaderSubStatus.Reading_Account_Info:
                info.append(line)     
                
        # print(overview)
        # print(performance)
        return self.statement
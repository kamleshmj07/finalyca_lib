from .BaseParser import BaseParser
from typing import Dict
from .nsdl_helper import *
from .common_func import make_clean_segments, clean_pdf_data, ReaderStatus, ReaderSubStatus, get_portfolio_account_summary

class NSDLParser(BaseParser):
    def __init__(self) -> None:
        BaseParser.__init__(self)

        self.summary = list()
        
        self.wip_owner : Owner = None
        self.wip_account : DematAccount = None
        self.wip_folio : FolioAccount = None

        self.skip_lines = [
            "No Transaction during the period",
            'Summary Holdings Transactions Your Account About NSDL',
            '•',
        ]

        # Sometimes the row headings become lines when the header is placed on the end of the page and the table starts from the next page.
        self.table_lines = [
            "Equities (E)",
            "Equity Shares",
            "Preference Shares (P)",
            "Mutual Funds (M)",
            "Corporate Bonds (C)",
            "Money Market Instruments (I)",
            "Securitised Instruments (S)",
            "Government Securities (G)",
            "Postal Saving Scheme (O)",
            "Mutual Fund Folios (F)",
        ]
        self.isin_status = ISINType.NONE
        
    def process(self, raw_content: Dict) -> Statement:
        clean_content = clean_pdf_data(raw_content, self.skip_lines, "Summary Holdings Transactions Your Account About NSDL")

        status = ReaderStatus.Is_Finding
        sub_status = ReaderSubStatus.Is_Waiting

        overview = list()
        nsdl_holdings_list = list()
        nsdl_holdings = list()

        cdsl_holdings_list = list()
        cdsl_holdings = list()

        mf_holdings_list = list()
        mf_holdings = list()

        for line in clean_content:
            if isinstance(line, str):
                if "YOUR CONSOLIDATED PORTFOLIO VALUE" in line:
                    status = ReaderStatus.Reading_Overview
                    print("Reading Overview table Started")

                
                if 'Mutual Fund Folios (F)' in line:
                    is_folio = False
                    # Check if the complete line has the string
                    # or first line in multiline has the string
                    sl = line.split('\n')
                    if sl[0] == "Mutual Fund Folios (F)":
                        is_folio = True

                    if is_folio:
                        status = ReaderStatus.Reading_Folio
                        sub_status = ReaderSubStatus.Reading_Holdings
                        print("New Mutual Fund Folio Section Found")

                if "Statement for the period from" in line and "Mutual Funds" not in line:
                    sl = line.split('from')
                    sl = sl.pop(-1)
                    sl = line.split('to')
                    as_on_date = sl.pop(-1).strip()
                    self.statement.as_on_date = as_on_date


            if status == ReaderStatus.Reading_Overview:
                overview.append(line)

            if status == ReaderStatus.Reading_NSDL_Demat and sub_status == ReaderSubStatus.Reading_Holdings:
                nsdl_holdings.append(line)
                
            if status == ReaderStatus.Reading_CDSL_Demat and sub_status == ReaderSubStatus.Reading_Holdings:
                cdsl_holdings.append(line)

            if status == ReaderStatus.Reading_Folio and sub_status == ReaderSubStatus.Reading_Holdings:
                mf_holdings.append(line)

            if isinstance(line, list):
                if 'Grand Total' in line and status == ReaderStatus.Reading_Overview:
                    self.statement.owners = get_portfolio_account_summary(overview)
                    status = ReaderStatus.Is_Finding
                    print("Reading Overview table Over")

                if line[0]:
                    new_account_found = False
                    if "NSDL Demat Account\n" in line[0]:
                        new_account_found = True
                        status = ReaderStatus.Reading_NSDL_Demat
                        print("New NSDL Account Found")

                    elif "CDSL Demat Account\n" in line[0]:
                        new_account_found = True
                        status = ReaderStatus.Reading_CDSL_Demat
                        print("New CDSL Account Found")

                    elif 'Mutual Fund Folios (F)' in line[0]:
                        status = ReaderStatus.Reading_Folio
                        sub_status = ReaderSubStatus.Reading_Holdings
                        print("New Mutual Fund Folio Section Found")

                    # This code only executes if there are direct holdings in NSDL/CDSL accounts
                    if new_account_found:
                        new_acc_line = get_clean_row(line)

                        sl = new_acc_line[0].split("\n")
                        dp_line = ""
                        for cell in sl:
                            if "DP ID" in cell:
                                dp_line = cell
                        depository = sl[0]
                        dp_name = sl[1]
                        # [dp_id, client_id] = make_clean_segments(sl[2], ["dp id", "client id"])
                        [dp_id, client_id] = make_clean_segments(dp_line, ["dp id", "client id"])
                        uid = F"{dp_id}{client_id}"
                        print(F"Reading for {client_id}")

                        for owner in self.statement.owners:
                            acc = owner.get_account(uid)
                            if acc:
                                self.wip_account = acc
                                self.wip_owner = owner
                                break

                        if len(new_acc_line) > 1 and "ACCOUNT HOLDER" in new_acc_line[1]:
                            sub_status = ReaderSubStatus.Reading_Holdings
                            print("Reading Holdings Started")

                        elif len(new_acc_line) > 1 and "Summary of Transactions" in new_acc_line[1]:
                            sub_status = ReaderSubStatus.Reading_Transactions
                            print("Reading Transactions Started")

                    # Case : If there are no direct holdings in NSDL/CDSL accounts then the owner of the statement is not set
                    # Solution : MF section (i.e. Mutual Fund Folios (F)) is at the bottom of statement and
                    # we can set the owner from this MF section using the following logic
                    # Still here the self.wip_account is not set as in other cases, will have to test the impact of it and then take the call.
                    if self.wip_owner == None and 'INF' in line[0] and status == ReaderStatus.Reading_Folio and sub_status == ReaderSubStatus.Reading_Holdings:
                        print("Setting owner using folio account")
                        for owner in self.statement.owners:
                            self.wip_owner = owner
                            break

                if 'Total' == line[0] and status == ReaderStatus.Reading_NSDL_Demat and sub_status == ReaderSubStatus.Reading_Holdings:
                    print("Reading NSDL Holdings Finished")
                    status = ReaderStatus.Is_Finding
                    self.wip_account.holdings = get_nsdl_holdings(nsdl_holdings)
                    nsdl_holdings_list.append(nsdl_holdings)
                    nsdl_holdings = list()

                if 'Total' == line[0] and status == ReaderStatus.Reading_CDSL_Demat and sub_status == ReaderSubStatus.Reading_Holdings:
                    print("Reading CDSL Holdings Finished")
                    status = ReaderStatus.Is_Finding
                    self.wip_account.holdings = get_cdsl_holdings(cdsl_holdings)
                    cdsl_holdings_list.append(cdsl_holdings)
                    cdsl_holdings = list()

                if 'Total' == line[0] and status == ReaderStatus.Reading_Folio and sub_status == ReaderSubStatus.Reading_Holdings:
                    print("Reading MF Holdings Finished")
                    status = ReaderStatus.Is_Finding
                    self.wip_owner.folio_accounts.extend(get_folio_holdings(mf_holdings))
                    mf_holdings_list.append(mf_holdings)
                    mf_holdings = list()

        return self.statement
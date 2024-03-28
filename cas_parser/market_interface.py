from typing import List
from enum import Enum
import json

class ISINType(Enum):
    NONE = "None"
    EQUITIES  = "Equities (E)"
    PREFERENCE_SHARES = "Preference Shares (P)"
    MUTUAL_FUNDS = "Mutual Funds (M)"
    CORPORATE_BONDS = "Corporate Bonds (C)"
    MONEY_MARKET_INSTRUMENTS = "Money Market Instruments (I)"
    SECURITISED_INSTRUMENTS = "Securitised Instruments (S)"
    GOVERNMENT_SECURITIES = "Government Securities (G)"
    POSTAL_SAVING_SCHEME = "Postal Saving Scheme (O)"
    MUTUAL_FUND_FOLIOS = "Mutual Fund Folios (F)"

    def __str__(self) -> str:
        return self.name

class Statement:
    def __init__(self) -> None:
        self.owners : List[Owner] = list()
        self.as_on_date = None

    def export_to_csv(self, dir_path) -> None:
        for owner_idx, owner in enumerate(self.owners):
            names = list()
            for investor in owner.investors:
                names.append(investor.name)
            
            owner_name = " ".join(names)
            header, rows = owner.get_consolidated_holdings()
            return header, rows
            # file_path = os.path.join(
            #     os.path.dirname(os.path.abspath(__file__)), dir_path, F"{owner_name}.csv"
            #     )
            # save_csv(file_path, header, rows)

    def export_to_json(self, no_empty_demat = False) -> None:
        json_obj = list()
        for owner in self.owners:
            owner_obj = dict()
            owner_obj["investors"] = list()
            for investor in owner.investors:
                owner_obj["investors"].append(investor.toJSON())
            owner_obj["as_on_date"] = self.as_on_date
            owner_obj["demat_accounts"] = list()
            for demat in owner.demat_accounts:
                include_account = True
                if no_empty_demat:
                    if len(demat.holdings) == 0:
                        include_account = False

                if include_account:
                    demat_obj = demat.toJSON()
                    owner_obj["demat_accounts"].append(demat_obj)

            owner_obj["folio_accounts"] = list()
            for folio in owner.folio_accounts:
                demat_obj = folio.toJSON()
                owner_obj["folio_accounts"].append(demat_obj)           
                    
            json_obj.append(owner_obj)
        return json_obj

class Investor:
    def __init__(self, name="", PAN="") -> None:
        self.name = name
        self.PAN = PAN

    def toJSON(self):
        return self.__dict__

class Holding:
    def __init__(self) -> None:
        self.isin = ""
        self.type = ISINType.NONE
        self.name = ""
        # Current balance of the securities
        self.total_units = 0
        self.unit_value = 0
        self.total_value = 0
        # Fields for debt instruments
        self.coupon_rate = 0
        self.maturity_date = None
        # anything that is not defined will go in info
        self.info = dict()

    def  __str__(self):
        return json.dumps(vars(self))
    
    def toJSON(self):
        obj = dict()
        obj["isin"] = self.isin
        obj["type"] = self.type.value
        obj["name"] = self.name
        obj["units"] = self.total_units
        obj["unit_value"] = self.unit_value
        obj["total_value"] = self.total_value
        obj["coupon_rate"] = self.coupon_rate
        obj["maturity_date"] = self.maturity_date
        obj["info"] = self.info
        return obj

class DematAccount:
    def __init__(self):
        self.depository = ""
        self.dp_name = ""
        self.dp_id = ""
        self.client_id = ""
        self.account_status = ""
        self.email_id = ""
        self.mobile_no = ""
        self.nominee = ""
        self.bo_status = ""
        self.bo_sub_status = ""
        self.rgess = ""
        self.BSDA = ""
        self.frozen = ""
        self.smart_registration = ""

        self.as_on_date = None
        
        self.security_count = None
        self.security_value_in_rs = None

        self.holdings : List[Holding]= list()

    def get_uid(self):
        return F"{self.dp_id}{self.client_id}"

    def toJSON(self):
        obj = dict()
        obj["depository"] = self.depository
        obj["dp_name"] = self.dp_name
        obj["client_id"] = self.get_uid()
        obj["account_status"] = self.account_status
        obj["email_id"] = self.email_id
        obj["mobile_no"] = self.mobile_no
        obj["nominee"] = self.nominee
        obj["bo_status"] = self.bo_status
        obj["bo_sub_status"] = self.bo_sub_status
        obj["rgess"] = self.rgess
        obj["BSDA"] = self.BSDA
        obj["frozen"] = self.frozen
        obj["smart_registration"] = self.smart_registration
        obj["as_on_date"] = self.as_on_date
        obj["holdings"] = list()
        for sec in self.holdings:
            sec_obj = sec.toJSON()
            obj["holdings"].append(sec_obj)
        return obj


class FolioAccount:
    def __init__(self):
        self.folio_no = ""
        self.holding_mode = ""
        self.ucc = ""
        self.kyc_status = ""
        self.nominee_status = ""
        self.mobile_no = ""
        self.email = ""

        self.holdings : List[Holding]= list()

    def toJSON(self):
        obj = dict()
        obj["folio_no"] = self.folio_no
        obj["holding_mode"] = self.holding_mode
        obj["ucc"] = self.ucc
        obj["kyc_status"] = self.kyc_status
        obj["nominee_status"] = self.nominee_status
        obj["mobile_no"] = self.mobile_no
        obj["email"] = self.email
        obj["holdings"] = list()
        for sec in self.holdings:
            sec_obj = sec.toJSON()
            obj["holdings"].append(sec_obj)
        return obj


class Owner:
    def __init__(self):
        self.investors : List[Investor] = list()
        self.demat_accounts : List[DematAccount] = list()
        self.folio_accounts : List[FolioAccount] = list()
        self.folio_count: int = 0
        self.folio_value: float = 0.0
        self.total = None
        # Following flag will be set when we have names, accounts meta and total
        self.meta_complete = False
    
    def get_consolidated_holdings(self):
        header = ["dp_name", "client_id", "date", "type", "isin", "name", "coupon rate", "maturity date", "units", "unit_price", "total_value"]
        holdings = list()
        for account in self.demat_accounts:
            for holding in account.holdings:
                obj = list()
                obj.append(account.dp_name)
                obj.append(F"{account.dp_id}{account.client_id}")
                obj.append(account.as_on_date)
                obj.append(holding.type.value)
                obj.append(holding.isin)
                obj.append(holding.name)
                obj.append(holding.coupon_rate)
                obj.append(holding.maturity_date)
                obj.append(holding.total_units)
                obj.append(holding.unit_value)
                obj.append(holding.total_value)
                holdings.append(obj)

        for folio in self.folio_accounts:
            for holding in folio.holdings:
                obj = list()
                obj.append("MF Folio")
                obj.append(folio.folio_no)
                obj.append(account.as_on_date)
                obj.append(ISINType.MUTUAL_FUNDS.value)
                obj.append(holding.isin)
                obj.append(holding.name)
                obj.append(None)
                obj.append(None)
                obj.append(holding.total_units)
                obj.append(holding.unit_value)
                obj.append(holding.total_value)
                holdings.append(obj)

        return header, holdings
                
    def get_account(self, unique_id):
        acc = None
        for account in self.demat_accounts:
            if unique_id == account.get_uid():
                acc = account
                break
        return acc 
    
    def get_cdsl_account(self, bo_id):
        acc = None
        for account in self.demat_accounts:
            if bo_id == account.get_uid():
                acc = account
                break
        return acc

    def get_nsdl_account(self, dp_id, client_id):
        acc = None
        for account in self.demat_accounts:
            if account.dp_id == dp_id and account.client_id == client_id:
                acc = account
        return acc

    # def get_folio_account(self, folio_no):
    #     acc = None
    #     for account in self.folio_accounts:
    #         if account.folio_no == folio_no:
    #             acc = account
    #     return acc

    def __str__(self):
        return F"{self.names}: {self.total}. Total account count: {len(self.demat_accounts)}"
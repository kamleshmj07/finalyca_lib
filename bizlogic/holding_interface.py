import enum

class HoldingType(enum.Enum):
    no_type = ""
    equity = "LISTED EQUITY" # "Equity"
    short_term_debt = "OTHER DEBTS" # "Short Term Debt"
    long_term_debt = "DEBT" # "Long Term Debt"
    cash = "CASH AND EQUIVALENTS" # "Cash and Cash Equivalent"
    mutual_funds = "MUTUAL FUNDS" # "Mutual Funds"
    commodity = "COMMODITY" # "Commodity"
    others = "OTHERS" # "Others"

class InstrumentType(enum.Enum):
    equity = "Equity"
    debt = "Debt"
    cash = "Cash and Equivalent"
    # MF, PMS etc that may change the underlying. 
    composite = "Composite"
    # Anything that does not go into existing fields -> real estate, commodities, InvIts etc
    alternates = "Alternates"

class Holding:
    def __init__(self) -> None:
        # ISIN will not be there for futures and cash components
        self.isin = ""
        self.name = ""
        self.instrument_type = ""
        self.asset_class = ""
        self.issuer = ""
        self.sector = ""
        self.sub_sector = ""
        self.market_cap = ""
        self.equity_style = ""
        self.risk_category = ""
        self.coupon_rate = ""
        self.maturity = ""
        self.meta = {}


"""
Every Fund has list of Holdings. Holdings could be independent securities or a group of securities. In that case, they could be normalized with existing funds to understand detailed portfolio behaviour. 
"""
"""

Holding Types in a Fund:


Asset Classes of a portfolio:
Equity -> listed, unlisted, international
Debt -> 
Cash and Equivalents 
Commodities
Alternates (InvITs/ Reits/ AIF, Real Estate etc)
Not Defined -> (when information is not available for the holding)


class HoldingType(enum.Enum):
    no_type = ""
    # We will have detailed understanding of the underlying if Indian. If international, still possible to get a proper info.
    equity = "Equity"
    debt = "Debt"
    cash = "Cash and Cash Equivalent"
    commodity = "Commodity"
    # Will not be part of equity analysis as it won't have much information available.
    unlisted = "Unlisted Equity"
    infra_funds = "InvIt/Reit"
    # Part of AIF or by uploading self property.
    real_estate = "Real Estate"
    # Funds with ISIN and mostly traded on exchange.
    mutual_funds = "Mutual Funds"
    # Does not have much info about the funds. will not be part of any analysis. Mostly will be part of Alternates.
    other_funds = "Other Funds"
    
# For the basic analysis. -> Ideally every MF scheme must be split into these asset classes when uploaded.
class AssetClass(enum.Enum):
    equity = "Equity"
    debt = "Debt"
    cash = "Cash and Equivalent"
    commodity = "Commodity"
    # Anything that does not go into existing fields -> real estate, InvIts, AIF etc
    alternates = "Alternates"

class SubAssetClass(enum.Enum):
    listed_indian_equity = "Listed Indian Equity"
    listed_intl_equity = "Listed International Equity"
    unlisted_indian_equity = "Unlisted Indian Equity"
    debt = "Debt"
    cash = "Cash and Equivalent"
    gold = "Gold"
    silver = "Silver"
    real_estate = "Real Estate"
    infra_funds = "InvIt/Reit"


class Holding:
    def __init__(self) -> None:
        # ISIN will not be there for futures and cash components
        self.isin = ""
        self.name = ""
        self.holding_type = ""
        self.asset_class = ""
        self.sub_asset_class = ""
        self.issuer = ""
        self.sector = ""
        self.sub_sector = ""
        self.market_cap = ""
        self.equity_style = ""
        self.risk_category = ""
        self.coupon_rate = ""
        self.maturity = ""
        self.meta = {}
        
        
"""
import enum

class CustomScreenAccess(enum.Enum):
    public = 'Public'
    organization = 'Organization'
    personal = 'Personal'

class HoldingSecurityType(enum.Enum):
    not_defined = "Not Defined"
    listed_equity = "Listed Equity"
    unlisted_equity = "Unlisted Equity"
    esops = "ESOPs"
    debt = "Debt"
    other_debt = "Other Debt"
    cash = "Cash and Equivalents"
    commodity = "Commodity"
    invit_reit = "InvIt/ReIt"
    mutual_funds = "Mutual Funds"
    other_funds = "Other Funds"
    real_estate = "Real Estate"

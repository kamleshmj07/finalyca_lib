from sqlalchemy import func, desc, and_
from sqlalchemy.orm import aliased
from utils import print_query

from fin_models.masters_models import HoldingSecurity, Sector, DebtSecurity
from fin_models.transaction_models import UnderlyingHoldings


def get_fund_underlying_holdings(db_session, fund_id, portfolio_date, limit):

    # get holdings for all portfolio dates for respective fund
    holding_qry = db_session.query(UnderlyingHoldings.Portfolio_Date)\
                            .filter(UnderlyingHoldings.Is_Deleted != 1,
                                    UnderlyingHoldings.Fund_Id == fund_id)

    # get the latest available portfolio
    if portfolio_date:
        holding_qry = holding_qry.filter(UnderlyingHoldings.Portfolio_Date <= portfolio_date)

    holding_record = holding_qry.order_by(desc(UnderlyingHoldings.Portfolio_Date)).first()

    if not holding_record:
        return []

    portfolio_date = holding_record.Portfolio_Date

    HoldingSecurity_ = aliased(HoldingSecurity)

    holding_qry = db_session.query(func.coalesce(HoldingSecurity_.HoldingSecurity_Id, HoldingSecurity.HoldingSecurity_Id).label("HoldingSecurity_Id"),
                                   DebtSecurity.DebtSecurity_Id,
                                   func.coalesce(HoldingSecurity_.HoldingSecurity_Name, HoldingSecurity.HoldingSecurity_Name, UnderlyingHoldings.Company_Security_Name).label("HoldingSecurity_Name"),
                                   HoldingSecurity.HoldingSecurity_Name,
                                   UnderlyingHoldings.Company_Security_Name,
                                   HoldingSecurity.Issuer_Name,
                                   HoldingSecurity.MarketCap,
                                   HoldingSecurity.Equity_Style,
                                   HoldingSecurity.HoldingSecurity_Type,
                                   func.coalesce(HoldingSecurity.ISIN_Code, UnderlyingHoldings.ISIN_Code).label("ISIN_Code"),
                                   HoldingSecurity.BSE_Code,
                                   HoldingSecurity.NSE_Symbol,
                                   HoldingSecurity.Co_Code,
                                   HoldingSecurity.Sub_SectorName,
                                   Sector.Sector_Code,
                                   Sector.Sector_Name,
                                   UnderlyingHoldings.Portfolio_Date,
                                   UnderlyingHoldings.Percentage_to_AUM,
                                   UnderlyingHoldings.Value_in_INR,
                                   UnderlyingHoldings.Purchase_Date,
                                   UnderlyingHoldings.Amount_Invested_Crs,
                                   UnderlyingHoldings.Total_Receipts_Crs,
                                   UnderlyingHoldings.Risk_Category,
                                   UnderlyingHoldings.Location_City,
                                   UnderlyingHoldings.Exit_Date,
                                   UnderlyingHoldings.Exit_IRR,
                                   UnderlyingHoldings.Exit_Multiple,
                                   UnderlyingHoldings.LISTED_UNLISTED,
                                   UnderlyingHoldings.LONG_SHORT,
                                   UnderlyingHoldings.Instrument,
                                   UnderlyingHoldings.Instrument_Rating,
                                   func.coalesce(UnderlyingHoldings.Asset_Class, HoldingSecurity.HoldingSecurity_Type).label("Asset_Class"),
                                   HoldingSecurity.Instrument_Type,
                                   HoldingSecurity.active,
                                   UnderlyingHoldings.Underlying_Holdings_Id
                                   )\
                            .select_from(UnderlyingHoldings)\
                            .join(HoldingSecurity, HoldingSecurity.HoldingSecurity_Id == UnderlyingHoldings.HoldingSecurity_Id, isouter=True)\
                            .join(DebtSecurity, HoldingSecurity.Co_Code == func.concat('BLV_', DebtSecurity.Bilav_Code), isouter=True)\
                            .join(Sector, Sector.Sector_Id == HoldingSecurity.Sector_Id, isouter=True)\
                            .join(HoldingSecurity_, and_(HoldingSecurity.Co_Code == HoldingSecurity_.Co_Code,  HoldingSecurity_.active == 1, HoldingSecurity_.Is_Deleted != 1), isouter=True)\
                            .filter(UnderlyingHoldings.Fund_Id == fund_id,
                                    UnderlyingHoldings.Is_Deleted != 1,
                                    UnderlyingHoldings.Portfolio_Date == portfolio_date,
                                    HoldingSecurity.Is_Deleted != 1,
                                    HoldingSecurity_.Is_Deleted != 1)\
                            .order_by(desc(UnderlyingHoldings.Percentage_to_AUM))

    # apply limit
    if limit:
        holding_qry = holding_qry.limit(limit)

    result = [r._asdict() for r in holding_qry]

    return result



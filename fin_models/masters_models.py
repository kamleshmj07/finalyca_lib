from sqlalchemy import BigInteger, Boolean, Column, Date, DateTime, Float, Identity, Index, Integer, Numeric, String, Table, Unicode, text, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()
metadata = Base.metadata


class AMC(Base):
    __tablename__ = 'AMC'
    __table_args__ = (
        Index('NonClusteredIndex-20191003-152913', 'AMC_Code', 'Is_Deleted', 'Product_Id'),
        {'schema': 'Masters'}
    )

    AMC_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    AMC_Name = Column(Unicode(100))
    AMC_Code = Column(Unicode(100))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)
    AMC_Description = Column(Unicode)
    AMC_Logo = Column(Unicode(500))
    Product_Id = Column(BigInteger)
    Address1 = Column(Unicode(500))
    Address2 = Column(Unicode(500))
    Website_link = Column(Unicode(250))
    Contact_Numbers = Column(Unicode(100))
    AMC_background = Column(Unicode(500))
    Corporate_Identification_Number = Column(Unicode(100))
    SEBI_Registration_Number = Column(Unicode(100))
    Contact_Person = Column(Unicode(100))
    Email_Id = Column(String(256, 'SQL_Latin1_General_CP1_CI_AS'))
    hide_fields = Column(Unicode(500))
    Facebook_url = Column(Unicode)
    Linkedin_url = Column(Unicode)
    Twitter_url = Column(Unicode)
    Youtube_url = Column(Unicode)

    def __str__(self) -> str:
        return self.AMC_Name


class AssetClass(Base):
    __tablename__ = 'AssetClass'
    __table_args__ = {'schema': 'Masters'}

    AssetClass_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    AssetClass_Name = Column(Unicode(100))
    AssetClass_Description = Column(Unicode(500))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.AssetClass_Name


class BenchmarkIndices(Base):
    __tablename__ = 'BenchmarkIndices'
    __table_args__ = (
        Index('NonClusteredIndex-20201112-130218', 'BenchmarkIndices_Code', 'Co_Code', 'TRI_Co_Code', 'BSE_Code'),
        {'schema': 'Masters'}
    )

    BenchmarkIndices_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    BenchmarkIndices_Name = Column(Unicode(200))
    BenchmarkIndices_Code = Column(Unicode(100))
    BenchmarkIndices_Description = Column(Unicode(200))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)
    Co_Code = Column(BigInteger)
    Short_Name = Column(Unicode(100))
    Long_Name = Column(Unicode(200))
    TRI_Co_Code = Column(BigInteger)
    BSE_Code = Column(BigInteger)
    NSE_Symbol = Column(Unicode(10))
    BSE_GroupName = Column(Unicode(10))
    Attribution_Flag = Column(Boolean)

    def __str__(self) -> str:
        return self.BenchmarkIndices_Name


class City(Base):
    __tablename__ = 'City'
    __table_args__ = {'schema': 'Masters'}

    City_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    City_Name = Column(Unicode(100))
    City_Code = Column(Unicode(5))
    State_Id = Column(Integer)
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.City_Name


class Classification(Base):
    __tablename__ = 'Classification'
    __table_args__ = {'schema': 'Masters'}

    Classification_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Classification_Name = Column(Unicode(200))
    Classification_Code = Column(Unicode(100))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)
    AssetClass_Id = Column(BigInteger)

    def __str__(self) -> str:
        return self.Classification_Name


class Content(Base):
    __tablename__ = 'Content'
    __table_args__ = {'schema': 'Masters'}

    Content_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Content_Category_Id = Column(BigInteger)
    Content_Type_Id = Column(BigInteger)
    Content_Header = Column(Unicode(250))
    Content_SubHeader = Column(Unicode(1000))
    Content_Detail = Column(Unicode)
    Content_Source = Column(Unicode(100))
    Content_DateTime = Column(DateTime)
    Is_Deleted = Column(Boolean)
    Images_URL = Column(Unicode(200))
    Content_Name = Column(Unicode(100))
    Is_Front_Dashboard = Column(Boolean)
    AMC_Id = Column(BigInteger)
    Product_Id = Column(BigInteger)
    Fund_Id = Column(BigInteger)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)


class ContentCategory(Base):
    __tablename__ = 'Content_Category'
    __table_args__ = {'schema': 'Masters'}

    Content_Category_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Content_Category_Name = Column(Unicode(100))
    Parent_Id = Column(BigInteger)
    Is_LeafCategory = Column(Boolean)
    Is_Delete = Column(Boolean)

    def __str__(self) -> str:
        return self.Content_Category_Name


class ContentType(Base):
    __tablename__ = 'Content_Type'
    __table_args__ = {'schema': 'Masters'}

    Content_Type_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Content_Type_Name = Column(Unicode(50))
    Is_Deleted = Column(Boolean)

    def __str__(self) -> str:
        return self.Content_Type_Name

class ContentUpload(Base):
    __tablename__ = 'Content_Upload'
    __table_args__ = {'schema': 'Masters'}

    Content_Upload_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Content_Upload_Type_Id = Column(BigInteger)
    AMC_Id = Column(BigInteger)
    Content_Upload_Name = Column(Unicode(100))
    Content_Upload_URL = Column(Unicode)
    Is_Deleted = Column(Boolean)


class ContentUploadType(Base):
    __tablename__ = 'Content_Upload_Type'
    __table_args__ = {'schema': 'Masters'}

    Content_Upload_Type_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Content_Upload_Type_Name = Column(Unicode(50))
    Is_Deleted = Column(Boolean)

    def __str__(self) -> str:
        return self.Content_Upload_Type_Name


class Country(Base):
    __tablename__ = 'Country'
    __table_args__ = {'schema': 'Masters'}

    Country_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Country_Name = Column(Unicode(100))
    Country_Code = Column(Unicode(5))
    Country_ISO_Code = Column(Unicode(5))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.Country_Name


class Currency(Base):
    __tablename__ = 'Currency'
    __table_args__ = {'schema': 'Masters'}

    Currency_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Currency_Name = Column(Unicode(200))
    Trading_Code = Column(Unicode(100))
    Bloomberg_Code = Column(Unicode(100))
    Country_Id = Column(BigInteger)
    Currency_ISO_Code = Column(Unicode(5))
    Currency_Fractional = Column(Unicode(100))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.Currency_Name


class ESecurity(Base):
    __tablename__ = 'E_Security'
    __table_args__ = {'schema': 'Masters'}

    E_Security_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    E_Security_Name = Column(Unicode(200))
    BSE_Code = Column(Unicode(100))
    NSE_Symbol = Column(Unicode(100))
    BSE_Group = Column(Unicode(100))
    NSE_Group = Column(Unicode(100))
    Sector_Id = Column(BigInteger)
    ISIN_Demat = Column(Unicode(200))
    ISIN_Physical = Column(Unicode(200))
    PaidUp = Column(Numeric(18, 9))
    Reut_Code = Column(Unicode(200))
    Blum_Code = Column(Unicode(200))
    MCX_Symbol = Column(Unicode(200))
    MCX_Group = Column(Unicode(200))
    Is_Deleted = Column(Boolean)

    def __str__(self) -> str:
        return self.E_Security_Name


class Fund(Base):
    __tablename__ = 'Fund'
    __table_args__ = {'schema': 'Masters'}

    Fund_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    AutoPopulate = Column(Boolean, nullable=False, server_default=text('((1))'))
    HideHoldingWeightage = Column(Boolean, nullable=False, server_default=text('((0))'))
    HideAttribution = Column(Boolean, nullable=False, server_default=text('((0))'))
    HidePortfolioHoldingChanges = Column(Boolean, nullable=False, server_default=text('((0))'))
    Fund_Name = Column(Unicode(200))
    Fund_Code = Column(Unicode(100), index=True)
    Fund_Description = Column(Unicode(1000))
    Fund_OfferLink = Column(Unicode(100))
    Fund_OldName = Column(Unicode(200))
    Is_Deleted = Column(Boolean)
    Top_Holding_ToBeShown = Column(Integer)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)
    AIF_INVESTMENT_THEME = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'), server_default=text('(NULL)'))
    AIF_MIN_INVESTMENT_AMOUNT = Column(Numeric(18, 9), server_default=text('(NULL)'))
    AIF_INITIAL_DRAWDOWN = Column(Numeric(18, 9), server_default=text('(NULL)'))
    AIF_TENURE_OF_FUND = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'), server_default=text('(NULL)'))
    AIF_CURRENCY = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), server_default=text('(NULL)'))
    AIF_CATEGORY = Column(Integer, server_default=text('(NULL)'))
    AIF_SUB_CATEGORY = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), server_default=text('(NULL)'))
    AIF_CLASS_OF_UNITS = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'), server_default=text('(NULL)'))
    AIF_MANAGEMENT_EXPENSES = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'), server_default=text('(NULL)'))
    AIF_ADMIN_EXPENSES = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'), server_default=text('(NULL)'))
    AIF_SET_UP_FEE = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'), server_default=text('(NULL)'))
    AIF_HURDLE_RATE = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'), server_default=text('(NULL)'))
    AIF_PERFORMANCE_FEES = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'), server_default=text('(NULL)'))
    Is_Active = Column(Boolean, server_default=text('(NULL)'))
    Is_Closed_Permanently = Column(Boolean, server_default=text('(NULL)'))
    ClosedDate = Column(DateTime, server_default=text('(NULL)'))
    Is_Closed_For_Subscription = Column(Boolean, server_default=text('(NULL)'))
    fund_comments = Column(Unicode(1000), server_default=text('(NULL)'))
    AIF_SPONSOR_COMMITMENT_IN_CR = Column(Numeric(18, 9), server_default=text('(NULL)'))
    AIF_TARGET_FUND_SIZE_IN_CR = Column(Numeric(18, 9), server_default=text('(NULL)'))
    AIF_NRI_INVESTMENT_ALLOWED = Column(Boolean, server_default=text('(NULL)'))
    Fund_manager = Column(Unicode(1000), server_default=text('(NULL)'))


    def __str__(self) -> str:
        return self.Fund_Name


class FundManager(Base):
    __tablename__ = 'FundManager'
    __table_args__ = {'schema': 'Masters'}

    FundManager_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Funds_Managed = Column(Integer, nullable=False, server_default=text('((0))'))
    AUM = Column(Numeric(18, 2), nullable=False, server_default=text('((0))'))
    FundManager_Name = Column(Unicode(200))
    FundManager_Code = Column(Unicode(100))
    FundManager_Description = Column(Unicode(2000))
    Fund_Id = Column(BigInteger)
    Is_Deleted = Column(Boolean)
    FundManager_Image = Column(Unicode(500))
    FundManager_Designation = Column(Unicode(300))
    DateFrom = Column(DateTime)
    DateTo = Column(DateTime)
    AMC_Id = Column(BigInteger)
    Product_Id = Column(BigInteger)
    Linkedin_url = Column(Unicode)

    def __str__(self) -> str:
        return self.FundManager_Name


class FundManagers(Base):
    __tablename__ = 'FundManagers'
    __table_args__ = {'schema': 'Masters'}

    FundManager_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    FundManager_Name = Column(Unicode(200))
    FundManager_Code = Column(Unicode(100))
    FundManager_Description = Column(Unicode(2000))
    Fund_Id = Column(BigInteger)
    Is_Deleted = Column(Boolean)
    FundManager_Image = Column(Unicode(500))
    FundManager_Designation = Column(Unicode(300))
    DateFrom = Column(DateTime)
    DateTo = Column(DateTime)


class FundType(Base):
    __tablename__ = 'FundType'
    __table_args__ = {'schema': 'Masters'}

    FundType_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    FundType_Name = Column(Unicode(200))
    FundType_Code = Column(Unicode(100))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.FundType_Name


class HoldingSecurity(Base):
    __tablename__ = 'HoldingSecurity'
    __table_args__ = {'schema': 'Masters'}

    HoldingSecurity_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    HoldingSecurity_Name = Column(Unicode(500))
    ISIN_Code = Column(Unicode(50))
    Sector_Id = Column(BigInteger)
    Asset_Class = Column(Unicode(200))
    Instrument_Type = Column(Unicode(100))
    Issuer_Code = Column(Unicode(100))
    Issuer_Name = Column(Unicode(500))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)
    Issuer_Id = Column(BigInteger)
    MarketCap = Column(Unicode(50))
    Equity_Style = Column(Unicode(20))
    HoldingSecurity_Type = Column(Unicode(50))
    BSE_Code = Column(BigInteger)
    NSE_Symbol = Column(Unicode(20))
    BSE_GroupName = Column(Unicode(20))
    Co_Code = Column(Unicode(50))
    Short_CompanyName = Column(Unicode(200))
    Sub_SectorName = Column(Unicode(100))
    active = Column(Boolean)
    Currency = Column(Unicode(4), default="INR")
    Maturity_Date = Column(Date)
    # Credit_Ratings = Column(Unicode(20))
    # Credit_Ratings_Agency = Column(Unicode(200))
    Interest_Rate = Column(Float)
    Is_Listed = Column(Boolean)
    Face_Value = Column(Float)
    Paid_Up_Value = Column(Float)

    def __str__(self) -> str:
        return self.HoldingSecurity_Name


t_Images = Table(
    'Images', metadata,
    Column('Images_Id', BigInteger, Identity(start=1, increment=1), nullable=False),
    Column('Images_Name', Unicode(100)),
    Column('Images_URL', Unicode),
    Column('Is_Deleted', Boolean),
    schema='Masters'
)


class Issuer(Base):
    __tablename__ = 'Issuer'
    __table_args__ = {'schema': 'Masters'}

    Issuer_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Issuer_Name = Column(Unicode(100))
    Issuer_Code = Column(Unicode(100))
    IssuerGroup_Id = Column(BigInteger)
    IssuerCategory_Id = Column(BigInteger)
    Issuer_External_Code = Column(Unicode(100))
    Issuer_InHouse = Column(Boolean)
    Issuer_Address1 = Column(Unicode(250))
    Issuer_Address2 = Column(Unicode(250))
    Issuer_Address3 = Column(Unicode(250))
    Issuer_Pin_Code = Column(Unicode(6))
    City_Id = Column(BigInteger)
    State_Id = Column(BigInteger)
    Country_Id = Column(BigInteger)
    Issuer_Contact = Column(Unicode(10))
    Issuer_Fax = Column(Unicode(10))
    Issuer_Email = Column(Unicode(150))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.Issuer_Name


class IssuerCategory(Base):
    __tablename__ = 'IssuerCategory'
    __table_args__ = {'schema': 'Masters'}

    IssuerCategory_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    IssuerCategory_Name = Column(Unicode(100))
    IssuerCategory_Code = Column(Unicode(100))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.IssuerCategory_Name


class IssuerGroup(Base):
    __tablename__ = 'IssuerGroup'
    __table_args__ = {'schema': 'Masters'}

    IssuerGroup_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    IssuerGroup_Name = Column(Unicode(100))
    IssuerGroup_Description = Column(Unicode(500))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.IssuerGroup_Name


class ListingStatus(Base):
    __tablename__ = 'ListingStatus'
    __table_args__ = {'schema': 'Masters'}

    ListingStatus_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    ListingStatus_Name = Column(Unicode(200))
    ListingStatus_Code = Column(Unicode(100))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.ListingStatus_Name


class MFSecurity(Base):
    __tablename__ = 'MF_Security'
    __table_args__ = (
        Index('NonClusteredIndex-20200710-120333', 'MF_Security_Code', 'Fund_Id', 'AssetClass_Id', 'Is_Deleted', 'Status_Id', 'FundType_Id', 'BenchmarkIndices_Id', 'AMC_Id', 'Classification_Id'),
        {'schema': 'Masters'}
    )

    MF_Security_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    MF_Security_Name = Column(Unicode(1000))
    MF_Security_Code = Column(Unicode(100))
    Fund_Id = Column(BigInteger)
    AssetClass_Id = Column(BigInteger)
    Status_Id = Column(BigInteger)
    FundType_Id = Column(BigInteger)
    BenchmarkIndices_Id = Column(BigInteger)
    AMC_Id = Column(BigInteger)
    Classification_Id = Column(BigInteger)
    MF_Security_UnitFaceValue = Column(Numeric(18, 9))
    MF_Security_OpenDate = Column(DateTime)
    MF_Security_CloseDate = Column(DateTime)
    MF_Security_ReopenDate = Column(DateTime)
    MF_Security_PurchaseAvailable = Column(Boolean)
    MF_Security_Redemption_Available = Column(Boolean)
    MF_Security_SIP_Available = Column(Boolean)
    MF_Security_Min_Purchase_Amount = Column(Numeric(18, 9))
    MF_Security_Purchase_Multiplies_Amount = Column(Numeric(18, 9))
    MF_Security_Add_Min_Purchase_Amount = Column(Numeric(18, 9))
    MF_Security_Add_Purchase_Multiplies_Amount = Column(Numeric(18, 9))
    MF_Security_Min_Redeem_Amount = Column(Numeric(18, 9))
    MF_Security_Min_Redeem_Units = Column(Numeric(18, 9))
    MF_Security_Trxn_Cut_Off_Time = Column(Unicode(10))
    MF_Security_SIP_Frequency = Column(Unicode(500))
    MF_SIP_Dates = Column(Unicode(2000))
    MF_Security_SIP_Min_Amount = Column(Unicode(500))
    MF_Security_SIP_Min_Agg_Amount = Column(Numeric(18, 9))
    MF_Security_Maturity_Date = Column(DateTime)
    MF_Security_Min_Balance_Unit = Column(Numeric(18, 9))
    MF_Security_Maturity_Period = Column(BigInteger)
    MF_Security_Min_Lockin_Period = Column(BigInteger)
    MF_Security_Investment_Strategy = Column(Unicode(2000))
    MF_Security_SIP_Min_Installment = Column(Unicode(200))
    MF_Security_STP_Available = Column(Boolean)
    MF_Security_STP_Frequency = Column(Unicode(100))
    MF_Security_STP_Min_Install = Column(Unicode(100))
    MF_Security_STP_Dates = Column(Unicode(2000))
    MF_Security_STP_Min_Amount = Column(Unicode(500))
    MF_Security_SWP_Available = Column(Boolean)
    MF_Security_SWP_Frequency = Column(Unicode(100))
    MF_Security_SWP_Min_Install = Column(Unicode(100))
    MF_Security_SWP_Dates = Column(Unicode(2000))
    MF_Security_SWP_Min_Amount = Column(Unicode(500))
    Is_Deleted = Column(Boolean)
    Fees_Structure = Column(Unicode(1000))
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)
    INS_SFINCode = Column(Unicode(100))
    INS_EquityMin = Column(Float(53))
    INS_EquityMax = Column(Float(53))
    INS_DebtMin = Column(Float(53))
    INS_DebtMax = Column(Float(53))
    INS_CommMin = Column(Float(53))
    INS_CommMax = Column(Float(53))
    INS_CashMoneyMarketMin = Column(Float(53))
    INS_CashMoneyMarketMax = Column(Float(53))
    INS_EquityDerivativesMin = Column(Float(53))
    INS_EquityDerivativesMax = Column(Float(53))
    Risk_Grade = Column(Unicode(100))

    def __str__(self) -> str:
        return self.MF_Security_Name


class MarketCap(Base):
    __tablename__ = 'MarketCap'
    __table_args__ = {'schema': 'Masters'}

    MarketCap_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    MarketCap_Name = Column(Unicode(100))
    MarketCap_Code = Column(Unicode(100))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.MarketCap_Name


class Options(Base):
    __tablename__ = 'Options'
    __table_args__ = {'schema': 'Masters'}

    Option_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Option_Name = Column(Unicode(200))
    Option_Code = Column(Unicode(100))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.Option_Name


class PlanType(Base):
    __tablename__ = 'PlanType'
    __table_args__ = {'schema': 'Masters'}

    PlanType_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    PlanType_Name = Column(Unicode(200))
    PlanType_Code = Column(Unicode(100))
    Is_Deleted = Column(Boolean)

    def __str__(self) -> str:
        return self.PlanType_Name

class Plans(Base):
    __tablename__ = 'Plans'
    __table_args__ = (
        Index('NonClusteredIndex-20190905-210733', 'Plan_Name', 'Plan_Code', 'MF_Security_Id', 'PlanType_Id', 'Option_Id', 'SwitchAllowed_Id'),
        {'schema': 'Masters'}
    )

    Plan_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Plan_Name = Column(Unicode(200))
    Plan_Code = Column(Unicode(100))
    MF_Security_Id = Column(BigInteger)
    PlanType_Id = Column(BigInteger)
    Option_Id = Column(BigInteger)
    SwitchAllowed_Id = Column(BigInteger)
    Plan_DivReinvOption = Column(Boolean)
    Plan_External_Map_Code = Column(Unicode(100))
    Plan_Demat = Column(Boolean)
    Plan_RTA_AMC_Code = Column(Unicode(200))
    ISIN = Column(Unicode(100))
    RTA_Code = Column(Unicode(100))
    RTA_Name = Column(Unicode(200))
    AMFI_Code = Column(Unicode(100))
    AMFI_Name = Column(Unicode(200))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)
    ISIN2 = Column(Unicode(200))
    Heartbeat_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.Plan_Name

class PricingMethod(Base):
    __tablename__ = 'PricingMethod'
    __table_args__ = {'schema': 'Masters'}

    PricingMethod_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    PricingMethod_Name = Column(Unicode(200))
    PricingMethod_Code = Column(Unicode(100))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.PricingMethod_Name


class Product(Base):
    __tablename__ = 'Product'
    __table_args__ = {'schema': 'Masters'}

    Product_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Product_Name = Column(Unicode(100))
    Product_Code = Column(Unicode(100))
    ProductCategory_Id = Column(BigInteger)
    ProductType_Id = Column(BigInteger)
    AssetClass_Id = Column(BigInteger)
    Issuer_Id = Column(BigInteger)
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)
    SortNo = Column(Integer)

    def __str__(self) -> str:
        return self.Product_Name


class ProductCategory(Base):
    __tablename__ = 'ProductCategory'
    __table_args__ = {'schema': 'Masters'}

    ProductCategory_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    ProductCategory_Name = Column(Unicode(100))
    ProductCategory_Code = Column(Unicode(100))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.ProductCategory_Name
    
class Report_Data_Issues(Base):
    __tablename__ = 'Report_Data_Issues'
    __table_args__ = {'schema': 'Masters'}

    Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Plan_Id = Column(BigInteger)
    Plan_Name = Column(Unicode(200))
    Cur_Nav_Date = Column(DateTime)
    Cur_Nav = Column(Numeric(18, 10))
    Last_Month_Nav = Column(Numeric(18, 10))
    Nav_Movement = Column(Numeric(18, 10))
    Fund_1Month_Performance = Column(Numeric(18, 10))
    Diff = Column(Numeric(18, 10))
    Issue_Type = Column(Unicode(100))
    Is_Fixed = Column(Boolean)

class Report_Plans_status(Base):
    __tablename__ = 'Report_Plans_status'
    __table_args__ = {'schema': 'Masters'}

    Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Plan_Id = Column(BigInteger)
    Shared_In_Last_Month_Report = Column(Boolean)
    Last_Month_Date = Column(DateTime)
    Shared_In_Current_Month = Column(Boolean)
    Current_Month_Date = Column(DateTime)
    added_on_portal = Column(DateTime)



class ProductType(Base):
    __tablename__ = 'ProductType'
    __table_args__ = {'schema': 'Masters'}

    ProductType_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    ProductType_Name = Column(Unicode(100))
    ProductType_Code = Column(Unicode(5))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.ProductType_Name


class RiskRating(Base):
    __tablename__ = 'RiskRating'
    __table_args__ = {'schema': 'Masters'}

    RiskRating_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    RiskRating_Name = Column(Unicode(200))
    RiskRating_Code = Column(Unicode(100))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.RiskRating_Name


class Sector(Base):
    __tablename__ = 'Sector'
    __table_args__ = {'schema': 'Masters'}

    Sector_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Sector_Name = Column(Unicode(200))
    Sector_Code = Column(Unicode(100))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.Sector_Name


class SecurityCategory(Base):
    __tablename__ = 'SecurityCategory'
    __table_args__ = {'schema': 'Masters'}

    SecurityCategory_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    SecurityCategory_Name = Column(Unicode(100))
    SecurityCategory_Code = Column(Unicode(100))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.SecurityCategory_Name


class State(Base):
    __tablename__ = 'State'
    __table_args__ = {'schema': 'Masters'}

    State_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    State_Name = Column(Unicode(100))
    State_Code = Column(Unicode(5))
    Country_Id = Column(Integer)
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.State_Name


class Status(Base):
    __tablename__ = 'Status'
    __table_args__ = {'schema': 'Masters'}

    Status_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Status_Name = Column(Unicode(200))
    Status_Code = Column(Unicode(100))
    Is_Deleted = Column(Boolean)

    def __str__(self) -> str:
        return self.Status_Name

class SwitchAllowed(Base):
    __tablename__ = 'SwitchAllowed'
    __table_args__ = {'schema': 'Masters'}

    SwitchAllowed_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    SwitchAllowed_Name = Column(Unicode(200))
    SwitchAllowed_Code = Column(Unicode(100))
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.SwitchAllowed_Name


class UploadFramework(Base):
    __tablename__ = 'UploadFramework'
    __table_args__ = {'schema': 'Masters'}

    UploadFramework_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    UploadFramework_Name = Column(Unicode(300))
    UploadFramework_SP_Name = Column(Unicode(300))
    Product_Id = Column(BigInteger)
    Is_Deleted = Column(Boolean)

    def __str__(self) -> str:
        return self.UploadFramework_Name


class UploadFrameworkStatus(Base):
    __tablename__ = 'UploadFrameworkStatus'
    __table_args__ = {'schema': 'Masters'}

    UploadFrameworkStatus_Id = Column(Integer, Identity(start=1, increment=1), primary_key=True)
    UploadFrameworkStatus_Name = Column(Unicode(100))
    Is_Deleted = Column(Boolean)

    def __str__(self) -> str:
        return self.UploadFrameworkStatus_Name


class Industry_Classification(Base):
    __tablename__ = 'Industry_Classification'
    __table_args__ = {'schema': 'Masters'}

    Basic_Ind_Code = Column(Unicode(100), primary_key=True)
    Basic_Industry = Column(Unicode(200))
    Ind_Code = Column(Unicode(100))
    Industry = Column(Unicode(200))
    Sect_Code = Column(Unicode(100))
    Sector = Column(Unicode(200))
    MES_Code = Column(Unicode(100))
    Macro_Economic_Sector = Column(Unicode(200))
    Sector_Id = Column(Integer)
    Description = Column(Unicode(2000))    
    Is_Deleted = Column(Boolean)
    Created_By = Column(BigInteger)
    Created_Date = Column(DateTime)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)
    CM_Sector_Code = Column(Unicode(20))
    CM_Sector_Name = Column(Unicode(200))

    def __str__(self) -> str:
        return self.Industry

    
class DebtSecurity(Base):
    __tablename__ = 'DebtSecurity'
    __table_args__ = {'schema': 'Masters'}
    
    Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    DebtSecurity_Id	= Column(BigInteger, nullable=False)
    Security_Name = Column(Unicode(100), nullable=False)
    ISIN = Column(Unicode(12), nullable=False)
    Exchange_1 = Column(Unicode(6))
    Exchange_1_Local_Code = Column(Unicode(50))
    Exchange_2 = Column(Unicode(6))
    Exchange_2_Local_Code  = Column(Unicode(50))
    Exchange_3 = Column(Unicode(6))
    Exchange_3_Local_Code = Column(Unicode(50))
    Security_Type = Column(Unicode(100), nullable=False)
    Bond_Type_Code = Column(Unicode(20), nullable=False)
    Bond_Type = Column(Unicode(100), nullable=False)
    Country = Column(Unicode(2), nullable=False)
    Bilav_Internal_Issuer_Id = Column(BigInteger, nullable=False)
    Bilav_Code = Column(Unicode(50), nullable=False)
    LEI = Column(Unicode(25))
    CIN	= Column(Unicode(25))
    Issuer = Column(Unicode(70), nullable=False)
    Issue_Price	= Column(Numeric(16,6))
    Issue_Date = Column(Date)
    Maturity_Price = Column(Numeric(16,6), nullable=False)
    Maturity_Based_On = Column(Unicode(10), nullable=False)
    Maturity_Benchmark_Index = Column(Unicode(100), nullable=False)
    Maturity_Price_As_Perc = Column(Numeric(16,6), nullable=False)
    Is_Perpetual = Column(Unicode(5))
    On_Tap_Indicator = Column(Boolean)
    Deemed_Allotment_Date = Column(Date, nullable=False)
    Coupon_Type_Code = Column(Unicode(10), nullable=False)
    Coupon_Type = Column(Unicode(70), nullable=False)
    Interest_Payment_Frequency_Code = Column(Unicode(10))
    Interest_Payment_Frequency = Column(Unicode(70))
    Interest_Payout_1 = Column(Date)
    Is_Cumulative = Column(Boolean)
    Compounding_Frequency_Code = Column(Unicode(10))
    Compounding_Frequency = Column(Unicode(70))
    Interest_Accrual_Convention_Code = Column(Unicode(10))
    Interest_Accrual_Convention = Column(Unicode(70))
    Min_Investment_Amount = Column(Numeric(16,6))
    FRN_Index_Benchmark	= Column(Unicode(10))
    FRN_Index_Benchmark_Desc = Column(Unicode(100))
    Interest_Pay_Date_1	= Column(Unicode(4))
    Interest_Pay_Date_2	= Column(Unicode(4))
    Interest_Pay_Date_3 = Column(Unicode(4))
    Interest_Pay_Date_4	= Column(Unicode(4))
    Interest_Pay_Date_5	= Column(Unicode(4))
    Interest_Pay_Date_6	= Column(Unicode(4))
    Interest_Pay_Date_7	= Column(Unicode(4))
    Interest_Pay_Date_8	= Column(Unicode(4))
    Interest_Pay_Date_9	= Column(Unicode(4))
    Interest_Pay_Date_10 = Column(Unicode(4))
    Interest_Pay_Date_11 = Column(Unicode(4))
    Interest_Pay_Date_12 = Column(Unicode(4))
    Issuer_Type_Code = Column(Unicode(10), nullable=False)
    Issuer_Type	= Column(Unicode(10), nullable=False)
    Issue_Size = Column(Numeric(36,6))
    Outstanding_Amount = Column(Numeric(16,6))
    Outstanding_Amount_Date	= Column(Date)
    Yield_At_Issue = Column(Numeric(16,6))
    Maturity_Structure_Code	= Column(Unicode(20), nullable=False)
    Maturity_Structure = Column(Unicode(50), nullable=False)
    Convention_Method_Code = Column(Unicode(10), nullable=False)
    Convention_Method = Column(Unicode(10))
    Interest_BDC_Code = Column(Unicode(10))
    Interest_BDC = Column(Unicode(10))
    Is_Variable_Interest_Payment_Date = Column(Boolean, nullable=False)
    Interest_Commencement_Date = Column(Date, nullable=False)
    Coupon_Cut_Off_Days	= Column(BigInteger)
    Coupon_Cut_Off_Day_Convention = Column(Unicode(10))
    FRN_Type = Column(Unicode(10))
    FRN_Interest_Adjustment_Frequency = Column(Unicode(10))
    Markup = Column(Numeric(16,6))
    Minimum_Interest_Rate = Column(Numeric(16,6))
    Maximum_Interest_Rate = Column(Numeric(16,6))
    Is_Guaranteed = Column(Boolean)
    Is_Secured = Column(Boolean)
    Security_Charge	= Column(Unicode(10))
    Security_Collateral	= Column(Boolean)
    Tier = Column(BigInteger)
    Is_Upper = Column(Boolean)
    Is_Sub_Ordinate	= Column(Boolean)
    Is_Senior = Column(Unicode(50))
    Is_Callable	= Column(Boolean, nullable=False)
    Is_Puttable	= Column(Boolean, nullable=False)
    Strip = Column(Unicode(50))
    Is_Taxable = Column(Boolean)
    Latest_Applied_INTPY_Annual_Coupon_Rate	= Column(Numeric(16,6))
    Latest_Applied_INTPY_Annual_Coupon_Rate_Date = Column(Date)
    Bond_Notes = Column(Text)
    End_Use	= Column(Text)
    Initial_Fixing_Date	= Column(Date)
    Initial_Fixing_Level = Column(Unicode(100))
    Final_Fixing_Date = Column(Date)
    Final_Fixing_Level = Column(Unicode(100))
    PayOff_Condition = Column(Unicode(100))
    Majority_Anchor_Investor = Column(Unicode(100))
    Security_Cover_Ratio = Column(Numeric(16,6))
    Margin_TopUp_Trigger = Column(Unicode(100))
    Current_Yield = Column(Numeric(16,6))
    Security_Presentation_Link = Column(Unicode(500))
    Coupon_Reset_Event = Column(Unicode(100))
    MES_Code = Column(Unicode(100))
    Macro_Economic_Sector = Column(Unicode(200))
    Sect_Code = Column(Unicode(100))
    Sector = Column(Unicode(200))
    Ind_Code = Column(Unicode(100))
    Industry = Column(Unicode(200))
    Basic_Ind_Code = Column(Unicode(100))
    Basic_Industry = Column(Unicode(200))
    Is_Deleted = Column(Boolean, nullable=False)
    Created_By = Column(BigInteger, nullable=False)
    Created_Date = Column(DateTime, nullable=False)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)

    def __str__(self) -> str:
        return self.Security_Name


class DebtCallOption(Base):
    __tablename__ = 'DebtCallOption'
    __table_args__ = {'schema': 'Masters'}

    Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    DebtCallOption_Id = Column(BigInteger, nullable=False)
    DebtSecurity_Id = Column(BigInteger, nullable=False)
    ISIN = Column(Unicode(12), nullable=False)
    Call_Type_Code = Column(Unicode(10))
    Call_Type = Column(Unicode(70))
    From_Date = Column(Date)
    To_Date = Column(Date)
    Notice_From_Date = Column(Date)
    Notice_To_Date = Column(Date)
    Min_Notice_Days = Column(BigInteger)
    Max_Notice_Days	= Column(BigInteger)
    Currency = Column(Unicode(3))
    Call_Price = Column(Numeric(16,6))
    Call_Price_As_Perc = Column(Numeric(16,6))
    Is_Formulae_Based = Column(Boolean)
    Is_Mandatory_Call = Column(Boolean)
    Is_Part_Call = Column(Boolean)
    Is_Deleted = Column(Boolean, nullable=False)
    Created_By = Column(BigInteger, nullable=False)
    Created_Date = Column(DateTime, nullable=False)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)


class DebtPutOption(Base):
    __tablename__ = 'DebtPutOption'
    __table_args__ = {'schema': 'Masters'}

    Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    DebtPutOption_Id = Column(BigInteger, nullable=False)
    DebtSecurity_Id = Column(BigInteger, nullable=False)
    ISIN = Column(Unicode(12), nullable=False)
    Put_Type_Code = Column(Unicode(10))
    Put_Type = Column(Unicode(70))
    From_Date = Column(Date)
    To_Date = Column(Date)
    Notice_From_Date = Column(Date)
    Notice_To_Date = Column(Date)
    Min_Notice_Days = Column(BigInteger)
    Max_Notice_Days	= Column(BigInteger)
    Currency = Column(Unicode(3))
    Put_Price = Column(Numeric(16,6))
    Put_Price_As_Perc = Column(Numeric(16,6))
    Is_Formulae_Based = Column(Boolean)
    Is_Mandatory_Put = Column(Boolean)
    Is_Part_Put = Column(Boolean)
    Is_Deleted = Column(Boolean, nullable=False)
    Created_By = Column(BigInteger, nullable=False)
    Created_Date = Column(DateTime, nullable=False)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)


class DebtRedemption(Base):
    __tablename__ = 'DebtRedemption'
    __table_args__ = {'schema': 'Masters'}

    Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    DebtRedemption_Id = Column(BigInteger, nullable=False)
    DebtSecurity_Id = Column(BigInteger)
    ISIN = Column(Unicode(10), nullable=False)
    Redemption_Date = Column(Date, nullable=False)
    Redemption_Type_Code = Column(Unicode(20), nullable=False)
    Redemption_Type = Column(Unicode(70), nullable=False)
    Redemption_Currency = Column(Unicode(3), nullable=False)
    Redemption_Price = Column(Numeric(16,6), nullable=False)
    Redemption_Amount = Column(Numeric(16,6))
    Redemption_Price_As_Perc = Column(Numeric(12,8), nullable=False)
    Redemption_Percentage = Column(Numeric(12,9))
    Redemption_Premium	= Column(Numeric(16,6))
    Redemption_Premium_As_Perc = Column(Numeric(12,8))
    Is_Mandatory_Redemption	= Column(Boolean, nullable=False)
    Is_Part_Redemption = Column(Boolean, nullable=False)
    Is_Deleted = Column(Boolean, nullable=False)
    Created_By = Column(BigInteger, nullable=False)
    Created_Date = Column(DateTime, nullable=False)
    Updated_By = Column(BigInteger)
    Updated_Date = Column(DateTime)


class DebtCreditRating(Base):
    __tablename__ = 'DebtCreditRating'
    __table_args__ = {'schema': 'Masters'}

    Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    DebtCreditRating_Id = Column(BigInteger, nullable=False)
    DebtSecurity_Id = Column(BigInteger, nullable=False)
    ISIN = Column(Unicode(10), nullable=False)
    Rating_Agency = Column(Unicode(100), nullable=False)
    Rating_Date = Column(Date, nullable=False)
    Rating_Symbol = Column(Unicode(10), nullable=False)
    Rating_Direction_Code = Column(Unicode(10), nullable=False)
    Rating_Direction = Column(Unicode(70), nullable=False)
    Watch_Flag_Code = Column(Unicode(10))
    Watch_Flag = Column(Unicode(70))
    Watch_Flag_Reason_Code = Column(Unicode(10))
    Watch_Flag_Reason = Column(Unicode(70))
    Rating_Prefix = Column(Unicode(10))
    Prefix_Description = Column(Unicode(70))
    Rating_Suffix = Column(Unicode(10))
    Suffix_Description = Column(Unicode(70))
    Rating_Outlook_Description = Column(Unicode(70))
    Expected_Loss = Column(Unicode(10))
    AsofDate = Column(Date, nullable=False)
    Is_Deleted = Column(Boolean, nullable=False)
    Created_By = Column(BigInteger, nullable=False)
    Created_Date = Column(DateTime, nullable=False)
    Updated_By = Column(BigInteger, nullable=False)
    Updated_Date = Column(DateTime, nullable=False)

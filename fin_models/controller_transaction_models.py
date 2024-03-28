from sqlalchemy import BigInteger, Boolean, Column, DateTime, Identity, Table, Unicode, Integer,\
                       JSON, UniqueConstraint, Float, SmallInteger, text, Text, Date
from sqlalchemy.dialects.mssql import TINYINT
from .controller_master_models import ControllerBase
from .common_enum import CustomScreenAccess

metadata = ControllerBase.metadata

t_ApplicationUser = Table(
    'ApplicationUser', metadata,
    Column('ApplicationUser_Id', BigInteger, Identity(start=1, increment=1), nullable=False),
    Column('Application_Id', BigInteger, nullable=False),
    Column('User_Id', BigInteger, nullable=False),
    Column('Is_Active', Boolean, nullable=False),
    Column('Created_By_User_Id', BigInteger, nullable=False),
    Column('Created_Date_Time', DateTime, nullable=False),
    Column('Modified_By_User_Id', BigInteger),
    Column('Modified_Date_Time', DateTime),
    schema='Transactions'
)


class Password(ControllerBase):
    __tablename__ = 'Password'
    __table_args__ = {'schema': 'Transactions'}

    Password_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    User_Id = Column(BigInteger, nullable=False)
    Password = Column(Unicode(100), nullable=False)
    Expiry_Date_Time = Column(DateTime, nullable=False)
    Type = Column(TINYINT, nullable=False)
    Is_Active = Column(Boolean, nullable=False)
    Created_By_User_Id = Column(BigInteger, nullable=False)
    Created_Date_Time = Column(DateTime, nullable=False)
    Modified_By_User_Id = Column(BigInteger)
    Modified_Date_Time = Column(DateTime)


class RoleMenuPermission(ControllerBase):
    __tablename__ = 'RoleMenuPermission'
    __table_args__ = {'schema': 'Transactions'}

    RoleMenuPermission_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Role_Id = Column(BigInteger, nullable=False)
    Menu_Id = Column(BigInteger, nullable=False)
    View_Access = Column(Boolean, nullable=False)
    Add_Access = Column(Boolean, nullable=False)
    Modify_Access = Column(Boolean, nullable=False)
    Delete_Access = Column(Boolean, nullable=False)
    All_Access = Column(Boolean, nullable=False)
    Is_Visible = Column(Boolean, nullable=False)
    Is_Disabled = Column(Boolean, nullable=False)
    Is_Active = Column(Boolean, nullable=False)
    Created_By_User_Id = Column(BigInteger, nullable=False)
    Created_Date_Time = Column(DateTime, nullable=False)
    ModifiedBy_User_Id = Column(BigInteger)
    Modified_Date_Time = Column(DateTime)


class CustomScreens(ControllerBase):
    __tablename__ = 'CustomScreens'
    __table_args__ = {'schema': 'Transactions'}

    id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    uuid = Column(Unicode(100), nullable=False)
    name = Column(Unicode(100), nullable=False, unique=True)
    description = Column(Unicode(1000))
    query_json = Column(JSON, nullable=False)
    access = Column(Unicode(100), nullable=False, server_default= CustomScreenAccess.public.name)
    is_active = Column(Boolean, nullable=False, server_default= "1")
    created_by = Column(BigInteger, nullable=False)
    created_at = Column(DateTime, nullable=False)
    org_id  = Column(BigInteger, nullable=False, comment="organization of the user that created the screen")
    modified_by = Column(BigInteger)
    modified_at = Column(DateTime)


class APILog(ControllerBase):
    __tablename__ = 'APILog'
    __table_args__ = {'schema': 'Transactions'}

    id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    entity_id = Column(BigInteger)
    entity_type = Column(Unicode(10))
    remote_addr = Column(Unicode(100))
    http_method = Column(Unicode(10))
    url_path = Column(Unicode(1000))
    query_str = Column(Text)
    req_ts = Column(DateTime)
    req_payload = Column(JSON)
    req_has_files = Column(Boolean)
    resp_status_code = Column(Integer)
    resp_payload = Column(JSON)
    resp_error = Column(JSON)
    resp_time_ms = Column(Integer)
    resp_size_bytes = Column(Integer)
    fqdn = Column(Unicode(200))


class RazorpayLog(ControllerBase):
    __tablename__ = 'RazorpayLog'
    __table_args__ = {'schema': 'Transactions'}

    id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    org_id = Column(BigInteger)
    razorpay_order_id = Column(Unicode(500))
    razorpay_payment_id = Column(Unicode(500))
    razorpay_signature = Column(Unicode(500))
    razorpay_order = Column(JSON)
    razorpay_payment = Column(JSON)


class OrgFundSettings(ControllerBase):
    __tablename__ = 'OrgFundSettings'
    __table_args__ = {'schema': 'Transactions'}
    
    id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    org_id = Column(BigInteger)
    amc_id = Column(BigInteger, nullable=False, comment="Later on replace this with amc code.")
    fund_code = Column(Unicode(100))
    can_show = Column(Boolean, server_default=text('1'))
    can_export = Column(Boolean, server_default=text('0'))
    can_buy = Column(Boolean, server_default=text('0'))
    facilitator_id = Column(BigInteger)
    distributor_org_id =  Column(Unicode(500))
    distributor_token =  Column(Unicode(500))
    # distributor_pan_no = Column(Unicode(100), comment="for 1SB, it will contain distributor pan number.")
    created_by = Column(BigInteger, nullable=False)
    created_at = Column(DateTime, nullable=False)
    modified_by = Column(BigInteger)
    modified_at = Column(DateTime)

    ux_idx = UniqueConstraint(org_id, amc_id, fund_code)


class Investor(ControllerBase):
    __tablename__ = 'Investor'
    __table_args__ = {'schema': 'Transactions'}

    id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    is_deleted = Column(TINYINT, server_default=text('0'))
    created_by = Column(BigInteger)
    created_date = Column(DateTime)
    updated_by = Column(BigInteger)
    updated_date = Column(DateTime)
    label = Column(Unicode(100))
    name = Column(Unicode(100))
    pan_no = Column(Unicode(10))


class AccountAggregatorAPIStatus(ControllerBase):
    __tablename__ = 'AccountAggregatorAPIStatus'
    __table_args__ = {'schema': 'Transactions'}

    id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    user_id = Column(Unicode(100), comment="Encoded value for user_id")
    txn_id = Column(Unicode(100), comment="transaction id of the API")
    data_start_date = Column(Date, comment="Start date for the data fetch")
    data_end_date = Column(Date, comment="End date for the data fetch")
    consent_init_req_time = Column(DateTime, comment="time when consent was initiated")
    consent_init_resp = Column(JSON)
    consent_status_time = Column(DateTime, comment="time when consent callback was called")
    consent_status = Column(Unicode(10))
    consent_accounts = Column(JSON)
    fetch_init_req_time = Column(DateTime, comment="time when fetch was initiated")
    fetch_init_resp = Column(JSON)
    fetch_callback_req_time = Column(DateTime, comment="time when fetch was initiated")
    report_fetch_resp = Column(JSON)

class InvestorAccount(ControllerBase):
    __tablename__ = 'InvestorAccount'
    __table_args__ = {'schema': 'Transactions'}

    id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    is_deleted = Column(TINYINT, server_default=text('0'))
    created_by = Column(BigInteger)
    created_date = Column(DateTime)
    updated_by = Column(BigInteger)
    updated_date = Column(DateTime)
    investor_id = Column(BigInteger)
    owners = Column(JSON, comment="dictionary with name and pan no.")
    account_type = Column(Unicode(10), comment="demat or folio")
    depository = Column(Unicode(100))
    dp_name = Column(Unicode(100))
    account_no = Column(Unicode(100))
    label = Column(Unicode(100))
    mapped_fund_code = Column(Unicode(100), comment="fund_code from PMS")
    is_dummy = Column(SmallInteger)
    # Add is_closed field to check if the account is closed
    # potential start date
    # potential account closure date


class InvestorHoldings(ControllerBase):
    __tablename__ = 'InvestorHoldings'
    __table_args__ = {'schema': 'Transactions'}

    id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    is_deleted = Column(TINYINT, server_default=text('0'))
    created_by = Column(BigInteger)
    created_date = Column(DateTime)
    updated_by = Column(BigInteger)
    updated_date = Column(DateTime)
    account_id = Column(BigInteger, comment="FK to InvestorAccount")
    as_of_date = Column(DateTime)
    isin = Column(Unicode(20))
    name = Column(Unicode(200))
    type = Column(Unicode(10), comment="should match with holding type.")
    coupon_rate = Column(Unicode(50))
    maturity_date = Column(Unicode(50))
    units = Column(Float)
    unit_price = Column(Float)
    total_price = Column(Float)


class InvestorTransactions(ControllerBase):
    __tablename__ = 'InvestorTransactions'
    __table_args__ = {'schema': 'Transactions'}

    id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    is_deleted = Column(TINYINT, server_default=text('0'))
    created_by = Column(BigInteger)
    created_date = Column(DateTime)
    updated_by = Column(BigInteger)
    updated_date = Column(DateTime)
    account_id = Column(BigInteger, comment="FK to InvestorAccount")
    tran_date = Column(DateTime)
    isin = Column(Unicode(20))
    name = Column(Unicode(200))
    type = Column(Unicode(10), comment="should match with holding type.")
    tran_type = Column(Unicode(10), comment="should be buy - b or sell - s.")
    units = Column(Float)
    unit_price = Column(Float)
    total_price = Column(Float)
    stamp_duty = Column(Float)
    is_valid_tran = Column(TINYINT, server_default=text('0'))
    status = Column(Unicode(400))



class InvestorRecommendation(ControllerBase):
    __tablename__ = 'InvestorRecommendation'
    __table_args__ = {'schema': 'Transactions'}

    id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    is_deleted = Column(TINYINT, server_default=text('0'))
    created_by = Column(BigInteger)
    created_date = Column(DateTime)
    updated_by = Column(BigInteger)
    updated_date = Column(DateTime)
    investor_id = Column(BigInteger)
    suggestion_date  = Column(DateTime)
    observation = Column(Unicode)
    suggestion = Column(Unicode)
    account_id = Column(BigInteger, comment="FK to InvestorAccount")
    isin = Column(Unicode(20))
    action_type = Column(Unicode(20), comment="buy or sell")
    units = Column(Float)


class ModelPortfolio(ControllerBase):
    __tablename__ = 'ModelPortfolio'
    __table_args__ = {'schema': 'Transactions'}

    id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    is_deleted = Column(TINYINT, server_default=text('0'))
    created_by = Column(BigInteger)
    created_date = Column(DateTime)
    updated_by = Column(BigInteger)
    updated_date = Column(DateTime)
    name = Column(Unicode(100))
    description = Column(Unicode(1000))


class ModelPortfolioHoldings(ControllerBase):
    __tablename__ = 'ModelPortfolioHoldings'
    __table_args__ = {'schema': 'Transactions'}

    id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    is_deleted = Column(TINYINT, server_default=text('0'))
    created_by = Column(BigInteger)
    created_date = Column(DateTime)
    updated_by = Column(BigInteger)
    updated_date = Column(DateTime)
    model_portfolio_id = Column(BigInteger, comment="FK to InvestorAccount")
    as_of_date = Column(DateTime)
    isin = Column(Unicode(20))
    name = Column(Unicode(200))
    weight = Column(Float)


class ModelPortfolioReturns(ControllerBase):
    __tablename__ = 'ModelPortfolioReturns'
    __table_args__ = {'schema': 'Transactions'}

    id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    is_deleted = Column(TINYINT, server_default=text('0'))
    created_by = Column(BigInteger)
    created_date = Column(DateTime)
    updated_by = Column(BigInteger)
    updated_date = Column(DateTime)
    model_portfolio_id = Column(BigInteger, comment="FK to InvestorAccount")
    as_of_date = Column(DateTime)
    return_1_month = Column(Float)



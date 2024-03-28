from sqlalchemy import BigInteger, Boolean, Column, Date, DateTime, Float, Identity, Index, Integer, Numeric, String, Table, Unicode, ForeignKey, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship

import enum

Base = declarative_base()

class ServiceType(enum.Enum):
    total = "Total"
    discretionary = "Discretionary"
    non_discretionary = "Non-Discretionary"
    advisory = "Advisory"

class AMC(Base):
    __tablename__ = 'AMC'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    sebi_nr = Column(Unicode(100), comment="Registration Number defined by SEBI. cannot be unique as AMC tend to change names.")
    name = Column(Unicode(255), nullable=False)
    register_date = Column(Date, comment="Registration Date")
    name_change_date = Column(Date)
    is_active = Column(Boolean, server_default='1')
    deactivation_reason = Column(Unicode(1000))
    created_at = Column(DateTime)

    def __str__(self) -> str:
        return self.name

class AMCScrapperStatus(Base):
    __tablename__ = 'AMCScrapperStatus'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    sebi_nr = Column(Unicode(100), comment="Registration Number defined by SEBI")
    as_of = Column(Date, comment="Records update date")
    last_tried_on = Column(DateTime, comment="The last date scrapper tried to read the data.")
    raw_html_path = Column(Unicode(1000))
    has_data = Column(Boolean, server_default='0')
    is_successful = Column(Boolean, server_default='0')
    error_description = Column(Unicode(1000))
    to_ignore = Column(Boolean, server_default='0', comment="While checking for missing data, skip this record as we know there is no data for this.")
    ignore_reason = Column(Unicode(1000))
    created_at = Column(DateTime)
    
class AMCInfo(Base):
    __tablename__ = 'AMCInfo'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    sebi_nr = Column(Unicode(100), comment="Registration Number defined by SEBI")
    as_of = Column(Date, comment="Records update date")
    address = Column(Unicode(500), comment="Registered Address")
    principal_officer_name = Column(Unicode(1000))
    principal_officer_email = Column(Unicode(1000))
    principal_officer_contact = Column(Unicode(1000))
    compliance_officer_name = Column(Unicode(1000))
    compliance_officer_email = Column(Unicode(1000))
    total_client = Column(Integer)
    total_aum_in_cr = Column(Float)
    is_discretionary_active = Column(Boolean, server_default='0')
    is_non_discretionary_active = Column(Boolean, server_default='0')
    is_advisory_active = Column(Boolean, server_default='0')
    is_active = Column(Boolean, server_default='1')
    created_at = Column(DateTime)

class AMCComplaints(Base):
    __tablename__ = 'AMCComplaints'

    id = Column(Integer, primary_key=True, autoincrement=True)
    sebi_nr = Column(Unicode(100), comment="Registration Number defined by SEBI")
    as_of = Column(Date, comment="Records update date")
    pf_pending_at_month_start = Column(Integer)
    pf_received_during_month = Column(Integer)
    pf_resolved_during_month = Column(Integer)
    pf_pending_at_month_end = Column(Integer)
    corporates_pending_at_month_start = Column(Integer)
    corporates_received_during_month = Column(Integer)
    corporates_resolved_during_month = Column(Integer)
    corporates_pending_at_month_end = Column(Integer)
    non_corporates_pending_at_month_start = Column(Integer)
    non_corporates_received_during_month = Column(Integer)
    non_corporates_resolved_during_month = Column(Integer)
    non_corporates_pending_at_month_end = Column(Integer)
    nri_pending_at_month_start = Column(Integer)
    nri_received_during_month = Column(Integer)
    nri_resolved_during_month = Column(Integer)
    nri_pending_at_month_end = Column(Integer)
    fpi_pending_at_month_start = Column(Integer)
    fpi_received_during_month = Column(Integer)
    fpi_resolved_during_month = Column(Integer)
    fpi_pending_at_month_end = Column(Integer)
    others_pending_at_month_start = Column(Integer)
    others_received_during_month = Column(Integer)
    others_resolved_during_month = Column(Integer)
    others_pending_at_month_end = Column(Integer)
    is_active = Column(Boolean, server_default='1')
    created_at = Column(DateTime)

class AMCTransactions(Base):
    __tablename__ = 'AMCTransactions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    sebi_nr = Column(Unicode(100), comment="Registration Number defined by SEBI")
    as_of = Column(Date, comment="Records update date")
    service_type = Column(Unicode(1000))
    sales_in_month_in_cr = Column(Float)
    purchase_in_month_in_cr = Column(Float)
    ptr = Column(Float, comment="Portfolio Turnover Ratio = (Higher of Purchase or Sales in the month/ Average AUM)")
    is_active = Column(Boolean, server_default='1')
    created_at = Column(DateTime)

class AMCClients(Base):
    __tablename__ = 'AMCClients'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    sebi_nr = Column(Unicode(100), comment="Registration Number defined by SEBI")
    as_of = Column(Date, comment="Records update date")
    service_type = Column(Unicode(1000))
    pf_clients = Column(Integer)
    corporates_clients = Column(Integer)
    non_corporates_clients = Column(Integer)
    nri_clients = Column(Integer)
    fpi_clients = Column(Integer)
    others_clients = Column(Integer)
    pf_aum = Column(Float)
    corporates_aum = Column(Float)
    non_corporates_aum = Column(Float)
    nri_aum = Column(Float)
    fpi_aum = Column(Float)
    others_aum = Column(Float)
    is_active = Column(Boolean, server_default='1')
    created_at = Column(DateTime)

class AMCAssetClasses(Base):
    __tablename__ = 'AMCAssetClasses'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    sebi_nr = Column(Unicode(100), comment="Registration Number defined by SEBI")
    as_of = Column(Date, comment="Records update date")
    service_type = Column(Unicode(1000))
    listed_equity = Column(Float)
    unlisted_equity = Column(Float)
    listed_plain_debt = Column(Float)
    unlisted_plain_debt = Column(Float)
    listed_structured_debt  = Column(Float)
    unlisted_structured_debt = Column(Float)
    equity_derivatives = Column(Float)
    commodity_derivatives = Column(Float)
    other_derivatives = Column(Float)
    mutual_funds = Column(Float)
    others = Column(Float)
    total = Column(Float)    
    is_active = Column(Boolean, server_default='1')
    created_at = Column(DateTime)

class NonDiscretionaryDetails(Base):
    __tablename__ = 'NonDiscretionaryDetails'

    id = Column(Integer, primary_key=True, autoincrement=True)
    as_of = Column(Date, comment="Records update date")
    sebi_nr = Column(Unicode(100), comment="Registration Number defined by SEBI")
    aum = Column(Float)
    inflow_month_in_cr = Column(Float)
    outflow_month_in_cr = Column(Float)
    net_flow_month_in_cr = Column(Float)
    inflow_fy_in_cr = Column(Float)
    outflow_fy_in_cr = Column(Float)
    net_flow_fy_in_cr = Column(Float)
    ptr_1_month = Column(Float, comment="Portfolio turnover ratio")
    ptr_1_yr = Column(Float, comment="Portfolio turnover ratio")
    returns_1_month = Column(Float)
    returns_1_yr = Column(Float)
    is_active = Column(Boolean, server_default='1')
    created_at = Column(DateTime)

class DiscretionaryDetails(Base):
    __tablename__ = 'DiscretionaryDetails'

    id = Column(Integer, primary_key=True, autoincrement=True)
    as_of = Column(Date, comment="Records update date")
    sebi_nr = Column(Unicode(100), comment="Registration Number defined by SEBI")
    aum = Column(Float)
    inflow_month_in_cr = Column(Float)
    outflow_month_in_cr = Column(Float)
    net_flow_month_in_cr = Column(Float)
    inflow_fy_in_cr = Column(Float)
    outflow_fy_in_cr = Column(Float)
    net_flow_fy_in_cr = Column(Float)
    ptr_1_month = Column(Float, comment="Portfolio turnover ratio")
    ptr_1_yr = Column(Float, comment="Portfolio turnover ratio")
    is_active = Column(Boolean, server_default='1')
    created_at = Column(DateTime)

class Scheme(Base):
    __tablename__ = 'Scheme'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Unicode(500), nullable=False)
    sebi_nr = Column(Unicode(100), comment="Registration Number defined by SEBI")
    scheme_code = Column(Unicode(100))
    is_active = Column(Boolean, server_default='1')
    deactivation_reason = Column(Unicode(1000))
    created_at = Column(DateTime)

    ux_idx = UniqueConstraint(name, sebi_nr)

    def __str__(self) -> str:
        return self.name

class SchemeAssetClasses(Base):
    __tablename__ = 'SchemeAssetClasses'

    id = Column(Integer, primary_key=True, autoincrement=True)
    as_of = Column(Date, comment="Records update date")
    scheme_id = Column(Integer, ForeignKey(u'Scheme.id'))
    listed_equity = Column(Float)
    unlisted_equity = Column(Float)
    listed_plain_debt = Column(Float)
    unlisted_plain_debt = Column(Float)
    listed_structured_debt  = Column(Float)
    unlisted_structured_debt = Column(Float)
    equity_derivatives = Column(Float)
    commodity_derivatives = Column(Float)
    other_derivatives = Column(Float)
    mutual_funds = Column(Float)
    others = Column(Float)
    total = Column(Float)    
    is_active = Column(Boolean, server_default='1')
    created_at = Column(DateTime)

    scheme = relationship(u'Scheme')

class SchemePerformance(Base):
    __tablename__ = 'SchemePerformance'

    id = Column(Integer, primary_key=True, autoincrement=True)
    as_of = Column(Date, comment="Records update date")
    scheme_id = Column(Integer, ForeignKey(u'Scheme.id'))
    aum = Column(Float)
    scheme_returns_1_month = Column(Float)
    scheme_returns_1_yr = Column(Float)
    ptr_1_month = Column(Float, comment="Portfolio turnover ratio")
    ptr_1_yr = Column(Float, comment="Portfolio turnover ratio")    
    benchmark_name = Column(Unicode(1000))
    benchmark_returns_1_month = Column(Float)
    benchmark_returns_1_yr = Column(Float)
    created_at = Column(DateTime)

    scheme = relationship(u'Scheme')

class SchemeFlow(Base):
    __tablename__ = 'SchemeFlow'

    id = Column(Integer, primary_key=True, autoincrement=True)
    as_of = Column(Date, comment="Records update date")
    scheme_id = Column(Integer, ForeignKey(u'Scheme.id'))
    inflow_month_in_cr = Column(Float)
    outflow_month_in_cr = Column(Float)
    net_flow_month_in_cr = Column(Float)
    inflow_fy_in_cr = Column(Float)
    outflow_fy_in_cr = Column(Float)
    net_flow_fy_in_cr = Column(Float)
    created_at = Column(DateTime)

    scheme = relationship(u'Scheme')

class SebiISIN(Base):
    __tablename__ = 'SebiISIN'

    id = Column(Integer, primary_key=True, autoincrement=True)
    issuer_name = Column(Unicode(1000))
    issuer_type = Column(Unicode(1000))
    security_type = Column(Unicode(1000))
    security_sub_type = Column(Unicode(1000))
    isin = Column(Unicode(1000))
    status = Column(Unicode(1000))
    demat_isin = Column(Unicode(1000))
    created_at = Column(DateTime)

class SebiISINScrapperStatus(Base):
    __tablename__ = 'SebiISINScrapperStatus'
    
    # TODO: Merge this table with AMC Scrapper Status and make a single scrapper status table that has models, is_successful, should_retry and error details in json.

    id = Column(Integer, primary_key=True, autoincrement=True)
    last_tried_on = Column(DateTime)
    has_data = Column(Boolean, server_default='0')
    is_successful = Column(Boolean, server_default='0')
    error_description = Column(Unicode(1000))
    created_at = Column(DateTime)
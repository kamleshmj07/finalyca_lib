from sqlalchemy import BigInteger, Boolean, Column, Date, DateTime, Float, Identity, Integer, SmallInteger, String, Unicode, UnicodeText, UniqueConstraint, JSON
from sqlalchemy.dialects.mssql import TINYINT
from sqlalchemy.orm import declarative_base

ControllerBase = declarative_base()


class Application(ControllerBase):
    __tablename__ = 'Application'
    __table_args__ = {'schema': 'Masters'}

    Application_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Application_Name = Column(Unicode(100), nullable=False)
    Is_Active = Column(Boolean, nullable=False)
    Created_By_User_Id = Column(BigInteger, nullable=False)
    Created_Date_Time = Column(DateTime, nullable=False)
    Modified_By_User_Id = Column(BigInteger)
    Modified_Date_Time = Column(DateTime)


class Company(ControllerBase):
    __tablename__ = 'Company'
    __table_args__ = {'schema': 'Masters'}

    Company_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Company_Name = Column(String(100, 'SQL_Latin1_General_CP1_CI_AS'))
    Company_Address_1 = Column(String(200, 'SQL_Latin1_General_CP1_CI_AS'))
    Company_Address_2 = Column(String(200, 'SQL_Latin1_General_CP1_CI_AS'))
    Company_Address_3 = Column(String(200, 'SQL_Latin1_General_CP1_CI_AS'))


class Menu(ControllerBase):
    __tablename__ = 'Menu'
    __table_args__ = {'schema': 'Masters'}

    Menu_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Menu_Name = Column(Unicode(50), nullable=False)
    Application_Id = Column(BigInteger, nullable=False)
    Parent_Id = Column(BigInteger, nullable=False)
    Menu_Order = Column(TINYINT, nullable=False)
    Menu_Level = Column(TINYINT, nullable=False)
    Is_Active = Column(Boolean, nullable=False)
    Created_By_User_Id = Column(BigInteger, nullable=False)
    Created_Date_Time = Column(DateTime, nullable=False)
    Menu_URL = Column(Unicode(100))
    Modified_By_User_Id = Column(BigInteger)
    Modified_Date_Time = Column(DateTime)
    CSS_Class = Column(String(500, 'SQL_Latin1_General_CP1_CI_AS'))
    CSS_Icon = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))
    Angular_CSS_Icon = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))
    Angular_Menu_URL = Column(String(100, 'SQL_Latin1_General_CP1_CI_AS'))

class Role(ControllerBase):
    __tablename__ = 'Role'
    __table_args__ = {'schema': 'Masters'}

    Role_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Role_Name = Column(Unicode(50), nullable=False)
    Application_Id = Column(BigInteger, nullable=False)
    Is_Active = Column(TINYINT, nullable=False)
    Created_By_User_Id = Column(BigInteger, nullable=False)
    Created_Date_Time = Column(DateTime, nullable=False)
    Modified_By_User_Id = Column(BigInteger)
    Modified_Date_Time = Column(DateTime)
    Company_Id = Column(BigInteger)
    
class Organization(ControllerBase):
    __tablename__ = 'Organization'
    __table_args__ = {'schema': 'Masters'}

    Organization_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Organization_Name = Column(String(100, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    # TODO: Remove following column. it should be replaced with the 3 columns below.
    No_Of_Licenses = Column(Integer, nullable=False)
    # No_Of_L1_Licenses = Column(Integer, nullable=False)
    # No_Of_L2_Licenses = Column(Integer, nullable=False)
    # No_Of_L3_Licenses = Column(Integer, nullable=False)
    No_Of_Silver_Licenses = Column(Integer, nullable=False)
    No_Of_Lite_Licenses = Column(Integer, nullable=False)
    No_Of_Pro_Licenses = Column(Integer, nullable=False)
    No_Of_RM_Licenses = Column(Integer, nullable=False)
    License_Expiry_Date = Column(Date, nullable=False)
    Adminuser_Email = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Adminuser_Mobile = Column(String(20, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Adminuser_Fullname = Column(String(100, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Is_Mobile_Mandatory = Column(Boolean)
    Is_Active = Column(Boolean, nullable=False)
    Is_DatacontrolEnable = Column(Boolean)
    AMC_Id = Column(BigInteger)
    Is_Enterprise_Value = Column(Boolean)
    Is_WhiteLabel_Value = Column(Boolean)
    Application_Title = Column(Unicode(100))
    Logo_Img = Column(Unicode)
    Disclaimer_Img = Column(Unicode)
    Disclaimer_Img2 = Column(Unicode)
    is_api_enabled = Column(Boolean)    
    api_access_level = Column(Integer)
    api_available_hits = Column(BigInteger)
    api_remote_addr = Column(Unicode(1000), comment="List of servers IP and/or host name from where this api key will be consumed.")
    is_excel_export_enabled = Column(Boolean)
    excel_export_count = Column(BigInteger)
    is_buy_enable = Column(Boolean)
    is_self_subscribed = Column(Boolean)
    is_payment_pending = Column(Boolean)
    disclaimer = Column(Unicode(5000))
    usertype_id = Column(BigInteger)
    otp_allowed_over_mail = Column(Boolean)
    gst_number = Column(String(20, 'SQL_Latin1_General_CP1_CI_AS'))


class DOFacilitator(ControllerBase):
    # DOFacilitator -> Digital Onboarding Facilitator
    __tablename__ = 'DOFacilitator'
    __table_args__ = {'schema': 'Masters'}

    id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    created_by = Column(BigInteger, nullable=False)
    created_at = Column(DateTime, nullable=False)
    edited_by = Column(BigInteger)
    edited_at = Column(DateTime)
    is_active = Column(Boolean)
    name = Column(Unicode(200), nullable=False)
    facilitator_url = Column(Unicode(1000), nullable=False)
    # TODO: treat following fiels as password
    # facilitator_token = Column(Unicode(200), nullable=False)
    facilitator_settings = Column(JSON, server_default='{}')

    def __str__(self) -> str:
        return self.name

class User(ControllerBase):
    __tablename__ = 'User'
    __table_args__ = {'schema': 'Masters'}

    User_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    User_Name = Column(Unicode(50), nullable=False)
    Salutation = Column(Unicode(5), nullable=False)
    First_Name = Column(Unicode(20), nullable=False)
    Middle_Name = Column(Unicode(20), nullable=False)
    Last_Name = Column(Unicode(20), nullable=False)
    Gender = Column(TINYINT, nullable=False)
    Marital_Status = Column(TINYINT, nullable=False)
    Birth_Date = Column(Date, nullable=False)
    Contact_Number = Column(Unicode(50), nullable=False)
    Login_Failed_Attempts = Column(TINYINT, nullable=False)
    Is_Account_Locked = Column(Boolean, nullable=False)
    Last_Login_Date_Time = Column(DateTime, nullable=False)
    Is_Active = Column(Boolean, nullable=False)
    Created_By_User_Id = Column(BigInteger, nullable=False)
    Created_Date_Time = Column(DateTime, nullable=False)
    Display_Name = Column(Unicode(50))
    Email_Address = Column(String(100, 'SQL_Latin1_General_CP1_CI_AS'))
    Account_Locked_Till_Date = Column(Date)
    Secret_Question_Id = Column(BigInteger)
    Hint_Word = Column(Unicode(50))
    Secret_Answer = Column(Unicode(100))
    Referred_By_Id = Column(BigInteger)
    Reference_Code = Column(Unicode(50))
    Modified_By_User_Id = Column(BigInteger)
    Modified_Date_Time = Column(DateTime)
    Role_Id = Column(BigInteger)
    Organization_Id = Column(BigInteger)
    Activation_Code = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))
    OTP = Column(Integer)
    Login_Count = Column(BigInteger)
    Designation = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))
    City = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))
    State = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))
    Pin_Code = Column(String(10, 'SQL_Latin1_General_CP1_CI_AS'))
    Session_Id = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))
    Access_Level = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'))
    Is_SSO_Login = Column(Boolean)
    downloadnav_enabled = Column(Boolean, nullable=False)
    Profile_Picture = Column(Unicode(100))

class API(ControllerBase):
    __tablename__ = 'API'
    __table_args__ = {'schema': 'Masters'}

    id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    name = Column(Unicode(100), nullable=False)
    org_id = Column(BigInteger, nullable=False)
    api_key = Column(Unicode(100), nullable=False)
    requested_by = Column(BigInteger, nullable=False)
    requested_at = Column(DateTime, nullable=False)
    edited_by = Column(BigInteger)
    edited_at = Column(DateTime)
    is_active = Column(Boolean)
    inactive_reason = Column(Unicode(100))

    ux_idx = UniqueConstraint(name, org_id)

class UserLog(ControllerBase):
    __tablename__ = 'User_log'
    __table_args__ = {'schema': 'Masters'}

    logid = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    User_Id = Column(BigInteger, nullable=False)
    login_timestamp = Column(DateTime)
    details = Column(Unicode)

class UserType(ControllerBase):
    __tablename__ = 'user_type'
    __table_args__ = {'schema': 'Masters'}

    id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    usertype_name = Column(Unicode(400), nullable=False)
    is_deleted = Column(Boolean)

    def __str__(self) -> str:
        return self.usertype_name

class NewsLetterSubscribers(ControllerBase):
    __tablename__ = 'NewsLetter_Subscribers'
    __table_args__ = {'schema': 'Masters'}

    Subscriber_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Name = Column(Unicode(20))
    Organization_Name = Column(Unicode(20))
    Email_Address = Column(String(100, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Contact_Number = Column(Unicode(50))
    Is_Active = Column(Boolean, nullable=False)
    Created_By = Column(BigInteger, nullable=False)
    Created_Date = Column(DateTime, nullable=False)

    def __str__(self) -> str:
        return self.Name
    
class Favorites(ControllerBase):
    __tablename__ = 'Favorites'
    __table_args__ = {'schema': 'Masters'}

    Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Created_Date = Column(DateTime, nullable=False)
    Created_By = Column(BigInteger, nullable=False)
    Display_Name = Column(Unicode(100), nullable=False)
    Table_Name = Column(Unicode(30), nullable=False)
    Table_Row_Id = Column(Unicode(20), nullable=False)
    Table_Row_Code = Column(Unicode(20))
    Table_Obj = Column(JSON)

    def __str__(self) -> str:
        return self.Table_Name
    
class ReportIssue(ControllerBase):
    __tablename__ = 'ReportIssue'
    __table_args__ = {'schema': 'Masters'}

    Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Created_Date = Column(DateTime, nullable=False)
    Created_By = Column(BigInteger, nullable=False)
    Updated_Date = Column(DateTime, nullable=False)
    Updated_By = Column(BigInteger, nullable=False)
    Report_Title = Column(Unicode(100), nullable=False)
    Report_Description = Column(Unicode(500), nullable=False)
    Report_Attachment = Column(Unicode(100))
    Issue_Type = Column(Unicode(50))
    Is_Active = Column(Boolean, default=True)

    def __str__(self) -> str:
        return self.Table_Name
    
class RecentlyViewed(ControllerBase):
    __tablename__ = 'RecentlyViewed'
    __table_args__ = {'schema': 'Masters'}

    Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Created_Date = Column(DateTime, nullable=False)
    Created_By = Column(BigInteger, nullable=False)
    Display_Name = Column(Unicode(100), nullable=False)
    Table_Name = Column(Unicode(30), nullable=False)
    Table_Row_Id = Column(Unicode(20), nullable=False)
    Table_Row_Code = Column(Unicode(20))
    Table_Obj = Column(JSON)

    def __str__(self) -> str:
        return self.Table_Name
    
class UserInterestedAMC(ControllerBase):
    __tablename__ = 'UserInterestedAMC'
    __table_args__ = {'schema': 'Masters'}

    Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Created_Date = Column(DateTime, nullable=False)
    Created_By = Column(BigInteger, nullable=False)
    AMC_Id = Column(BigInteger, nullable=False)
    AMC_Name = Column(Unicode(30), nullable=False)
    Plan_Id = Column(BigInteger, nullable=False)
    Plan_Name = Column(BigInteger, nullable=False)

    def __str__(self) -> str:
        return self.Table_Name

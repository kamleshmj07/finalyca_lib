from sqlalchemy import BigInteger, Boolean, Column, DateTime, Identity, SmallInteger, String, Unicode
from sqlalchemy.dialects.mssql import TINYINT, XML
from sqlalchemy.orm import declarative_base

ServiceManagerBase = declarative_base()

class LogManager(ServiceManagerBase):
    __tablename__ = 'LogManager'
    __table_args__ = {'schema': 'Common'}

    Log_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Log_Time = Column(DateTime, nullable=False)
    Service_Id = Column(TINYINT, nullable=False)
    Module_Id = Column(TINYINT, nullable=False)
    Job_Id = Column(BigInteger, nullable=False)
    Status = Column(TINYINT, nullable=False)
    Status_Message = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    Start_Time = Column(DateTime)
    End_Time = Column(DateTime)

class DeliveryManager(ServiceManagerBase):
    __tablename__ = 'DeliveryManager'
    __table_args__ = {'schema': 'Delivery'}

    Channel_Id = Column(TINYINT, Identity(start=1, increment=1), primary_key=True)
    Channel_Name = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Channel_Description = Column(String(200, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Assembly_Name = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Assembly_Class = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Assembly_Method = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Frequency_In_Seconds = Column(SmallInteger, nullable=False)
    Enabled = Column(Boolean, nullable=False)
    Log_Mode = Column(TINYINT, nullable=False)
    Last_Run = Column(DateTime, nullable=False)
    Parameters = Column(XML)
    Status = Column(TINYINT)
    Status_Message = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))

class DeliveryRequest(ServiceManagerBase):
    __tablename__ = 'DeliveryRequest'
    __table_args__ = {'schema': 'Delivery'}

    Request_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Channel_Id = Column(TINYINT, nullable=False)
    Type = Column(String(10, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Recipients = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Body = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Request_Time = Column(DateTime, nullable=False)
    Template_Id = Column(SmallInteger)
    RecipientsCC = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    RecipientsBCC = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    Subject = Column(String(1000, 'SQL_Latin1_General_CP1_CI_AS'))
    IsBodyHTML = Column(Boolean)
    Attachments = Column(XML)
    Parameters = Column(XML)
    Resources = Column(XML)
    Pick_Time = Column(DateTime)
    Completion_Time = Column(DateTime)
    Status = Column(TINYINT)
    Status_Message = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    Response = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    Priority = Column(TINYINT)
    Pickup_Schedule = Column(DateTime)
    Created_By = Column(TINYINT)
    X_Token = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    Is_Deleted = Column(Boolean)

class DeliveryTemplates(ServiceManagerBase):
    __tablename__ = 'DeliveryTemplates'
    __table_args__ = {'schema': 'Delivery'}

    Template_Id = Column(SmallInteger, Identity(start=1, increment=1), primary_key=True)
    Enabled = Column(Boolean, nullable=False)
    Subject = Column(String(1000, 'SQL_Latin1_General_CP1_CI_AS'))
    Subject_Override = Column(Boolean)
    Body = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    Body_Override_Mode = Column(TINYINT)
    Template_File = Column(String(100, 'SQL_Latin1_General_CP1_CI_AS'))
    SQLBO = Column(String(100, 'SQL_Latin1_General_CP1_CI_AS'))
    Parameters = Column(XML)
    Resources = Column(XML)

class ReportJobLogs(ServiceManagerBase):
    __tablename__ = 'ReportJobLogs'
    __table_args__ = {'schema': 'Reporting'}

    Log_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    Job_Id = Column(TINYINT, nullable=False)
    Start_Time = Column(DateTime, nullable=False)
    Status = Column(TINYINT, nullable=False)
    End_Time = Column(DateTime)
    Status_Message = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))

class ReportJobs(ServiceManagerBase):
    __tablename__ = 'ReportJobs'
    __table_args__ = {'schema': 'Reporting'}

    Job_Id = Column(TINYINT, Identity(start=1, increment=1), primary_key=True)
    Job_Name = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Job_Description = Column(String(200, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Report_Type = Column(TINYINT, nullable=False)
    Enabled = Column(Boolean, nullable=False)
    Enabled_Python = Column(Boolean, nullable=False, server_default='0')
    Last_Run = Column(DateTime, nullable=False)
    Schedule_Id = Column(SmallInteger, nullable=False)
    Report_Object = Column(String(100, 'SQL_Latin1_General_CP1_CI_AS'))
    Parameters = Column(XML)
    Status = Column(TINYINT)
    Status_Message = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    Channel_Id = Column(TINYINT)

class ReportSchedules(ServiceManagerBase):
    __tablename__ = 'ReportSchedules'
    __table_args__ = {'schema': 'Reporting'}

    Schedule_Id = Column(TINYINT, Identity(start=1, increment=1), primary_key=True)
    Schedule_Name = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Schedule_Description = Column(String(200, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Start_Date = Column(DateTime, nullable=False)
    Type = Column(String(20, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Frequency = Column(SmallInteger, nullable=False)
    Enabled = Column(Boolean, nullable=False)
    End_Date = Column(DateTime)
    Pickup_Hours = Column(TINYINT)
    Pickup_Minutes = Column(TINYINT)

class UploadRequest(ServiceManagerBase):
    __tablename__ = 'UploadRequest'
    __table_args__ = {'schema': 'Upload'}

    UploadRequest_Id = Column(BigInteger, Identity(start=1, increment=1), primary_key=True)
    UploadTemplates_Id = Column(TINYINT, nullable=False)
    File_Name = Column(String(1000, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Request_Time = Column(DateTime, nullable=False)
    Status = Column(TINYINT, nullable=False)
    Pick_Time = Column(DateTime)
    Completion_Time = Column(DateTime)
    Status_Message = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))
    Priority = Column(TINYINT)
    Pickup_Schedule = Column(DateTime)
    Is_Deleted = Column(Boolean)
    File_Url = Column(Unicode(1000))
    Created_By = Column(BigInteger)

class UploadTemplates(ServiceManagerBase):
    __tablename__ = 'UploadTemplates'
    __table_args__ = {'schema': 'Upload'}

    UploadTemplates_Id = Column(TINYINT, Identity(start=1, increment=1), primary_key=True)
    UploadTemplates_Name = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Template_Description = Column(String(200, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Parameters = Column(Unicode)
    Status = Column(TINYINT)
    Is_Deleted = Column(Boolean)
    Enabled_Python = Column(Boolean, nullable=False, server_default='0')

class ServiceManager(ServiceManagerBase):
    __tablename__ = 'ServiceManager'
    __table_args__ = {'schema': 'Services'}

    Service_Id = Column(TINYINT, Identity(start=1, increment=1), primary_key=True)
    Service_Name = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Service_Description = Column(String(200, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Assembly_Name = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Assembly_Class = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Assembly_Method = Column(String(50, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False)
    Frequency_In_Seconds = Column(SmallInteger, nullable=False)
    Enabled = Column(Boolean, nullable=False)
    Log_Mode = Column(TINYINT, nullable=False)
    Last_Run = Column(DateTime, nullable=False)
    Parameters = Column(XML)
    Status = Column(TINYINT)
    Status_Message = Column(String(collation='SQL_Latin1_General_CP1_CI_AS'))

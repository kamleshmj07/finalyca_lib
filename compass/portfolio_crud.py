import datetime
from werkzeug.exceptions import Unauthorized
from sqlalchemy import func, or_
from fin_models.controller_transaction_models import Investor, InvestorAccount, InvestorHoldings, InvestorTransactions

from fin_models.controller_master_models import User, Organization

def get_all_investors(db_session, user_id):
    investors = list()
    q = db_session.query(Investor).filter(Investor.is_deleted != 1)
    if user_id:
        q = q.filter(Investor.created_by == user_id)
    sql_objs = q.all()

    for sql_obj in sql_objs:
        investor = dict()
        investor["id"] = sql_obj.id
        investor["name"] = sql_obj.name
        investor["label"] = sql_obj.label
        investor["pan_no"] = sql_obj.pan_no
        investors.append(investor)
    return investors

def get_investor_dashboard_info(db_session, investor_id, user_id):
    sql_investor = db_session.query(Investor).filter(Investor.id == investor_id).one_or_none()

    if sql_investor.created_by != user_id:
        raise Unauthorized(description=F"User does not have access to Investor Id: {investor_id}")

    investor = dict()
    investor["name"] = sql_investor.name
    investor["label"] = sql_investor.label
    investor["pan_no"] = sql_investor.pan_no

    # accounts
    sql_accounts = db_session.query(InvestorAccount).filter(InvestorAccount.investor_id==investor_id).filter(InvestorAccount.is_deleted != 1).all()
    accounts = list()
    for sql_acc in sql_accounts:
        obj = dict()
        obj["account_id"] = sql_acc.id        
        obj["owners"] = sql_acc.owners
        obj["account_type"] = sql_acc.account_type
        obj["depository"] = sql_acc.depository
        obj["dp_name"] = sql_acc.dp_name
        obj["account_no"] = sql_acc.account_no
        obj["label"] = sql_acc.label
        obj["mapped_fund_code"] = sql_acc.mapped_fund_code
        obj["is_dummy"] = sql_acc.is_dummy
        accounts.append(obj)

    # statement dates
    sql_statements = db_session.query(func.distinct(InvestorHoldings.as_of_date)).join(InvestorAccount, InvestorAccount.id==InvestorHoldings.account_id).filter(InvestorAccount.investor_id==investor_id).filter(InvestorHoldings.is_deleted != 1).all()
    dates = list()
    for sql_date in sql_statements:
        dates.append(sql_date[0])

    # Information about the user who has setup the investor
    sql_user = db_session.query(User.User_Id, User.Display_Name, User.Email_Address, User.Contact_Number, User.Designation, User.City, User.State, Organization.Organization_Id, Organization.Organization_Name, Organization.Logo_Img).join(Organization, User.Organization_Id==Organization.Organization_Id).filter(User.User_Id==user_id).one_or_none()
    advisor = dict()
    advisor["id"] = sql_user.User_Id
    advisor["name"] = sql_user.Display_Name
    advisor["email"] = sql_user.Email_Address
    advisor["phone"] = sql_user.Contact_Number
    advisor["designation"] = sql_user.Designation
    advisor["city"] = sql_user.City
    advisor["state"] = sql_user.State
    advisor["org_id"] = sql_user.Organization_Id
    advisor["org_name"] = sql_user.Organization_Name
    advisor["org_logo"] = sql_user.Logo_Img

    investor["accounts"] = accounts
    investor["statement_dates"] = dates
    investor["advisor"] = advisor

    return investor

def find_or_create_investor(db_session, name, pan_no, label, user_id):    
    investor = db_session.query(Investor).filter(Investor.is_deleted != 1).filter(Investor.pan_no==pan_no).filter(Investor.created_by == user_id).one_or_none()
    
    if not investor:
        investor = Investor()
        investor.name = name
        investor.label = label
        investor.pan_no = pan_no
        investor.created_by = user_id
        investor.created_date = datetime.datetime.now()
        db_session.add(investor)
        db_session.commit()
    
    return investor.id

def find_or_create_investor_account(db_session, investor_id, owners, account_type, depository, dp_name, client_id, label, linked_pms_code, user_id):
    sql_demat = db_session.query(InvestorAccount).filter(InvestorAccount.is_deleted != 1).filter(InvestorAccount.account_type == account_type).filter(InvestorAccount.depository==depository).filter(InvestorAccount.dp_name==dp_name).filter(InvestorAccount.account_no==client_id).filter(InvestorAccount.created_by == user_id).one_or_none()
    if not sql_demat:
        sql_demat = InvestorAccount()
        sql_demat.investor_id = investor_id
        sql_demat.owners = owners
        sql_demat.account_type = account_type if account_type != "None" else None
        sql_demat.depository = depository
        sql_demat.dp_name = dp_name
        sql_demat.account_no = client_id
        sql_demat.label = label
        sql_demat.mapped_fund_code = linked_pms_code
        sql_demat.created_by = user_id
        sql_demat.created_date = datetime.datetime.now()
        db_session.add(sql_demat)
        db_session.commit()
    
    return sql_demat.id

def find_or_create_dummy_investor_account(db_session, investor_id, owners, label, linked_pms_code, user_id):
    sql_demat = db_session.query(InvestorAccount).filter(
        InvestorAccount.is_deleted != 1,
        InvestorAccount.investor_id == investor_id,
        or_(InvestorAccount.account_type == None, InvestorAccount.account_type =='excel_load'),
        InvestorAccount.depository==None,
        InvestorAccount.dp_name==None,
        InvestorAccount.account_no==None,
        InvestorAccount.is_dummy==1,
        InvestorAccount.created_by == user_id).one_or_none()
        
    if not sql_demat:
        sql_demat = InvestorAccount()
        sql_demat.investor_id = investor_id
        sql_demat.owners = owners
        sql_demat.account_type = 'excel_load'
        sql_demat.depository = None
        sql_demat.dp_name = None
        sql_demat.account_no = None
        sql_demat.label = label
        sql_demat.mapped_fund_code = linked_pms_code
        sql_demat.created_by = user_id
        sql_demat.created_date = datetime.datetime.now()
        sql_demat.is_dummy = 1
        db_session.add(sql_demat)
        db_session.commit()

    return sql_demat.id

def find_or_create_investor_holding(db_session, account_id, as_on_date, isin, name, type, coupon_rate, maturity_date, units, unit_price, total_price, user_id):
    sql_holding = db_session.query(InvestorHoldings).filter(InvestorHoldings.is_deleted != 1).filter(InvestorHoldings.account_id == account_id).filter(InvestorHoldings.as_of_date == as_on_date).filter(InvestorHoldings.isin == isin).filter(InvestorHoldings.type == type).filter(InvestorHoldings.name == name).filter(InvestorHoldings.created_by == user_id).one_or_none()
    if not sql_holding:
        sql_holding = InvestorHoldings()
        sql_holding.account_id = account_id
        sql_holding.as_of_date = as_on_date
        sql_holding.isin = isin
        sql_holding.name = name
        sql_holding.type = type
        sql_holding.coupon_rate = coupon_rate
        sql_holding.maturity_date = maturity_date
        sql_holding.units = units
        sql_holding.unit_price = unit_price
        sql_holding.total_price = total_price
        sql_holding.created_by = user_id
        sql_holding.created_date = datetime.datetime.now()
        db_session.add(sql_holding)
        db_session.commit()
    
    return sql_holding.id

def update_investor_account(db_session, investor_account_id, investor_account_data, user_id):
    sql_investor_account = db_session.query(InvestorAccount).filter(InvestorAccount.is_deleted != 1).filter(InvestorAccount.id == investor_account_id).one_or_none()

    if not sql_investor_account:
        return None

    if sql_investor_account:
        sql_investor_account.label = investor_account_data["label"]
        sql_investor_account.mapped_fund_code = investor_account_data["mapped_fund_code"]
        db_session.commit()
        sql_investor_account.updated_by = user_id
        sql_investor_account.updated_date = datetime.datetime.now()
        return sql_investor_account.id
    
def delete_investor(db_session, investor_id, user_id):
    """
    - first we will get investor accounts associated with investor id 
    - then we will delete each holdings associated with investor account, then  delete investor account
    - At last delete investor
    """

    sql_investor_accounts = db_session.query(InvestorAccount).filter(InvestorAccount.is_deleted != 1).filter(InvestorAccount.investor_id == investor_id).filter(InvestorAccount.created_by == user_id).all()

    if sql_investor_accounts:
        for sql_investor_account in sql_investor_accounts:
            db_session.query(InvestorHoldings).filter(InvestorHoldings.is_deleted != 1).filter(InvestorHoldings.account_id == sql_investor_account.id).filter(InvestorHoldings.created_by == user_id).delete()

            db_session.query(InvestorAccount).filter(InvestorAccount.id == sql_investor_account.id).filter(InvestorAccount.created_by == user_id).delete()

    db_session.query(Investor).filter(Investor.id == investor_id).filter(Investor.created_by == user_id).delete()

    return investor_id

def update_investor(db_session, investor_id, user_id, name, label):
    """
    - following will update the investor details
    """

    sql_investor = db_session.query(Investor).filter(Investor.id == investor_id).filter(Investor.created_by == user_id).one_or_none()

    if not sql_investor:
        return False
    
    if sql_investor:
        sql_investor.name = name
        sql_investor.label = label

        db_session.commit()

        return sql_investor.id

        
        
def find_or_create_investor_transactions(db_session, account_id, tran_date, isin, name, type, tran_type, units, unit_price, total_price, user_id):
    sql_trans = db_session.query(InvestorTransactions)\
                            .filter(InvestorTransactions.is_deleted != 1)\
                            .filter(InvestorTransactions.account_id == account_id)\
                            .filter(InvestorTransactions.tran_date == tran_date)\
                            .filter(InvestorTransactions.isin == isin)\
                            .filter(InvestorTransactions.tran_type == tran_type)\
                            .filter(InvestorTransactions.total_price == total_price)\
                            .filter(InvestorTransactions.units == units)\
                            .filter(InvestorTransactions.unit_price == unit_price)\
                            .filter(InvestorTransactions.created_by == user_id).one_or_none()
    if not sql_trans:
        sql_trans = InvestorTransactions()
        sql_trans.account_id = account_id
        sql_trans.tran_date = tran_date
        sql_trans.isin = isin
        sql_trans.name = name
        sql_trans.type = type
        sql_trans.tran_type = tran_type
        sql_trans.units = units
        sql_trans.unit_price = unit_price
        sql_trans.total_price = total_price
        sql_trans.created_by = user_id
        sql_trans.created_date = datetime.datetime.now()
        sql_trans.is_deleted = 0
        sql_trans.is_valid_tran = 0
        sql_trans.status = ''
        db_session.add(sql_trans)
        db_session.commit()
    
    return sql_trans.id

def get_investor_transactions(db_session, account_ids):
    resp = list()
    sql_transactions = db_session.query(InvestorTransactions)\
                             .filter(InvestorTransactions.is_deleted != 1,
                                     InvestorTransactions.account_id.in_(account_ids)).all()
    
    for transaction in sql_transactions:
        data = dict()
        data['id'] = transaction.id
        data['account_id'] = transaction.account_id
        data['tran_date'] = transaction.tran_date
        data['isin'] = transaction.isin
        data['name'] = transaction.name
        data['type'] = transaction.type
        data['tran_type'] = transaction.tran_type
        data['units'] = transaction.units
        data['unit_price'] = transaction.unit_price
        data['total_price'] = transaction.total_price
        data['stamp_duty'] = transaction.stamp_duty
        data['is_valid_tran'] = transaction.is_valid_tran
        data['status'] = transaction.status
        resp.append(data)
    
    return resp

def validate_investor_transactions(db_session, df):
    if not df.empty:
        df['is_valid_tran'] = 1 #mark all transactions as valid transaction
        df['status'] = ''

        df.loc[df['tran_type'] == 'S', 'units'] = -df['units']

        #Raise exception if sell unit is greater than purchased
        tran_df1 = df.groupby(
            ['isin'], dropna=False, as_index=False
            ).agg(
            **{
                'isin' : ("isin", "first"),
                'units' : ("units", "sum")
            }
        )

        df.loc[df['isin'].isin(
                            tran_df1.loc[tran_df1['units'] < 0]['isin']), 
                            'is_valid_tran'] = 0
        
        df.loc[df['isin'].isin(
                            tran_df1.loc[tran_df1['units'] < 0]['isin']), 
                            'status'] = 'Sell units are greater than buy units.'
        
        #Update status and is_valid_tran against transaction
        for index, row in df.iterrows():
            update_values = {
                InvestorTransactions.is_valid_tran : row['is_valid_tran'],
                InvestorTransactions.status : row['status'],
            }
            sql_trans = db_session.query(InvestorTransactions)\
                            .filter(InvestorTransactions.id == row['id']).update(update_values)
            
            db_session.commit()        

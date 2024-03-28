import os
from utils import get_config
from utils.finalyca_store import get_finalyca_scoped_session, is_production_config
from datetime import timedelta
from datetime import datetime as dt
from fin_models.masters_models import Plans, MFSecurity
from fin_models.transaction_models import NAV, PlanProductMapping
from sqlalchemy import func, and_
import pandas as pd
from utils.finalyca_store import *
from bizlogic.importer_helper import save_nav

def mf_ulip_fill_missing_nav(db_session):
    fill_missing_nav(db_session, [1,2], None)
    
def fill_missing_nav(db_session, product_id=[], plan_id=None):
    if not db_session:
        config_file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../config/local.config.yaml"
        )
        config = get_config(config_file_path)

        db_session = get_finalyca_scoped_session(is_production_config(config))

    TODAY = dt.today()

    sql_aum_query = db_session.query(Plans.Plan_Name, 
                                    Plans.Plan_Id, 
                                    func.lag(NAV.NAV_Date).over(partition_by=(NAV.Plan_Id), order_by=NAV.NAV_Date).label('prev_date'),
                                    func.lag(NAV.NAV).over(partition_by=(NAV.Plan_Id), order_by=NAV.NAV_Date).label('prev_nav'),
                                    NAV.NAV_Date, 
                                    NAV.NAV, 
                                    func.datediff(text('Day'), func.lag(NAV.NAV_Date).over(partition_by=(NAV.Plan_Id), order_by=NAV.NAV_Date), NAV.NAV_Date).label('diff_in_days'))\
                                    .select_from(Fund)\
                                    .join(MFSecurity, MFSecurity.Fund_Id == Fund.Fund_Id)\
                                    .join(Plans, Plans.MF_Security_Id == MFSecurity.MF_Security_Id)\
                                    .join(PlanProductMapping, Plans.Plan_Id == PlanProductMapping.Plan_Id)\
                                    .join(Product, PlanProductMapping.Product_Id == Product.Product_Id)\
                                    .join(NAV, and_(NAV.Plan_Id == Plans.Plan_Id, NAV.NAV_Type == 'P'))\
                                    .filter(PlanProductMapping.Is_Deleted != 1, Fund.Is_Deleted != 1, Plans.Is_Deleted != 1, MFSecurity.Is_Deleted != 1, MFSecurity.Status_Id == 1)

    if product_id:
        sql_aum_query = sql_aum_query.filter(Product.Product_Id.in_(product_id))
    if plan_id:
        sql_aum_query = sql_aum_query.filter(Plans.Plan_Id == plan_id)

    df_nav_data = pd.DataFrame(sql_aum_query)

    df_data = df_nav_data.loc[df_nav_data['diff_in_days'] > 1]

    df_data = df_data.reset_index(drop=True)

    for row in df_data.iterrows():    
        start_date = row[1]['prev_date']
        start_date = start_date + timedelta(days=1)

        end_date = row[1]['NAV_Date']
        end_date = end_date - timedelta(days=1)

        fill_gap_between_dates(db_session, start_date, end_date, row[1]['prev_nav'], row[1]['Plan_Id'])
        

def fill_gap_between_dates(db_session, start_date, end_date, nav, plan_id):
    TODAY = dt.today()
    
    while start_date <= end_date:        
        save_nav(db_session, plan_id, 'P', start_date, nav, 1, TODAY)        
        start_date = start_date + timedelta(days=1)


# if __name__ == '__main__':
#     fill_missing_nav(None, [], 7)
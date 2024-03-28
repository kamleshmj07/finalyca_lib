from fin_models.controller_master_models import ReportIssue, UserInterestedAMC
from fin_models.masters_models import AMC, Plans
from datetime import datetime
from bizlogic.common_helper import get_user_details
from fin_resource.flask_api import save_image_file
from jinja2 import Environment, FileSystemLoader
from async_tasks.send_email import send_email_async

def get_reported_issues(db_session, config):
    sql_report_issue = db_session.query(ReportIssue).all()

    report_issue_list = list()
    for report_issue in sql_report_issue:
        report_issue_obj = dict()
        report_issue_obj["title"] = report_issue.Report_Title
        report_issue_obj["description"] = report_issue.Report_Description
        report_issue_obj["created_date"] = report_issue.Created_Date
        report_issue_obj["issue_type"] = report_issue.Issue_Type
        report_issue_obj["is_active"] = True if report_issue.Is_Active else False

        if report_issue.Report_Attachment:
            report_issue["report_attachment"] = F"{config['IMAGE_PATH']}/{report_issue.Report_Attachment}"

        user_obj = get_user_details(db_session, report_issue.Created_By)
        if user_obj:
            report_issue_obj["user"] =  user_obj.Display_Name

        report_issue_list.append(report_issue_obj)

    return report_issue_list

def save_report_issue(db_session, config, data, file, user_info):
    sql_report_issue = ReportIssue()

    sql_report_issue.Created_By = user_info["id"]
    sql_report_issue.Created_Date = datetime.now()
    sql_report_issue.Updated_By = user_info["id"]
    sql_report_issue.Updated_Date = datetime.now()
    sql_report_issue.Report_Title = data["title"]
    sql_report_issue.Report_Description = data["description"]
    sql_report_issue.Is_Active = True
    
    if file.get('report_issue'):
        file_obj = file['report_issue']
        sql_report_issue.Report_Attachment = save_image_file(config['DOC_ROOT_PATH'], config['IMAGES_DIR'], file_obj)

    db_session.add(sql_report_issue)
    db_session.commit()

    # send an email to the dev team
    environment = Environment(loader=FileSystemLoader('./src/templates'), keep_trailing_newline=False, trim_blocks=True, lstrip_blocks=True)

    template = environment.get_template("report_issue.html")
    html_msg = template.render(title=data["title"], description=data["description"])
    
    send_email_async(config["EMAIL_SERVER"], config["EMAIL_PORT"], config["EMAIL_ID"], config["EMAIL_PASS"], "devteam@finalyca.com", "BUG / ISSUE", html_msg)

    return sql_report_issue.Id

def user_is_interested_in_amc_fund(db_session, config, user_info, amc_id, plan_id):
    try:
        # get amc data from amc id, get scheme data from plan_id
        sql_amc_data = db_session.query(AMC).filter(AMC.AMC_Id == amc_id).one_or_none()
        if not sql_amc_data:
            raise Exception(F'AMC not found for amc id {amc_id}')
        
        sql_plan_data = db_session.query(Plans).filter(Plans.Plan_Id == plan_id).one_or_none()
        if not sql_plan_data:
            raise Exception(F'Plan not found for plan id {plan_id}')

        # destructuring amc, fund and user name
        amc_name = sql_amc_data.AMC_Name
        plan_name = sql_plan_data.Plan_Name
        user_name = user_info["user_name"]
        user_email_id = user_info["email"]

        # send an email to respective associated person of amc if present
        if sql_amc_data.Email_Id:
            environment = Environment(loader=FileSystemLoader('./src/templates'), keep_trailing_newline=False, trim_blocks=True, lstrip_blocks=True)
            template = environment.get_template("user_interested_amc.html")
            html_msg = template.render(user_name=user_name, amc_name=amc_name, plan_name=plan_name, user_email_id=user_email_id)
            
            send_email_async(config["EMAIL_SERVER"], config["EMAIL_PORT"], config["EMAIL_ID"], config["EMAIL_PASS"], sql_amc_data.Email_Id, F"Investor Interest in {plan_name} via Finalyca Technologies", html_msg)

        # make an entry to log table, that user is interested
        sql_user_interested_amc = UserInterestedAMC()
        sql_user_interested_amc.Created_By = user_info["id"]
        sql_user_interested_amc.Created_Date = datetime.now()
        sql_user_interested_amc.AMC_Id = amc_id
        sql_user_interested_amc.AMC_Name = amc_name
        sql_user_interested_amc.Plan_Id = plan_id
        sql_user_interested_amc.Plan_Name = plan_name
        db_session.add(sql_user_interested_amc)
        db_session.commit()

        return sql_user_interested_amc.Id
    except Exception as exe:
        print(str(exe))

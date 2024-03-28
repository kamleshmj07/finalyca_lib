import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import logging
import threading
import os
from typing import List 

def send_email_async(smtp_server, smtp_tls_port, sender_email, sender_pswd, to_list, subject, html_message):
    logging.info(F"sending email to {to_list}")
    t = threading.Thread(target=send_email, args=(smtp_server, smtp_tls_port, sender_email, sender_pswd, to_list, subject, html_message))
    t.start()

def send_email(smtp_server, smtp_tls_port, sender_email, sender_pswd, to_list, subject, html_message):
    try:
        # create message
        message = MIMEMultipart("alternative")
        message["From"] = sender_email
        message["To"] = to_list
        message["Subject"] = subject
        
        h = MIMEText(html_message, "html")
        message.attach(h)

        server = smtplib.SMTP(smtp_server, smtp_tls_port)

        context = ssl.create_default_context()
        # Secure the connection 
        server.starttls(context=context)        
        server.login(sender_email, sender_pswd)
        server.sendmail(sender_email, to_list, message.as_string())
        server.quit()
        logging.info(F"Email Subject {subject} sent from {sender_email} to {to_list}")
    except Exception as e:
        logging.exception(e)

# 
def send_email_with_attachements(smtp_server, smtp_tls_port, sender_email, sender_pswd, to_list: List[str], subject, html_message, attachment_paths):
    try:
        # create message
        message = MIMEMultipart("alternative")
        message["From"] = sender_email
        message["To"] = ', '.join(to_list)
        message["Subject"] = subject
        
        h = MIMEText(html_message, "html")
        message.attach(h)

        for attachment in attachment_paths:
            file_name = os.path.basename(attachment)
            doc = MIMEApplication(open(attachment, 'rb').read())
            doc.add_header('Content-Disposition', 'attachment', filename= file_name)
            message.attach(doc)

        server = smtplib.SMTP(smtp_server, smtp_tls_port)

        context = ssl.create_default_context()
        # Secure the connection 
        server.starttls(context=context)        
        server.login(sender_email, sender_pswd)
        server.sendmail(sender_email, to_list, message.as_string())
        server.quit()
        logging.info(F"Email Subject {subject} sent from {sender_email} to {to_list}")
    except Exception as e:
        logging.exception(e)

def send_email_complete(smtp_server, smtp_tls_port, sender_email, sender_pswd, to_list, cc_list, bcc_list, subject, html_message, attachment_paths):
    '''
    to_list: comma separated email ids
    cc_list: comma separated email ids
    bcc_list: comma separated email ids
    '''
    try:
        # create message
        message = MIMEMultipart("alternative")
        message["From"] = sender_email
        message["To"] = to_list
        message["Cc"] = cc_list
        message["Bcc"] = bcc_list
        message["Subject"] = subject
        
        h = MIMEText(html_message, "html")
        message.attach(h)

        for attachment in attachment_paths:
            file_name = os.path.basename(attachment)
            doc = MIMEApplication(open(attachment, 'rb').read())
            doc.add_header('Content-Disposition', 'attachment', filename= file_name)
            message.attach(doc)

        server = smtplib.SMTP(smtp_server, smtp_tls_port)

        context = ssl.create_default_context()
        # Secure the connection 
        server.starttls(context=context)        
        server.login(sender_email, sender_pswd)
        # server.sendmail(sender_email, to_list, message.as_string())
        server.send_message(message)
        server.quit()
        logging.info(F"Email Subject {subject} sent from {sender_email} to {to_list}")
    except Exception as e:
        logging.exception(e)

if __name__ == '__main__':
    to_list = 'vijay.shah.1987@gmail.com,vijay.shah@finalyca.com'
    cc_list = 'ibrahim.saifuddin@finalyca.com,sachin.jaiswal@finalyca.com'
    bcc_list = ''
    subject = 'Email send API check'
    html_message = """Checking if this is working"""
    sender_email = "no-reply@finalyca.com"
    sender_pswd = "Pms@1234"
    smtp_server = "smtp.gmail.com"
    smtp_tls_port= 587
    EMAIL_SECURITY= "TLS"
    
    send_email_complete(smtp_server, smtp_tls_port, sender_email, sender_pswd, to_list, cc_list, bcc_list, subject, html_message, [])
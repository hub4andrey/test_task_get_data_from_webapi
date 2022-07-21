# Native Python libs:
import logging
import smtplib, ssl
from email.message import EmailMessage
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# 3rd party Python libs:


# own modules and libs:
from utils.init_params import opts

# Get root (__main__) logger:
logger = logging.getLogger(f"__main__.report.{__name__}")


def send(to=None, subject='report', body=None, html=None):  
    # https://fedingo.com/how-to-send-html-mail-with-attachment-using-python/
    # create email

    # msg = EmailMessage()
    # msg.set_content("This is eamil message")
    msg = MIMEMultipart("alternative")
    msg['Subject'] = subject
    msg['From'] = opts.smtp_user_name
    msg['To'] = to
        
    part = MIMEText(str(html), "html")
    msg.attach(part)
    
    # send email
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(opts.smtp_host, 465, context=context) as smtp:
            # smtp.set_debuglevel(1)
            logger.debug(msg.as_string())
            if opts.dryrun:
                logger.debug('e-mail has been composed succesffully. Do nothing in dryrun mode')
            else:
                logger.debug('e-mail has been composed succesffully. Sending it out')
                smtp.login(opts.smtp_user_name, opts.smtp_user_pass)
                smtp.send_message(msg)
    except Exception as e:
        logger.error(e)

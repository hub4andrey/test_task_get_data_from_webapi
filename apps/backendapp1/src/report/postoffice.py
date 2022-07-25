# Native Python libs:
import logging
import smtplib, ssl
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# 3rd party Python libs:


# own modules and libs:
from utils.init_params import opts

# Get root (__main__) logger:
logger = logging.getLogger(f"__main__.report.{__name__}")


class PostOffice:
    def __init__(self, email_to: list =None, email_subject: str ='report', body_text: str =None, body_html =None) -> None:
        self._from = opts.smtp_user_name
        self.to = email_to
        self.subject = email_subject
        self.body_text = body_text
        self.body_html = body_html
        self.msg = None
    
    def _build_email(self):
        """create email as MIMEMultipart with HTML in MIMEText"""
        # https://fedingo.com/how-to-send-html-mail-with-attachment-using-python/
        
        # Plain text message:
        # self.msg = EmailMessage()
        # self.msg.set_content("This is eamil message")
        
        # HTML message:        
        self.msg = MIMEMultipart("alternative")
        self.msg['Subject'] = self.subject
        self.msg['From'] = opts.smtp_user_name
        self.msg['To'] = self.to
        self.msg.attach(MIMEText(str(self.body_html), "html"))


    def send_email(self):
        # try:
        self._build_email()
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(opts.smtp_host, 465, context=context) as smtp_ssl:
            # smtp.set_debuglevel(1)
            logger.debug(self.msg.as_string())
            if opts.dryrun:
                logger.debug('e-mail has been composed succesffully. Do nothing in dryrun mode')
                result = "dummy email"
            else:
                logger.debug('e-mail has been composed succesffully. Sending it out')
                smtp_ssl.login(opts.smtp_user_name, opts.smtp_user_pass)
                result = smtp_ssl.send_message(self.msg)

        return result

        # except Exception as e:
        #     print(e)

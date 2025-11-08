import smtplib
import configparser
import logging
import os
from dotenv import load_dotenv
from email.message import EmailMessage

load_dotenv()

try:
    config = configparser.ConfigParser()
    config.read('conf/email_config.ini')
    SMTP_SERVER = config['SMTP']['SMTP_SERVER']
    SMTP_PORT = int(config['SMTP']['SMTP_PORT'])
    SMTP_USER = config['SMTP']['SMTP_USER']
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
    IS_EMAIL_CONFIGURED = True
except Exception as e:
    logging.warning(f"Email configuration incomplete or not valid. Disabled.")
    IS_EMAIL_CONFIGURED = False


def send_email_notification(recipient_email, subject, body):
    if not IS_EMAIL_CONFIGURED or not SMTP_USER or not SMTP_PASSWORD:
        logging.error("Email configuration is missing or incomplete. Cannot send email.")
        return False

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = SMTP_USER
    msg['To'] = recipient_email
    msg.set_content(body)

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
            logging.info(f"Email sent to {recipient_email}")
            return True
    except smtplib.SMTPException as e:
        logging.error(f"SMTP error while sending to {recipient_email}: {e}")
        return False
    except Exception as e:
        logging.error(f"Error while sending email: {e}")
        return False
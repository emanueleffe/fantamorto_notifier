import requests
import configparser
import logging

config = configparser.ConfigParser()

try:
    config.read('conf/telegram_config.ini')
    GLOBAL_TG_BOT_TOKEN = config['TELEGRAM']['tg_bot_token']
    GLOBAL_TG_CHAT_ID = config['TELEGRAM']['tg_chat_id']
except Exception as e:
    logging.warning(f"Error while reading telegram config: {e}. Admin notifications disabled.")
    GLOBAL_TG_BOT_TOKEN = None
    GLOBAL_TG_CHAT_ID = None


def _send_message(chat_id, message):
    if not GLOBAL_TG_BOT_TOKEN:
        logging.error("Token not set for Telegram bot. Cannot send notifications.")
        return False
        
    if not chat_id:
        logging.error("ChatId not set. Cannot send notification.")
        return False

    url = f"https://api.telegram.org/bot{GLOBAL_TG_BOT_TOKEN}/sendMessage"
    params = {
        'chat_id': chat_id,
        'parse_mode': 'Markdown',
        'text': message,
        'disable_web_page_preview': 'true'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            logging.info(f"Notification sent to: {str(chat_id)[:4]}...")
            return True
        else:
            logging.error(f"Error while sending notification to chat_id: {str(chat_id)[:4]}...: {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Error (requestexception) while sending notification: {e}")
        return False


def send_telegram_notification(message):
    if not GLOBAL_TG_CHAT_ID:
        logging.error("ChatID not set for global notifications. Cannot send admin notifications.")
        return False
        
    full_message = "*FANTAMORTO*\n\n" + message
    return _send_message(GLOBAL_TG_CHAT_ID, full_message)


def send_specific_telegram_notification(chat_id, message):
    logging.info(f"Sending specific notification: {str(chat_id)[:4]}...")
    return _send_message(chat_id, message)

def get_global_chat_id():
    return GLOBAL_TG_CHAT_ID
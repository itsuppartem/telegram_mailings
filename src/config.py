import os
from dotenv import load_dotenv
from typing import List

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MONGODB_URI = os.getenv("MONGODB_URI")
TIMEZONE = os.getenv("TIMEZONE", "UTC")

# Database configuration
DB_NAME_MAIN = os.getenv("DB_NAME_MAIN", "mailing_db")
DB_NAME_KO = os.getenv("DB_NAME_KO", "client_bot_db")
DB_NAME_VROOM = os.getenv("DB_NAME_VROOM", "vroom_bot")

# Collection names
COLLECTION_MAILINGS = os.getenv("COLLECTION_MAILINGS", "mailings")
COLLECTION_REPORTS = os.getenv("COLLECTION_REPORTS", "reports")
COLLECTION_TOKENS = os.getenv("COLLECTION_TOKENS", "tokens")
COLLECTION_USERS_KO = os.getenv("COLLECTION_USERS_KO", "users")
COLLECTION_USERS_KO_OLD = os.getenv("COLLECTION_USERS_KO_OLD", "users_old")
COLLECTION_USERS_VROOM = os.getenv("COLLECTION_USERS_VROOM", "users")

# API URLs
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


# Admin lists
def parse_admin_list(env_var: str) -> List[int]:
    return [int(admin_id) for admin_id in os.getenv(env_var, "").split(",") if admin_id]


KO_ADMIN_LIST = parse_admin_list("KO_ADMIN_LIST")
VROOM_ADMIN_LIST = parse_admin_list("VROOM_ADMIN_LIST")

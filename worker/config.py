import os
import pytz
from typing import List, Tuple

MONGO_DETAILS = os.getenv("MONGO_DETAILS", "mongodb://user:pass@host:port")
BATCH_SIZE_PER_WORKER = 5
MAX_CONCURRENT_WORKERS_PER_MAILING = max(1, os.cpu_count() - 1)
TELEGRAM_API_URL = "https://api.telegram.org/bot"
POLL_INTERVAL_SECONDS = 5
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

BOT_TOKENS = {"ko": ["BOT_TOKEN_1", "BOT_TOKEN_2"], "vroom": ["BOT_TOKEN_3"]}

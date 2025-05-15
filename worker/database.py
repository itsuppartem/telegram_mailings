import logging
import motor.motor_asyncio
from typing import Optional

from config import MONGO_DETAILS

logger = logging.getLogger(__name__)

db_client_main_loop = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS)
mailings_collection_main_loop = db_client_main_loop.mailing_db.mailings


async def get_user_phone_worker(user_id: int, bot_name_str: str, db_client_worker) -> Optional[str]:
    user = None
    users_coll_ko = db_client_worker.client_bot_db.users
    users_old_coll_ko = db_client_worker.client_bot_db.users_old
    users_coll_vroom = db_client_worker.vroom_bot.users
    try:
        if bot_name_str == "ko":
            user = await users_coll_ko.find_one({"chat_id": user_id})
            if not user:
                user = await users_old_coll_ko.find_one({"chat_id": user_id})
        elif bot_name_str == "vroom":
            user = await users_coll_vroom.find_one({"user_id": user_id})
        return user.get("phone") if user else None
    except Exception as e:
        logger.error(f"Ошибка получения телефона для user {user_id}, bot {bot_name_str}: {e}")
        return None

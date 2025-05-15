import asyncio
import httpx
import json
import logging
import os
from asyncio_throttle import Throttler
from multiprocessing import Process
from typing import Dict, List

from config import BATCH_SIZE_PER_WORKER
from database import get_user_phone_worker
from telegram import get_bot_tokens, actual_send_message_worker

logger = logging.getLogger(__name__)


async def message_sender_worker_async_logic(mailing_doc_dict: Dict, recipient_ids_batch: List[int], worker_id: int):
    worker_db_client = None
    try:
        worker_db_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS, serverSelectionTimeoutMS=5000)
        worker_mailings_collection = worker_db_client.mailing_db.mailings
        logger.info(
            f"[Воркер {worker_id}] Запущен. Обработка {len(recipient_ids_batch)} получателей для рассылки '{mailing_doc_dict['name']}'.")

        bot_tokens = await get_bot_tokens(mailing_doc_dict["bot"])
        if not bot_tokens:
            logger.error(f"[Воркер {worker_id}] Не найдены токены для бота {mailing_doc_dict['bot']}")
            return

        processed_successfully_ids = []
        processed_failed_ids = []

        async with httpx.AsyncClient(timeout=30.0) as http_session:
            worker_throttler = Throttler(rate_limit=7, period=1)

            for chat_id in recipient_ids_batch:
                message_spec = {"chat_id": chat_id, "text": mailing_doc_dict["text"],
                    "photo": mailing_doc_dict.get("photo"), "animation": mailing_doc_dict.get("animation")}

                if mailing_doc_dict.get("promo_codes"):
                    user_phone = await get_user_phone_worker(chat_id, mailing_doc_dict["bot"], worker_db_client)
                    if user_phone and user_phone in mailing_doc_dict["promo_codes"]:
                        message_spec["promo_code"] = mailing_doc_dict["promo_codes"][user_phone]
                        logger.info(f"[Воркер {worker_id}] Добавлен промокод для пользователя {chat_id}")

                send_status = await actual_send_message_worker(http_session, worker_throttler, message_spec, bot_tokens)

                if send_status == 200:
                    processed_successfully_ids.append(chat_id)
                else:
                    processed_failed_ids.append(chat_id)
                    logger.error(
                        f"[Воркер {worker_id}] Ошибка отправки сообщения пользователю {chat_id}, статус: {send_status}")

        if processed_successfully_ids or processed_failed_ids:
            update_operation = {
                "$inc": {"sent_count": len(processed_successfully_ids), "failed_count": len(processed_failed_ids)},
                "$pullAll": {"pending_receivers_ids": processed_successfully_ids + processed_failed_ids}}
            try:
                await worker_mailings_collection.update_one({"name": mailing_doc_dict["name"]}, update_operation)
            except Exception as e:
                logger.error(
                    f"[Воркер {worker_id}] КРИТИЧНО: Не удалось обновить MongoDB для '{mailing_doc_dict['name']}': {e}")

    except Exception as e:
        logger.error(f"[Воркер {worker_id}] Ошибка при обработке рассылки: {e}")
    finally:
        if worker_db_client is not None:
            try:
                await worker_db_client.close()
            except Exception as e:
                logger.error(f"[Воркер {worker_id}] Ошибка при закрытии соединения с БД: {e}")
        logger.info(f"[Воркер {worker_id}] Завершил обработку пачки для рассылки '{mailing_doc_dict['name']}'.")


def message_sender_process_entrypoint(mailing_doc_json_str: str, recipient_ids_batch: List[int], worker_id: int):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
    mailing_doc_dict = json.loads(mailing_doc_json_str)
    try:
        asyncio.run(message_sender_worker_async_logic(mailing_doc_dict, recipient_ids_batch, worker_id))
    except Exception as e:
        logger.error(f"Необработанное исключение в процессе воркера (PID {os.getpid()}): {e}", exc_info=True)

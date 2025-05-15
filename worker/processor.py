import asyncio
import datetime
import json
import logging
from multiprocessing import Process
from typing import Dict, List, Tuple

from config import (MAX_CONCURRENT_WORKERS_PER_MAILING, BATCH_SIZE_PER_WORKER, POLL_INTERVAL_SECONDS, MOSCOW_TZ)
from database import mailings_collection_main_loop
from worker import message_sender_process_entrypoint

logger = logging.getLogger(__name__)


def is_within_time_window(time_spoon: Tuple[int, int]) -> bool:
    if not time_spoon:
        return True
    moscow_time = datetime.datetime.now(MOSCOW_TZ)
    current_hour = moscow_time.hour
    return time_spoon[0] <= current_hour < time_spoon[1]


async def estimate_messages_for_current_window(mailing_doc: Dict) -> int:
    if not is_within_time_window(mailing_doc.get("time_spoon")):
        return 0

    total_pending = len(mailing_doc.get("pending_receivers_ids", []))
    if total_pending == 0:
        return 0

    est_total_msg_per_sec = MAX_CONCURRENT_WORKERS_PER_MAILING * 5
    remaining_sec = 3600
    qty_to_send = min(total_pending, int(remaining_sec * est_total_msg_per_sec))
    return qty_to_send


active_mailing_tasks = {}


async def process_single_mailing_task(mailing_name: str, mailings_coll):
    try:
        logger.info(f"Задача для рассылки '{mailing_name}' запущена.")
        mailing_doc = await mailings_coll.find_one({"name": mailing_name})
        if not mailing_doc:
            logger.error(f"Рассылка '{mailing_name}' не найдена в БД")
            return

        start_time = datetime.datetime.now(MOSCOW_TZ)

        await mailings_coll.update_one({"name": mailing_name},
                                       {"$set": {"status": "Выполняется"}, "$push": {"launch_history": start_time}})
        mailing_doc = await mailings_coll.find_one({"name": mailing_name})

        current_pending_ids = mailing_doc.get("pending_receivers_ids", [])
        if not current_pending_ids:
            await mailings_coll.update_one({"name": mailing_name}, {"$set": {"status": "Завершена"}})
            return

        quantity_for_this_cycle = await estimate_messages_for_current_window(mailing_doc)
        if quantity_for_this_cycle == 0:
            await mailings_coll.update_one({"name": mailing_name}, {"$set": {"status": "Ждет следующего дня"}})
            return

        ids_to_process_now = current_pending_ids[:quantity_for_this_cycle]
        mailing_doc_json_str = json.dumps(mailing_doc,
                                          default=lambda x: x.isoformat() if isinstance(x, datetime.datetime) else str(
                                              x))

        sub_batches = [ids_to_process_now[i:i + BATCH_SIZE_PER_WORKER] for i in
                       range(0, len(ids_to_process_now), BATCH_SIZE_PER_WORKER)]

        worker_processes = []
        for i, batch in enumerate(sub_batches):
            p = Process(target=message_sender_process_entrypoint, args=(mailing_doc_json_str, batch, i))
            worker_processes.append(p)
            p.start()

        for p in worker_processes:
            p.join()

        final_doc_state = await mailings_coll.find_one({"name": mailing_name})
        remaining_pending = final_doc_state.get("pending_receivers_ids", [])

        end_time = datetime.datetime.now(MOSCOW_TZ)
        duration = end_time - start_time

        if not remaining_pending:
            await mailings_coll.update_one({"name": mailing_name}, {"$set": {"status": "Завершена", "report": {
                "total_sent": final_doc_state.get("sent_count", 0),
                "total_failed": final_doc_state.get("failed_count", 0), "duration_seconds": duration.total_seconds(),
                "start_time": start_time, "end_time": end_time}}})
        else:
            if not is_within_time_window(final_doc_state.get("time_spoon")):
                await mailings_coll.update_one({"name": mailing_name}, {"$set": {"status": "Ждет следующего дня"}})
            else:
                await mailings_coll.update_one({"name": mailing_name}, {"$set": {"status": "Готова к продолжению"}})
    except Exception as e:
        logger.error(f"Ошибка в обработке рассылки '{mailing_name}': {e}", exc_info=True)
        await mailings_coll.update_one({"name": mailing_name},
                                       {"$set": {"status": "Ошибка", "last_error_message": str(e)}})
    finally:
        if mailing_name in active_mailing_tasks:
            del active_mailing_tasks[mailing_name]


async def main_processor_loop():
    logger.info("Сервис Обработки Рассылок Запущен. Опрос задач...")
    while True:
        try:
            query = {"status": {"$in": ["Готова к запуску", "Готова к продолжению", "Выполняется"]},
                     "name": {"$nin": list(active_mailing_tasks.keys())}}
            logger.info(f"Поиск рассылок с запросом: {query}")

            mailing_doc_to_run = await mailings_collection_main_loop.find_one(query)

            if mailing_doc_to_run:
                name = mailing_doc_to_run["name"]
                logger.info(f"Найдена рассылка для обработки: {name}")
                task = asyncio.create_task(process_single_mailing_task(name, mailings_collection_main_loop))
                active_mailing_tasks[name] = task
            else:
                logger.debug("Нет рассылок для обработки")

            await asyncio.sleep(POLL_INTERVAL_SECONDS)
        except Exception as e:
            logger.error(f"Критическая ошибка в main_processor_loop: {e}", exc_info=True)
            await asyncio.sleep(POLL_INTERVAL_SECONDS * 2)


if __name__ == "__main__":
    try:
        asyncio.run(main_processor_loop())
    except KeyboardInterrupt:
        logger.info("Получен сигнал завершения. Корректное завершение работы...")

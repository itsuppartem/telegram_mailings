import logging
import motor.motor_asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path

from api import router
from config import (MONGODB_URI, TIMEZONE, DB_NAME_MAIN, DB_NAME_KO, DB_NAME_VROOM, COLLECTION_MAILINGS,
                    COLLECTION_REPORTS, COLLECTION_TOKENS, COLLECTION_USERS_KO, COLLECTION_USERS_KO_OLD,
                    COLLECTION_USERS_VROOM)
from monitoring import MonitoringService
from time import TimeWindowService

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

client = motor.motor_asyncio.AsyncIOMotorClient(MONGODB_URI)
client_bot_db = client[DB_NAME_KO]
vroom_bot = client[DB_NAME_VROOM]
mailing_db = client[DB_NAME_MAIN]

users_collection_vroom = vroom_bot[COLLECTION_USERS_VROOM]
users_collection_ko = client_bot_db[COLLECTION_USERS_KO]
users_old_collection_ko = client_bot_db[COLLECTION_USERS_KO_OLD]
mailings_collection = mailing_db[COLLECTION_MAILINGS]
reports_collection = mailing_db[COLLECTION_REPORTS]
tokens_collection = mailing_db[COLLECTION_TOKENS]

collections = {"users_vroom": users_collection_vroom, "users_ko": users_collection_ko,
    "users_old_ko": users_old_collection_ko}

scheduler = AsyncIOScheduler()


async def trigger_launch():
    mailings = await mailings_collection.find().to_list(None)
    for mailing in mailings:
        if mailing["status"] == "Не начата" and mailing["launch_date"] < datetime.now():
            await mailings_collection.update_one({"name": mailing["name"]}, {
                "$set": {"status": "Готова к запуску", "pending_receivers_ids": mailing.get("receivers_ids", []),
                    "total_recipients": len(mailing.get("receivers_ids", [])), "sent_count": 0, "failed_count": 0}})


async def continue_send():
    current_time = datetime.now()
    mailings = await mailings_collection.find().to_list(None)
    for mailing in mailings:
        if mailing["status"] == "Ждет следующего дня":
            launch_history_dates = [launch.date() for launch in mailing["launch_history"]]
            if current_time.date() in launch_history_dates:
                continue

            if mailing.get("time_spoon"):
                start_hour, end_hour = mailing["time_spoon"]
                start_time = current_time.replace(hour=start_hour, minute=0, second=0, microsecond=0)
                end_time = current_time.replace(hour=end_hour, minute=0, second=0, microsecond=0)

                if start_time <= current_time <= end_time:
                    await mailings_collection.update_one({"name": mailing["name"]},
                        {"$set": {"status": "Готова к запуску"}})


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        scheduler.start()
        scheduler.add_job(trigger_launch, IntervalTrigger(seconds=60))
        scheduler.add_job(continue_send, IntervalTrigger(seconds=5))
        logger.info("Successfully started scheduler")
        yield
    except Exception as e:
        logger.error(f"Failed to start application: {str(e)}")
        raise
    finally:
        scheduler.shutdown()
        client.close()
        logger.info("Successfully shut down application")


static_dir = Path("static")
templates_dir = Path("templates")
static_dir.mkdir(exist_ok=True)
templates_dir.mkdir(exist_ok=True)

app = FastAPI(lifespan=lifespan)
app.include_router(router)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

monitoring_service = MonitoringService(client)
time_window_service = TimeWindowService(timezone=TIMEZONE)

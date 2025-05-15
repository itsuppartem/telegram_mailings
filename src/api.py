import httpx
import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Header, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import Dict

from config import TELEGRAM_API_URL, KO_ADMIN_LIST, VROOM_ADMIN_LIST
from handlers import UserHandlerFactory
from models import Mailing, MailingProgress, BotName
from monitoring import MonitoringService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/monitoring", tags=["monitoring"])


async def send_telegram_request(method: str, payload: dict):
    async with httpx.AsyncClient() as client:
        url = f"{TELEGRAM_API_URL}/{method}"
        response = await client.post(url, json=payload)
        response.raise_for_status()


async def verify_token(token: str = Header(...), tokens_collection=None):
    token_data = await tokens_collection.find_one({"token": token})
    if not token_data:
        raise HTTPException(status_code=401, detail="Invalid token")


@router.get("/mailings/{mailing_name}/progress", response_model=MailingProgress)
async def get_mailing_progress(mailing_name: str, monitoring_service: MonitoringService):
    progress = await monitoring_service.get_mailing_progress(mailing_name)
    if not progress:
        raise HTTPException(status_code=404, detail=f"Mailing {mailing_name} not found")
    return progress


@router.get("/mailings/active", response_model=Dict[str, MailingProgress])
async def get_active_mailings(monitoring_service: MonitoringService):
    return await monitoring_service.get_active_mailings()


@router.get("/mailings/{mailing_name}/errors")
async def get_mailing_errors(mailing_name: str, monitoring_service: MonitoringService):
    progress = await monitoring_service.get_mailing_progress(mailing_name)
    if not progress:
        raise HTTPException(status_code=404, detail=f"Mailing {mailing_name} not found")

    return {"mailing_name": mailing_name, "error_rate": progress.error_rate, "total_errors": progress.failed,
            "last_updated": progress.last_updated}


@router.get("/config/time-windows")
async def get_time_windows(mailings_collection):
    mailings = await mailings_collection.find({"time_spoon": {"$exists": True}},
                                              {"name": 1, "time_spoon": 1, "_id": 0}).to_list(None)

    return {mailing["name"]: {"time_window": mailing["time_spoon"], "timezone": "Europe/Moscow"} for mailing in
            mailings}


@router.get("/dashboard")
async def get_dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})


@router.get("/mailings/completed", response_model=Dict[str, MailingProgress])
async def get_completed_mailings(monitoring_service: MonitoringService):
    return await monitoring_service.get_completed_mailings()


@router.delete("/mailings/{mailing_name}")
async def delete_mailing(mailing_name: str, mailings_collection):
    result = await mailings_collection.delete_one({"name": mailing_name})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail=f"Mailing {mailing_name} not found")
    return {"message": f"Mailing {mailing_name} deleted successfully"}


@router.get("/mailings/all", response_model=Dict[str, MailingProgress])
async def get_all_mailings(monitoring_service: MonitoringService):
    return await monitoring_service.get_all_mailings()


@router.post("/create_mailing")
async def create_mailing(mailing: Mailing, token: str = Header(...), tokens_collection=None, mailings_collection=None,
                         collections: dict = None):
    await verify_token(token, tokens_collection)
    user_handler = UserHandlerFactory.get_handler(mailing.bot, collections)

    if not mailing.receivers_phones:
        receivers_chat_ids = await user_handler.get_all_chat_ids()
        logger.info(f"Создание рассылки всем пользователям. Найдено получателей: {len(receivers_chat_ids)}")
    else:
        receivers_chat_ids = await user_handler.get_chat_ids_by_phones(mailing.receivers_phones)
        logger.info(f"Создание рассылки по списку телефонов. Найдено получателей: {len(receivers_chat_ids)}")

    if not receivers_chat_ids:
        raise HTTPException(status_code=400, detail="Не найдено получателей для рассылки")

    mailing_data = MailingMongodb(name=mailing.name, bot=str(mailing.bot.value), text=mailing.text, photo=mailing.photo,
                                  animation=mailing.animation, receivers_ids=receivers_chat_ids,
                                  launch_date=mailing.launch_date, time_spoon=mailing.time_spoon, status="Не начата",
                                  launch_history=[], report_is_sent=False, total_recipients=len(receivers_chat_ids),
                                  sent_count=0, failed_count=0, pending_receivers_ids=receivers_chat_ids,
                                  promo_codes=mailing.promo_codes)

    result = await mailings_collection.insert_one(mailing_data.dict())
    if result.inserted_id:
        return {"id": str(result.inserted_id), "message": "Mailing created successfully"}
    raise HTTPException(status_code=500, detail="Failed to create mailing")

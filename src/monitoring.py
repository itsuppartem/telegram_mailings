import logging
import motor.motor_asyncio
from datetime import datetime
from typing import Dict, Optional

from models import MailingProgress

logger = logging.getLogger(__name__)


class MonitoringService:
    def __init__(self, mongo_client: motor.motor_asyncio.AsyncIOMotorClient):
        self.mailing_db = mongo_client.mailing_db
        self.mailings_collection = self.mailing_db.get_collection("mailings")
        self.reports_collection = self.mailing_db.get_collection("reports")
        self.MAX_ERROR_RATE_PERCENT = 5
        logger.info("Initialized MonitoringService")

    async def initialize_mailing_progress(self, mailing_name: str, total_receivers: int) -> None:
        progress = MailingProgress(total=total_receivers, processed=0, successful=0, failed=0,
            remaining=total_receivers, percent_complete=0.0, last_updated=datetime.now(), status="Выполняется",
            error_rate=0.0)

        await self.reports_collection.update_one({"name": mailing_name}, {"$set": progress.dict()}, upsert=True)
        logger.info(f"Initialized progress tracking for mailing {mailing_name}")

    async def update_mailing_progress(self, mailing_name: str, success: bool) -> None:
        try:
            update_result = await self.reports_collection.update_one({"name": mailing_name}, {
                "$inc": {"processed": 1, "successful": 1 if success else 0, "failed": 0 if success else 1,
                    "remaining": -1}, "$set": {"last_updated": datetime.now()}})

            if update_result.modified_count > 0:
                report = await self.reports_collection.find_one({"name": mailing_name})
                if report:
                    processed = report["processed"]
                    total = report["total"]
                    failed = report["failed"]

                    percent_complete = (processed / total) * 100 if total > 0 else 0
                    error_rate = (failed / processed) * 100 if processed > 0 else 0

                    await self.reports_collection.update_one({"name": mailing_name},
                        {"$set": {"percent_complete": percent_complete, "error_rate": error_rate}})

                    if error_rate > self.MAX_ERROR_RATE_PERCENT and not report.get("alert_sent"):
                        await self.send_error_alert(mailing_name, error_rate)
                        await self.reports_collection.update_one({"name": mailing_name}, {"$set": {"alert_sent": True}})

                    logger.debug(
                        f"Updated progress for mailing {mailing_name}: {percent_complete}% complete, error rate: {error_rate}%")
        except Exception as e:
            logger.error(f"Error updating progress for mailing {mailing_name}: {str(e)}")
            raise

    async def send_error_alert(self, mailing_name: str, error_rate: float) -> None:
        try:
            logger.warning(f"High error rate alert for mailing {mailing_name}: {error_rate}%")
        except Exception as e:
            logger.error(f"Error sending alert for mailing {mailing_name}: {str(e)}")

    async def get_mailing_progress(self, mailing_name: str) -> Optional[MailingProgress]:
        try:
            mailing = await self.mailings_collection.find_one({"name": mailing_name})
            if not mailing:
                return None

            total = mailing.get("total_recipients", 0)
            sent = mailing.get("sent_count", 0)
            failed = mailing.get("failed_count", 0)
            remaining = len(mailing.get("pending_receivers_ids", []))
            processed = sent + failed

            percent_complete = (processed / total * 100) if total > 0 else 0
            error_rate = (failed / processed * 100) if processed > 0 else 0

            return MailingProgress(total=total, processed=processed, successful=sent, failed=failed,
                remaining=remaining, percent_complete=percent_complete, error_rate=error_rate,
                status=mailing.get("status", "Неизвестно"), last_updated=datetime.now(),
                alert_sent=mailing.get("alert_sent", False))
        except Exception as e:
            logger.error(f"Error getting mailing progress: {str(e)}")
            return None

    async def get_active_mailings(self) -> Dict[str, MailingProgress]:
        try:
            active_mailings = {}
            cursor = self.mailings_collection.find(
                {"status": {"$in": ["Выполняется", "Готова к запуску", "Готова к продолжению"]}})

            async for mailing in cursor:
                try:
                    total = mailing.get("total_recipients", 0)
                    sent = mailing.get("sent_count", 0)
                    failed = mailing.get("failed_count", 0)
                    remaining = len(mailing.get("pending_receivers_ids", []))
                    processed = sent + failed

                    progress = MailingProgress(total=total, processed=processed, successful=sent, failed=failed,
                        remaining=remaining, percent_complete=(processed / total * 100) if total > 0 else 0,
                        error_rate=(failed / processed * 100) if processed > 0 else 0,
                        status=mailing.get("status", "Неизвестно"), last_updated=datetime.now(),
                        alert_sent=mailing.get("alert_sent", False))

                    active_mailings[mailing["name"]] = progress
                except Exception as e:
                    logger.error(f"Error processing active mailing {mailing.get('name', 'unknown')}: {str(e)}")
                    continue

            return active_mailings
        except Exception as e:
            logger.error(f"Error getting active mailings: {str(e)}")
            return {}

    async def get_completed_mailings(self) -> Dict[str, MailingProgress]:
        try:
            completed_mailings = {}
            cursor = self.mailings_collection.find({"status": "Завершена"})

            async for mailing in cursor:
                try:
                    total = mailing.get("total_recipients", 0)
                    sent = mailing.get("sent_count", 0)
                    failed = mailing.get("failed_count", 0)
                    processed = sent + failed

                    progress = MailingProgress(total=total, processed=processed, successful=sent, failed=failed,
                        remaining=0, percent_complete=100.0,
                        error_rate=(failed / processed * 100) if processed > 0 else 0, status="Завершена",
                        last_updated=datetime.now(), alert_sent=mailing.get("alert_sent", False))

                    completed_mailings[mailing["name"]] = progress
                except Exception as e:
                    logger.error(f"Error processing completed mailing {mailing.get('name', 'unknown')}: {str(e)}")
                    continue

            return completed_mailings
        except Exception as e:
            logger.error(f"Error getting completed mailings: {str(e)}")
            return {}

    async def get_all_mailings(self) -> Dict[str, MailingProgress]:
        try:
            all_mailings = {}
            cursor = self.mailings_collection.find()

            async for mailing in cursor:
                try:
                    total = mailing.get("total_recipients", 0)
                    sent = mailing.get("sent_count", 0)
                    failed = mailing.get("failed_count", 0)
                    remaining = len(mailing.get("pending_receivers_ids", []))
                    processed = sent + failed

                    progress = MailingProgress(total=total, processed=processed, successful=sent, failed=failed,
                        remaining=remaining, percent_complete=(processed / total * 100) if total > 0 else 0,
                        error_rate=(failed / processed * 100) if processed > 0 else 0,
                        status=mailing.get("status", "Неизвестно"), last_updated=datetime.now(),
                        alert_sent=mailing.get("alert_sent", False))

                    all_mailings[mailing["name"]] = progress
                except Exception as e:
                    logger.error(f"Error processing mailing {mailing.get('name', 'unknown')}: {str(e)}")
                    continue

            return all_mailings
        except Exception as e:
            logger.error(f"Error getting all mailings: {str(e)}")
            return {}

import datetime
import logging
import pytz
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class TimeWindowService:
    def __init__(self, timezone: str = "UTC"):
        self.timezone = pytz.timezone(timezone)
        logger.info(f"Initialized TimeWindowService with timezone {timezone}")

    async def is_within_window(self, time_spoon: Optional[Tuple[int, int]]) -> bool:
        if not time_spoon:
            logger.debug("No time window specified, returning True")
            return True

        start_hour, end_hour = time_spoon
        current_time = datetime.datetime.now(self.timezone)

        if start_hour > end_hour:
            is_in_window = (current_time.hour >= start_hour or current_time.hour < end_hour)
        else:
            is_in_window = (current_time.hour >= start_hour and current_time.hour < end_hour)

        logger.debug(
            f"Time window check: {is_in_window} (current hour: {current_time.hour}, window: {start_hour}-{end_hour})")
        return is_in_window

    async def calculate_next_window_start(self, time_spoon: Optional[Tuple[int, int]]) -> datetime.datetime:
        if not time_spoon:
            logger.debug("No time window specified, returning current time")
            return datetime.datetime.now(self.timezone)

        start_hour, _ = time_spoon
        current_time = datetime.datetime.now(self.timezone)

        if current_time.hour < start_hour:
            next_window = current_time.replace(hour=start_hour, minute=0, second=0, microsecond=0)
        else:
            next_window = current_time.replace(hour=start_hour, minute=0, second=0, microsecond=0) + datetime.timedelta(
                days=1)

        logger.debug(f"Next window start calculated: {next_window}")
        return next_window

    async def get_remaining_window_time(self, time_spoon: Optional[Tuple[int, int]]) -> float:
        if not time_spoon:
            logger.debug("No time window specified, returning 0")
            return 0

        _, end_hour = time_spoon
        current_time = datetime.datetime.now(self.timezone)

        if current_time.hour < end_hour:
            end_time = current_time.replace(hour=end_hour, minute=0, second=0, microsecond=0)
        else:
            end_time = current_time.replace(hour=end_hour, minute=0, second=0, microsecond=0) + datetime.timedelta(
                days=1)

        remaining_seconds = (end_time - current_time).total_seconds()
        logger.debug(f"Remaining window time: {remaining_seconds} seconds")
        return max(0, remaining_seconds)

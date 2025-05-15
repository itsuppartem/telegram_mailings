import numpy as np
from datetime import datetime
from enum import Enum
from pydantic import BaseModel
from typing import List, Union, Dict, Tuple, Optional


class BotName(Enum):
    GOOCOM_KO = "ko"
    VROOM = "vroom"


class Mailing(BaseModel):
    name: str
    bot: BotName
    text: str
    photo: Optional[str] = None
    animation: Optional[str] = None
    receivers_phones: List[str]
    launch_date: Optional[datetime] = None
    time_spoon: Optional[Tuple[int, int]] = None
    promo_codes: Optional[Dict[str, str]] = None


class MailingMongodb(BaseModel):
    name: str
    bot: BotName
    text: str
    photo: Optional[str] = None
    animation: Optional[str] = None
    receivers_ids: List[Union[int, np.int64]] = []
    launch_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    time_spoon: Optional[Tuple[int, int]] = None
    status: str = "Не начата"
    launch_history: List[datetime] = []
    report_is_sent: bool = False
    total_recipients: int = 0
    sent_count: int = 0
    failed_count: int = 0
    pending_receivers_ids: List[Union[int, np.int64]] = []
    last_error_message: Optional[str] = None
    promo_codes: Optional[Dict[str, str]] = None

    class Config:
        arbitrary_types_allowed = True
        use_enum_values = True


class MailingProgress(BaseModel):
    total: int
    processed: int
    successful: int
    failed: int
    remaining: int
    percent_complete: float
    last_updated: datetime
    status: str
    error_rate: float
    alert_sent: bool = False

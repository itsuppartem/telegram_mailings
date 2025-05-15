import backoff
import httpx
import logging
from typing import List, Dict, Tuple

from config import TELEGRAM_API_URL, BOT_TOKENS

logger = logging.getLogger(__name__)


async def get_bot_tokens(bot_name_str: str) -> List[str]:
    return BOT_TOKENS.get(bot_name_str, [])


@backoff.on_exception(backoff.expo, httpx.HTTPStatusError, max_tries=3, logger=logger, max_time=30,
    giveup=lambda e: e.response.status_code != 429)
async def _send_tg_request_worker(session: httpx.AsyncClient, method: str, payload: dict, bot_token: str):
    url = f"{TELEGRAM_API_URL}{bot_token}/{method}"
    response = await session.post(url, json=payload)
    if response.status_code == 400 or response.status_code == 403:
        logger.warning(
            f"Неповторяемая ошибка Telegram API: {response.status_code} для {payload.get('chat_id')}. Ответ: {response.text}")
    response.raise_for_status()
    return response.status_code, response.json()


async def actual_send_message_worker(session: httpx.AsyncClient, throttler, message_spec: Dict,
                                     bot_tokens: List[str]) -> int:
    status_code = 500
    text_to_send = message_spec["text"]
    if message_spec.get("promo_code"):
        text_to_send = f"{text_to_send}\n\nВаш промокод: {message_spec['promo_code']}"

    payload_base = {"chat_id": message_spec["chat_id"], "parse_mode": "HTML"}
    method_to_call = ""

    if message_spec.get("photo"):
        method_to_call = "sendPhoto"
        payload_base["photo"] = message_spec["photo"]
        payload_base["caption"] = text_to_send
    elif message_spec.get("animation"):
        method_to_call = "sendAnimation"
        payload_base["animation"] = message_spec["animation"]
        payload_base["caption"] = text_to_send
    elif text_to_send:
        method_to_call = "sendMessage"
        payload_base["text"] = text_to_send
    else:
        return 900

    for token_index, token in enumerate(bot_tokens):
        async with throttler:
            try:
                status, _ = await _send_tg_request_worker(session, method_to_call, payload_base, token)
                if status == 200:
                    return 200
            except httpx.HTTPStatusError as e:
                status_code = e.response.status_code
                if status_code == 403:
                    if token_index == len(bot_tokens) - 1:
                        return 403
                    continue
                return status_code
            except Exception as e:
                logger.error(f"Ошибка отправки сообщения: {e}")
                return 500
    return status_code

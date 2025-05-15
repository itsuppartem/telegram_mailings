import motor.motor_asyncio
from abc import ABC, abstractmethod
from typing import List

from models import BotName


class UserCollectionHandler(ABC):
    @abstractmethod
    async def get_all_chat_ids(self) -> List[int]:
        pass

    @abstractmethod
    async def get_chat_ids_by_phones(self, phone_numbers: List[str]) -> List[int]:
        pass


class GoocomKoUserHandler(UserCollectionHandler):
    def __init__(self, collection, old_collection):
        self.collection = collection
        self.old_collection = old_collection

    async def get_all_chat_ids(self) -> List[int]:
        chat_ids_ko = [doc["chat_id"] async for doc in self.collection.find({"otpisan": ""}, {"_id": 0, "chat_id": 1})]
        chat_ids_ko_old = [doc["chat_id"] async for doc in
                           self.old_collection.find({"otpisan": ""}, {"_id": 0, "chat_id": 1})]
        all_chat_ids = list(set(chat_ids_ko + chat_ids_ko_old))
        return all_chat_ids

    async def get_chat_ids_by_phones(self, phone_numbers: List[str]) -> List[int]:
        chat_ids_ko = [doc["chat_id"] async for doc in
                       self.collection.find({"phone": {"$in": phone_numbers}, "otpisan": ""}, {"_id": 0, "chat_id": 1})]
        chat_ids_ko_old = [doc["chat_id"] async for doc in
                           self.old_collection.find({"phone": {"$in": phone_numbers}, "otpisan": ""},
                                                    {"_id": 0, "chat_id": 1})]
        all_chat_ids = list(set(chat_ids_ko + chat_ids_ko_old))
        return all_chat_ids


class VroomUserHandler(UserCollectionHandler):
    def __init__(self, collection):
        self.collection = collection

    async def crutch(self) -> List[int]:
        cursor = self.collection.find({}, {"_id": 0, "user_id": 1})
        return [doc["user_id"] async for doc in cursor]

    async def get_all_chat_ids(self) -> List[int]:
        return await self.crutch()

    async def get_chat_ids_by_phones(self, phone_numbers: List[str]) -> List[int]:
        return await self.crutch()


class UserHandlerFactory:
    @staticmethod
    def get_handler(bot_name: BotName, collections: dict) -> UserCollectionHandler:
        if bot_name == BotName.GOOCOM_KO:
            return GoocomKoUserHandler(collections["users_ko"], collections["users_old_ko"])
        elif bot_name == BotName.VROOM:
            return VroomUserHandler(collections["users_vroom"])
        else:
            raise ValueError("Unsupported bot name")

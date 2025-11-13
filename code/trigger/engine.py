from .scheduler import ScheduleTrigger
from .http_request import HTTPTrigger
from .mqtt_event import MQTTTrigger

import asyncio

class TriggerEngine:
    def __init__(self, config):
        self.__scheduler = ScheduleTrigger(config)
        self.__http = HTTPTrigger(self.__scheduler, config)
        self.__mqtt = MQTTTrigger(config)

    def start(self):
        asyncio.run(self.main())

    async def main(self):
        await asyncio.gather(
            self.__scheduler.run(),
            self.__http.run(),
            self.__mqtt.run(),
        )

    @property
    def http(self):
        return self.__http

    @property
    def scheduler(self):
        return self.__scheduler

    @property
    def mqtt(self):
        return self.__mqtt

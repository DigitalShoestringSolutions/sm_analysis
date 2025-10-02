import logging
import asyncio
from paho.mqtt.client import Client as MQTTClient, CallbackAPIVersion
import json
from .mqtt_handler import MQTTHandler

logger = logging.getLogger(__name__)

class MQTTTrigger:
    def __init__(self, config):
        self.mqtt_handler = MQTTHandler()
        self.config = config
        
    def should_run(self):
        return self.mqtt_handler.has_entries()

    async def run(self,broker, port=1883):
        mqttc = MQTTClient(CallbackAPIVersion.VERSION2)
        mqttc.on_connect = mqtt_on_connect
        mqttc.on_message = mqtt_on_message
        mqttc.on_disconnect = mqtt_on_disconnect
        mqttc.user_data_set(self.mqtt_handler)

        mqttc.connect(broker, port, 60)

        try:
            while True:
                await asyncio.sleep(0.5)
                rc = mqttc.loop(timeout=0.5)
                if rc == 0:
                    await self.mqtt_handler.call_functions_for_messages(self.config)

        finally:
            mqttc.disconnect()
            logger.info("MQTT connection closed")

    def register_topic(self,topic,func):
        self.mqtt_handler.register_function_to_topic(topic, func)

    # this is a decorator
    def event(self,topic):
        def inner(func):
            self.register_topic(topic,func)
            return func
        return inner


def mqtt_on_connect(
    client: MQTTClient, handler: MQTTHandler, flags, reason_code, properties
):
    logger.info(f"MQTT client connected with result code {reason_code}")
    for topic in handler.subscriptions:
        client.subscribe(topic)


def mqtt_on_message(client: MQTTClient, handler: MQTTHandler, msg):
    handler.add_msg(msg.topic,json.loads(msg.payload))
    logger.debug(f"MQTT IN - topic: {msg.topic} payload: {msg.payload}")


def mqtt_on_disconnect(client: MQTTClient, userdata, flags, reason_code, properties):
    logger.info("MQTT disconnected")

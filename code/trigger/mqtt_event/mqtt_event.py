import logging
import asyncio
from paho.mqtt.client import Client as MQTTClient, CallbackAPIVersion
import json
import signal
from .mqtt_handler import MQTTHandler

logger = logging.getLogger(__name__)

terminate_flag = False


def graceful_signal_handler(sig, _frame):
    logger.info(
        f"Received {signal.Signals(sig).name}. Triggering graceful termination."
    )
    global terminate_flag
    terminate_flag = True
    signal.alarm(10)


class MQTTTrigger:
    def __init__(self, config):
        self.mqtt_handler = MQTTHandler()
        self.config = config

        self.initial = 5
        self.backoff = 2
        self.limit = 60

    def should_run(self):
        return self.mqtt_handler.has_entries()

    async def mqtt_connect(self, client, first_time=False):
        timeout = self.initial
        exceptions = True
        while exceptions and terminate_flag is False:
            try:
                if first_time:
                    client.connect(self.broker, self.port, 60)
                else:
                    logger.error("Attempting to reconnect...")
                    client.reconnect()
                logger.info("Connected!")
                await asyncio.sleep(self.initial)  # to give things time to settle
                exceptions = False
            except Exception:
                logger.error(f"Unable to connect, retrying in {timeout} seconds")
                await asyncio.sleep(timeout)
                if timeout < self.limit:
                    timeout = timeout * self.backoff
                else:
                    timeout = self.limit

    async def run(self,broker, port=1883):
        # Setup signal handlers for graceful termination
        signal.signal(signal.SIGINT, graceful_signal_handler)
        signal.signal(signal.SIGTERM, graceful_signal_handler)

        self.broker = broker
        self.port = port

        mqttc = MQTTClient(CallbackAPIVersion.VERSION2)
        mqttc.on_connect = mqtt_on_connect
        mqttc.on_message = mqtt_on_message
        mqttc.on_disconnect = self.mqtt_on_disconnect
        mqttc.user_data_set(self.mqtt_handler)

        await self.mqtt_connect(mqttc, first_time=True)

        try:
            while terminate_flag is False:
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

    def mqtt_on_disconnect(self,client: MQTTClient, userdata, flags, reason_code, properties):
        if reason_code != 0:
            logger.error(
                f"Unexpected MQTT disconnection (rc:{reason_code}), reconnecting..."
            )
            self.mqtt_connect(client)


def mqtt_on_connect(
    client: MQTTClient, handler: MQTTHandler, flags, reason_code, properties
):
    logger.info(f"MQTT client connected with result code {reason_code}")
    for topic in handler.subscriptions:
        client.subscribe(topic)


def mqtt_on_message(client: MQTTClient, handler: MQTTHandler, msg):
    handler.add_msg(msg.topic,json.loads(msg.payload))
    logger.debug(f"MQTT IN - topic: {msg.topic} payload: {msg.payload}")

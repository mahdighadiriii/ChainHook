import json

import pika

from .config import settings


def publish_event(event_data: dict):
    connection = pika.BlockingConnection(pika.URLParameters(settings.rabbitmq_url))
    channel = connection.channel()
    channel.queue_declare(queue="events.detected", durable=True)
    channel.basic_publish(
        exchange="", routing_key="events.detected", body=json.dumps(event_data)
    )
    connection.close()

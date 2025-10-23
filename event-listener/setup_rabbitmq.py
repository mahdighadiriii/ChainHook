import pika

connection = pika.BlockingConnection(
    pika.URLParameters("amqp://guest:guest@rabbitmq:5672/")
)
channel = connection.channel()

# Create exchanges
channel.exchange_declare(
    exchange="events.exchange", exchange_type="topic", durable=True
)
channel.exchange_declare(exchange="events.dlx", exchange_type="topic", durable=True)

# Create queues
channel.queue_declare(queue="events.dlq", durable=True)
channel.queue_declare(
    queue="events.detected",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "events.dlx",
        "x-dead-letter-routing-key": "events.failed",
        "x-message-ttl": 300000,
    },
)

# Bind queues
channel.queue_bind(
    exchange="events.exchange", queue="events.detected", routing_key="events.*"
)
channel.queue_bind(
    exchange="events.dlx", queue="events.dlq", routing_key="events.failed"
)

print("RabbitMQ setup complete!")
connection.close()

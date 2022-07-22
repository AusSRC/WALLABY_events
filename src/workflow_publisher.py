#!/usr/bin/env python3

from aio_pika import connect_robust, Message, ExchangeType, DeliveryMode


WALLABY_WORKFLOW_EXCHANGE = 'aussrc.workflow.submit'
WALLABY_WORKFLOW_ROUTING_KEY = 'pipeline'


class WorkflowPublisher(object):
    """Publisher for submitting workflows into the AusSRC event system

    """
    def __init__(self):
        self.dsn = None
        self.conn = None
        self.channel = None
        self.exchange = None

    async def setup(self, loop, dsn):
        self.dsn = dsn
        self.conn = await connect_robust(self.dsn, loop=loop)
        self.channel = await self.conn.channel()
        await self.channel.set_qos(prefetch_count=1)

        # Ensure exchange exists
        self.exchange = await self.channel.declare_exchange(
            WALLABY_WORKFLOW_EXCHANGE,
            ExchangeType.DIRECT,
            durable=True
        )

    async def publish(self, body):
        msg = Message(body, delivery_mode=DeliveryMode.PERSISTENT)
        await self.exchange.publish(msg, routing_key=WALLABY_WORKFLOW_ROUTING_KEY)

    async def close(self):
        await self.conn.close()

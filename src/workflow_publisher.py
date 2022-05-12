#!/usr/bin/env python3

import sys
import json
import asyncio
from aio_pika import connect_robust, Message, ExchangeType, DeliveryMode


CASDA_EXCHANGE = 'aussrc.casda'
CASDA_QUEUE = 'aussrc.casda.wallaby'
WALLABY_WORKFLOW_EXCHANGE = 'aussrc.workflow.submit'


class WorkflowPublisher(object):
    """Publisher for submitting workflows into the AusSRC event system

    """
    def __init__(self):
        self.r_dsn = None
        self.rabbitmq_conn = None
        self.rabbitmq_channel = None
        self.casda_exchange = None
        self.casda_queue = None
        self.workflow_exchange = None          
    
    async def setup(self, loop, r_dsn):
        self.r_dsn = r_dsn

        # RabbitMQ connection and channel
        self.rabbitmq_conn = await connect_robust(self.r_dsn, loop=loop)
        self.rabbitmq_channel = await self.rabbitmq_conn.channel()
        await self.rabbitmq_channel.set_qos(prefetch_count=1)

        # Create RabbitMQ exchanges and queues (make sure they exist)
        self.workflow_exchange = await self.rabbitmq_channel.declare_exchange(
            WALLABY_WORKFLOW_EXCHANGE,
            ExchangeType.DIRECT,
            durable=True
        )

    async def publish(self, body):
        """Publish message to workflow exchange.

        """
        msg = Message(body, delivery_mode=DeliveryMode.PERSISTENT)
        await self.workflow_exchange.publish(msg, routing_key="")

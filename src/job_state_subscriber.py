#!/usr/bin/env python3

import asyncio
import asyncpg
import json
import logging
from aio_pika import connect_robust, IncomingMessage, ExchangeType
from src.utils import parse_config


logging.basicConfig(level=logging.INFO)


WORKFLOW_STATE_EXCHANGE = 'aussrc.workflow.state'
WORKFLOW_STATE_QUEUE = 'aussrc.workflow.state.email'


class JobStateSubscriber(object):
    def __init__(self):
        self.db_dsn = None
        self.db_pool = None
        self.r_conn = None
        self.r_channel = None
        self.r_exchange = None
        self.r_queue = None

    async def setup(self, loop, db_dsn, r_dsn):
        """Set up RabbitMQ and database connections

        """
        logging.info("Setting up RabbitMQ and database connections in workflow state subscriber")
        self.r_conn = await connect_robust(r_dsn, loop=loop)
        self.r_channel = await self.r_conn.channel()
        await self.r_channel.set_qos(prefetch_count=1)

        self.r_exchange = await self.r_channel.declare_exchange(
            WORKFLOW_STATE_EXCHANGE,
            ExchangeType.FANOUT,
            durable=True
        )
        self.r_queue = await self.r_channel.declare_queue(
            name=WORKFLOW_STATE_QUEUE,
            durable=True
        )
        await self.r_queue.bind(self.r_exchange)
        self.db_pool = await asyncpg.create_pool(dsn=None, **db_dsn)
        logging.info("RabbitMQ and database connections setup complete")

    async def _on_postprocessing_pipeline_update(self, body, params):
        """Actions to trigger based on post-processing pipeline job status change

        """
        # update database entry and link run on complete
        if body['state'] == 'COMPLETED':
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    run = await conn.fetchrow(
                        "SELECT id FROM wallaby.run WHERE name=$1",
                        params['RUN_NAME']
                    )
                    await conn.execute(
                        "UPDATE wallaby.postprocessing \
                        SET run_id=$1, status=$2 \
                        WHERE name=$3",
                        run['id'], body['state'], params['RUN_NAME']
                    )
                    # TODO(austin): update location for wallaby.tile image cube and weights
            logging.info(f"Postprocessing run {params['RUN_NAME']} completed")

        # action on state update
        if (body['state'] == 'FAILED') or (body['state'] == 'RUNNING') or (body['state'] == 'CANCELLED'):
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "UPDATE wallaby.postprocessing \
                        SET status=$1 \
                        WHERE name=$2",
                        body['state'], params['RUN_NAME']
                    )
            logging.info(f"Updated state of run {params['RUN_NAME']} to {body['state']}")

    async def _on_quality_check_pipeline_update(self, body, params):
        """Actions to trigger based on quality check pipeline jop status update

        """
        if body['state'] == 'COMPLETED':
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "UPDATE wallaby.observation \
                        SET status=$1 \
                        WHERE sbid=$2",
                        body['state'], params['SBID']
                    )
            logging.info(f"Observation {params['SBID']} quality check completed")

            # TODO(austin): email relevant users

        # action on state update
        if (body['state'] == 'FAILED') or (body['state'] == 'RUNNING') or (body['state'] == 'CANCELLED'):
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "UPDATE wallaby.observation \
                        SET status=$1 \
                        WHERE sbid=$2",
                        body['state'], params['SBID']
                    )
            logging.info(f"Updated state of observation {params['SBID']} to {body['state']}")

    async def on_message(self, message: IncomingMessage):
        """Callback function to be triggered on receiving message in response to Slurm event.

        """
        try:
            body = json.loads(message.body)
            params = json.loads(body['params'])
            logging.info(f"Received event update: {body}")

            # postprocessing pipeline
            if body['repository'] == 'https://github.com/AusSRC/WALLABY_pipeline':
                await self._on_postprocessing_pipeline_update(body, params)

            # quality check pipeline
            if body['repository'] == 'https://github.com/AusSRC/WALLABY_footprint_check':
                await self._on_quality_check_pipeline_update(body, params)

            await message.ack()
        except Exception:
            logging.error("Error", exc_info=True)
            await message.nack()
            await asyncio.sleep(5)
            if self.db_pool:
                await self.db_pool.expire_connections()
            return

    async def consume(self):
        await self.r_queue.consume(self.on_message, no_ack=False)

    async def close(self):
        await self.r_conn.close()


async def main(loop):
    """Listen to postprocessing job status updates

    """
    db_dsn, r_dsn, _ = parse_config()

    # initialise subscriber
    subscriber = JobStateSubscriber()
    await subscriber.setup(loop, db_dsn, r_dsn['dsn'])
    await subscriber.consume()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))
    loop.run_forever()

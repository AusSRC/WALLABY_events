#!/usr/bin/env python3

import json
import asyncio
import asyncpg
import logging
import fnmatch
from utils import parse_config, parse_casda_credentials
from aio_pika import connect_robust, IncomingMessage, Message, ExchangeType, DeliveryMode


logging.basicConfig(level=logging.INFO)


CASDA_EXCHANGE = 'aussrc.casda'
CASDA_QUEUE = 'aussrc.casda.wallaby'
WALLABY_WORKFLOW_EXCHANGE = 'aussrc.workflow.submit'
WALLABY_PROJECT_CODE = 'AS102'


class CASDASubscriber(object):
    def __init__(self):
        self.db_dsn = None
        self.db_pool = None
        self.r_conn = None
        self.r_channel = None
        self.casda_exchange = None
        self.casda_queue = None
        self.workflow_exchange = None
        self.casda_credentials = None
        self.pipeline_key = None

    async def setup(self, loop, db_dsn, r_dsn, pipeline_key, project_code):
        """Establish psql and rabbitmq connections. Create exchanges and
        queues for messaging.

        """
        logging.info("Initialising WALLABY CASDA subscriber.")
        self.pipeline_key = pipeline_key

        # RabbitMQ connection and channel
        self.r_conn = await connect_robust(r_dsn, loop=loop)
        self.r_channel = await self.r_conn.channel()
        await self.r_channel.set_qos(prefetch_count=1)

        # Create RabbitMQ exchanges and queues
        self.workflow_exchange = await self.r_channel.declare_exchange(
            WALLABY_WORKFLOW_EXCHANGE,
            ExchangeType.DIRECT,
            durable=True
        )
        self.casda_exchange = await self.r_channel.declare_exchange(
            CASDA_EXCHANGE,
            ExchangeType.FANOUT,
            durable=True
        )
        self.casda_queue = await self.r_channel.declare_queue(name=CASDA_QUEUE, durable=True)
        await self.casda_queue.bind(self.casda_exchange)

        # WALLABY database PostgreSQL connection
        self.db_pool = await asyncpg.create_pool(dsn=None, **db_dsn)
        logging.info("WALLABY CASDA subscriber successfully initialised.")

    async def on_message(self, message: IncomingMessage):
        """Callback receiving message on WALLABY queue.
        Write observation to the database and submit workflow to execute quality check pipeline.

        """
        try:
            body = json.loads(message.body)
            if body['project_code'] == WALLABY_PROJECT_CODE:
                logging.info(f"Received WALLABY observation: {body}")
                files = body['files']
                sbid = body['sbid']
                image_cube = fnmatch.filter(files, "image.restored.i.*.cube.contsub.fits")
                weights_cube = fnmatch.filter(files, "weights.i.*.cube.fits")

                # check only one image or weights cube
                if (len(image_cube) > 1) or (len(weights_cube) > 1):
                    raise Exception("Found more than one image or weights cube for a given SBID.")

                # Submit to workflow
                # TODO(austin): remove CASDA credentials from here.
                params = {
                    'pipeline_key': self.pipeline_key,
                    'params': {
                        'SBID': sbid,
                        'CASDA_USERNAME': 'austin.shen@csiro.au',
                        'CASDA_PASSWORD': 'Y*Q2wQb_C4w9s-b37D',
                        'SOFIA_PARAMETER_FILE': '/group/ja3/ashen/wallaby/pre_runs/sofia.par',
                        'S2P_TEMPLATE': '/group/ja3/ashen/wallaby/pre_runs/s2p_setup.ini'
                    }
                }
                message = Message(json.dumps(params).encode(), delivery_mode=DeliveryMode.PERSISTENT)
                # await self.workflow_exchange.publish(message, routing_key="")

                # Add WALLABY observation
                # async with self.db_pool.acquire() as conn:
                #     async with conn.transaction():
                #         await conn.execute(
                #             "INSERT INTO wallaby.observation (sbid, image_cube, weights_cube) "
                #             "VALUES ($1, $2, $3) "
                #             "ON CONFLICT DO NOTHING",
                #             int(sbid), image_cube[0], weights_cube[0]
                #         )
            # await message.ack()

        except Exception:
            logging.error("on_message", exc_info=True)
            message.nack()
            await asyncio.sleep(5)
            if self.db_pool:
                await self.db_pool.expire_connections()
            return

    async def consume(self):
        await self.casda_queue.consume(self.on_message, no_ack=False)

    async def close(self):
        await self.r_conn.close()


async def main(loop):
    """Listen for CASDA event updates

    """
    db_dsn, r_dsn, pipeline = parse_config()
    casda_credentials = parse_casda_credentials()

    # Initialise subscriber
    subscriber = CASDASubscriber()
    await subscriber.setup(loop, db_dsn, r_dsn['dsn'], pipeline['footprint_check_key'], WALLABY_PROJECT_CODE)
    await subscriber.consume()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))
    loop.run_forever()

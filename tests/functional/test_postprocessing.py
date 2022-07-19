"""
Tests for submission of post-processing pipelines based on database content.

  Includes tests for:
    - Observation available in CASDA should trigger pipeline
    - Pairs of available observations available on file system
      should trigger postprocessing pipeline on central region
    - On completion of first post-processing pipeline run, borders with
      no adjacent tiles should be processed
    - Regions between pairs of tiles should be processed when two adjacent
      tiles become available
    - Regions between three tiles should be processed when they are available
"""

import json
import pytest
import asyncio
import asyncpg
from aio_pika import connect_robust, ExchangeType, Message
from src.utils import parse_config, parse_casda_credentials
from src.casda_subscriber import CASDASubscriber


SLEEP = 2
CASDA_EXCHANGE = 'aussrc.casda'
CASDA_QUEUE = 'aussrc.casda.wallaby'
WORKFLOW_EXCHANGE = 'aussrc.workflow.submit'
WALLABY_PROJECT_CODE = 'AS102'


@pytest.fixture
def config():
    return parse_config('./etc/test.config.ini')


@pytest.mark.asyncio
@pytest.fixture
async def database_pool(event_loop, config):
    db_dsn, _, _ = config
    pool = await asyncpg.create_pool(dsn=None, **db_dsn)
    yield pool
    await pool.expire_connections()


@pytest.mark.asyncio
@pytest.fixture
async def casda_queue(event_loop, config):
    _, r_dsn, _ = config
    conn = await connect_robust(r_dsn['dsn'], loop=event_loop)
    channel = await conn.channel()
    await channel.set_qos(prefetch_count=1)
    exchange = await channel.declare_exchange(
        CASDA_EXCHANGE,
        ExchangeType.FANOUT,
        durable=True
    )
    queue = await channel.declare_queue(name=CASDA_QUEUE, durable=True)
    yield queue, exchange
    await conn.close()


@pytest.mark.asyncio
@pytest.fixture
async def workflow_queue(event_loop, config):
    _, r_dsn, _ = config
    conn = await connect_robust(r_dsn['dsn'], loop=event_loop)
    channel = await conn.channel()
    await channel.set_qos(prefetch_count=1)
    exchange = await channel.declare_exchange(
        WORKFLOW_EXCHANGE,
        ExchangeType.DIRECT,
        durable=True
    )
    queue = await channel.declare_queue(
        name='aussrc.workflow.submit.pull',
        durable=True
    )
    await queue.bind(exchange, routing_key="")
    yield queue, exchange
    await queue.purge()
    await conn.close()


@pytest.mark.asyncio
@pytest.fixture
async def observation_NGC5044_3A(event_loop, casda_queue):
    """Submit an event for NGC5044 Tile 3A to the CASDA queue.

    """
    SBID = 31536
    _, exchange = casda_queue
    msg = Message(json.dumps({
        'sbid': str(SBID),
        'ra': '203.12948419',
        'dec': '-16.74992583',
        'project_code': 'AS102',
        'project_name': 'ASKAP Pilot Survey for WALLABY',
        'obs_start': '2022-07-06T04:34:24',
        'files': [
            'image.restored.i.NGC5044_3A_band2.SB31536.cube.contsub.fits',
            'weights.i.NGC5044_3A_band2.SB31536.cube.fits'
        ]
    }).encode())
    await exchange.publish(msg, routing_key="")
    return SBID


@pytest.mark.asyncio
async def test_receive_new_observation(config, event_loop, database_pool, workflow_queue, observation_NGC5044_3A):
    """On receiving a new WALLABY observation.
    Entry should appear in the database and trigger the footprint check pipeline.
    Tests behaviour of the casda_subscriber.py code.

    """
    db_dsn, r_dsn, pipeline = config
    casda_credentials = parse_casda_credentials()

    # Initialise subscriber
    subscriber = CASDASubscriber()
    await subscriber.setup(
        event_loop,
        db_dsn,
        r_dsn['dsn'],
        casda_credentials,
        pipeline['quality_check_key'],
        WALLABY_PROJECT_CODE
    )
    await subscriber.consume()
    await asyncio.sleep(SLEEP)
    await subscriber.close()

    # Test database entry
    async with database_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM wallaby.observation WHERE sbid=31536")
        assert(row is not None)

    # Assert message to workflow queue was received
    queue, exchange = workflow_queue
    assert(queue.declaration_result.message_count == 1)

    # TODO(assert message content)
    await queue.consume(lambda x: print(x.body.decode('utf-8')))

"""
Tests for job state updates and subsequent actions that should be triggered.

  Includes tests for:
    - State change for for postprocessing pipeline
    - State change for source_finding pipeline
    - State change for quality_check pipeline
"""

import json
import asyncio
import asyncpg
import pytest
from aio_pika import connect_robust, ExchangeType, Message
from src.utils import parse_config
from src.job_state_subscriber import JobStateSubscriber


SLEEP = 2
WORKFLOW_STATE_EXCHANGE = 'aussrc.workflow.state'
WORKFLOW_STATE_QUEUE = 'aussrc.workflow.state.email'


@pytest.fixture
def config():
    return parse_config('./etc/test.config.ini')


@pytest.mark.asyncio
@pytest.fixture
async def workflow_state_queue(event_loop, config):
    _, r_dsn, _ = config
    conn = await connect_robust(r_dsn['dsn'], loop=event_loop)
    channel = await conn.channel()
    await channel.set_qos(prefetch_count=1)
    exchange = await channel.declare_exchange(
        WORKFLOW_STATE_EXCHANGE,
        ExchangeType.FANOUT,
        durable=True
    )
    queue = await channel.declare_queue(
        name=WORKFLOW_STATE_QUEUE,
        durable=True
    )
    yield queue, exchange
    await conn.close()


@pytest.mark.asyncio
@pytest.fixture
async def job_complete_message(workflow_state_queue):
    msg = Message(
        json.dumps({
            'branch': 'main',
            'email': 'austin.shen@csiro.au',
            'host': 'carnaby.aussrc.org',
            'id': 1169,
            'name': 'ffe5a2a281440295e74ec10da6cbd43764744daabc485176d',
            'params': '{"REGION": "201.597021261625, 205.597021261625, -18.739472638526998, -14.739472638526998", "WEIGHTS": "/mnt/shared/wallaby/data/weights.i.NGC5044_3B_band2.SB40905.cube.fits,/mnt/shared/wallaby/data/weights.i.NGC5044_3A_band2.SB31536.cube.fits", "RUN_NAME": "204-17", "FOOTPRINTS": "/mnt/shared/wallaby/data/image.restored.i.NGC5044_3B_band2.SB40905.cube.contsub.fits, /mnt/shared/wallaby/data/image.restored.i.NGC5044_3A_band2.SB31536.cube.contsub.fits"}',  # noqa
            'pipeline_id': 23,
            'pipeline_key': 'faef1194-e7a0-48d7-b828-8ad23bf9097c',
            'profile': 'carnaby',
            'repository': 'https://github.com/AusSRC/WALLABY_pipeline',
            'sent': False,
            'slurm_job_id': 17077,
            'state': 'COMPLETED',
            'submitted': '2022-06-29 05:09:25.013961',
            'updated': '2022-06-29 05:09:29.526600',
            'username': 'ashen'
        }).encode()
    )
    _, exchange = workflow_state_queue
    await exchange.publish(
        msg,
        routing_key=""
    )
    return


@pytest.mark.asyncio
@pytest.fixture
async def job_running_message(workflow_state_queue):
    msg = Message(json.dumps({
        'branch': 'main',
        'email': 'austin.shen@csiro.au',
        'host': 'carnaby.aussrc.org',
        'id': 1169,
        'name': 'ffe5a2a281440295e74ec10da6cbd43764744daabc485176d',
        'params': '{"REGION": "201.597021261625, 205.597021261625, -18.739472638526998, -14.739472638526998", "WEIGHTS": "/mnt/shared/wallaby/data/weights.i.NGC5044_3B_band2.SB40905.cube.fits,/mnt/shared/wallaby/data/weights.i.NGC5044_3A_band2.SB31536.cube.fits", "RUN_NAME": "204-17", "FOOTPRINTS": "/mnt/shared/wallaby/data/image.restored.i.NGC5044_3B_band2.SB40905.cube.contsub.fits, /mnt/shared/wallaby/data/image.restored.i.NGC5044_3A_band2.SB31536.cube.contsub.fits"}',  # noqa
        'pipeline_id': 23,
        'pipeline_key': 'faef1194-e7a0-48d7-b828-8ad23bf9097c',
        'profile': 'carnaby',
        'repository': 'https://github.com/AusSRC/WALLABY_pipeline',
        'sent': False,
        'slurm_job_id': 17077,
        'state': 'RUNNING',
        'submitted': '2022-06-29 05:09:25.013961',
        'updated': '2022-06-29 05:09:29.526600',
        'username': 'ashen'
    }).encode())
    _, exchange = workflow_state_queue
    await exchange.publish(msg, routing_key="")
    return


@pytest.mark.asyncio
@pytest.fixture
async def database_pool(event_loop, config):
    db_dsn, _, _ = config
    pool = await asyncpg.create_pool(dsn=None, **db_dsn)
    yield pool
    await pool.expire_connections()


@pytest.mark.asyncio
@pytest.fixture
async def job_entry(event_loop, database_pool):
    async with database_pool.acquire() as conn:
        sanity_thresholds = json.dumps({'test': 'test'})
        await conn.execute(
            "INSERT INTO wallaby.run (name, sanity_thresholds) "
            "VALUES ('204-17', $1) "
            "ON CONFLICT DO NOTHING",
            sanity_thresholds
        )
        await conn.execute(
            "INSERT INTO wallaby.postprocessing (status, name) "
            "VALUES ('QUEUED', '204-17') "
            "ON CONFLICT (name) DO UPDATE "
            "SET status='QUEUED'"
        )


@pytest.mark.asyncio
async def test_receive_job_complete_message(event_loop, config, job_entry, job_complete_message, database_pool):
    """Test job on complete action.

    """
    db_dsn, r_dsn, _ = config

    # Run job state subscriber
    subscriber = JobStateSubscriber()
    await subscriber.setup(event_loop, db_dsn, r_dsn['dsn'])
    await subscriber.consume()
    await asyncio.sleep(SLEEP)
    await subscriber.close()

    # Assert database entry updated
    async with database_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM wallaby.postprocessing WHERE name='204-17'")
        assert(row['status'] == 'COMPLETED')

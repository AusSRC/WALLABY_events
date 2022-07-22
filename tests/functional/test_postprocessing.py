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
import logging
from aio_pika import connect_robust, ExchangeType, Message
from src.utils import parse_config, parse_casda_credentials, tile_mosaic_name
from src.casda_subscriber import CASDASubscriber
from src.postprocessing import process_observations


logging.basicConfig(level=logging.INFO)
SLEEP = 2
CASDA_EXCHANGE = 'aussrc.casda'
CASDA_QUEUE = 'aussrc.casda.wallaby'
WORKFLOW_EXCHANGE = 'aussrc.workflow.submit'
WORKFLOW_QUEUE = 'aussrc.workflow.submit.test'
WORKFLOW_ROUTING_KEY = 'pipeline'
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
    await queue.purge()
    yield queue, exchange
    await queue.purge()
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
    # TODO(austin): this does not need to be a durable queue
    queue = await channel.declare_queue(
        name=WORKFLOW_QUEUE,
        durable=True
    )
    await queue.bind(exchange, routing_key=WORKFLOW_ROUTING_KEY)
    await queue.purge()
    yield queue, exchange
    await queue.purge()
    await conn.close()


@pytest.mark.asyncio
@pytest.fixture
async def casda_subscriber(config, event_loop):
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
    yield
    await subscriber.close()


@pytest.mark.asyncio
@pytest.fixture
async def tiles(event_loop, database_pool):
    """Submit tile entries to the database.

    """
    tiles = [
        (197.733, -13.378, '198-13', 'NGC5044 Tile 1', 'Pilot 2'),
        (197.739, -18.775, '198-19', 'NGC5044 Tile 2', 'Pilot 2'),
        (203.597, -16.739, '204-17', 'NGC5044 Tile 3', 'Pilot 2'),
        (203.502, -22.368, '204-22', 'NGC5044 Tile 4', 'Pilot 2'),
        (195.132, 5.773, '195+06', 'NGC4808', 'Pilot 2')
    ]
    async with database_pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO wallaby.tile \
                (ra, dec, identifier, description, phase) \
            VALUES \
                ($1, $2, $3, $4, $5) \
            ON CONFLICT DO NOTHING",
            tiles
        )
    return


@pytest.mark.asyncio
@pytest.fixture
async def observation_NGC5044_3A(event_loop, casda_queue):
    """Submit an event for NGC5044 Tile 3A to the CASDA queue.

    """
    data = {
        'sbid': '31536',
        'ra': '203.12948419',
        'dec': '-16.74992583',
        'project_code': 'AS102',
        'project_name': 'ASKAP Pilot Survey for WALLABY',
        'obs_start': '2022-07-06T04:34:24',
        'files': [
            'image.restored.i.NGC5044_3A_band2.SB31536.cube.contsub.fits',
            'weights.i.NGC5044_3A_band2.SB31536.cube.fits'
        ]
    }
    _, exchange = casda_queue
    msg = Message(json.dumps(data).encode())
    await exchange.publish(msg, routing_key="")
    return data


@pytest.mark.asyncio
@pytest.fixture
async def observation_NGC5044_3B(event_loop, casda_queue):
    """Submit an event for NGC5044 Tile 3B to the CASDA queue.

    """
    data = {
        'sbid': '40905',
        'ra': '204.0645583',
        'dec': '-16.7290194',
        'project_code': 'AS102',
        'project_name': 'ASKAP Pilot Survey for WALLABY',
        'obs_start': '2022-07-06T04:34:24',
        'files': [
            'image.restored.i.NGC5044_3B_band2.SB40905.cube.contsub.fits',
            'weights.i.NGC5044_3B_band2.SB40905.cube.fits'
        ]
    }
    _, exchange = casda_queue
    msg = Message(json.dumps(data).encode())
    await exchange.publish(msg, routing_key="")
    return data


@pytest.mark.asyncio
async def test_receive_new_observation(config, event_loop, database_pool, workflow_queue, casda_subscriber, observation_NGC5044_3A):  # noqa
    """On receiving a new WALLABY observation.
    Entry should appear in the database and trigger the footprint check pipeline.
    Tests behaviour of the casda_subscriber.py code.

    """
    await asyncio.sleep(SLEEP)
    _, _, pipeline = config

    # Test database entry
    async with database_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM wallaby.observation WHERE sbid=$1",
            int(observation_NGC5044_3A['sbid']))
        assert(row is not None)

    # Assert message to workflow queue was received
    queue, _ = workflow_queue
    msg = await queue.get()
    body = json.loads(msg.body)
    assert(body['pipeline_key'] == pipeline['quality_check_key'])


@pytest.mark.asyncio
async def test_observation_pairs(config, event_loop, database_pool, casda_subscriber, workflow_queue, tiles, observation_NGC5044_3A, observation_NGC5044_3B):  # noqa
    """On receiving a two WALLABY observation for a given tile.
    Both entries should appear in the database and should trigger the first post-processing pipeline.
    Tests postprocessing.py code.

    """
    await asyncio.sleep(SLEEP)
    _, _, pipeline = config

    # pass quality check
    # TODO(austin): generate UUID from observations in test
    TILE_UUID = '204-17'
    async with database_pool.acquire() as conn:
        A = await conn.fetchrow(
            "SELECT * FROM wallaby.observation WHERE sbid = $1",
            int(observation_NGC5044_3A['sbid'])
        )
        B = await conn.fetchrow(
            "SELECT * FROM wallaby.observation WHERE sbid = $1",
            int(observation_NGC5044_3B['sbid'])
        )
        assert(A is not None and B is not None)
        await conn.execute(
            "UPDATE wallaby.observation SET \
            quality='PASSED', status='COMPLETED' \
            WHERE sbid IN ($1, $2)",
            int(observation_NGC5044_3A['sbid']),
            int(observation_NGC5044_3B['sbid'])
        )

    # run postprocessing
    queue, _ = workflow_queue
    await process_observations(event_loop, config=config)
    await asyncio.sleep(SLEEP)

    # assert tile entry updated
    async with database_pool.acquire() as conn:
        tile = await conn.fetchrow(
            "SELECT * FROM wallaby.tile WHERE identifier = $1",
            TILE_UUID
        )
        assert(tile['footprint_A'] is not None and tile['footprint_B'] is not None)

    # assert submit job into database
    async with database_pool.acquire() as conn:
        job = await conn.fetchrow(
            "SELECT * FROM wallaby.postprocessing WHERE name=$1",
            TILE_UUID
        )
        assert(job is not None)

    # assert each observation message
    msgA = await queue.get()
    assert(json.loads(msgA.body)['pipeline_key'] == pipeline['quality_check_key'])
    msgB = await queue.get()
    assert(json.loads(msgB.body)['pipeline_key'] == pipeline['quality_check_key'])

    # wait for postprocessing entry
    await asyncio.sleep(SLEEP)
    msg = await queue.get()
    body = json.loads(msg.body)
    files = f"{body['params']['FOOTPRINTS']}, {body['params']['WEIGHTS']}"
    assert(body['pipeline_key'] == pipeline['postprocessing_key'])
    assert(all(f in files for f in observation_NGC5044_3A['files']))
    assert(all(f in files for f in observation_NGC5044_3B['files']))


@pytest.mark.asyncio
async def test_incoming_tile(config, event_loop, database_pool, workflow_queue, tiles):  # noqa
    """New incoming tile entry. Expect processing of adjacent region between tiles
    and border regions on top and bottom where there are no neighbouring tiles.

    """
    _, _, pipeline = config
    TILE_3_UUID = '204-17'
    TILE_4_UUID = '204-22'
    name = tile_mosaic_name([TILE_3_UUID, TILE_4_UUID])
    obs_A = (25701, 203.2560, -22.144, 'image.restored.i.NGC5044_4A.SB25701.cube.contsub.fits', 'weights.i.NGC5044_4A.SB25701.cube.fits')  # noqa
    obs_B = (25750, 203.7480, -22.593, 'image.restored.i.NGC5044_4B.SB25750.cube.contsub.fits', 'weights.i.NGC5044_4B.SB25750.cube.fits')  # noqa

    # create necessary observations and tiles
    async with database_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO wallaby.observation (sbid, ra, dec, image_cube_file, weights_cube_file) \
            VALUES ($1, $2, $3, $4, $5) \
            ON CONFLICT DO NOTHING",
            *obs_A
        )
        await conn.execute(
            "INSERT INTO wallaby.observation (sbid, ra, dec, image_cube_file, weights_cube_file) \
            VALUES ($1, $2, $3, $4, $5) \
            ON CONFLICT DO NOTHING",
            *obs_B
        )
        A = await conn.fetchrow(
            "SELECT * FROM wallaby.observation WHERE sbid = $1",
            obs_A[0]
        )
        B = await conn.fetchrow(
            "SELECT * FROM wallaby.observation WHERE sbid = $1",
            obs_B[0]
        )
        await conn.execute(
            "UPDATE wallaby.tile SET \
            image_cube_file = 'cube.a.fits', weights_cube_file = 'weights.a.fits' \
            WHERE identifier = $1",
            TILE_3_UUID
        )
        await conn.execute(
            'UPDATE wallaby.tile SET \
            image_cube_file = \'cube.b.fits\', weights_cube_file = \'weights.b.fits\', \
            "footprint_A" = $2, "footprint_B" = $3 \
            WHERE identifier = $1',
            TILE_4_UUID, A['id'], B['id']
        )
        await conn.execute(
            "UPDATE wallaby.postprocessing SET status = 'COMPLETED' WHERE name = $1",
            TILE_3_UUID
        )

    # assert tile entry updated
    async with database_pool.acquire() as conn:
        tile = await conn.fetchrow(
            "SELECT * FROM wallaby.tile WHERE identifier = $1",
            TILE_3_UUID
        )
        assert(tile['footprint_A'] is not None and tile['footprint_B'] is not None)

    # run postprocessing
    queue, _ = workflow_queue
    await process_observations(event_loop, config=config)
    await asyncio.sleep(SLEEP)

    # assert postprocessing entry created
    async with database_pool.acquire() as conn:
        job = await conn.fetchrow(
            "SELECT * FROM wallaby.postprocessing WHERE name = $1",
            name
        )
        assert(job is not None)

    # assert adjacent region job submission
    msg = await queue.get()
    body = json.loads(msg.body)
    assert(name == body['params']['RUN_NAME'])
    assert(body['pipeline_key'] == pipeline['postprocessing_key'])

    # TODO(austin): assert border regions job submission

    # undo status change
    async with database_pool.acquire() as conn:
        await conn.execute(
            "UPDATE wallaby.postprocessing SET status = 'QUEUED' WHERE name = $1",
            TILE_3_UUID
        )


@pytest.mark.asyncio
async def test_tile_group(config, event_loop, database_pool, workflow_queue, tiles):  # noqa
    """Test new incoming tile forming a group.
    Should process region between three tiles and additonal boundary regions for the
    new tile.

    """
    _, _, pipeline = config
    TILE_2_UUID = '198-19'
    TILE_3_UUID = '204-17'
    TILE_4_UUID = '204-22'
    name = tile_mosaic_name([TILE_2_UUID, TILE_3_UUID, TILE_4_UUID])
    obs_A = (34166, 197.500, -18.550, 'image.restored.i.NGC5044_2A.SB34166.cube.contsub.fits', 'weights.i.NGC5044_2A.SB34166.cube.fits')  # noqa
    obs_B = (34275, 197.979, -18.999, 'image.restored.i.NGC5044_2B.SB34275.cube.contsub.fits', 'weights.i.NGC5044_2B.SB34275.cube.fits')  # noqa

    # create necessary observations and tiles
    async with database_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO wallaby.observation (sbid, ra, dec, image_cube_file, weights_cube_file) \
            VALUES ($1, $2, $3, $4, $5) \
            ON CONFLICT DO NOTHING",
            *obs_A
        )
        await conn.execute(
            "INSERT INTO wallaby.observation (sbid, ra, dec, image_cube_file, weights_cube_file) \
            VALUES ($1, $2, $3, $4, $5) \
            ON CONFLICT DO NOTHING",
            *obs_B
        )
        A = await conn.fetchrow(
            "SELECT * FROM wallaby.observation WHERE sbid = $1",
            obs_A[0]
        )
        B = await conn.fetchrow(
            "SELECT * FROM wallaby.observation WHERE sbid = $1",
            obs_B[0]
        )
        await conn.execute(
            "UPDATE wallaby.tile SET \
            image_cube_file = 'cube.c.fits', weights_cube_file = 'weights.c.fits' \
            WHERE identifier = $1",
            TILE_2_UUID
        )
        await conn.execute(
            'UPDATE wallaby.tile SET \
            image_cube_file = \'cube.c.fits\', weights_cube_file = \'weights.c.fits\', \
            "footprint_A" = $2, "footprint_B" = $3 \
            WHERE identifier = $1',
            TILE_2_UUID, A['id'], B['id']
        )
        await conn.execute(
            "UPDATE wallaby.postprocessing SET status = 'COMPLETED' WHERE name IN ($1, $2, $3)",
            TILE_3_UUID, TILE_4_UUID, tile_mosaic_name([TILE_3_UUID, TILE_4_UUID])
        )

    # run postprocessing
    queue, _ = workflow_queue
    await process_observations(event_loop, config=config)
    await asyncio.sleep(SLEEP)

    # assert postprocessing job submitted
    async with database_pool.acquire() as conn:
        job = await conn.fetchrow(
            "SELECT * FROM wallaby.postprocessing WHERE name = $1",
            name
        )
        assert(job is not None)

    # assert messages published
    msg = await queue.get()
    body = json.loads(msg.body)
    assert(name == body['params']['RUN_NAME'])
    assert(body['pipeline_key'] == pipeline['postprocessing_key'])

    # undo status change
    async with database_pool.acquire() as conn:
        await conn.execute(
            "UPDATE wallaby.postprocessing SET status = 'QUEUED' WHERE name IN ($1, $2, $3)",
            TILE_3_UUID, TILE_4_UUID, tile_mosaic_name([TILE_3_UUID, TILE_4_UUID])
        )

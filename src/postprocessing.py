#!/usr/bin/env python3

import os
import logging
import math
import json
import asyncio
import asyncpg

from utils import parse_config, generate_tile_uuid, region_from_tile_centre
from logic import get_observation_pairs, get_adjacent_tiles, get_tile_groups, \
                  adjacent_below, adjacent_above, adjacent_right, adjacent_left
from workflow_publisher import WorkflowPublisher


logging.basicConfig(level=logging.INFO)


def add_footprints_to_tiles(observations, pairs):
    """Method for cross matching pairs of observations with pre-filled tiles.
    TODO(austin): Current method cannot distinguish between footprint A and B

    """
    # get central RA and Dec for pair
    match = []
    for pair in pairs:
        i, j = pair
        A_id = observations[i].get('id')
        B_id = observations[j].get('id')
        ra_centre = (float(observations[i].get('ra')) + float(observations[j].get('ra'))) / 2.0
        dec_centre = (float(observations[i].get('dec')) + float(observations[j].get('dec'))) / 2.0
        uuid = generate_tile_uuid(ra_centre, dec_centre)
        match.append((A_id, B_id, uuid))
    return match


async def centre_regions(conn, publisher, pipeline_key, res):
    """Identify centre regions from observations.
    1. Match observation pairs (two footprints for a given tile)
    2. Update tiles with footprint pairs
    3. Check if post-processing job has been run before for the tile centre region
    4. Run post-processing pipeline

    """
    pairs = get_observation_pairs(res)
    matches = add_footprints_to_tiles(res, pairs)

    for v in list(zip(pairs, matches)):
        pair, match = v
        A_idx, B_idx = pair
        A_id, B_id, uuid = match
        logging.info(
            f"Found observation pairs for tile {uuid}: ({A_id}, {B_id}) \
            with list index values ({A_idx}, {B_idx}) in PostgreSQL query response"
        )

        # Update database
        await conn.fetch(
            'UPDATE wallaby.tile \
            SET "footprint_A" = $1, "footprint_B" = $2 \
            WHERE identifier = $3',
            A_id, B_id, uuid
        )

        # See if there is a completed post-processing job for the same tile
        completed_job = await conn.fetch(
            "SELECT * FROM wallaby.postprocessing \
            WHERE name = $1 AND status = 'COMPLETED'",
            uuid
        )

        # Submit a post-processing job
        if not completed_job:
            ra_centre = (float(res[A_idx]['ra']) + float(res[B_idx]['ra'])) / 2
            dec_centre = (float(res[A_idx]['dec']) + float(res[B_idx]['dec'])) / 2
            region = region_from_tile_centre(ra_centre, dec_centre)
            logging.info(f"Submitting job for centre region of tile {uuid}")
            params = {
                'username': 'ashen',
                'pipeline_key': pipeline_key,
                'params': {
                    "RUN_NAME": uuid,
                    "REGION": region,
                    "FOOTPRINTS": f"{res[A_idx]['image_cube_file']}, {res[B_idx]['image_cube_file']}",
                    "WEIGHTS": f"{res[A_idx]['weights_cube_file']}, {res[B_idx]['weights_cube_file']}"
                }
            }
            logging.info(f"Job parameters: {params}")
            msg = json.dumps(params).encode()
            await publisher.publish(msg)

            # Write entry to database
            await conn.execute(
                "INSERT INTO wallaby.postprocessing \
                (name, status, region) \
                VALUES ($1, $2, $3) \
                ON CONFLICT ON CONSTRAINT postprocessing_name_key \
                DO NOTHING;",
                uuid, "QUEUED", region
            )
            logging.info(f"Adding postprocessing entry with name={uuid} into the WALLABY database.")
        else:
            logging.info(f"Tile {uuid} already processed, skipping.")


async def declination_band(conn, publisher, pipeline_key, res):
    """Process region between adjacent tiles.
    1. Get pairs of adjacent tiles
    2. Define region between tile pairs
    3. Run post-processing pipeline

    """
    pairs = get_adjacent_tiles(res)
    for (i, j) in pairs:
        # Check job not complete
        # TODO(austin): reverse name check may not be required
        tileA = res[i]
        tileB = res[j]
        name = f"{tileA['identifier']}_{tileB['identifier']}"
        reverse_name = f"{tileB['identifier']}_{tileA['identifier']}"
        completed_job = await conn.fetch(
            "SELECT * FROM wallaby.postprocessing \
            WHERE (name = $1 OR name = $2) AND status = 'COMPLETED'",
            name, reverse_name
        )
        if not completed_job:
            logging.info(
                f"Submitting job for adjacent region between tiles \
                {tileA['identifier']} and {tileB['identifier']}."
            )

            # Identify region
            ra_centre = (float(tileA['ra']) + float(tileB['ra'])) / 2.0
            dec_centre = (float(tileA['dec']) + float(tileB['dec'])) / 2.0
            region = region_from_tile_centre(ra_centre, dec_centre)

            # Submit job
            params = {
                'username': 'ashen',
                'pipeline_key': pipeline_key,
                'params': {
                    "RUN_NAME": name,
                    "REGION": region,
                    "FOOTPRINTS": f"{tileA['image_cube_file']}, {tileB['image_cube_file']}",
                    "WEIGHTS": f"{tileA['weights_cube_file']}, {tileB['weights_cube_file']}"
                }
            }
            logging.info(f"Job parameters: {params}")
            msg = json.dumps(params).encode()
            await publisher.publish(msg)

            # Write entry to database
            await conn.execute(
                "INSERT INTO wallaby.postprocessing \
                (name, status, region) \
                VALUES ($1, $2, $3) \
                ON CONFLICT ON CONSTRAINT postprocessing_name_key \
                DO NOTHING;",
                name, "QUEUED", region
            )
            logging.info(f"Adding postprocessing entry with name={name} into the WALLABY database.")
        else:
            logging.info(
                f"Adjacent region between {tileA['identifier']} and {tileB['identifier']} already processed, skipping."
            )


async def outer_region_single_tile(conn, publisher, pipeline_key, res, phase):
    """Run post-processing on regions of tiles with no adjacent tiles.
    1. Get tile that has been processed
    2. Compare against all expected tiles to see if it is adjacent
    3. If no adjacent on either side, submit a post-processing job for that side of the cube.

    """
    expected_tiles = await conn.fetch(
        "SELECT * FROM wallaby.tile WHERE phase = $1",
        phase
    )
    for tile in res:
        # Compare against all other tiles
        c_i = (math.radians(float(tile['ra'])), math.radians(float(tile['dec'])))
        has_above = False
        has_below = False
        has_left = False
        has_right = False
        for t in expected_tiles:
            c = (math.radians(float(t['ra'])), math.radians(float(t['dec'])))
            if adjacent_above(c_i, c):
                has_above = True
            if adjacent_below(c_i, c):
                has_below = True
            if adjacent_left(c_i, c):
                has_left = True
            if adjacent_right(c_i, c):
                has_right = True

        # Submit jobs if there is an outer region
        if not has_above:
            run_name = f"{tile['identifier']}_a"
            completed_job = await conn.fetch(
                "SELECT * FROM wallaby.postprocessing \
                WHERE (name = $1) AND status = 'COMPLETED'",
                run_name
            )
            if not completed_job:
                logging.info(f"Processing top outer region for tile {tile['identifier']}")
                ra_centre = float(tile['ra'])
                dec_centre = float(tile['dec']) + 3.0
                region = region_from_tile_centre(ra_centre, dec_centre)
                params = {
                    'username': 'ashen',
                    'pipeline_key': pipeline_key,
                    'params': {
                        "RUN_NAME": run_name,
                        "REGION": region,
                        "IMAGE_CUBE": tile['image_cube_file'],
                    }
                }
                logging.info(f"Job parameters: {params}")
                msg = json.dumps(params).encode()
                await publisher.publish(msg)

                # Write entry to database
                await conn.execute(
                    "INSERT INTO wallaby.postprocessing \
                    (name, status, region) \
                    VALUES ($1, $2, $3) \
                    ON CONFLICT ON CONSTRAINT postprocessing_name_key \
                    DO NOTHING;",
                    run_name, "QUEUED", region
                )
                logging.info(f"Adding postprocessing entry with name={run_name} into the WALLABY database.")
        if not has_below:
            run_name = f"{tile['identifier']}_b"
            completed_job = await conn.fetch(
                "SELECT * FROM wallaby.postprocessing \
                WHERE (name = $1) AND status = 'COMPLETED'",
                run_name
            )
            if not completed_job:
                logging.info(f"Processing bottom outer region for tile {tile['identifier']}")
                ra_centre = float(tile['ra'])
                dec_centre = float(tile['dec']) - 3.0
                region = region_from_tile_centre(ra_centre, dec_centre)
                params = {
                    'username': 'ashen',
                    'pipeline_key': pipeline_key,
                    'params': {
                        "RUN_NAME": run_name,
                        "REGION": region,
                        "IMAGE_CUBE": tile['image_cube_file'],
                    }
                }
                logging.info(f"Job parameters: {params}")
                msg = json.dumps(params).encode()
                await publisher.publish(msg)

                # Write entry to database
                await conn.execute(
                    "INSERT INTO wallaby.postprocessing \
                    (name, status, region) \
                    VALUES ($1, $2, $3) \
                    ON CONFLICT ON CONSTRAINT postprocessing_name_key \
                    DO NOTHING;",
                    run_name, "QUEUED", region
                )
                logging.info(f"Adding postprocessing entry with name={run_name} into the WALLABY database.")
        if not has_left:
            run_name = f"{tile['identifier']}_l"
            completed_job = await conn.fetch(
                "SELECT * FROM wallaby.postprocessing \
                WHERE (name = $1) AND status = 'COMPLETED'",
                run_name
            )
            if not completed_job:
                logging.info(f"Processing left outer region for tile {tile['identifier']}")
                ra_centre = float(tile['ra']) + 3.0
                dec_centre = float(tile['dec'])
                region = region_from_tile_centre(ra_centre, dec_centre)
                params = {
                    'username': 'ashen',
                    'pipeline_key': pipeline_key,
                    'params': {
                        "RUN_NAME": run_name,
                        "REGION": region,
                        "IMAGE_CUBE": tile['image_cube_file'],
                    }
                }
                logging.info(f"Job parameters: {params}")
                msg = json.dumps(params).encode()
                await publisher.publish(msg)

                # Write entry to database
                await conn.execute(
                    "INSERT INTO wallaby.postprocessing \
                    (name, status, region) \
                    VALUES ($1, $2, $3) \
                    ON CONFLICT ON CONSTRAINT postprocessing_name_key \
                    DO NOTHING;",
                    run_name, "QUEUED", region
                )
                logging.info(f"Adding postprocessing entry with name={run_name} into the WALLABY database.")
        if not has_right:
            run_name = f"{tile['identifier']}_r"
            completed_job = await conn.fetch(
                "SELECT * FROM wallaby.postprocessing \
                WHERE (name = $1) AND status = 'COMPLETED'",
                run_name
            )
            if not completed_job:
                logging.info(f"Processing right outer region for tile {tile['identifier']}")
                ra_centre = float(tile['ra']) - 3.0
                dec_centre = float(tile['dec'])
                region = region_from_tile_centre(ra_centre, dec_centre)
                params = {
                    'username': 'ashen',
                    'pipeline_key': pipeline_key,
                    'params': {
                        "RUN_NAME": run_name,
                        "REGION": region,
                        "IMAGE_CUBE": tile['image_cube_file'],
                    }
                }
                logging.info(f"Job parameters: {params}")
                msg = json.dumps(params).encode()
                await publisher.publish(msg)

                # Write entry to database
                await conn.execute(
                    "INSERT INTO wallaby.postprocessing \
                    (name, status, region) \
                    VALUES ($1, $2, $3) \
                    ON CONFLICT ON CONSTRAINT postprocessing_name_key \
                    DO NOTHING;",
                    run_name, "QUEUED", region
                )
                logging.info(f"Adding postprocessing entry with name={run_name} into the WALLABY database.")


async def adjacent_regions_three_tiles(conn, publisher, pipeline_key, res, phase):
    """Run post-processing pipeline on regions between three adjacent tiles where
    the data is available.
    1. Find adjacent pairs of tiles
    2. Check if tile pairs have another tile perpendicular to that pair
    3. Run postprocessing pipeline

    """
    groups = get_tile_groups(res)
    for (i, j, k) in groups:
        # Check job not complete
        tile_i = res[i]
        tile_j = res[j]
        tile_k = res[k]
        name_i = tile_i['identifier']
        name_j = tile_j['identifier']
        name_k = tile_k['identifier']
        logging.info(
            f'Found adjacent tiles ({name_i}, {name_j}, {name_k})'
        )
        name = f"{name_i}_{name_j}_{name_k}"
        completed_job = await conn.fetch(
            "SELECT * FROM wallaby.postprocessing \
            WHERE (name = $1) AND status = 'COMPLETED'",
            name
        )
        if not completed_job:
            logging.info(
                f"Submitting job for region between group of tiles tiles \
                ({name_i}, {name_j}, {name_k})."
            )

            # Identify region
            # TODO(austin): centre calculated incorrectly...
            ra_centre = ((float(tile_i['ra']) + float(tile_j['ra'])) / 2.0 + float(tile_k['ra'])) / 2.0
            dec_centre = ((float(tile_i['dec']) + float(tile_j['dec'])) / 2.0 + float(tile_k['dec'])) / 2.0
            region = region_from_tile_centre(ra_centre, dec_centre)

            # Submit job
            params = {
                'username': 'ashen',
                'pipeline_key': pipeline_key,
                'params': {
                    "RUN_NAME": name,
                    "REGION": region,
                    "FOOTPRINTS":
                        f"{tile_i['image_cube_file']}, {tile_j['image_cube_file']}, {tile_k['image_cube_file']}",
                    "WEIGHTS":
                        f"{tile_i['weights_cube_file']}, {tile_j['weights_cube_file']}, {tile_k['weights_cube_file']}"
                }
            }
            logging.info(f"Job parameters: {params}")
            msg = json.dumps(params).encode()
            await publisher.publish(msg)

            # Write entry to database
            await conn.execute(
                "INSERT INTO wallaby.postprocessing \
                (name, status, region) \
                VALUES ($1, $2, $3) \
                ON CONFLICT ON CONSTRAINT postprocessing_name_key \
                DO NOTHING;",
                name, "QUEUED", region
            )
            logging.info(f"Adding postprocessing entry with name={name} into the WALLABY database.")
        else:
            logging.info(
                f"Group region between ({name_i}, {name_j}, {name_k}) already processed, skipping."
            )


# TODO(austin): Message format in logs
async def process_observations(loop):
    """Run post-processing pipeline on observations as they become avaialable in the database.

    """
    PHASE = "Pilot 2"
    conn = None
    db_dsn, r_dsn, workflow_keys = parse_config()
    try:
        # Set up database and rabbitMQ connections
        conn = await asyncpg.connect(dsn=None, **db_dsn)
        publisher = WorkflowPublisher()
        await publisher.setup(loop, r_dsn['dsn'])

        # Region 1
        logging.info("Processing centre regions of tiles")
        obs_res = await conn.fetch(
            f"SELECT * FROM wallaby.observation \
            WHERE phase = '{PHASE}' \
            AND quality = 'PASSED'"
        )
        logging.info(f"Found {len(obs_res)} observations in the database")
        for obs in obs_res:
            logging.info(f"Observation: {obs}")
        await centre_regions(conn, publisher, workflow_keys['postprocessing_key'], obs_res)

        # Region 2
        logging.info("Processing adjacent tiles in declination bands")
        tile_res = await conn.fetch(
            f"SELECT * FROM wallaby.tile WHERE phase = '{PHASE}' AND image_cube_file IS NOT NULL"
        )
        logging.info(f"Found {len(tile_res)} tiles in the database")
        for t in tile_res:
            logging.info(f"Tile: {t}")
        await declination_band(conn, publisher, workflow_keys['postprocessing_key'], tile_res)

        # Region 3
        logging.info("Processing region between three adjancent tiles")
        await adjacent_regions_three_tiles(conn, publisher, workflow_keys['postprocessing_key'], tile_res, PHASE)

        # Region 4
        logging.info("Processing outer regions of tiles with no other adjacent tiles")
        await outer_region_single_tile(conn, publisher, workflow_keys['source_finding_key'], tile_res, PHASE)

        return
    except Exception:
        raise
    finally:
        if conn:
            await conn.close()


async def _repeat(loop):
    """Run process_centre_regions periodically

    """
    INTERVAL = os.getenv('POSTPROCESSING_INTERAL', 3600)
    while True:
        await process_observations(loop)
        await asyncio.sleep(INTERVAL)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_repeat(loop))

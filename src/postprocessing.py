#!/usr/bin/env python3

import logging
import sys
import json
import asyncio
import asyncpg
import datetime
import numpy as np

from utils import parse_config, generate_tile_uuid
from workflow_publisher import WorkflowPublisher


logging.basicConfig(level=logging.INFO)


def get_footprint_pairs(observations, distance_threshold=0.7):
    """Method for fetching pairs of footprints from observations in the WALLABY database.
    TODO(austin): are the RA and Dec values for beam 0 or the centre of the footprint?
    
    """
    N = len(observations)
        
    # calculate distance between points
    coord_array = np.array([[float(r['ra']), float(r['dec'])] for r in observations])
    distance_matrix = np.zeros((N, N))
    for i in range(N):
        for j in range(N):
            # TODO(austin): use angular separation rather than euclidean distance
            distance_matrix[i, j] = np.linalg.norm(coord_array[i] - coord_array[j])

    # identify pairs based on threshold
    pair_matrix = (0 < distance_matrix) & (distance_matrix < distance_threshold)
    footprint_pairs = []
    for i in range(N):
        for j in range(i):
            if pair_matrix[i][j]:
                footprint_pairs.append((i, j))

    return footprint_pairs

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

def get_adjacent_tiles(tiles):
    """Find adjacent tiles in declination bands to process.
    Returns pairs as list of indices for database query result list.

    """
    # TODO(austin): Get this from somewhere else (imported constants?)
    ra_threshold = 7.0
    decimal = 1
    pairs = []
    for i in range(len(tiles)):
        for j in range(i):
            if i == j:
                continue
            # TODO(austin): Is it safe to compare just integer values for RA/Dec?
            ra_A = round(float(tiles[i]['ra']), decimal)
            dec_A = round(float(tiles[i]['dec']), decimal)
            ra_B = round(float(tiles[j]['ra']), decimal)
            dec_B = round(float(tiles[j]['dec']), decimal)
            
            # Logic for determining if tiles are adjacent
            # TODO(austin): Flip ra and dec in this logic for full survey
            # if (dec_A == dec_B) and (abs(ra_A - ra_B) < ra_threshold):
            if (ra_A == ra_B) and (abs(dec_A - dec_B) < ra_threshold):
                logging.info(f'Found adjacent tiles {tiles[i]["identifier"]} and {tiles[j]["identifier"]}')
                pairs.append((i, j))
    return pairs

async def centre_regions(conn, publisher, pipeline_key, res):
    """Identify centre regions from observations.
    1. Match observation pairs (two footprints for a given tile)
    2. Update tiles with footprint pairs
    3. Check if post-processing job has been run before for the tile centre region
    4. Run post-processing pipeline

    """
    pairs = get_footprint_pairs(res)
    matches = add_footprints_to_tiles(res, pairs)

    for v in list(zip(pairs, matches)):
        pair, match = v
        A_idx, B_idx = pair
        A_id, B_id, uuid = match
        logging.info(f"Found observation pairs for tile {uuid}: ({A_id}, {B_id}) with list index values ({A_idx}, {B_idx}) in PostgreSQL query response")

        # Update database
        await conn.fetch(
            'UPDATE wallaby.tile \
            SET "footprint_A" = $1, "footprint_B" = $2 \
            WHERE wallaby.tile.identifier = $3',
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
            region = f"{ra_centre - 2.0}, {ra_centre + 2.0}, {dec_centre - 2.0}, {dec_centre + 2.0}"
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
            logging.info(f"Submitting job for adjacent region between tiles {tileA['identifier']} and {tileB['identifier']}.")

            # Identify region
            ra_centre = (float(tileA['ra']) + float(tileB['ra'])) / 2.0
            dec_centre = (float(tileA['dec']) + float(tileB['dec'])) / 2.0
            region = f"{ra_centre - 2.0}, {ra_centre + 2.0}, {dec_centre - 2.0}, {dec_centre + 2.0}"

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
            logging.info(f"Adjacent region between {tileA} and {tileB} already processed, skipping.")

async def process_observations(loop):
    """Run post-processing pipeline on observations as they become avaialable in the database.
    2. Process central regions
    3. Process declination band regions
    
    """
    PHASE = "Pilot 2"
    conn = None
    db_dsn, r_dsn, workflow_keys = parse_config()
    pipeline_key = workflow_keys['postprocessing_key']
    try:
        # Set up database and rabbitMQ connections
        conn = await asyncpg.connect(dsn=None, **db_dsn)
        publisher = WorkflowPublisher()
        await publisher.setup(loop, r_dsn)

        # 1. Processing centre regions of tiles
        logging.info("Processing centre regions of tiles")
        obs_res = await conn.fetch(
            f"SELECT * FROM wallaby.observation \
            WHERE phase = '{PHASE}' \
            AND quality = 'PASSED'"
        )
        logging.info(f"Found {len(obs_res)} observations in the database")
        for obs in obs_res:
            logging.info(f"Observation: {obs}")
        await centre_regions(conn, publisher, pipeline_key, obs_res)

        # 2. Process adjacent tiles in declination bands
        logging.info("Processing adjacent tiles in declination bands")
        tile_res = await conn.fetch(
            f"SELECT * FROM wallaby.tile WHERE phase = '{PHASE}' AND image_cube_file IS NOT NULL"
        )
        logging.info(f"Found {len(tile_res)} observations in the database")
        for t in tile_res:
            logging.info(f"Tile: {t}")
        await declination_band(conn, publisher, pipeline_key, tile_res)
        
        return
    except Exception as e:
        raise
    finally:
        if conn:
            await conn.close()

async def _repeat(loop):
    """Run process_centre_regions periodically

    """
    # TODO(austin): Update interval to something meaningful
    INTERVAL = 500
    while True:
        await process_observations(loop)
        await asyncio.sleep(INTERVAL)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_repeat(loop))

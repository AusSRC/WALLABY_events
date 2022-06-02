#!/usr/bin/env python3

import logging
import sys
import json
import asyncio
import asyncpg
import datetime
import numpy as np

from utils import parse_config
from workflow_publisher import WorkflowPublisher


logging.basicConfig(level=logging.INFO)


def generate_tile_uuid(ra, dec):
    """Function to generate the WALLABY tile UUID from RA and Dec.
    
    """
    return f"{str(int(round(ra))).rjust(3, '0')}{'+' if dec >= 0 else '-'}{str(int(abs(round(dec)))).rjust(2, '0')}"

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

async def process_centre_regions(loop):
    """Run post-processing pipeline on centre regions as they become avaialable.
    1. Retreive observations from database
    2. Match observation pairs (two footprints for a given tile)
    3. Update tiles with footprint pairs
    4. Check if post-processing job has been run before for the tile centre region
    5. Run post-processing pipeline
    
    """
    db_dsn, r_dsn, workflow_keys = parse_config()
    conn = None
    try:
        conn = await asyncpg.connect(dsn=None, **db_dsn)
        res = await conn.fetch(
            "SELECT * FROM wallaby.observation \
            WHERE phase = 'Pilot 2' \
            AND quality = 'PASSED'"
        )
        for obs in res:
            logging.info(f"Observation: {obs}")
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

            # Set up publisher
            publisher = WorkflowPublisher()
            await publisher.setup(loop, r_dsn)

            # Submit a post-processing job
            if not completed_job:
                ra_centre = (float(res[A_idx]['ra']) + float(res[B_idx]['ra'])) / 2
                dec_centre = (float(res[A_idx]['dec']) + float(res[B_idx]['dec'])) / 2
                region = f"{ra_centre - 2.0}, {ra_centre + 2.0}, {dec_centre - 2.0}, {dec_centre + 2.0}"
                logging.info(f"Submitting job for tile {uuid} to process region: {region}")
                params = {
                    'username': 'ashen',
                    'pipeline_key': workflow_keys['postprocessing_key'],
                    'params': {
                        "RUN_NAME": uuid,
                        "REGION": region,
                        "FOOTPRINTS": f"{res[A_idx]['image_cube_file']}, {res[B_idx]['image_cube_file']}",
                        "WEIGHTS": f"{res[A_idx]['weights_cube_file']}, {res[B_idx]['weights_cube_file']}"
                    }
                }
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
        return
    except Exception as e:
        raise
    finally:
        if conn:
            await conn.close()

async def _repeat(loop):
    """Run process_centre_regions periodically

    """
    INTERVAL = 500

    while True:
        await process_centre_regions(loop)
        await asyncio.sleep(INTERVAL)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_repeat(loop))

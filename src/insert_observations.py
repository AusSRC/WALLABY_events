#!/usr/bin/env python3

import sys
import json
import asyncio
import asyncpg
import optparse
import configparser
import datetime
from astroquery.utils.tap.core import TapPlus


URL = 'https://casda.csiro.au/casda_vo_tools/tap'
TAP_QUERY = "SELECT * FROM casda.observation_event as e LEFT JOIN ivoa.obscore o ON e.sbid = o.obs_id WHERE project_code = 'AS102' AND (filename LIKE 'image.restored.i.%.cube.contsub.fits' OR filename LIKE 'weights.i.%.cube.fits')"


async def insert_observations():
    """Add CASDA observations from TAP query to the WALLABY database observations table.
    To be done once at initialisation.

    """
    args = optparse.OptionParser()
    args.add_option('-c', dest="config", default="./config.ini")
    options, arguments = args.parse_args()
    parser = configparser.ConfigParser()
    parser.read(options.config)

    database_creds = parser['Database']
    dsn = {
        'host': database_creds['host'],
        'port': database_creds['port'],
        'user': database_creds['user'],
        'password': database_creds['password'],
        'database': database_creds['database']
    }

    # TAP query
    loop = asyncio.get_event_loop()
    tap = await loop.run_in_executor(None, TapPlus, URL)
    results = await loop.run_in_executor(None, tap.launch_job_async, TAP_QUERY)

    # produce map of SBIDs and corresponding data products
    # TODO(austin): check the RA and Dec values are for the image cubes
    results_map = {}
    for r in results.get_results():
        sbid = r['sbid']
        val = results_map.get(sbid, None)
        if not val:
            results_map[sbid] = {
                'sbid': sbid,
                'project_code': r['project_code'],
                'project_name': r['project_name'],
                'ra': r['s_ra'],
                'dec': r['s_dec'],
                'obs_start': datetime.datetime.strptime(r['obs_start'], "%Y-%m-%dT%H:%M:%S.%fZ").isoformat(),
                'files': [r['filename']]
            }
        else:
            if r['filename'] not in val['files']:
                val['files'].append(r['filename'])

    # Write observations to database
    conn = None
    try:
        conn = await asyncpg.connect(dsn=None, **dsn)
        async with conn.transaction():
            await conn.executemany(
                "INSERT INTO wallaby.observation \
                    (sbid, ra, dec) \
                    VALUES ($1, $2, $3) \
                    ON CONFLICT ON CONSTRAINT observation_sbid_key \
                    DO NOTHING; \
                ",
                [(k, v['ra'], v['dec']) for k, v in results_map.items()]
            )
    except Exception as e:
        raise
    finally:
        if conn:
            await conn.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(insert_observations())
    finally:
        loop.close()

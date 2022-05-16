#!/usr/bin/env python3

import sys
import json
import asyncio
import asyncpg
import optparse
import configparser
import datetime

from astroquery.utils.tap.core import TapPlus
from aio_pika import connect_robust, Message, DeliveryMode, ExchangeType


URL = 'https://casda.csiro.au/casda_vo_tools/tap'
TAP_QUERY = "SELECT * FROM casda.observation_event as e LEFT JOIN ivoa.obscore o ON e.sbid = o.obs_id WHERE project_code = 'AS102' AND (filename LIKE 'image.restored.i.%.cube.contsub.fits' OR filename LIKE 'weights.i.%.cube.fits')"
QUEUE = "aussrc.casda.wallaby"
EXCHANGE = "aussrc.casda"
ROUTING_KEY = "casda"


async def fetch_entries():
    """Retrieve all observations for WALLABY from CASDA TAP (historic), publish them
    to the workflow queue and add entries into the database.
    This should only be run once for initialising the WALLABY observations database.
    Required because the CASDA publisher only fetches new (after the most recent) TAP entries.

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
    results_map = {}
    for r in results.get_results():
        sbid = r['sbid']
        val = results_map.get(sbid, None)
        if not val:
            results_map[sbid] = {
                'sbid': sbid,
                'project_code': r['project_code'],
                'project_name': r['project_name'],
                'obs_start': datetime.datetime.strptime(r['obs_start'], "%Y-%m-%dT%H:%M:%S.%fZ").isoformat(),
                'files': [r['filename']]
            }
        else:
            if r['filename'] not in val['files']:
                val['files'].append(r['filename'])

    sys.exit()

    # Publish to channel
    rabbitmq_conn = await connect_robust(parser['Rabbitmq']['dsn'])
    channel = await rabbitmq_conn.channel()
    queue = await channel.declare_queue(QUEUE)
    exchange = await channel.declare_exchange(EXCHANGE, ExchangeType.FANOUT, durable=True)
    for key, value in results_map.items():
        message = Message(
            json.dumps(value).encode(),
            delivery_mode=DeliveryMode.PERSISTENT
        )
        await exchange.publish(message, routing_key=ROUTING_KEY)
        sys.exit()

    conn = None
    try:
        conn = await asyncpg.connect(dsn=None, **dsn)
        async with conn.transaction():
            pass
    except Exception as e:
        raise
    finally:
        if conn:
            await conn.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(fetch_entries())
    loop.run_forever()

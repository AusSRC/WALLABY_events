import asyncio
import optparse
import configparser

from .casda_subscriber import Subscriber


async def main(loop):
    # Parse config from args
    command_args = optparse.OptionParser()
    command_args.add_option('-c', dest="config", default="./config.ini")
    options, arguments = command_args.parse_args()
    parser = configparser.ConfigParser()
    parser.read(options.config)
    await loop.run_in_executor(None, parser.read, options.config)

    # Config
    rabbitmq_dsn = parser['RabbitMQ']['dsn']
    db = parser['Database']
    db_dsn = {
        'host': db['host'],
        'port': db['port'],
        'user': db['user'],
        'password': db['password'],
        'database': db['database']
    }
    project_code = parser['WALLABY']['project_code']
    key = parser['Pipeline']['key']

    # Initialise subscriber
    sub = Subscriber()
    await sub.setup(loop, db_dsn, rabbitmq_dsn, key, project_code)
    await sub.consume()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))
    loop.run_forever()

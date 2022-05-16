import configparser
import optparse


def parse_config():
    """Get database credentials from config file.

    """
    args = optparse.OptionParser()
    args.add_option('-c', dest="config", default="./config.ini")
    options, arguments = args.parse_args()
    parser = configparser.ConfigParser()
    parser.read(options.config)

    database_creds = parser['Database']
    db_dsn = {
        'host': database_creds['host'],
        'port': database_creds['port'],
        'user': database_creds['user'],
        'password': database_creds['password'],
        'database': database_creds['database']
    }
    rabbitmq_dsn = parser['RabbitMQ']['dsn']
    workflow_keys = {
        'footprint_check_key': parser['Pipeline']['footprint_check_key'],
        'postprocessing_key': parser['Pipeline']['postprocessing_key']
    }

    return db_dsn, rabbitmq_dsn, workflow_keys

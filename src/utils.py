import os
import configparser


def parse_config():
    """Get database credentials from config file.

    """
    config_file = os.getenv('CONFIG', './etc/config.ini')
    parser = configparser.ConfigParser()
    parser.read(config_file)
    return dict(parser['Database']), dict(parser['RabbitMQ']), dict(parser['Pipeline'])


def parse_casda_credentials():
    """Get CASDA account details from configuration file.

    """
    casda_credentials_file = os.getenv('CASDA_CREDENTIALS_CONFIG', './etc/casda.ini')
    parser = configparser.ConfigParser()
    parser.read(casda_credentials_file)
    return parser['CASDA']


def generate_tile_uuid(ra, dec):
    """Function to generate the WALLABY tile UUID from RA and Dec.

    """
    return f"{str(int(round(ra))).rjust(3, '0')}{'+' if dec >= 0 else '-'}{str(int(abs(round(dec)))).rjust(2, '0')}"


def region_from_tile_centre(ra, dec):
    """Run post-processing in fixed region sizes in RA and Dec based on centre position.
    Makes 4 x 4 degree region on sky.

    """
    return f"{ra - 2.0}, {ra + 2.0}, {dec - 2.0}, {dec + 2.0}"

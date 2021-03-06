import os
import configparser


def parse_config(config_file=None):
    """Get database credentials from config file.

    """
    if config_file is None:
        config_file = os.getenv('CONFIG', './etc/config.ini')
    parser = configparser.ConfigParser()
    parser.read(config_file)
    return dict(parser['Database']), dict(parser['RabbitMQ']), dict(parser['Pipeline'])


def parse_casda_credentials(cred_file=None):
    """Get CASDA account details from configuration file.

    """
    if cred_file is None:
        cred_file = os.getenv('CASDA_CREDENTIALS_CONFIG', './etc/casda.ini')
    parser = configparser.ConfigParser()
    parser.read(cred_file)
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


def tile_mosaic_name(components):
    """Deterministic name for a mosaicked tile from individual tiles.

    """
    components.sort()
    return '_'.join(components)

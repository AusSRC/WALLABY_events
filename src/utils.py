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
    return dict(parser['Database']), dict(parser['RabbitMQ']), dict(parser['Pipeline'])


def generate_tile_uuid(ra, dec):
    """Function to generate the WALLABY tile UUID from RA and Dec.
    
    """
    return f"{str(int(round(ra))).rjust(3, '0')}{'+' if dec >= 0 else '-'}{str(int(abs(round(dec)))).rjust(2, '0')}"


def region_from_tile_centre(ra, dec):
    """Run post-processing in fixed region sizes in RA and Dec based on centre position.
    Makes 4 x 4 degree region on sky.

    """
    return f"{ra - 2.0}, {ra + 2.0}, {dec - 2.0}, {dec + 2.0}"
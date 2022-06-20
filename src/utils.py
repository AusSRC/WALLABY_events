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
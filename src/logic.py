"""Logic for comparing tiles and observations.

Leave one blank line.  The rest of this docstring should contain an
overall description of the module or program.  Optionally, it may also
contain a brief description of exported classes and functions and/or usage
examples.

  Contains the following module constants that are used throughout:

  - LOGIC_DECIMAL:                      Number of decimal places to round the RA and Dec values to
  - LOGIC_ADJACENT_MAX_THRESHOLD:       Maximum threshold in RA/Dec for two tiles to be considered adjacent
  - LOGIC_ADJACENT_MIN_THRESHOLD:       Minimum threshold in RA/Dec for two tiles to be considered adjacent
  - LOGIC_ADJACENT_OFFSET_THRESHOLD:    Centre to slightly offset centre
"""
# TODO(austin): A diagram somewhere to explain constants...

import os
import math
import logging


logging.basicConfig(level=logging.INFO)


LOGIC_DECIMAL = os.getenv('LOGIC_DECIMAL', 1)
LOGIC_ADJACENT_MAX_THRESHOLD = os.getenv('LOGIC_ADJACENT_MAX_THRESHOLD', 7.0)
LOGIC_ADJACENT_MIN_THRESHOLD = os.getenv('LOGIC_ADJACENT_MIN_THRESHOLD', 1.0)
LOGIC_ADJACENT_OFFSET_THRESHOLD = os.getenv('LOGIC_ADJACENT_OFFSET_THRESHOLD', 4.0)


def get_adjacent_tiles(tiles):
    """Find adjacent pairs in RA or Dec bands from list of tiles.

    Args:
        tiles: List of tiles from database query.

    Returns:
        List of tuples for pairs of tiles.
    """
    pairs = []
    for i in range(len(tiles)):
        for j in range(i):
            if i == j:
                continue
            ra_A = round(float(tiles[i]['ra']), LOGIC_DECIMAL)
            dec_A = round(float(tiles[i]['dec']), LOGIC_DECIMAL)
            ra_B = round(float(tiles[j]['ra']), LOGIC_DECIMAL)
            dec_B = round(float(tiles[j]['dec']), LOGIC_DECIMAL)

            # Adjacent in RA bands
            if (abs(ra_A - ra_B) < LOGIC_ADJACENT_MIN_THRESHOLD) and \
               (abs(dec_A - dec_B) < LOGIC_ADJACENT_MAX_THRESHOLD):
                logging.info(
                    f'Found adjacent tiles {tiles[i]["identifier"]} and \
                    {tiles[j]["identifier"]} in right ascension band'
                )
                pairs.append((j, i))

            # Adjacent in declination bands
            if (abs(ra_A - ra_B) < LOGIC_ADJACENT_MAX_THRESHOLD) and \
               (abs(dec_A - dec_B) < LOGIC_ADJACENT_MIN_THRESHOLD):
                logging.info(
                    f'Found adjacent tiles {tiles[i]["identifier"]} and \
                    {tiles[j]["identifier"]} along declination band'
                )
                pairs.append((j, i))
    return pairs


# TODO(austin): constants to named variables.
def adjacent_above(c_incoming, c_compare):
    """Check if input tile centre is above a comparison tile centre

    """
    ra_i, dec_i = c_incoming
    ra_c, dec_c = c_compare
    if (dec_c > dec_i) & (dec_c < dec_i + math.radians(LOGIC_ADJACENT_MAX_THRESHOLD)) & \
       (ra_i > ra_c - math.radians(LOGIC_ADJACENT_OFFSET_THRESHOLD)) & \
       (ra_i < ra_c + math.radians(LOGIC_ADJACENT_OFFSET_THRESHOLD)):
        return True
    return False


def adjacent_below(c_incoming, c_compare):
    """Check if input tile centre is below a comparison tile centre

    """
    ra_i, dec_i = c_incoming
    ra_c, dec_c = c_compare
    if (dec_c < dec_i) & (dec_c > dec_i - math.radians(LOGIC_ADJACENT_MAX_THRESHOLD)) & \
       (ra_i > ra_c - math.radians(LOGIC_ADJACENT_OFFSET_THRESHOLD)) & \
       (ra_i < ra_c + math.radians(LOGIC_ADJACENT_OFFSET_THRESHOLD)):
        return True
    return False


def adjacent_left(c_incoming, c_compare):
    """Check if input tile centre is to the left of a comparison tile centre

    """
    ra_i, dec_i = c_incoming
    ra_c, dec_c = c_compare
    if (ra_i < ra_c) & (ra_i > ra_c - math.radians(LOGIC_ADJACENT_MAX_THRESHOLD)) & \
       (dec_i > dec_c - math.radians(LOGIC_ADJACENT_OFFSET_THRESHOLD)) & \
       (dec_i < dec_c + math.radians(LOGIC_ADJACENT_OFFSET_THRESHOLD)):
        return True
    return False


def adjacent_right(c_incoming, c_compare):
    """Check if input tile centre is to the right of a comparison tile centre

    """
    ra_i, dec_i = c_incoming
    ra_c, dec_c = c_compare
    if (ra_i > ra_c) & (ra_i < ra_c + math.radians(LOGIC_ADJACENT_MAX_THRESHOLD)) & \
       (dec_i > dec_c - math.radians(LOGIC_ADJACENT_OFFSET_THRESHOLD)) & \
       (dec_i < dec_c + math.radians(LOGIC_ADJACENT_OFFSET_THRESHOLD)):
        return True
    return False

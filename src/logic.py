"""Logic for comparing tiles and observations.

Leave one blank line.  The rest of this docstring should contain an
overall description of the module or program.  Optionally, it may also
contain a brief description of exported classes and functions and/or usage
examples.

  Contains the following module constants that are used throughout:

  - LOGIC_DECIMAL:                      Number of decimal places to round the RA and Dec values to
  - LOGIC_OBSERVATION_THRESHOLD:        Maximum offset for two observations for a given tile (radians)
  - LOGIC_ADJACENT_MAX_THRESHOLD:       Maximum threshold in RA/Dec for two tiles to be considered adjacent (deg)
  - LOGIC_ADJACENT_MIN_THRESHOLD:       Minimum threshold in RA/Dec for two tiles to be considered adjacent (deg)
  - LOGIC_ADJACENT_OFFSET_THRESHOLD:    Centre to slightly offset centre (deg)
"""
# TODO(austin): A diagram somewhere to explain constants...

import os
import math
import numpy as np
from astropy.coordinates import angular_separation


LOGIC_DECIMAL = os.getenv('LOGIC_DECIMAL', 1)
LOGIC_OBSERVATION_THRESHOLD = os.getenv('LOGIC_OBSERVATION_THRESHOLD', math.radians(1.0))
LOGIC_ADJACENT_MAX_THRESHOLD = os.getenv('LOGIC_ADJACENT_MAX_THRESHOLD', 7.0)
LOGIC_ADJACENT_MIN_THRESHOLD = os.getenv('LOGIC_ADJACENT_MIN_THRESHOLD', 1.0)
LOGIC_ADJACENT_OFFSET_THRESHOLD = os.getenv('LOGIC_ADJACENT_OFFSET_THRESHOLD', 4.0)


def get_observation_pairs(observations):
    """Get pairs of observations from a list of observations based on their
    central RA and Dec.

    Args:
        observations: List of observation objects from database query

    Returns:
        List of tuples of pairs of observations
    """
    N = len(observations)
    coord_array = [(math.radians(float(r['ra'])), math.radians(float(r['dec']))) for r in observations]
    separation_matrix = np.zeros((N, N))
    for i in range(N):
        for j in range(i):
            separation_matrix[i, j] = angular_separation(*coord_array[i], *coord_array[j])

    # get pairs based on angular separation
    pair_matrix = (separation_matrix < LOGIC_OBSERVATION_THRESHOLD)
    pairs = []
    for i in range(N):
        for j in range(i):
            if pair_matrix[i][j]:
                pairs.append((j, i))

    return pairs


def get_adjacent_tiles(tiles):
    """Find adjacent pairs in RA or Dec bands from list of tiles.

    Args:
        tiles: List of tile objects from database query.

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
                pairs.append((j, i))

            # Adjacent in declination bands
            if (abs(ra_A - ra_B) < LOGIC_ADJACENT_MAX_THRESHOLD) and \
               (abs(dec_A - dec_B) < LOGIC_ADJACENT_MIN_THRESHOLD):
                pairs.append((j, i))
    return pairs


def get_tile_groups(tiles):
    """Find groups of three tiles for processing central regions.

    Args:
        tiles: List of tile objects from database query.

    Returns:
        List of tuples containing indices for tile groups.
    """
    groups = []
    pairs = get_adjacent_tiles(tiles)
    for pair in pairs:
        i, j = pair
        pair_object = {
            'ra': (float(tiles[i]['ra']) + float(tiles[j]['ra'])) / 2.0,
            'dec': (float(tiles[i]['dec']) + float(tiles[j]['dec'])) / 2.0,
            'identifier': 'pair'
        }
        pair_rec = get_adjacent_tiles([pair_object] + tiles)
        group = [(*pair, j - 1) for (i, j) in pair_rec if j - 1 not in pair and i == 0]  # so sorry
        groups += group
    return groups


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

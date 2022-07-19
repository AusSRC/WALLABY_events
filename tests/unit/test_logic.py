from src.logic import get_observation_pairs, get_adjacent_tiles, get_tile_groups


PHASE_2_SAMPLE_TILES = [
    {'ra': 195.132, 'dec': 5.773, 'identifier': '195+06'},
    {'ra': 203.502, 'dec': -22.368, 'identifier': '204-22'},
    {'ra': 197.733, 'dec': -13.378, 'identifier': '198-13'},
    {'ra': 197.739, 'dec': -18.775, 'identifier': '198-19'},
    {'ra': 203.365, 'dec': -16.969, 'identifier': '203-17'}
]

FULL_SURVEY_SAMPLE_TILES = [
    {'ra': 344.210, 'dec': 29.7, 'identifier': '344+30'},
    {'ra': 350.526, 'dec': 29.7, 'identifier': '351+30'},
    {'ra': 356.842, 'dec': 29.7, 'identifier': '357+30'},
    {'ra': 3.051, 'dec': 24.0, 'identifier': '003+24'},
    {'ra': 9.152, 'dec': 24.0, 'identifier': '009+24'},
    {'ra': 15.254, 'dec': 24.0, 'identifier': '015+24'},
]

# TODO(austin): check full survey strategy...
# [
#     {'ra': 344.745, 'dec': 24.0, 'identifier': '003+24'},
#     {'ra': 350.847, 'dec': 24.0, 'identifier': '009+24'},
#     {'ra': 356.949, 'dec': 24.0, 'identifier': '015+24'}
# ]

PHASE_1_SAMPLE_OBSERVATIONS = [
    {'sbid': 10609, 'ra': 153.952, 'dec': -27.378, 'description': "Hydra 1A"},
    {'sbid': 10269, 'ra': 154.464, 'dec': -27.823, 'description': "Hydra 1B"},
    {'sbid': 10612, 'ra': 159.853, 'dec': -27.378, 'description': "Hydra 2A"},
    {'sbid': 10626, 'ra': 160.366, 'dec': -27.823, 'description': "Hydra 2B"},
    {'sbid': 10809, 'ra': 189.508, 'dec': -0.4491, 'description': "Norma 1A"},
    {'sbid': 10812, 'ra': 189.959, 'dec': -0.8991, 'description': "Norma 1B"},
    {'sbid': 10736, 'ra': 189.510, 'dec': 4.95179, 'description': "Norma 2A"},
    {'sbid': 12193, 'ra': 253.875, 'dec': -59.480, 'description': "NGC4636 2A"},
    {'sbid': 12209, 'ra': 254.789, 'dec': -59.914, 'description': "NGC4636 2B"},
    {'sbid': 11816, 'ra': 244.145, 'dec': -59.480, 'description': "NGC4636 1A"},
    {'sbid': 11832, 'ra': 245.059, 'dec': -59.914, 'description': "NGC4636 1B"},
]


PHASE_2_SAMPLE_OBSERVATIONS = [
    {'sbid': 33714, 'ra': 195.359, 'dec': 5.547, 'description': "NGC4808 B old observation"},
    {'sbid': 33681, 'ra': 194.906, 'dec': 5.998, 'description': "NGC4808 A"},
    {'sbid': 37604, 'ra': 195.359, 'dec': 5.547, 'description': "NGC4808 B"},
    {'sbid': 33879, 'ra': 197.501, 'dec': -13.155, 'description': "NGC5044 Tile 1A"},
    {'sbid': 25701, 'ra': 203.256, 'dec': -22.144, 'description': "NGC5044 Tile 4A"},
    {'sbid': 25750, 'ra': 203.748, 'dec': -22.593, 'description': "NGC5044 Tile 4B"},
    {'sbid': 34166, 'ra': 197.500, 'dec': -18.550, 'description': "NGC5044 Tile 2A"},
    {'sbid': 34302, 'ra': 197.964, 'dec': -13.602, 'description': "NGC5044 Tile 1B"},
    {'sbid': 34275, 'ra': 197.979, 'dec': -18.999, 'description': "NGC5044 Tile 2B"},
    {'sbid': 31536, 'ra': 203.129, 'dec': -16.749, 'description': "NGC5044 Tile 3A"},
    {'sbid': 40905, 'ra': 204.064, 'dec': -16.729, 'description': "NGC5044 Tile 3B"}
]


def test_get_correct_observation_pairs():
    """Test get_observation_pairs function to make sure it works for
    Pilot Phase1 and Phase 2.

    """
    PHASE_1_PAIRS = set([(0, 1), (2, 3), (4, 5), (7, 8), (9, 10)])
    PHASE_2_PAIRS = set([(0, 1), (0, 2), (1, 2), (3, 7), (4, 5), (6, 8), (9, 10)])
    assert(PHASE_1_PAIRS == set(get_observation_pairs(PHASE_1_SAMPLE_OBSERVATIONS)))
    assert(PHASE_2_PAIRS == set(get_observation_pairs(PHASE_2_SAMPLE_OBSERVATIONS)))


def test_get_correct_adjacent_tiles():
    """Test get_adjacent_tiles function to ensure it works correctly for
    both Pilot Phase 2 and Full Survey sample tiles.

    """
    PHASE_2_ADJACENT = set([(2, 3), (1, 4)])
    FULL_SURVEY_ADJACENT = set([(0, 1), (1, 2), (3, 4), (4, 5)])
    assert(PHASE_2_ADJACENT == set(get_adjacent_tiles(PHASE_2_SAMPLE_TILES)))
    assert(FULL_SURVEY_ADJACENT == set(get_adjacent_tiles(FULL_SURVEY_SAMPLE_TILES)))


def test_get_correct_tile_group():
    """Test get_tile_groups for getting groups of three tiles for Pilot Phase 2.

    """
    PHASE_2_TILE_GROUPS = set([(2, 3, 4), (1, 4, 3)])
    assert(PHASE_2_TILE_GROUPS == set(get_tile_groups(PHASE_2_SAMPLE_TILES)))


if __name__ == "__main__":
    test_get_correct_tile_group()

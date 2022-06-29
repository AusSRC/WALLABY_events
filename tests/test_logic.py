from src.logic import get_adjacent_tiles


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


def test_get_correct_adjacent_tiles():
    """Test get_adjacent_tiles function to ensure it works correctly for
    both Pilot Phase 2 and Full Survey sample tiles.

    """
    PHASE_2_ADJACENT = [(2, 3), (1, 4)]
    FULL_SURVEY_ADJACENT = [(0, 1), (1, 2), (3, 4), (4, 5)]
    assert(PHASE_2_ADJACENT == get_adjacent_tiles(PHASE_2_SAMPLE_TILES))
    assert(FULL_SURVEY_ADJACENT == get_adjacent_tiles(FULL_SURVEY_SAMPLE_TILES))


if __name__ == "__main__":
    test_get_correct_adjacent_tiles()

# WALLABY events

Collection of publisher/subscriber code that runs on the AusSRC event system to automate the post-processing for the WALLABY survey.

Includes:

- `postprocessing.py` - submit jobs periodically
- `on_state_change.py` - updates job states (long-running)

## CASDA subscriber

Listens to published messages on the CASDA queue for WALLABY observations. 
Publishes message on workflow queue to run the footprint quality check pipeline.

## Overlap postprocessing publisher

Identifies when footprints have been observed based on the distance between sky coordinates in RA/Dec. Submits a post-processing pipeline for the two footprints to run on the overlapping region.

## TBA...

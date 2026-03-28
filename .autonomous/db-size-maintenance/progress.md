2026-03-28

- Measured the local Dexter cluster and found the real growth driver was not backups.
- Before compaction, the local database was about 751 MB.
- `phase2_mint_snapshots` consumed about 562 MB, mostly TOAST from duplicated `holders`, `price_history`, and full `snapshot_payload` JSON.
- `mints` consumed about 112 MB even with only 30 active rows because large JSON blobs were updated repeatedly.
- Found a lifecycle bug: stale rows can remain stranded in `mints` after restarts unless maintenance or monitor recovery picks them back up.
- Implemented snapshot compaction, active snapshot throttling, periodic storage maintenance, stale-mint reconciliation, and holder compaction.
- Ran maintenance plus `VACUUM FULL` on the local managed cluster and reduced the database from about 751 MB to about 74.7 MB.
- Verified with a synthetic stale mint that maintenance moved it from `mints` to `stagnant_mints` and compacted its holders payload.

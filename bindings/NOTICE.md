# Binding provenance

The packaged native market implementation is derived from FLOCK4H/Mamba.
The source repository, exact commit, and integration patch checksum are recorded
in `source.json`; the executable wheel checksum is recorded in `manifest.json`.
The Python package is named `dexter_sdk` to make Dexter's runtime independent.
Upstream authorship and dependency notices remain applicable. Wheel repackaging
preserves the original metadata payloads other than package name, version and
summary, and regenerates the wheel's RECORD hashes.

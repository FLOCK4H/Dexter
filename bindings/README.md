# Market bindings

Dexter ships its own `dexter-market-sdk` wheel and imports `dexter_sdk`.
It uses PyO3's stable Python ABI to call Rust directly. Rust is compiled into the
wheel; no Rust-to-Python source translator, server, Mamba installation or adjacent
checkout is involved in normal launch. `manifest.json` pins the shipped wheel and
its SHA-256 checksum. The bootstrap verifies this before installation.

The included build supports **Windows x64, CPython 3.10 and newer**. The current
release has been verified on Python 3.12. Other platforms need a matching wheel
added by a maintainer; the bootstrap fails clearly if one is missing.

## Updating markets

Maintain protocol code in upstream [Mamba](https://github.com/FLOCK4H/Mamba).
Dexter's source pin and integration patch are recorded in `source.json` and
`source.patch`. They are maintainer inputs, never read by the trader. This is a
patch and a binary distribution, not a second full Mamba checkout.

To reproduce the recorded binding on its target platform, install Rust, Git and
`maturin>=1.8,<2`, then run from the Dexter root:

```console
python scripts/build_bindings.py --version 0.1.0
python dexter_bootstrap.py --repair
.venv\Scripts\python.exe -m unittest discover -s tests
```

Build evidence stays under ignored `.dexter/binding-build/`. Each platform build
adds its wheel to the manifest. Use one binding version for all wheels in a
release. The current source is pinned to the recorded commit plus its patch;
it does not silently follow the upstream branch.

When upstream changes, update the commit and integration patch (including its
checksum) in `source.json`. Run the builder and verify buy/sell plans, event
decoding, migration, and the offline suite. Release the resulting wheel and
manifest with Dexter. Users update Dexter's bindings, with no edits to their
private `.env`.

If a tested upstream wheel is already built, package it directly instead:

```console
python scripts/update_bindings.py --from-wheel PATH_TO_WHEEL --version 0.1.1 --source-commit FULL_COMMIT
```

Commit `bindings/wheels/`, `manifest.json`, `source.json`, and `source.patch` together.
The build uses the upstream locked dependencies and pinned IDL inputs.

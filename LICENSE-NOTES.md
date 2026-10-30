# Licensing notes (for the owner; not a license, not legal advice)

The repo has no LICENSE file, so by default all rights are reserved and nobody else may reuse any of it.
This memo lists what bears on the choice. No license file or metadata (pyproject.toml, Cargo.toml) was added.
Points that need a lawyer are marked **[lawyer]**.

## 1. What in the repo relates to Overpass (AGPL-3.0)

Overpass API is AGPL-3.0. The repo cannot tell us whether code was copied or reimplemented; it only shows what the
authors say about it.

| File | What the repo says | Assessment |
|---|---|---|
| `src/quadtile.py` | "Exact port of Overpass's spatial index math (`index_computations.h`)"; same bit-spread constants, `^ 0x40000000`, `+91.0` lat offset, rounding rules | Translation of the C++ logic. Highest risk of being a derivative work. The idea (Morton interleave) is generic, but the constants, `calc_index` level flags and range logic are Overpass-specific. |
| `rust/osm-extract/src/quadtile.rs` | "port of `index_computations.h`", mirrors `quadtile.py` | Same (a second-generation port). |
| `tools/qt_oracle.cc` | `#include`s Overpass's `index_computations.h` and `template_db/ranges.def.h` | Small original driver that calls Overpass code; building it needs Overpass source, and a distributed binary would be AGPL. The built binary `tools/qt_oracle` is gitignored. |
| `tests/test_quadtile.py` | Differential fuzz test against the oracle | Independent test code that checks behaviour. |
| `src/ql.py` | "parser (`frontend/map_ql_parser.cc` subset)"; error messages imitate Overpass ("static error: ...") | A Python recursive-descent reimplementation of a public language, apparently with the C++ parser as reference. Not provably a port. Needs the author to confirm it was written from docs and black-box behaviour, not translated from the C++ parser or statement classes. |
| `src/engine.py`, `src/evaluator.py`, `src/output.py` | `results/NOTES.md` says bbox, recursion and tag semantics "follow the C++ source" (`bbox_filter.cc`, `geometry.cc`, `recurse.cc`); output field order and presence "follow the C++ writers" | Semantics were read from source. Matching behaviour is generally not copying **[lawyer]**, but line-by-line translation would be. Needs author confirmation. |
| `src/build_lake.py`, `docs/design.md`, `docs/architecture.md` | Say layout "mirrors Overpass"; cite `index_computations.h` line numbers and other Overpass files | Documentation of study, not copied code. It does show the authors read the AGPL source, which matters if anyone argues "clean room". |
| `tools/compare_overpass.py`, `tools/sync_reference.py`, `queries/ql_corpus.py` | Run against a separately built reference Overpass | Black-box HTTP use. The "turbo_default" query mimics overpass-turbo's wizard output (short, functional). |
| `tools/turbo_check.js` | Drives overpass-turbo.eu in a browser | Black-box use; nothing vendored. |
| `src/server.py`, `src/output.py` | Emit the standard Overpass ODbL copyright string | Short factual notice. |

Not established from the repo alone:
- Whether any file reproduces Overpass text verbatim. A diff of `quadtile.py`, `quadtile.rs` and `ql.py` against upstream (needs network) would answer that.
- Authorship: git history shows 40 commits authored as "Claude" and 15 by Ian Dees, so provenance is mixed.
- No Overpass source files are vendored (the only C++ is `tools/qt_oracle.cc`). No copyright headers exist anywhere. I found no vendored third-party files and no copied tables other than the quadtile constants.
- `docs/` and `results/NOTES.md` discuss other projects (one is noted as GPL-3.0) only as evaluated references; no code from them appears to be included.

## 2. Dependencies

Local metadata was not available: there is no `.venv`, and `cargo metadata --offline` fails because the crate registry is not cached. The licenses below come from general knowledge, not from this repo, so **verify them** (`uv pip show` or `pip-licenses`; `cargo license` or `cargo deny`) before relying on them.

Python (pyproject.toml; 37 packages in uv.lock):
- boto3, botocore, s3transfer, pyarrow, pylance, lance-namespace, python-multipart: Apache-2.0.
- duckdb, pytz, pytest (dev), pydantic: MIT.
- numpy, starlette, uvicorn: BSD-3-Clause.
- certifi: MPL-2.0 (file-level copyleft; fine as an unmodified dependency).
- osmium (pyosmium, used by `src/extract.py`): BSD-2-Clause binding; the wheels bundle libosmium (BSL-1.0) and protozero. Verify the wheel metadata.

Rust (`rust/osm-extract/Cargo.toml`; 150 crates in Cargo.lock):
- Direct deps anyhow, arrow, parquet, rayon, roaring, memmap2: MIT and/or Apache-2.0.
- mimalloc: MIT (bundles C mimalloc, MIT).
- osmpbf: MIT (check its protobuf dependency).
- Transitive crates are overwhelmingly MIT/Apache-2.0. Run `cargo license` to confirm none is GPL/LGPL or unusual.

None of the direct dependencies is GPL or AGPL, so none forces a choice, and all are compatible with every option below. Distributing the Rust binary needs the usual notice/attribution files.

## 3. OSM data (ODbL 1.0) and fixtures

- No OSM extracts or PBFs are committed (`data` is gitignored, as are `.venv` and `.env`). `tests/` has no data fixtures; the tests need an external extract or the oracle.
- `results/*.json` and `results/sizing/*` are benchmark and diff reports (counts, timings, query names, sample ids). Some contain keys named `tags` or `nodes`. I did not find dumps of element geometry, but **confirm by inspection** that they hold no OSM records.
- `results/turbo.png` is a screenshot of overpass-turbo showing an OSM-based map. It is OSM-derived imagery needing "(c) OpenStreetMap contributors" attribution; consider attributing or dropping it.
- Query corpora reference Berlin bboxes and real OSM ids; those are facts, not extracted database content.
- ODbL affects the data, not the code license. Databases the software builds from OSM (the lake, Parquet, Lance) are derived databases and must be offered under ODbL with attribution. The server already emits the ODbL notice; add an explicit "Data (c) OpenStreetMap contributors, ODbL" line to the README. **[lawyer]** for how ODbL derived-database and produced-work rules apply to published lakes or hosted query results.

## 4. Options

A. **AGPL-3.0 for everything.** Compatible with the Overpass-derived parts however they were written. Others may reuse and modify, but anyone running a modified version as a network service must publish their source. Some companies avoid AGPL. Simplest and safest legally, and fits the OSM tooling ecosystem.
B. **MIT or Apache-2.0 for the whole repo** (Apache-2.0 adds a patent grant). Maximum reuse, including closed-source. Only valid if `quadtile.py`, `quadtile.rs` and `ql.py` are independent expressions rather than translations. If they are translations of AGPL C++, a permissive license on them would breach Overpass's license **[lawyer]**.
C. **Split licensing.** AGPL-3.0 for `src/quadtile.py`, `rust/osm-extract/src/quadtile.rs` and whichever of `ql.py`, `output.py`, `engine.py` are judged derived; MIT/Apache-2.0 for independent parts (lake builder, updater, deploy scripts, tools). A combined work that imports AGPL modules is effectively AGPL when distributed or served, so the split mainly helps people who take only the independent files. Check whether `rust/osm-extract/src/main.rs` depends on `quadtile.rs`, since that would make the whole extractor derived.
D. **Clean-room rewrite** of the ported parts from a written behaviour spec (one person specifies from the public wiki and black-box outputs, another implements), then license permissively. Costly. The oracle test can still check behaviour as long as the oracle is not distributed.
E. **Ask the Overpass author and contributors for a separate permission** for the index math. Practical for a small, well-defined piece; needs written consent from the copyright holders **[lawyer]**.

What each allows others to do: A requires source for distribution and for network-served modifications; B and the permissive part of C allow closed-source reuse; D and E are routes to B.

## 5. Owner must confirm first

1. Was `quadtile.py` / `quadtile.rs` copied or transcribed from `index_computations.h`, or written from a description? The docstrings say "exact port" and the constants match; treat as derived unless shown otherwise.
2. Was `ql.py` written while reading `map_ql_parser.cc` and the statement sources, or only from the QL docs and black-box testing? Same question for field ordering in `output.py` and the semantics in `engine.py` and `evaluator.py`.
3. Was any AI-generated code prompted with, or does it reproduce, Overpass source text? Who holds copyright in commits authored as "Claude"? Rules on AI-assisted works are unsettled **[lawyer]**.
4. Will `tools/qt_oracle.cc` or a built oracle be distributed (it links AGPL headers)?
5. Verify dependency licenses with real tooling, and whether `main.rs` requires `quadtile.rs`.
6. Inspect `results/*.json` and `results/turbo.png` for OSM-derived content.
7. Do other contributors (any beyond Ian Dees) need to agree to the license? Decide on a CLA or plain inbound=outbound.

## 6. Suggested default

Add no license until questions 1 and 2 are answered. If either answer is "translated", AGPL-3.0 (option A, or C with a lawyer's help) is the conservative route. If both are "reimplemented", MIT or Apache-2.0 is defensible. Have a lawyer confirm either way.

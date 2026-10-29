# Planet estimate from regional history cuts

Planet (2026-10-19T00:00:04Z): 16,657,902,975 node versions, 2,471,108,843 way versions, 67,784,271 relation versions; 15,943,118,773 node position changes; max ids n=14,298,154,526 w=1,569,574,897.

Cut pass over the whole history planet: 84 min, peak RSS 11.59 GB.

| Region | Node versions | Way versions | Rel. versions | Share of planet versions | Way geometry slices per way version |
|---|---|---|---|---|---|
| berlin | 20,686,421 | 6,719,648 | 439,449 | 0.145% | 0.30 |
| luxembourg | 14,138,405 | 2,727,996 | 260,646 | 0.089% | 0.38 |
| plateau | 44,713,036 | 9,658,360 | 661,309 | 0.287% | 0.40 |

| Planet, full history | GB (range across regions) |
|---|---|
| Raw extraction (local disk) | 799.3–846.4 |
| Node position timeline (`--dense-index`) | 369.5 |
| Way geometry timeline | 112.8–120.1 |
| **Peak local disk, extract** (raw + timelines; the PBF streamed) | **1,281.6–1,336.1** |
| Lake in R2 (Parquet) | 607.5–616.9 |
| Lance id store in R2 | 488.4–551.5 |

| Step | Measured on the regions | Planet estimate |
|---|---|---|
| build | berlin 205 s / 11.14 GB, luxembourg 89 s / 7.27 GB, plateau 246 s / 11.98 GB | 23.8–39.2 h at the measured core count |
| areas | berlin 2 s / 0.33 GB, luxembourg 2 s / 0.35 GB, plateau 10 s / 0.37 GB | 0.1–0.3 h at the measured core count |
| extract (history processing, excluding decode) | 225.3 s, 136.3 s, 315.6 s | 30.6–43.1 h, plus the decode pass |

Times scale linearly with data and roughly inversely with cores (DuckDB and the extractor's group processing are parallel); disk and lake sizes don't depend on the machine.

# Initial paired primary-format comparison

This directory preserves the first remote paired performance campaign for the
rank-affine candidate before the later scan and patch-path cost fixes. The
campaign compared candidate revision `32ed66fdbde1d1769f2089a652f527bd32fa87c9`
with base revision `0318dfab081763eb2b2eb1262aa22e723949651e` on one isolated
Linux runner. It used six alternating complete pairs and retained the raw
per-arm benchmark logs, paired samples, build metadata, and comparison JSON.

The result is a rejection record, not qualification evidence. The candidate
saved primary bytes in the separate exact corpus census, but the first
implementation added measurable scan and replacement work:

| Operation | Candidate/base ratio | Paired 95% interval |
| --- | ---: | ---: |
| Low-cardinality store scan | 1.2087 | 1.1971–1.2194 |
| High-cardinality store scan | 1.0783 | 1.0724–1.0993 |
| Negative-value store scan | 1.3226 | 1.2838–1.3459 |
| Low-cardinality patch scoring | 1.1893 | 1.1845–1.1939 |
| High-cardinality patch scoring | 1.0889 | 1.0724–1.0993 |
| Durable low scan | 1.0809 | 1.0771–1.0842 |
| Durable high scan | 1.0466 | 1.0389–1.0550 |
| Durable put | 1.0224 | 1.0134–1.0325 |

Point reads and replacement identifiers improved in the same run, but that
does not offset the scan and mutation regressions under the project's stated
constraint. The later candidate must be measured as a new complete paired
campaign against the same base; these samples must not be pooled with it.

The campaign was run by workflow `33988684368`, performance job
`101366860346`. `comparison.json` is the machine-readable source of the
ratios and confidence intervals; `pairs.json` retains the paired measurements.

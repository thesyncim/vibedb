# Qualification in progress

- Existing compact tests pass with production emission enabled.
- New independent tests pass for ascending/descending values, shape-missing fields, all native integer predicates, exact spellings, projection/groups, checked descriptors, late writer rejection, and scalar replacement break/restore against a complete rebuild.
- The initial broad Darwin command used a six-minute package limit across storeio and durable primary tests. Storeio passed; durable reached the global timeout while TestFilePrimaryChurnQualification was running (1m52s in that test, 100k-row/100k-mutation configuration). This is an incomplete qualification, not a pass or a measured latency comparison. The repository's isolated CI durable-churn shard has a 25-minute limit and remains required.
- Local one-iteration benchmark runs check harness execution only. They are not performance evidence on the shared host.
- Production primary bytes in results.md were measured against the frozen f05df25e-era storage source. The branch includes fetched main `4a19249f`; git diff f05df25e..0318dfab and 0318dfab..4a19249f under `internal/storeio` and `store/durable` is empty. Upper SQL/RF3 changes still require current-main qualification.
- The initial paired remote performance campaign failed the no-regression gate on scans and patch scoring; a fresh campaign for the final source is required. Whole RF3 qualification remains required. The rejected maintenance implementation has been removed; only its historical source/evidence is retained.
- The [initial remote paired performance comparison](initial-comparison/README.md) is retained as a rejection record because scan and patch-score paths regressed. Its raw logs and paired samples must not be pooled with a later candidate campaign.

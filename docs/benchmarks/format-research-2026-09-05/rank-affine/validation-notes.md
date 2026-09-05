# Qualification in progress

- Existing compact tests pass with production emission enabled.
- New independent tests pass for ascending/descending values, shape-missing fields, all native integer predicates, exact spellings, projection/groups, checked descriptors, late writer rejection, and scalar replacement break/restore against a complete rebuild.
- The initial broad Darwin command used a six-minute package limit across storeio and durable primary tests. Storeio passed; durable reached the global timeout while TestFilePrimaryChurnQualification was running (1m52s in that test, 100k-row/100k-mutation configuration). This is an incomplete qualification, not a pass or a measured latency comparison. The repository's isolated CI durable-churn shard has a 25-minute limit and remains required.
- Local one-iteration benchmark runs check harness execution only. They are not performance evidence on the shared host.
- Production primary bytes in results.md were measured against the frozen f05df25e-era storage source. Main advanced to 0318dfab; git diff f05df25e..0318dfab -- internal/storeio store/durable is empty. Upper SQL/RF3 changes still require current-main qualification.
- Paired remote performance and whole RF3 qualification are pending. The rejected maintenance implementation has been removed; only its historical source/evidence is retained.

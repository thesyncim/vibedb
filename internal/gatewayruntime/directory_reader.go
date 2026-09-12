package gatewayruntime

import "github.com/thesyncim/vibedb/gateway"

// DirectoryReader returns the catalog authority which authenticated this
// frontend. A shard node uses it for node-control requests so a target reads
// the committed enrollment intent from the replicated authority instead of a
// controller supplied cache. The authority is installed before Open returns;
// callers must still treat a nil result as unavailable and fail closed.
func (runtime *Runtime) DirectoryReader() gateway.DirectoryReader {
	if runtime == nil {
		return nil
	}
	return runtime.authority
}

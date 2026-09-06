package shardservice

import "sync"

// shardRequestPool recycles the ~1KB request descriptor shells decoded once
// per request. Only the shell is reused: every string, parameter, and frame
// buffer is freshly decoded per request, so pooled shells retain no caller
// memory once scrubbed. A missed release is benign (the shell is collected);
// use after release is a programming error, so borrowing is confined to the
// connection loop, which serves strictly sequentially and releases each
// request after its handler returns.
var shardRequestPool = sync.Pool{New: func() any { return &ShardRequest{} }}

// borrowShardRequest returns a zeroed request shell.
func borrowShardRequest() *ShardRequest {
	req := shardRequestPool.Get().(*ShardRequest)
	*req = ShardRequest{}
	return req
}

// releaseShardRequest scrubs and recycles a request shell. It must run after
// the request's last use within the serving iteration.
func releaseShardRequest(req *ShardRequest) {
	if req == nil {
		return
	}
	*req = ShardRequest{}
	shardRequestPool.Put(req)
}

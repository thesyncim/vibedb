package shardservice

import "github.com/thesyncim/vibedb/internal/routegate"

// Route-gate status has one fixed canonical grammar; no mutation result,
// arbitrary hidden-row key, or follower-read flag is carried on this path.
func AppendReplicatedRouteGateReadValue(dst []byte, status routegate.Status) ([]byte, error) {
	return routegate.AppendStatus(dst, status)
}
func OpenReplicatedRouteGateReadValue(raw []byte) (routegate.Status, error) {
	status, err := routegate.OpenStatus(raw)
	if err != nil {
		return routegate.Status{}, ErrReplicatedWire
	}
	return status, nil
}
func validReplicatedRouteGateReadValue(raw []byte) bool {
	_, err := OpenReplicatedRouteGateReadValue(raw)
	return err == nil
}

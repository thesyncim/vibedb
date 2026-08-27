package main

import (
	"github.com/thesyncim/vibedb/gateway"
	vibejson "github.com/thesyncim/vibejson"
)

// validGatewayMetricsRequest keeps the operator endpoint deliberately narrow:
// no SQL, routing hint, identity, or result budget is interpreted beside the
// operation. Authentication and CapabilityTopology are enforced by the caller.
func validGatewayMetricsRequest(request serveRequest) bool {
	return request.Op == "metrics" && request.RequestID == "" && request.InstallationID == "" &&
		request.IssuerEpoch == 0 && request.LaneOrdinal == 0 && request.GrantDigest == "" &&
		request.IssuerSequence == 0 && request.IssuerLane == "" &&
		request.IssuerAuthenticator == "" && request.SQL == "" && request.Class == "" &&
		request.MaxResultBytes == 0 && len(request.Params) == 0 && len(request.Statements) == 0
}

// writeGatewayMetrics emits a stable, allocation-free field walk over the
// lock-free counter snapshot. Counters are cumulative for this process
// incarnation; consumers derive rates outside the serving path.
func writeGatewayMetrics(writer *vibejson.Writer, metrics gateway.MetricsSnapshot) error {
	if err := writer.Key("metrics"); err != nil {
		return err
	}
	if err := writer.BeginObject(); err != nil {
		return err
	}
	fields := [...]struct {
		name  string
		value uint64
	}{
		{"route_single", metrics.RouteSingle},
		{"route_targeted", metrics.RouteTargeted},
		{"route_scatter", metrics.RouteScatter},
		{"route_empty", metrics.RouteEmpty},
		{"shards_fanned", metrics.ShardsFanned},
		{"rows_returned", metrics.RowsReturned},
		{"bytes_returned", metrics.BytesReturned},
		{"retries", metrics.Retries},
		{"scatter_all_shards", metrics.ScatterAllShards},
		{"scatter_unknown_route", metrics.ScatterUnknownRoute},
	}
	for _, field := range fields {
		if err := writer.Key(field.name); err != nil {
			return err
		}
		if err := writer.Uint(field.value); err != nil {
			return err
		}
	}
	return writer.EndObject()
}

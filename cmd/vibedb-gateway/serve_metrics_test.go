package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	vibejson "github.com/thesyncim/vibejson"
)

func TestGatewayMetricsRequestIsTopologyAuthorizedAndClosed(t *testing.T) {
	request := serveRequest{Op: "metrics"}
	if !validGatewayMetricsRequest(request) ||
		serveRequestCapability(&request) != serviceauthz.CapabilityTopology {
		t.Fatal("metrics request did not require exact topology authority")
	}
	for _, mutate := range []func(*serveRequest){
		func(request *serveRequest) { request.SQL = "SELECT 1" },
		func(request *serveRequest) { request.Class = "admin" },
		func(request *serveRequest) { request.MaxResultBytes = 1 },
		func(request *serveRequest) { request.Params = []serveParam{{Kind: "null"}} },
		func(request *serveRequest) { request.RequestID = "00" },
	} {
		invalid := request
		mutate(&invalid)
		if validGatewayMetricsRequest(invalid) {
			t.Fatalf("accepted mixed metrics request: %+v", invalid)
		}
	}
}

func TestWriteGatewayMetricsIsCanonicalAndComplete(t *testing.T) {
	metrics := gateway.MetricsSnapshot{
		RouteSingle: 1, RouteTargeted: 2, RouteScatter: 3, RouteEmpty: 4,
		ShardsFanned: 5, RowsReturned: 6, BytesReturned: 7, Retries: 8,
		ScatterAllShards: 9, ScatterUnknownRoute: 10,
	}
	var output bytes.Buffer
	if err := writeServeResponse(vibejson.NewWriter(&output), &serveResponse{Metrics: &metrics}); err != nil {
		t.Fatal(err)
	}
	want := `{"metrics":{"route_single":1,"route_targeted":2,"route_scatter":3,"route_empty":4,"shards_fanned":5,"rows_returned":6,"bytes_returned":7,"retries":8,"scatter_all_shards":9,"scatter_unknown_route":10}}` + "\n"
	if got := output.String(); got != want {
		t.Fatalf("metrics response=%q want=%q", got, want)
	}
	if strings.Contains(output.String(), "RouteSingle") {
		t.Fatal("metrics response exposed Go field names")
	}
}

func TestGatewayMetricsServingRequiresTopologyCapability(t *testing.T) {
	for _, allowed := range []bool{false, true} {
		server, client := net.Pipe()
		done := make(chan struct{})
		var requested serviceauthz.Capability
		go func() {
			defer close(done)
			handleConnPolicyDurable(context.Background(), server,
				gateway.NewExecutor(nil, nil, gateway.Options{}), nil, nil,
				func(string, ...any) {}, func(capability serviceauthz.Capability) bool {
					requested = capability
					return allowed
				})
		}()
		if _, err := io.WriteString(client, "{\"op\":\"metrics\"}\n"); err != nil {
			t.Fatal(err)
		}
		response, err := bytes.NewBuffer(nil), error(nil)
		line := make([]byte, 512)
		count, readErr := client.Read(line)
		if readErr != nil {
			err = readErr
		} else {
			_, err = response.Write(line[:count])
		}
		if err != nil {
			t.Fatal(err)
		}
		if requested != serviceauthz.CapabilityTopology {
			t.Fatalf("requested capability=%x", requested)
		}
		if allowed && !strings.HasPrefix(response.String(), `{"metrics":{`) {
			t.Fatalf("authorized response=%q", response.String())
		}
		if !allowed && response.String() != "{\"error\":\"authorization denied\"}\n" {
			t.Fatalf("denied response=%q", response.String())
		}
		_ = client.Close()
		<-done
	}
}

func BenchmarkWriteGatewayMetrics(b *testing.B) {
	metrics := gateway.MetricsSnapshot{RouteSingle: 1, ShardsFanned: 2, BytesReturned: 3}
	var output bytes.Buffer
	output.Grow(512)
	writer := vibejson.NewWriter(&output)
	b.ReportAllocs()
	for b.Loop() {
		output.Reset()
		if err := writeServeResponse(writer, &serveResponse{Metrics: &metrics}); err != nil {
			b.Fatal(err)
		}
	}
}

package gatewayruntime

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clustercontrol"
)

type blockingClusterControlDirectory struct {
	gateway.DirectoryReader
	nodesStarted  chan struct{}
	statusStarted chan struct{}
}

func (directory *blockingClusterControlDirectory) ListNodes(ctx context.Context) ([]gateway.NodeRecord, error) {
	close(directory.nodesStarted)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (directory *blockingClusterControlDirectory) ReadScalingIntent(ctx context.Context, _ [32]byte) (gateway.ScalingIntent, error) {
	close(directory.statusStarted)
	<-ctx.Done()
	return gateway.ScalingIntent{}, ctx.Err()
}

func TestExecuteClusterControlReadRequestsHonorCancellation(t *testing.T) {
	requestID, err := clustercontrol.NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	operationID := [32]byte{1}
	tests := []struct {
		name    string
		request clustercontrol.Request
		started func(*blockingClusterControlDirectory) <-chan struct{}
	}{
		{
			name:    "nodes",
			request: clustercontrol.Request{Format: clustercontrol.Format, Op: clustercontrol.OpNodes, RequestID: requestID},
			started: func(directory *blockingClusterControlDirectory) <-chan struct{} { return directory.nodesStarted },
		},
		{
			name: "status",
			request: clustercontrol.Request{Format: clustercontrol.Format, Op: clustercontrol.OpStatus,
				RequestID: requestID, OperationID: hex.EncodeToString(operationID[:])},
			started: func(directory *blockingClusterControlDirectory) <-chan struct{} { return directory.statusStarted },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := &blockingClusterControlDirectory{
				nodesStarted:  make(chan struct{}),
				statusStarted: make(chan struct{}),
			}
			backend := &ScalingOperatorBackend{directory: directory}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan clustercontrol.Response, 1)
			go func() { result <- backend.ExecuteClusterControl(ctx, test.request) }()
			select {
			case <-test.started(directory):
			case <-time.After(time.Second):
				t.Fatal("read did not reach the blocking directory call")
			}
			cancel()
			select {
			case response := <-result:
				if response.OK || response.Error == "" {
					t.Fatalf("canceled %s request returned success: %+v", test.name, response)
				}
			case <-time.After(time.Second):
				t.Fatal("canceled read request did not return promptly")
			}
		})
	}
}

type durableClusterControlDirectory struct {
	gateway.DirectoryReader
	listContext context.Context
}

func (directory *durableClusterControlDirectory) ListScalingIntents(ctx context.Context) ([]gateway.ScalingIntent, error) {
	directory.listContext = ctx
	return nil, nil
}

type durableClusterControlWriter struct {
	gateway.DirectoryWriter
	putContext context.Context
}

func (writer *durableClusterControlWriter) PutScalingIntent(ctx context.Context, _ gateway.ScalingIntent, _ uint64) error {
	writer.putContext = ctx
	return nil
}

type durableClusterControlCatalog struct{}

func (durableClusterControlCatalog) Read(context.Context) (*gateway.Snapshot, error) {
	return &gateway.Snapshot{}, nil
}

func TestExecuteClusterControlPersistsIntentAfterClientCancellation(t *testing.T) {
	requestID, err := clustercontrol.NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	directory := &durableClusterControlDirectory{}
	writer := &durableClusterControlWriter{}
	backend := &ScalingOperatorBackend{directory: directory, writer: writer, catalog: durableClusterControlCatalog{}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	response := backend.ExecuteClusterControl(ctx, clustercontrol.Request{
		Format: clustercontrol.Format, Op: clustercontrol.OpRebalance, RequestID: requestID,
		MaxMoves: 1, MaxMigrationBytes: 1,
	})
	if !response.OK || response.OperationID == "" {
		t.Fatalf("canceled durable submission failed: %+v", response)
	}
	if directory.listContext == nil || directory.listContext.Err() != nil {
		t.Fatalf("intent lookup did not use detached context: err=%v", directory.listContext.Err())
	}
	if writer.putContext == nil || writer.putContext.Err() != nil {
		t.Fatalf("intent write did not use detached context: err=%v", writer.putContext.Err())
	}
}

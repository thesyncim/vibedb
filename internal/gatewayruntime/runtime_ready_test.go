package gatewayruntime

import (
	"context"
	"net"
	"path/filepath"
	"testing"
)

func TestRuntimeReadyWaitsForPostgreSQLStartup(t *testing.T) {
	for _, failBind := range []bool{false, true} {
		name := "success"
		if failBind {
			name = "bind-failure"
		}
		t.Run(name, func(t *testing.T) {
			address := "127.0.0.1:0"
			if failBind {
				occupied, err := net.Listen("tcp", address)
				if err != nil {
					t.Fatal(err)
				}
				defer occupied.Close()
				address = occupied.Addr().String()
			}
			runtime, err := Open(context.Background(), Config{
				CatalogPath: runtimeLifecycleCatalog(t), DevStaticCatalog: true, DevPlaintext: true,
				Listener: newBlockingRuntimeListener(), Logf: func(string, ...any) {},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = runtime.Close() })
			select {
			case <-runtime.Ready():
				t.Fatal("Open published readiness before Serve started")
			default:
			}
			// Replace only the remote durable service in this lifecycle test;
			// exercise the real PostgreSQL writer, journal and listener startup.
			runtime.durable = &postgresTableServiceStub{}
			runtime.config.CatalogSessionJournal = filepath.Join(t.TempDir(), "session")
			runtime.config.PGListenAddress = address
			served := make(chan error, 1)
			go func() { served <- runtime.Serve(context.Background()) }()
			if failBind {
				if err := awaitRuntimeError(t, served, "failed PostgreSQL bind"); err == nil {
					t.Fatal("Serve succeeded despite occupied PostgreSQL address")
				}
				select {
				case <-runtime.Ready():
					t.Fatal("failed PostgreSQL startup published readiness")
				default:
				}
			} else {
				awaitRuntimeSignal(t, runtime.Ready(), "PostgreSQL readiness")
				if runtime.pg == nil || runtime.pgWriter == nil {
					t.Fatal("readiness preceded PostgreSQL listener or writer startup")
				}
				if err := runtime.Drain(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := awaitRuntimeError(t, served, "Serve after Drain"); err != nil {
					t.Fatal(err)
				}
			}
			// Both success and failed bind must join writer recovery before
			// returning, so the same durable outbox can be reopened safely.
			writer, err := openPostgresTableWriters(runtime.config.CatalogSessionJournal+".pg-writes",
				runtime.config.InternalAuthority, &postgresTableServiceStub{})
			if err != nil {
				t.Fatalf("startup/drain retained the PostgreSQL writer journal: %v", err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

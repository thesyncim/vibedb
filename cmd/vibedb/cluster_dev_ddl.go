package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// Only the local dev supervisor owns provisioning paths and child processes.
// A private Unix socket exposes that cold capability to its gateway, without
// putting filesystem paths or process control in the SQL/query protocol.
func startDevDDL(ctx context.Context, cluster devClusterManifest, binary string, children []*devChild) (string, func(), error) {
	root := filepath.Dir(cluster.CatalogPath)
	directory, err := os.MkdirTemp("", "vibedb-ddl-")
	if err != nil {
		return "", nil, err
	}
	path := filepath.Join(directory, "control.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		_ = os.Remove(directory)
		return "", nil, err
	}
	var mu sync.Mutex
	server := &http.Server{ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 2 * time.Minute, MaxHeaderBytes: 4096,
		BaseContext: func(net.Listener) context.Context { return ctx },
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/create-table" && r.URL.Path != "/drop-table" {
				http.NotFound(w, r)
				return
			}
			raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, sqldriver.ReplicatedChildSchemaMaxBytes))
			if err != nil {
				http.Error(w, "DDL exceeds its size bound", http.StatusBadRequest)
				return
			}
			tree, err := sqlast.ParseStatement(string(raw))
			if err != nil || r.URL.Path == "/create-table" && tree.CreateTable == nil ||
				r.URL.Path == "/drop-table" && tree.DropTable == nil {
				http.Error(w, "expected matching table DDL", http.StatusBadRequest)
				return
			}
			if tree.DropTable != nil {
				mu.Lock()
				defer mu.Unlock()
				removed, retireErr := retireDevTable(root, binary, &cluster, tree.DropTable.Table)
				if retireErr != nil {
					http.Error(w, retireErr.Error(), http.StatusInternalServerError)
					return
				}
				if removed {
					for _, child := range children {
						if signalErr := child.command.Process.Signal(syscall.SIGHUP); signalErr != nil {
							http.Error(w, signalErr.Error(), http.StatusServiceUnavailable)
							return
						}
					}
					if reloadErr := waitDevDDLReload(ctx, root, cluster.dataServeManifests); reloadErr != nil {
						http.Error(w, reloadErr.Error(), http.StatusServiceUnavailable)
						return
					}
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}
			// Canonicalize the optional IF NOT EXISTS prefix. The gateway checks
			// existence against the replicated catalog, not this local inventory.
			ddl := "CREATE TABLE " + string(raw[tree.CreateTable.Pos:])
			name, _, err := parseDevTableDDL(ddl)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			fmt.Fprintf(os.Stdout, "development DDL preparing table=%q\n", name)
			if err := ctx.Err(); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			schemaPath := filepath.Join(root, "ddl-create-table.sql")
			if err := replaceDevFile(schemaPath, []byte(ddl)); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if err := ensureDevTables(root, binary, &cluster, schemaPath); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			fmt.Fprintf(os.Stdout, "development DDL prepared table=%q; loading three members\n", name)
			for _, child := range children {
				if err := child.command.Process.Signal(syscall.SIGHUP); err != nil {
					http.Error(w, err.Error(), http.StatusServiceUnavailable)
					return
				}
			}
			if err := waitDevDDLReload(ctx, root, cluster.dataServeManifests); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			fragment, err := readDevFile(filepath.Join(root, "table-"+name+"-catalog.vibejson"), 4<<20)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(fragment)
		}),
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	stop := func() {
		_ = server.Close()
		<-done
		// Join any in-flight provisioning before its child processes stop.
		mu.Lock()
		mu.Unlock()
		_ = os.Remove(directory)
	}
	if err := os.Chmod(path, 0600); err != nil {
		stop()
		return "", nil, errors.Join(err, ctx.Err())
	}
	return path, stop, nil
}

// waitDevDDLReload makes the local supervisor's DDL response the durability
// barrier for all three data processes. SIGHUP delivery alone is not an ack:
// without this barrier, a fast DROP followed by CREATE can collapse two valid
// group transitions into one invalid replacement across a crash or restart.
func waitDevDDLReload(ctx context.Context, root string, manifests []string) error {
	if ctx == nil || len(manifests) != devClusterRF3 {
		return errDevCluster
	}
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		complete := true
		for index, manifestPath := range manifests {
			raw, err := readDevFile(manifestPath, 4<<20)
			if err != nil {
				return err
			}
			want := sha256.Sum256(raw)
			memberRoot := filepath.Join(root, fmt.Sprintf("data-member-%d", index+1))
			inventory, inventoryErr := readDevFile(filepath.Join(memberRoot, "adopted-groups.state"), 64<<10)
			admissions, admissionsErr := readDevFile(filepath.Join(memberRoot, "child-preparations.state"), 64<<10)
			if inventoryErr != nil || admissionsErr != nil || len(inventory) < 40 || len(admissions) < 48 ||
				!bytes.Equal(inventory[8:40], want[:]) || !bytes.Equal(admissions[16:48], want[:]) {
				complete = false
				break
			}
		}
		if complete {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("%w: data members did not durably acknowledge DDL reload", errDevCluster)
		case <-ticker.C:
		}
	}
}

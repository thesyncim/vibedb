// Command vibedb-pgtest-server exposes one disposable VibeDB catalog to
// external PostgreSQL compatibility suites over the real pgwire package.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/thesyncim/vibedb/pgwire"
	vibedriver "github.com/thesyncim/vibedb/sql/driver"
)

func main() {
	log.SetFlags(0)
	listenAddress := flag.String("listen", "127.0.0.1:0", "loopback TCP address")
	catalogPath := flag.String("catalog", "", "path to the disposable VibeDB catalog")
	readyFile := flag.String("ready-file", "", "file that receives the bound host:port")
	flag.Parse()

	if *catalogPath == "" || *readyFile == "" {
		log.Fatal("-catalog and -ready-file are required")
	}

	database, err := vibedriver.Open(*catalogPath)
	if err != nil {
		log.Fatalf("open catalog: %v", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			log.Printf("close catalog: %v", err)
		}
	}()

	server, err := pgwire.NewServer(database, pgwire.Options{
		// This executable only binds to loopback and exists solely for a local
		// disposable test target. Trust is deliberately explicit.
		Auth:     pgwire.Trust(),
		Database: "",
	})
	if err != nil {
		log.Fatalf("create pgwire server: %v", err)
	}

	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	bound, ok := listener.Addr().(*net.TCPAddr)
	if !ok || !bound.IP.IsLoopback() {
		_ = listener.Close()
		log.Fatalf("refusing non-loopback test listener %q", listener.Addr())
	}

	if err := writeReadyFile(*readyFile, listener.Addr().String()); err != nil {
		_ = listener.Close()
		log.Fatalf("publish ready address: %v", err)
	}
	fmt.Println(listener.Addr().String())

	serveResult := make(chan error, 1)
	go func() {
		serveResult <- server.Serve(listener)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		if err := server.Close(); err != nil {
			log.Fatalf("close pgwire server: %v", err)
		}
		if err := <-serveResult; !errors.Is(err, pgwire.ErrServerClosed) {
			log.Fatalf("serve after close: %v", err)
		}
	case err := <-serveResult:
		if !errors.Is(err, pgwire.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}
}

func writeReadyFile(path, address string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, []byte(address+"\n"), 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

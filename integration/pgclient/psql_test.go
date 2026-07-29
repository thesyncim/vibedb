package pgclient_test

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/pgwire"
)

const (
	psqlTestOptIn = "VIBEDB_TEST_PSQL"
	psqlImageEnv  = "VIBEDB_PSQL_IMAGE"

	// This is the multi-platform postgres:18.4-alpine image. Pinning the digest
	// makes the CI wire-protocol client reproducible instead of silently moving
	// when an image tag is republished.
	defaultPSQLImage = "postgres@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15"
)

// TestPSQLClient18 is the stock-client interoperability gate. It is explicitly
// opt-in, so ordinary `go test ./...` remains hermetic and does not acquire a
// Docker or network dependency. An opted-in run pulls the digest-pinned client
// before forbidding implicit pulls in every actual probe.
func TestPSQLClient18(t *testing.T) {
	if os.Getenv(psqlTestOptIn) != "1" {
		t.Skip("set VIBEDB_TEST_PSQL=1 to run the digest-pinned psql gate")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("find docker executable: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	image := os.Getenv(psqlImageEnv)
	if image == "" {
		image = defaultPSQLImage
	}
	pullOutput, err := dockerPullPSQL(ctx, image)
	if err != nil {
		t.Fatalf("pull pinned psql client: %v\n%s", err, pullOutput)
	}
	version, err := dockerPSQLVersion(ctx, image)
	if err != nil {
		t.Fatalf("run pinned psql client: %v\n%s", err, version)
	}
	if !strings.Contains(version, "psql (PostgreSQL) 18.4") {
		t.Fatalf("psql version = %q, want PostgreSQL 18.4", strings.TrimSpace(version))
	}
	t.Log(strings.TrimSpace(version))

	database := openCatalog(t, filepath.Join(t.TempDir(), "psql.vdb"))

	// First prove that a stock libpq client sees and rejects the server's
	// explicit SSL refusal. Keep this expected failed handshake on a separate
	// server so the clean-session OnError assertion below stays meaningful.
	refusal, _ := serveCatalogAt(t, database, "0.0.0.0:0", nil)
	refusalOutput, refusalErr := dockerPSQL(ctx, psqlInvocation{
		image:      image,
		port:       listenerPort(t, refusal.listener),
		sslMode:    "require",
		gssEncMode: "disable",
		script:     "SELECT 1;\n",
	})
	if refusalErr == nil {
		t.Fatalf("sslmode=require unexpectedly succeeded:\n%s", refusalOutput)
	}
	if !strings.Contains(strings.ToLower(refusalOutput), "server does not support ssl") {
		t.Fatalf("SSL refusal output did not identify unsupported SSL:\n%s", refusalOutput)
	}
	refusal.stop()

	sessionErrors := make(chan error, 4)
	server, _ := serveCatalogAt(t, database, "0.0.0.0:0", func(options *pgwire.Options) {
		options.MaxConnections = 1
		options.OnError = func(err error) {
			select {
			case sessionErrors <- err:
			default:
			}
		}
	})
	port := listenerPort(t, server.listener)

	setupOutput, err := dockerPSQL(ctx, psqlInvocation{
		image:       image,
		port:        port,
		onErrorStop: true,
		script: `
SHOW application_name;
SHOW client_encoding;
CREATE TABLE psql_users (
	id STRING PRIMARY KEY,
	name STRING NOT NULL,
	tier STRING,
	big NUMBER
);
CREATE INDEX psql_users_by_tier ON psql_users(tier);
CREATE TABLE psql_orders (
	id STRING PRIMARY KEY,
	user_id STRING NOT NULL,
	total NUMBER NOT NULL
);
CREATE INDEX psql_orders_by_user ON psql_orders(user_id);
INSERT INTO psql_users VALUES ({"id":"u1","name":"Ada","tier":"pro","big":9007199254740993123});
BEGIN;
INSERT INTO psql_orders VALUES ({"id":"rolled","user_id":"u1","total":1.25});
SELECT COUNT(*) FROM psql_orders WHERE id = 'rolled';
ROLLBACK;
INSERT INTO psql_orders VALUES ({"id":"o1","user_id":"u1","total":12.50});
SELECT psql_users.name, psql_users.big, psql_orders.total
FROM psql_users
JOIN psql_orders ON psql_users.id = psql_orders.user_id
WHERE psql_users.tier = 'pro';
\pset tuples_only off
\dt
\d psql_users
\echo __VIBEDB_PSQL_SETUP_OK__
\q
`,
	})
	if err != nil {
		t.Fatalf("psql supported-SQL session: %v\n%s", err, setupOutput)
	}
	assertOutputContains(t, setupOutput,
		"psql",
		"UTF8",
		"1",
		`"Ada"|9007199254740993123|12.50`,
		// \dt answered by the catalog recognition shim: both tables, in the
		// single "public" namespace, owned by the connected user. tuples_only
		// is switched off first because it would suppress the \d footer that
		// carries the index list.
		"List of tables",
		"public|psql_orders|table|"+testUser,
		"public|psql_users|table|"+testUser,
		// \d psql_users: the pg_attribute rows in the catalog's canonical
		// order, then the primary-key and secondary exact indexes rendered
		// from the shim's index definitions.
		"id|string||not null|",
		"tier|string|||",
		`"psql_users_pkey" PRIMARY KEY, exact (id)`,
		`"psql_users_by_tier" exact (tier)`,
		"__VIBEDB_PSQL_SETUP_OK__",
	)

	// ON_ERROR_STOP is intentionally off for this invocation. A stock psql
	// must consume ErrorResponse, wait for ReadyForQuery, run the next
	// statement successfully, then send Terminate for the explicit \q.
	resyncOutput, err := dockerPSQL(ctx, psqlInvocation{
		image: image,
		port:  port,
		script: `
SELECT * FROM psql_table_that_does_not_exist;
SELECT 'VIBEDB_SERVER_RESYNC_OK';
\d psql_missing
\echo __VIBEDB_PSQL_RESYNC_OK__
\q
`,
	})
	if err != nil {
		t.Fatalf("psql error/resync session: %v\n%s", err, resyncOutput)
	}
	assertOutputContains(t, resyncOutput,
		"42P01",
		"VIBEDB_SERVER_RESYNC_OK",
		// \d of a missing table resolves to zero rows on the server, and psql
		// prints its own diagnostic — a psql-side error, which is why it runs
		// in this ON_ERROR_STOP=off session.
		`Did not find any relation named "psql_missing".`,
		"__VIBEDB_PSQL_RESYNC_OK__",
	)

	// Close waits for all session goroutines, so an empty hook after this point
	// proves both psql processes ended cleanly rather than by socket failure.
	server.stop()
	select {
	case err := <-sessionErrors:
		t.Fatalf("clean psql session ended with server error: %v", err)
	default:
	}
}

type psqlInvocation struct {
	image       string
	port        int
	sslMode     string
	gssEncMode  string
	onErrorStop bool
	script      string
}

func dockerPSQLVersion(ctx context.Context, image string) (string, error) {
	command := exec.CommandContext(ctx,
		"docker", "run", "--rm", "--pull=never", image, "psql", "--version")
	output, err := command.CombinedOutput()
	return string(output), err
}

func dockerPullPSQL(ctx context.Context, image string) (string, error) {
	command := exec.CommandContext(ctx, "docker", "pull", image)
	output, err := command.CombinedOutput()
	return string(output), err
}

func dockerPSQL(ctx context.Context, invocation psqlInvocation) (string, error) {
	args := []string{
		"run", "--rm", "-i", "--pull=never",
		"--add-host=host.docker.internal:host-gateway",
		"-e", "LC_ALL=C",
		"-e", "PGPASSWORD=" + testPassword,
		"-e", "PGCLIENTENCODING=UTF8",
		"-e", "PGCONNECT_TIMEOUT=10",
	}
	if invocation.sslMode != "" {
		args = append(args, "-e", "PGSSLMODE="+invocation.sslMode)
	}
	if invocation.gssEncMode != "" {
		args = append(args, "-e", "PGGSSENCMODE="+invocation.gssEncMode)
	}
	args = append(args,
		invocation.image,
		"psql",
		"-X",
		"--no-align",
		"--tuples-only",
		"--set=VERBOSITY=sqlstate",
		"--set=ON_ERROR_STOP="+strconv.FormatBool(invocation.onErrorStop),
		"--host=host.docker.internal",
		"--port="+strconv.Itoa(invocation.port),
		"--username="+testUser,
		"--dbname="+testDatabase,
	)
	command := exec.CommandContext(ctx, "docker", args...)
	command.Stdin = strings.NewReader(invocation.script)
	output, err := command.CombinedOutput()
	if ctxErr := ctx.Err(); ctxErr != nil {
		err = errors.Join(err, ctxErr)
	}
	return string(output), err
}

func listenerPort(t *testing.T, listener net.Listener) int {
	t.Helper()
	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener address has type %T, want *net.TCPAddr", listener.Addr())
	}
	return address.Port
}

func assertOutputContains(t *testing.T, output string, fragments ...string) {
	t.Helper()
	for _, fragment := range fragments {
		if !strings.Contains(output, fragment) {
			t.Errorf("psql output missing %q:\n%s", fragment, output)
		}
	}
}

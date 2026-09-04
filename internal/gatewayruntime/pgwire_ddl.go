package gatewayruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func newGatewayDevDDL(socket string, authority *gateway.ReplicatedCatalogAuthority,
	schema *gatewaySchemaDDLRuntime,
	loggers ...func(string, ...any),
) func(context.Context, serviceauthz.Authority, string) error {
	var mu sync.Mutex
	transport := &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	client := &http.Client{Transport: transport, Timeout: 2 * time.Minute}
	return func(ctx context.Context, principal serviceauthz.Authority, text string) error {
		var err error
		if ctx, err = serviceauthz.WithAuthority(ctx, principal); err != nil {
			return err
		}
		tree, err := sqlast.ParseStatement(text)
		if err != nil {
			return err
		}
		if tree.DropTable != nil {
			if schema == nil {
				return sqlast.NewFeatureNotSupportedError(text, 0, "distributed table retirement is unavailable")
			}
			if err = schema.DropTable(ctx, tree.DropTable.Table, tree.DropTable.IfExists); err != nil {
				return err
			}
			cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancelCleanup()
			request, requestErr := http.NewRequestWithContext(cleanupCtx, http.MethodPost, "http://dev/drop-table", strings.NewReader(text))
			if requestErr != nil {
				fmt.Fprintf(os.Stderr, "committed DROP TABLE cleanup deferred table=%q: %v\n", tree.DropTable.Table, requestErr)
				return nil
			}
			response, requestErr := client.Do(request)
			if requestErr != nil {
				fmt.Fprintf(os.Stderr, "committed DROP TABLE cleanup deferred table=%q: %v\n", tree.DropTable.Table, requestErr)
				return nil
			}
			defer response.Body.Close()
			raw, readErr := io.ReadAll(io.LimitReader(response.Body, (4<<20)+1))
			if readErr != nil {
				fmt.Fprintf(os.Stderr, "committed DROP TABLE cleanup response unreadable table=%q: %v\n", tree.DropTable.Table, readErr)
				return nil
			}
			if response.StatusCode != http.StatusNoContent {
				fmt.Fprintf(os.Stderr, "committed DROP TABLE cleanup deferred table=%q status=%s: %s\n",
					tree.DropTable.Table, response.Status, strings.TrimSpace(string(raw)))
				return nil
			}
			if confirmErr := authority.ConfirmProvisionedTableRetirement(cleanupCtx, tree.DropTable.Table); confirmErr != nil {
				fmt.Fprintf(os.Stderr, "committed DROP TABLE cleanup confirmation deferred table=%q: %v\n",
					tree.DropTable.Table, confirmErr)
			}
			return nil
		}
		if tree.CreateTable == nil {
			if schema == nil {
				return sqlast.NewFeatureNotSupportedError(text, 0, "distributed schema rollout is unavailable")
			}
			return schema.Execute(ctx, text)
		}
		mu.Lock()
		defer mu.Unlock()
		ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		current, err := authority.Read(ctx)
		if err != nil {
			return err
		}
		if _, exists := current.Placement(tree.CreateTable.Table); exists {
			if tree.CreateTable.IfNotExists {
				return nil
			}
			return fmt.Errorf("%w: %s", sqldriver.ErrTableExists, tree.CreateTable.Table)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://dev/create-table", strings.NewReader(text))
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(response.Body, (4<<20)+1))
		if err != nil {
			return err
		}
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("DDL provisioning failed: %s", strings.TrimSpace(string(raw)))
		}
		addition, err := gateway.OpenReplicatedTableProvision(raw)
		if err != nil {
			return err
		}
		declarations := addition.ReplicatedTableDeclarations()
		if len(declarations) != 1 || declarations[0].Table != tree.CreateTable.Table {
			return gateway.ErrInvalidCatalog
		}
		return registerGatewayDevTable(ctx, func(ctx context.Context) error {
			return authority.RegisterProvisionedTable(ctx, addition)
		}, loggers...)
	}
}

// Both online CREATE and restart wait for an authenticated serving fence.
// Retry only election/read-index transients, within the caller's deadline;
// malformed schemas and deterministic refusals must never become busy loops.
func registerGatewayDevTable(ctx context.Context, register func(context.Context) error, loggers ...func(string, ...any)) error {
	reported := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := register(ctx)
		if err == nil {
			return nil
		}
		retryable := gateway.IsReplicatedReadRetryable(err)
		if !reported && len(loggers) == 1 && loggers[0] != nil {
			// Report bounded categories once. Do not emit SQL, manifest bytes,
			// identities, endpoints, or the possibly large joined error text.
			var refusal *gateway.ReplicatedRefusalError
			code := -1
			if errors.As(err, &refusal) && refusal != nil {
				code = int(refusal.Code)
			}
			loggers[0]("gateway: table registration pending retryable=%t refusal=%d leader=%t read_behind=%t invalid_route=%t stale_fence=%t unauthorized=%t buffer_bound=%t catalog_pending=%t",
				retryable, code, errors.Is(err, gateway.ErrReplicatedLeader), errors.Is(err, gateway.ErrReplicatedReadBehind),
				errors.Is(err, gateway.ErrReplicatedRoute), errors.Is(err, raftservice.ErrServingFence),
				errors.Is(err, gateway.ErrReplicatedUnauthorized), errors.Is(err, gateway.ErrReplicatedReadBufferBound),
				errors.Is(err, gateway.ErrReplicatedCatalogPending))
			reported = true
		}
		if !retryable {
			return err
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, ctx.Err())
		}
	}
}

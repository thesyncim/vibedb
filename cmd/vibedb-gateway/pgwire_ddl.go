package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func newGatewayDevDDL(socket string, authority *gateway.ReplicatedCatalogAuthority) func(context.Context, serviceauthz.Authority, string) error {
	var mu sync.Mutex
	transport := &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	client := &http.Client{Transport: transport, Timeout: 2 * time.Minute}
	return func(ctx context.Context, principal serviceauthz.Authority, text string) error {
		if _, err := serviceauthz.WithAuthority(ctx, principal); err != nil {
			return err
		}
		tree, err := sqlast.ParseStatement(text)
		if err != nil {
			return err
		}
		if tree.CreateTable == nil {
			return sqlast.NewFeatureNotSupportedError(text, 0, "this coordinator currently supports CREATE TABLE; other DDL requires a certified schema rollout")
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
		for {
			err := authority.RegisterProvisionedTable(ctx, addition)
			if err == nil {
				return nil
			}
			if !errors.Is(err, gateway.ErrReplicatedLeader) && !errors.Is(err, gateway.ErrReplicatedReadBehind) {
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
}

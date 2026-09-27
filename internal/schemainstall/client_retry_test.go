package schemainstall

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type controlRetryTestOpener func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)

type controlOpenErrorFixture struct {
	name string
	err  error
}

func (opener controlRetryTestOpener) OpenShardControl(
	ctx context.Context,
	node rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	return opener(ctx, node)
}

func TestClientClassifiesOnlyTransientControlOpenCauses(t *testing.T) {
	request, _, _, bundle := schemaFixture(67)
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	reset := errors.Join(rafttransport.ErrPeerAuthentication, platformControlOpenResetError())
	tests := platformTransientControlOpenFixtures()
	tests = append([]controlOpenErrorFixture{{name: "TLS handshake reset", err: reset}}, tests...)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			client, err := NewClient(ClientOptions{
				Opener: controlRetryTestOpener(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
					calls++
					return nil, test.err
				}),
				ReadDeadline: deadline, WriteDeadline: deadline,
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.Prepare(context.Background(), rafttransport.NodeID{9}, request, bundle)
			if !errors.Is(err, ErrTransientControlOpen) || calls != 1 {
				t.Fatalf("open failure=%v calls=%d, want transient marker after one open", err, calls)
			}
		})
	}
}

func TestClientDoesNotClassifyControlAuthenticationOrCancellationAsTransient(t *testing.T) {
	request, _, _, bundle := schemaFixture(68)
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	reset := platformControlOpenResetError()
	tests := []struct {
		name string
		err  error
	}{
		{name: "certificate verification", err: errors.Join(rafttransport.ErrPeerAuthentication,
			&tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}, reset)},
		{name: "x509 unknown authority", err: errors.Join(rafttransport.ErrPeerAuthentication, x509.UnknownAuthorityError{}, reset)},
		{name: "TLS alert", err: errors.Join(rafttransport.ErrPeerAuthentication, tls.AlertError(42), reset)},
		{name: "build rejection", err: errors.Join(rafttransport.ErrPeerBuild, reset)},
		{name: "key rejection", err: errors.Join(rafttransport.ErrPeerKeyMismatch, reset)},
		{name: "enrollment rejection", err: errors.Join(rafttransport.ErrPeerUnauthorized, reset)},
		{name: "identity rejection", err: errors.Join(rafttransport.ErrWrongPeer, reset)},
		{name: "authorization rejection", err: errors.Join(rafttransport.ErrUnauthorized, reset)},
		{name: "authentication without transport cause", err: rafttransport.ErrPeerAuthentication},
		{name: "context cancellation with reset", err: errors.Join(context.Canceled, reset)},
		{name: "context deadline with reset", err: errors.Join(context.DeadlineExceeded, reset)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, err := NewClient(ClientOptions{
				Opener: controlRetryTestOpener(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
					return nil, test.err
				}),
				ReadDeadline: deadline, WriteDeadline: deadline,
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.Prepare(context.Background(), rafttransport.NodeID{9}, request, bundle)
			if errors.Is(err, ErrTransientControlOpen) || !errors.Is(err, test.err) {
				t.Fatalf("rejection=%v, want original terminal error without retry marker", err)
			}
		})
	}

	t.Run("cancellation during open", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client, err := NewClient(ClientOptions{
			Opener: controlRetryTestOpener(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
				cancel()
				return nil, errors.Join(rafttransport.ErrPeerAuthentication, reset)
			}),
			ReadDeadline: deadline, WriteDeadline: deadline,
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Prepare(ctx, rafttransport.NodeID{9}, request, bundle)
		if !errors.Is(err, context.Canceled) || !errors.Is(err, reset) ||
			errors.Is(err, ErrTransientControlOpen) {
			t.Fatalf("canceled open=%v, want cancellation and original reset without retry marker", err)
		}
	})
}

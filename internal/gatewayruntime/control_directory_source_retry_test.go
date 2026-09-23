package gatewayruntime

import (
	"context"
	"errors"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type scriptedFrontendDrainSource struct {
	errs  []error
	calls int
}

func (source *scriptedFrontendDrainSource) ReadLatestFrontendDrainCut(
	context.Context,
) (frontenddrain.PreparedAckCut, error) {
	source.calls++
	if source.calls <= len(source.errs) {
		return frontenddrain.PreparedAckCut{}, source.errs[source.calls-1]
	}
	// An invalid proof ends the scenario deterministically after the source
	// finally answers.
	return frontenddrain.PreparedAckCut{}, nil
}

// A restarting peer must not turn gateway open into a node failure: an
// unreachable canonical source is retried until it answers, while a
// non-transient error or runtime shutdown still ends startup.
func TestOpenControlDirectoryWaitsForUnreachableCanonicalSource(t *testing.T) {
	profile, _, cut, _, _ := frontendDrainSourceTestFixture(t)
	_, policy := runtimeControlTLSFixture(t, []serviceauthz.Entry{{
		Node: profile.LocalIdentity().Node, Capabilities: serviceauthz.AllCapabilities,
	}})
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	open := func(ctx context.Context, source *scriptedFrontendDrainSource) error {
		runtime := &Runtime{ctx: ctx, config: Config{
			ControlDirectory: &runtimeCanonicalCutReaderTest{cut: cut},
			TLSProfile:       profile, Authorization: policy,
			CanonicalFrontendDrainRuntimeSource: source,
		}}
		return runtime.openControlDirectory()
	}

	source := &scriptedFrontendDrainSource{errs: []error{
		errors.Join(errors.New("source reader unavailable"), refused), refused, refused,
	}}
	err := open(t.Context(), source)
	if source.calls != 4 || err == nil || !strings.Contains(err.Error(), "proof is invalid") {
		t.Fatalf("unreachable source calls=%d err=%v, want retries until it answers", source.calls, err)
	}

	source = &scriptedFrontendDrainSource{errs: []error{errors.New("authenticated refusal")}}
	if err := open(t.Context(), source); source.calls != 1 || err == nil {
		t.Fatalf("non-transient source error calls=%d err=%v, want immediate failure", source.calls, err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	forever := make([]error, 1<<16)
	for index := range forever {
		forever[index] = refused
	}
	started := time.Now()
	source = &scriptedFrontendDrainSource{errs: forever}
	if err := open(ctx, source); !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 2*time.Second {
		t.Fatalf("shutdown while waiting err=%v elapsed=%s", err, time.Since(started))
	}
}

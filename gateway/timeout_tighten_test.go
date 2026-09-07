package gateway

import (
	"context"
	"testing"
	"time"
)

func TestTightenTimeoutInheritsTighterParent(t *testing.T) {
	parent, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stop()
	opctx, cancel := tightenTimeout(parent, time.Hour)
	defer cancel()
	if opctx != parent {
		t.Fatalf("tightenTimeout armed a redundant timer under a tighter parent")
	}
	select {
	case <-opctx.Done():
		t.Fatal("inherited context already done")
	default:
	}
}

func TestTightenTimeoutArmsWhenTighter(t *testing.T) {
	opctx, cancel := tightenTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	deadline, ok := opctx.Deadline()
	if !ok {
		t.Fatal("tightenTimeout returned a context without a deadline")
	}
	if until := time.Until(deadline); until <= 0 || until > 50*time.Millisecond {
		t.Fatalf("fence deadline in %v, want (0,50ms]", until)
	}
}

func TestTightenTimeoutNonPositiveExpires(t *testing.T) {
	// WithTimeout with a non-positive duration reports done immediately;
	// inheritance must not launder a live parent into its place.
	opctx, cancel := tightenTimeout(context.Background(), -time.Second)
	defer cancel()
	select {
	case <-opctx.Done():
	default:
		t.Fatal("non-positive fence did not expire")
	}
}

func TestTightenTimeoutSignalCancelPropagatesWhenInherited(t *testing.T) {
	// Fanout aborts sibling shards through the fence cancel. When the fence
	// is inherited, that signal must still fire immediately instead of
	// degrading to a no-op that waits out the parent deadline.
	parent, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	opctx, cancel := tightenTimeoutSignal(parent, time.Hour)
	select {
	case <-opctx.Done():
		t.Fatal("inherited signal context already done")
	default:
	}
	cancel()
	select {
	case <-opctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("inherited fence cancel did not propagate")
	}
	if opctx == parent {
		t.Fatal("signal fence returned the parent itself; cancel cannot propagate")
	}
}

func TestTightenTimeoutSignalArmsWhenTighter(t *testing.T) {
	opctx, cancel := tightenTimeoutSignal(context.Background(), 50*time.Millisecond)
	defer cancel()
	deadline, ok := opctx.Deadline()
	if !ok {
		t.Fatal("signal fence returned a context without a deadline")
	}
	if until := time.Until(deadline); until <= 0 || until > 50*time.Millisecond {
		t.Fatalf("signal fence deadline in %v, want (0,50ms]", until)
	}
	cancel()
	select {
	case <-opctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("signal fence cancel did not propagate")
	}
}

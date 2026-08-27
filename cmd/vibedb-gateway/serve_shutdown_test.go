package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestGatewayShutdownRecognizesSignalCauseWithoutHidingFailures(t *testing.T) {
	signal := errors.New("terminated signal received")
	failure := errors.New("failed durable shutdown")
	for _, canceled := range []error{nil, context.Canceled, signal,
		fmt.Errorf("listener: %w", signal), errors.Join(signal, context.Canceled)} {
		if err := nonCanceledError(canceled, signal); err != nil {
			t.Fatalf("normal shutdown treated as failure: %v", err)
		}
		if err := nonCanceledError(errors.Join(canceled, failure), signal); !errors.Is(err, failure) {
			t.Fatalf("shutdown hid an independent failure: %v", err)
		}
	}
	if err := nonCanceledError(context.DeadlineExceeded, signal); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("shutdown hid a deadline failure")
	}
	if err := nonCanceledError(signal); err == nil {
		t.Fatal("an unobserved signal cause was ignored")
	}
}

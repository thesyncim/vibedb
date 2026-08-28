package serviceerrors

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

type uncomparableError []string

func (uncomparableError) Error() string { return "uncomparable failure" }

func TestWithoutPreservesUncomparableErrors(t *testing.T) {
	failure := uncomparableError{"durable shutdown"}
	if err := Without(failure, failure, context.Canceled); err == nil {
		t.Fatal("an error without comparable identity was discarded")
	}
}

func TestWithoutPreservesIndependentFailures(t *testing.T) {
	signal := errors.New("terminated signal received")
	failure := errors.New("failed durable shutdown")
	for _, canceled := range []error{nil, context.Canceled, signal,
		fmt.Errorf("listener: %w", signal), errors.Join(signal, context.Canceled)} {
		if err := Without(canceled, context.Canceled, signal); err != nil {
			t.Fatalf("normal shutdown treated as failure: %v", err)
		}
		for _, joined := range []error{errors.Join(canceled, failure),
			fmt.Errorf("shutdown: %w", errors.Join(canceled, failure))} {
			if err := Without(joined, context.Canceled, signal); !errors.Is(err, failure) {
				t.Fatalf("shutdown hid an independent failure: %v", err)
			}
		}
	}
	if err := Without(context.DeadlineExceeded, context.Canceled, signal); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("shutdown hid a deadline failure")
	}
	if err := Without(signal, context.Canceled); err == nil {
		t.Fatal("an unobserved signal cause was ignored")
	}
}

package raftsim

import (
	"errors"
	"testing"
)

func TestQueueOrdersTimePriorityAndSerial(t *testing.T) {
	q, err := NewQueue(4)
	if err != nil {
		t.Fatal(err)
	}
	inputs := []struct {
		time     uint64
		priority uint16
		value    uint64
	}{{2, 0, 1}, {1, 2, 2}, {1, 1, 3}, {1, 1, 4}}
	for _, in := range inputs {
		if err := q.Push(in.time, in.priority, Event{Kind: EventCrash, Node: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Push(0, 0, Event{Kind: EventCrash, Node: 1}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("overflow = %v", err)
	}
	want := []uint64{3, 4, 2, 1}
	for i, expected := range want {
		got, ok := q.Pop()
		if !ok || got.Serial != expected {
			t.Fatalf("pop %d = %+v, %v; want serial %d", i, got, ok, expected)
		}
	}
	if _, ok := q.Pop(); ok {
		t.Fatal("empty Pop succeeded")
	}
}

func TestQueueRejectsInvalidPastOverflowAndSerialExhaustion(t *testing.T) {
	if _, err := NewQueue(MaxTraceEvents + 1); !errors.Is(err, ErrInvalidQueueLimit) {
		t.Fatalf("oversized queue limit = %v", err)
	}
	q, err := NewQueue(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Push(1, 0, Event{Kind: EventCampaign}); !errors.Is(err, ErrInvalidTrace) {
		t.Fatalf("invalid event = %v", err)
	}
	if q.serial != 0 {
		t.Fatalf("failed admission consumed serial %d", q.serial)
	}
	if err := q.Push(5, 0, Event{Kind: EventCampaign, Node: 1}); err != nil {
		t.Fatal(err)
	}
	if _, ok := q.Pop(); !ok || q.Now() != 5 {
		t.Fatalf("pop/now = %d, %v", q.Now(), ok)
	}
	if err := q.Push(4, 0, Event{Kind: EventCrash, Node: 1}); !errors.Is(err, ErrTimeRegression) {
		t.Fatalf("past push = %v", err)
	}
	q.now = ^uint64(0)
	if err := q.PushAfter(1, 0, Event{Kind: EventCrash, Node: 1}); !errors.Is(err, ErrTimeRegression) {
		t.Fatalf("overflow delay = %v", err)
	}
	q.now = 5
	q.serial = ^uint64(0)
	if err := q.Push(5, 0, Event{Kind: EventCrash, Node: 1}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("serial exhaustion = %v", err)
	}
}

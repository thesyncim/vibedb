package raftsim

import "errors"

// ErrQueueFull reports bounded scheduler admission failure.
var ErrQueueFull = errors.New("raftsim: event queue full")

// ErrTimeRegression reports an attempt to schedule before the last delivered
// logical time.
var ErrTimeRegression = errors.New("raftsim: logical time would regress")

// ErrInvalidQueueLimit reports a scheduler bound outside 1..MaxTraceEvents.
var ErrInvalidQueueLimit = errors.New("raftsim: invalid event queue limit")

// ScheduledEvent is ordered lexicographically by logical time, priority, then
// the scheduler-assigned serial. Serial makes equal-time delivery independent
// of heap implementation details.
type ScheduledEvent struct {
	Time     uint64
	Priority uint16
	Serial   uint64
	Event    Event
}

// Queue is a bounded stable min-heap. It is intentionally not concurrency-safe:
// one simulator owner serializes all choices and deliveries.
type Queue struct {
	items  []ScheduledEvent
	limit  int
	serial uint64
	now    uint64
}

// NewQueue returns an empty queue with an exact, trace-bounded event limit.
func NewQueue(limit int) (*Queue, error) {
	if limit <= 0 || limit > MaxTraceEvents {
		return nil, ErrInvalidQueueLimit
	}
	return &Queue{limit: limit}, nil
}

// Len reports the number of retained events.
func (q *Queue) Len() int {
	if q == nil {
		return 0
	}
	return len(q.items)
}

// Now reports the logical time of the last popped event.
func (q *Queue) Now() uint64 {
	if q == nil {
		return 0
	}
	return q.now
}

// Push schedules e and assigns the next stable serial.
func (q *Queue) Push(at uint64, priority uint16, e Event) error {
	if !e.Valid() {
		return ErrInvalidTrace
	}
	if q == nil || len(q.items) >= q.limit || q.serial == ^uint64(0) {
		return ErrQueueFull
	}
	if at < q.now {
		return ErrTimeRegression
	}
	q.serial++
	e.Time = at
	item := ScheduledEvent{Time: at, Priority: priority, Serial: q.serial, Event: e}
	q.items = append(q.items, item)
	q.up(len(q.items) - 1)
	return nil
}

// PushAfter schedules relative to the current logical time and rejects
// overflow rather than wrapping into the past.
func (q *Queue) PushAfter(delay uint64, priority uint16, e Event) error {
	if q == nil || delay > ^uint64(0)-q.now {
		return ErrTimeRegression
	}
	return q.Push(q.now+delay, priority, e)
}

// Pop removes the next event in canonical order.
func (q *Queue) Pop() (ScheduledEvent, bool) {
	if q == nil || len(q.items) == 0 {
		return ScheduledEvent{}, false
	}
	first := q.items[0]
	q.now = first.Time
	last := len(q.items) - 1
	q.items[0] = q.items[last]
	q.items[last] = ScheduledEvent{}
	q.items = q.items[:last]
	if last != 0 {
		q.down(0)
	}
	return first, true
}

func (q *Queue) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !scheduledLess(q.items[i], q.items[parent]) {
			return
		}
		q.items[i], q.items[parent] = q.items[parent], q.items[i]
		i = parent
	}
}

func (q *Queue) down(i int) {
	for {
		left := 2*i + 1
		if left >= len(q.items) {
			return
		}
		small := left
		right := left + 1
		if right < len(q.items) && scheduledLess(q.items[right], q.items[left]) {
			small = right
		}
		if !scheduledLess(q.items[small], q.items[i]) {
			return
		}
		q.items[i], q.items[small] = q.items[small], q.items[i]
		i = small
	}
}

func scheduledLess(a, b ScheduledEvent) bool {
	if a.Time != b.Time {
		return a.Time < b.Time
	}
	if a.Priority != b.Priority {
		return a.Priority < b.Priority
	}
	return a.Serial < b.Serial
}

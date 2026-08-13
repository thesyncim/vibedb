package raftsim

import "testing"

func TestEventCanonicalFieldUse(t *testing.T) {
	valid := []Event{
		{Kind: EventCampaign, Node: 1},
		{Kind: EventCaptureReady, Node: 1, Ref: 1},
		{Kind: EventSendMessage, Node: 1, Ref: 2},
		{Kind: EventApplyEntry, Node: 1, Ref: 1, Value: 9},
		{Kind: EventDuplicateMessage, Node: 2, Peer: 1, Ref: 2, Value: 3},
		{Kind: EventRestart, Node: 1, Value: 2},
	}
	for _, event := range valid {
		if !event.Valid() {
			t.Fatalf("valid event rejected: %+v", event)
		}
	}
	invalid := []Event{
		{Kind: EventCampaign, Node: 1, Ref: 1},
		{Kind: EventPersistReady, Node: 1, Ref: 1, Value: 1},
		{Kind: EventDropMessage, Node: 2, Peer: 1, Ref: 2, Value: 3},
		{Kind: EventDuplicateMessage, Node: 2, Peer: 1, Ref: 2},
		{Kind: EventRestart, Node: 1},
		{Kind: EventPartitionLink, Node: 1, Peer: 2, Ref: 1},
		{Kind: EventPartitionLink, Node: 2, Peer: 1},
		{Kind: EventSendMessage, Node: 1, Peer: 2, Ref: 1},
	}
	for _, event := range invalid {
		if event.Valid() {
			t.Fatalf("noncanonical event accepted: %+v", event)
		}
	}
}

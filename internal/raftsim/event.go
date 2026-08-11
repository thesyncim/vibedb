package raftsim

import "fmt"

// EventKind is one named integration or fault boundary. Values are persisted
// in traces and therefore append-only within one TraceFormatVersion.
type EventKind uint8

const (
	EventCampaign EventKind = iota + 1
	EventLeaderTick
	EventPropose
	EventRequestRead
	EventCaptureReady
	EventPersistReady
	EventSendMessage
	EventInstallSnapshot
	EventApplyEntry
	EventRecordReadState
	EventServeRead
	EventRespondProposal
	EventAdvanceReady
	EventDeliverMessage
	EventDropMessage
	EventDuplicateMessage
	EventCrash
	EventRestart
	EventPartitionLink
	EventHealLink
	EventFailPersistDefinite
	EventFinishMessages
	EventFinishReadStates
	EventFailPersistAmbiguous
	eventKindLimit
)

// Event is the fixed-width decision recorded by a deterministic run. Ref is a
// scenario-defined proposal, read, Ready, or message identifier. Value carries
// a scenario-defined scalar; neither field is interpreted by the trace codec.
type Event struct {
	Step  uint32
	Kind  EventKind
	Time  uint64
	Node  uint64
	Peer  uint64
	Ref   uint64
	Value uint64
}

// Valid reports whether the event is structurally representable. Scenario
// state determines whether the action is currently enabled.
func (e Event) Valid() bool {
	if e.Kind == 0 || e.Kind >= eventKindLimit || e.Node == 0 {
		return false
	}
	switch e.Kind {
	case EventDeliverMessage, EventDropMessage, EventDuplicateMessage:
		if e.Peer == 0 || e.Peer == e.Node || e.Ref == 0 {
			return false
		}
		if e.Kind == EventDuplicateMessage {
			return e.Value != 0
		}
		return e.Value == 0
	case EventPartitionLink, EventHealLink:
		return e.Peer > e.Node && e.Ref == 0 && e.Value == 0
	case EventCaptureReady, EventPersistReady, EventFinishMessages,
		EventInstallSnapshot, EventRecordReadState,
		EventFinishReadStates, EventAdvanceReady, EventFailPersistDefinite,
		EventFailPersistAmbiguous:
		return e.Peer == 0 && e.Ref != 0 && e.Value == 0
	case EventApplyEntry:
		return e.Peer == 0 && e.Ref != 0
	case EventSendMessage:
		return e.Peer == 0 && e.Ref != 0 && e.Value == 0
	case EventPropose, EventRequestRead, EventServeRead, EventRespondProposal:
		return e.Peer == 0 && e.Ref != 0 && e.Value == 0
	case EventRestart:
		return e.Peer == 0 && e.Ref == 0 && e.Value != 0
	case EventCampaign, EventLeaderTick, EventCrash:
		return e.Peer == 0 && e.Ref == 0 && e.Value == 0
	}
	return false
}

func (k EventKind) String() string {
	switch k {
	case EventCampaign:
		return "campaign"
	case EventLeaderTick:
		return "leader-tick"
	case EventPropose:
		return "propose"
	case EventRequestRead:
		return "request-read"
	case EventCaptureReady:
		return "capture-ready"
	case EventPersistReady:
		return "persist-ready"
	case EventSendMessage:
		return "send-message"
	case EventInstallSnapshot:
		return "install-snapshot"
	case EventApplyEntry:
		return "apply-entry"
	case EventRecordReadState:
		return "record-read-state"
	case EventServeRead:
		return "serve-read"
	case EventRespondProposal:
		return "respond-proposal"
	case EventAdvanceReady:
		return "advance-ready"
	case EventDeliverMessage:
		return "deliver-message"
	case EventDropMessage:
		return "drop-message"
	case EventDuplicateMessage:
		return "duplicate-message"
	case EventCrash:
		return "crash"
	case EventRestart:
		return "restart"
	case EventPartitionLink:
		return "partition-link"
	case EventHealLink:
		return "heal-link"
	case EventFailPersistDefinite:
		return "fail-persist-definite"
	case EventFinishMessages:
		return "finish-messages"
	case EventFinishReadStates:
		return "finish-read-states"
	case EventFailPersistAmbiguous:
		return "fail-persist-ambiguous"
	default:
		return fmt.Sprintf("event-kind(%d)", uint8(k))
	}
}

// Package systemkey is the dependency-free registry for hidden replicated
// state keyspaces. A package owns one complete 16-prefix block; individual row
// grammars allocate only inside that block.
package systemkey

const (
	TransactionFirst byte = 0x10
	TransactionLast  byte = 0x1f

	RouteGateFirst byte = 0x20
	RouteGateLast  byte = 0x2f

	BackupFirst byte = 0x30
	BackupLast  byte = 0x3f

	RequestLedgerFirst byte = 0x40
	RequestLedgerLast  byte = 0x4f

	ExecutionPinFirst byte = 0x50
	ExecutionPinLast  byte = 0x5f

	// ReservedFirst through ReservedLast are unavailable until the registry is
	// deliberately extended. Unknown hidden keys remain reopen-fatal.
	ReservedFirst byte = 0x60
	ReservedLast  byte = 0xff
)

const (
	transactionWidth = int(TransactionLast) - int(TransactionFirst) - 15
	transactionRoute = int(RouteGateFirst) - int(TransactionLast) - 1
	routeWidth       = int(RouteGateLast) - int(RouteGateFirst) - 15
	routeBackup      = int(BackupFirst) - int(RouteGateLast) - 1
	backupWidth      = int(BackupLast) - int(BackupFirst) - 15
	backupLedger     = int(RequestLedgerFirst) - int(BackupLast) - 1
	ledgerWidth      = int(RequestLedgerLast) - int(RequestLedgerFirst) - 15
	ledgerPin        = int(ExecutionPinFirst) - int(RequestLedgerLast) - 1
	pinWidth         = int(ExecutionPinLast) - int(ExecutionPinFirst) - 15
	pinReserved      = int(ReservedFirst) - int(ExecutionPinLast) - 1
	reservedWidth    = int(ReservedLast) - int(ReservedFirst) - 159
)

// Paired positive/negative array bounds require every geometry expression to
// be exactly zero at compile time. Blocks therefore cannot overlap or acquire
// an unowned gap even if tests are skipped.
var (
	_ [transactionWidth]struct{}
	_ [-transactionWidth]struct{}
	_ [transactionRoute]struct{}
	_ [-transactionRoute]struct{}
	_ [routeWidth]struct{}
	_ [-routeWidth]struct{}
	_ [routeBackup]struct{}
	_ [-routeBackup]struct{}
	_ [backupWidth]struct{}
	_ [-backupWidth]struct{}
	_ [backupLedger]struct{}
	_ [-backupLedger]struct{}
	_ [ledgerWidth]struct{}
	_ [-ledgerWidth]struct{}
	_ [ledgerPin]struct{}
	_ [-ledgerPin]struct{}
	_ [pinWidth]struct{}
	_ [-pinWidth]struct{}
	_ [pinReserved]struct{}
	_ [-pinReserved]struct{}
	_ [reservedWidth]struct{}
	_ [-reservedWidth]struct{}
)

// Owner identifies one frozen hidden-key allocation block.
type Owner uint8

const (
	OwnerTransaction Owner = iota + 1
	OwnerRouteGate
	OwnerBackup
	OwnerRequestLedger
	OwnerExecutionPin
	OwnerReserved
)

// Block is one inclusive prefix range.
type Block struct {
	First byte
	Last  byte
}

// ForOwner returns the immutable allocation block for owner.
func ForOwner(owner Owner) (Block, bool) {
	switch owner {
	case OwnerTransaction:
		return Block{TransactionFirst, TransactionLast}, true
	case OwnerRouteGate:
		return Block{RouteGateFirst, RouteGateLast}, true
	case OwnerBackup:
		return Block{BackupFirst, BackupLast}, true
	case OwnerRequestLedger:
		return Block{RequestLedgerFirst, RequestLedgerLast}, true
	case OwnerExecutionPin:
		return Block{ExecutionPinFirst, ExecutionPinLast}, true
	case OwnerReserved:
		return Block{ReservedFirst, ReservedLast}, true
	default:
		return Block{}, false
	}
}

// Contains reports whether prefix is allocated to this block.
func (block Block) Contains(prefix byte) bool {
	return block.First <= block.Last && prefix >= block.First && prefix <= block.Last
}

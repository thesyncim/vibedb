package requestledger

import (
	"github.com/thesyncim/vibedb/internal/systemkey"
)

const (
	PrincipalQuotaStoragePrefix byte = systemkey.RequestLedgerFirst + 3
	PrincipalQuotaKeyBytes           = 1 + 32 + 16
	PrincipalQuotaRecordBytes        = 132
)

var principalQuotaMagic = [4]byte{'V', 'R', 'L', 'O'}

type PrincipalQuotaRecord struct {
	TenantDigest                                                Digest
	Principal                                                   PrincipalID
	Revision                                                    uint64
	ResidentBytes, ReservedBytes, TombstoneBytes, PlanningBytes uint64
	RequestCount, TombstoneCount, PlanningCount                 uint64
}

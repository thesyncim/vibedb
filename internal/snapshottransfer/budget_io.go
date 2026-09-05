package snapshottransfer

import (
	"context"
	"crypto/sha256"
	"io"

	"github.com/thesyncim/vibedb/internal/migrationbudget"
)

// budgetedWriter accounts serialized snapshot bytes before they enter the
// wrapped writer. It is intentionally small and allocation-free on the hot
// path; a nil budget keeps existing embedders source-compatible.
type budgetedWriter struct {
	ctx    context.Context
	budget *migrationbudget.Budget
	lease  *migrationbudget.Lease
	writer io.Writer
	cost   func(uint64) migrationbudget.Cost
}

func (writer budgetedWriter) Write(src []byte) (int, error) {
	if len(src) == 0 || writer.budget == nil {
		return writer.writer.Write(src)
	}
	total := 0
	for total < len(src) {
		count, err := consumeBudgetBytes(writer.ctx, writer.budget, writer.lease,
			uint64(len(src)-total), writer.cost)
		if err != nil {
			return total, err
		}
		part := src[total : total+int(count)]
		written, writeErr := writer.writer.Write(part)
		total += written
		if writeErr != nil {
			return total, writeErr
		}
		if written != len(part) {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

// budgetedReader accounts bytes read from a published artifact while the
// caller performs its target-side stage write. The disk read and write classes
// are separate, so an export's source I/O cannot consume the target receive
// allowance by accident.
type budgetedReader struct {
	ctx    context.Context
	budget *migrationbudget.Budget
	lease  *migrationbudget.Lease
	reader io.Reader
}

// budgetedVerifierReader paces the repository's final artifact verification.
// Verification reads and hashes an already staged file; it does not charge a
// target stage write, and it caps the actual Read call after any pressure
// downshift while waiting for tokens.
type budgetedVerifierReader struct {
	ctx    context.Context
	budget *migrationbudget.Budget
	lease  *migrationbudget.Lease
	reader io.Reader
}

func (reader budgetedVerifierReader) Read(dst []byte) (int, error) {
	if len(dst) == 0 || reader.budget == nil {
		return reader.reader.Read(dst)
	}
	count, err := consumeBudgetBytes(reader.ctx, reader.budget, reader.lease, uint64(len(dst)), func(bytes uint64) migrationbudget.Cost {
		return migrationbudget.Cost{CPU: bytes, DiskRead: bytes}
	})
	if err != nil {
		return 0, err
	}
	return reader.reader.Read(dst[:int(count)])
}

func (reader budgetedReader) Read(dst []byte) (int, error) {
	if len(dst) == 0 || reader.budget == nil {
		return reader.reader.Read(dst)
	}
	// Reserve all classes before touching the artifact. That bounds the actual
	// read and the stage write even if foreground pressure shrinks a burst while
	// the token wait is in progress. A short underlying read conservatively
	// consumes the reservation; refunds are deliberately impossible.
	count, err := consumeBudgetBytes(reader.ctx, reader.budget, reader.lease,
		uint64(len(dst)), func(bytes uint64) migrationbudget.Cost {
			return migrationbudget.Cost{CPU: bytes, DiskRead: bytes, DiskWrite: bytes}
		})
	if err != nil {
		return 0, err
	}
	return reader.reader.Read(dst[:int(count)])
}

func budgetContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func budgetedHashChunk(
	ctx context.Context, budget *migrationbudget.Budget, chunk []byte,
) ([sha256.Size]byte, error) {
	if budget == nil {
		return sha256.Sum256(chunk), nil
	}
	hash := sha256.New()
	for offset := 0; offset < len(chunk); {
		count, err := consumeBudgetBytes(ctx, budget, nil, uint64(len(chunk)-offset), func(bytes uint64) migrationbudget.Cost {
			return migrationbudget.Cost{CPU: bytes}
		})
		if err != nil {
			return [sha256.Size]byte{}, err
		}
		if _, err := hash.Write(chunk[offset : offset+int(count)]); err != nil {
			return [sha256.Size]byte{}, err
		}
		offset += int(count)
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result, nil
}

// consumeBudgetBytes reserves at most requested bytes and converts the
// per-resource result into one safe byte count for the real operation. Cost
// functions used by snapshot transfer are linear in bytes; a resource charged
// twice therefore permits half as many bytes. A pressure downshift can make
// different resource grants disagree, so the minimum is used and any excess
// reservation is intentionally conservative.
func consumeBudgetBytes(
	ctx context.Context, budget *migrationbudget.Budget, lease *migrationbudget.Lease,
	requested uint64, costForBytes func(uint64) migrationbudget.Cost,
) (uint64, error) {
	if requested == 0 || costForBytes == nil {
		if requested != 0 && costForBytes == nil {
			return 0, migrationbudget.ErrInvalidConfig
		}
		return 0, nil
	}
	if budget == nil {
		return requested, nil
	}
	requestedCost := costForBytes(requested)
	var granted migrationbudget.Cost
	var err error
	if lease != nil {
		granted, err = lease.ConsumeChunk(ctx, requestedCost)
	} else {
		granted, err = budget.ConsumeChunk(ctx, requestedCost)
	}
	if err != nil {
		return 0, err
	}
	result := requested
	perByte := costForBytes(1)
	for _, resource := range []struct {
		requested uint64
		granted   uint64
		unit      uint64
	}{
		{requestedCost.CPU, granted.CPU, perByte.CPU},
		{requestedCost.DiskRead, granted.DiskRead, perByte.DiskRead},
		{requestedCost.DiskWrite, granted.DiskWrite, perByte.DiskWrite},
		{requestedCost.NetworkSend, granted.NetworkSend, perByte.NetworkSend},
		{requestedCost.NetworkReceive, granted.NetworkReceive, perByte.NetworkReceive},
	} {
		if resource.requested == 0 {
			continue
		}
		if resource.unit != 0 && resource.granted/resource.unit < result {
			result = resource.granted / resource.unit
		}
	}
	if result == 0 {
		// A valid resource may charge more than one token per byte while its
		// configured burst is one token (for example, a second hash pass). The
		// initial reservation above can then return a partial cost whose byte
		// quotient is zero. Complete the one-byte reservation with bounded
		// follow-up reservations before allowing the caller to issue the byte;
		// otherwise a legal small-burst configuration would fail or bypass its
		// accounting boundary.
		target := costForBytes(1)
		if target == (migrationbudget.Cost{}) {
			return 0, migrationbudget.ErrInvalidConfig
		}
		for !costCovers(granted, target) {
			missing := costDifference(target, granted)
			var next migrationbudget.Cost
			if lease != nil {
				next, err = lease.ConsumeChunk(ctx, missing)
			} else {
				next, err = budget.ConsumeChunk(ctx, missing)
			}
			if err != nil {
				return 0, err
			}
			if next == (migrationbudget.Cost{}) {
				return 0, migrationbudget.ErrInvalidConfig
			}
			granted = addCost(granted, next)
		}
		return 1, nil
	}
	return result, nil
}

func costCovers(granted, target migrationbudget.Cost) bool {
	return granted.CPU >= target.CPU && granted.DiskRead >= target.DiskRead &&
		granted.DiskWrite >= target.DiskWrite && granted.NetworkSend >= target.NetworkSend &&
		granted.NetworkReceive >= target.NetworkReceive
}

func costDifference(target, granted migrationbudget.Cost) migrationbudget.Cost {
	return migrationbudget.Cost{
		CPU:            costDeficit(target.CPU, granted.CPU),
		DiskRead:       costDeficit(target.DiskRead, granted.DiskRead),
		DiskWrite:      costDeficit(target.DiskWrite, granted.DiskWrite),
		NetworkSend:    costDeficit(target.NetworkSend, granted.NetworkSend),
		NetworkReceive: costDeficit(target.NetworkReceive, granted.NetworkReceive),
	}
}

func addCost(left, right migrationbudget.Cost) migrationbudget.Cost {
	return migrationbudget.Cost{
		CPU:            left.CPU + right.CPU,
		DiskRead:       left.DiskRead + right.DiskRead,
		DiskWrite:      left.DiskWrite + right.DiskWrite,
		NetworkSend:    left.NetworkSend + right.NetworkSend,
		NetworkReceive: left.NetworkReceive + right.NetworkReceive,
	}
}

func costDeficit(target, granted uint64) uint64 {
	if granted >= target {
		return 0
	}
	return target - granted
}

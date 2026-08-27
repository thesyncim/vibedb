package durable

// AutomaticCompactionStatus is a lock-free operator snapshot. Debt is the
// larger of append high-water growth and newly fenced retirement bytes since
// the last successful automatic run; computing it never scans pages/extents.
type AutomaticCompactionStatus struct {
	Enabled, Armed, InFlight bool
	DebtBytes                uint64
	TriggerBytes             uint64
	RearmBytes               uint64
	BaseGeneration           uint64
	LastCheckGeneration      uint64
	LastAttemptGeneration    uint64
	Checks, Starts           uint64
	Successes, Failures      uint64
	ReaderSkips              uint64
	RecoveryFloorSkips       uint64
	CheckpointGroupSkips     uint64
}

// AutomaticCompactionStatus returns the current bounded scheduler state
// without file I/O or collection scans.
func (c *Collection) AutomaticCompactionStatus() AutomaticCompactionStatus {
	if c == nil {
		return AutomaticCompactionStatus{}
	}
	p := c.options.AutomaticCompaction
	return AutomaticCompactionStatus{
		Enabled: p.Enabled, Armed: c.autoCompactionArmed.Load(),
		InFlight:  c.autoCompactionWorker.Load(),
		DebtBytes: c.autoCompactionDebt.Load(), TriggerBytes: p.TriggerBytes,
		RearmBytes:            p.RearmBytes,
		BaseGeneration:        c.autoCompactionBaseGeneration.Load(),
		LastCheckGeneration:   c.autoCompactionLastCheck.Load(),
		LastAttemptGeneration: c.autoCompactionLastAttempt.Load(),
		Checks:                c.autoCompactionChecks.Load(), Starts: c.autoCompactionStarts.Load(),
		Successes: c.autoCompactionSuccesses.Load(), Failures: c.autoCompactionFailures.Load(),
		ReaderSkips:          c.autoCompactionReaderSkips.Load(),
		RecoveryFloorSkips:   c.autoCompactionRecoverySkips.Load(),
		CheckpointGroupSkips: c.autoCompactionOwnerSkips.Load(),
	}
}

func automaticCompactionDelta(current, baseline uint64) uint64 {
	if current <= baseline {
		return 0
	}
	return current - baseline
}

// considerAutomaticCompaction is called after a physical publication while
// writer remains held. It is normally one pair of atomic loads and returns;
// once per MinGenerationInterval it samples two bounded registries.
func (c *Collection) considerAutomaticCompaction(state *fileStoreState) {
	if c == nil || state == nil || !c.options.AutomaticCompaction.Enabled || c.closed {
		return
	}
	p := c.options.AutomaticCompaction
	generation := state.root.Generation
	lastCheck := c.autoCompactionLastCheck.Load()
	if generation < lastCheck || generation-lastCheck < p.MinGenerationInterval ||
		!c.autoCompactionLastCheck.CompareAndSwap(lastCheck, generation) {
		return
	}
	c.autoCompactionChecks.Add(1)
	retiredBytes := c.reclaimer.Stats().PendingBytes
	debt := max(
		automaticCompactionDelta(state.fileEnd, c.autoCompactionBaseFileEnd.Load()),
		automaticCompactionDelta(retiredBytes, c.autoCompactionBaseRetired.Load()),
	)
	c.autoCompactionDebt.Store(debt)
	if debt < p.TriggerBytes {
		return
	}
	lastAttempt := c.autoCompactionLastAttempt.Load()
	if generation < lastAttempt || generation-lastAttempt < p.MinGenerationInterval {
		return
	}
	if !c.autoCompactionArmed.Load() {
		lastDebt := c.autoCompactionLastAttemptDebt.Load()
		rearmDelta := p.TriggerBytes - p.RearmBytes
		if debt <= lastDebt || debt-lastDebt < rearmDelta {
			return
		}
		c.autoCompactionArmed.Store(true)
	}
	// Avoid duplicating work around a caller-owned explicit compaction.
	if c.autoCompactionFlight.Load() || c.autoCompactionWorker.Load() {
		return
	}
	readers := c.readerSummary(generation)
	if readers.active != 0 {
		c.autoCompactionReaderSkips.Add(1)
		return
	}
	recoveryFloor := c.committer.FallbackGeneration()
	if generation > recoveryFloor &&
		generation-recoveryFloor > p.MaxRecoveryLagGenerations {
		c.autoCompactionRecoverySkips.Add(1)
		return
	}
	if c.checkpointGroup.Load() != nil || c.checkpointGroupRetired.Load() {
		c.autoCompactionOwnerSkips.Add(1)
		return
	}
	if !c.autoCompactionWorker.CompareAndSwap(false, true) {
		return
	}
	c.autoCompactionArmed.Store(false)
	c.autoCompactionLastAttempt.Store(generation)
	c.autoCompactionLastAttemptDebt.Store(debt)
	c.autoCompactionStarts.Add(1)
	c.autoCompactionWait.Add(1)
	go c.runAutomaticCompaction()
}

func (c *Collection) runAutomaticCompaction() {
	defer c.autoCompactionWait.Done()
	defer c.autoCompactionWorker.Store(false)
	_, err := c.CompactOnline()
	if err != nil {
		c.autoCompactionFailures.Add(1)
		return
	}
	state := c.state.Load()
	if state != nil {
		c.autoCompactionBaseFileEnd.Store(state.fileEnd)
		c.autoCompactionBaseGeneration.Store(state.root.Generation)
	}
	if c.reclaimer != nil {
		c.autoCompactionBaseRetired.Store(c.reclaimer.Stats().PendingBytes)
	}
	c.autoCompactionDebt.Store(0)
	c.autoCompactionLastAttemptDebt.Store(0)
	c.autoCompactionArmed.Store(true)
	c.autoCompactionSuccesses.Add(1)
}

package shardservice

// scheduleJournalCompaction is constant-time and never delays the terminal
// transaction response. A capacity-one edge coalesces arbitrary retirement
// bursts into one background opportunity check.
func (s *Server) scheduleJournalCompaction() {
	if s == nil {
		return
	}
	select {
	case s.journalCompact <- struct{}{}:
	default:
	}
}

func (s *Server) runJournalCompactor() {
	var terminal error
	defer func() {
		s.maintenanceWG.Done()
		if terminal != nil && s.opts.OnError != nil {
			// Done precedes the callback so OnError retains Server's documented
			// permission to synchronously call Close without self-deadlocking.
			s.opts.OnError(terminal)
		}
	}()
	for {
		select {
		case <-s.baseCtx.Done():
			return
		case <-s.journalCompact:
			opportunity := s.journal.CompactionOpportunity()
			if !opportunity.Recommended {
				continue
			}
			if err := s.journal.Compact(); err != nil {
				terminal = err
				return
			}
		}
	}
}

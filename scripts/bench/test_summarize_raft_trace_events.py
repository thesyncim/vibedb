import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).with_name("summarize-raft-trace-events.py")
SPEC = importlib.util.spec_from_file_location("summarize_raft_trace_events", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def log(timestamp, category, message):
    return (
        f'M=1 P=0 G=7 Log Time={timestamp} Task=0 '
        f'Category="{category}" Message="{message}"'
    )


class SummarizeRaftTraceEventsTest(unittest.TestCase):
    def test_pairs_empty_and_entry_batches_and_reports_quantiles(self):
        lines = [
            log(1_000_000, "raft.append", "event=submit group=ab ready=1 index=0 entries=0 sync=true snapshot=false"),
            log(3_000_000, "raft.append", "event=complete group=ab ready=1 index=0 entries=0 sync=true snapshot=false"),
            log(5_000_000, "raft.append", "event=submit group=ab ready=2 index=8 entries=0 sync=false snapshot=false"),
            log(10_000_000, "raft.append", "event=complete group=ab ready=2 index=8 entries=0 sync=false snapshot=false"),
            log(12_000_000, "raft.append", "event=submit group=ab ready=3 index=0 entries=0 sync=false snapshot=true"),
            log(15_000_000, "raft.append", "event=complete group=ab ready=3 index=0 entries=0 sync=false snapshot=true"),
            log(16_000_000, "raft.append", "event=submit group=ab ready=4 index=0 entries=0 sync=true"),
            log(20_000_000, "raft.append", "event=complete group=ab ready=4 index=0 entries=0 sync=true"),
            log(22_000_000, "raft.append", "event=submit group=ab ready=5 index=12 entries=1 sync=true snapshot=false"),
            log(30_000_000, "raft.append", "event=complete group=ab ready=5 index=12 entries=1 sync=true snapshot=false"),
            log(31_000_000, "other.category", "event=submit group=ab ready=99 index=0 entries=0 sync=true snapshot=false"),
        ]

        result = MODULE.summarize(lines)
        append = result["append"]
        self.assertEqual(append["empty_sync"]["count"], 1)
        self.assertEqual(append["empty_sync"]["total_ms"], 2.0)
        self.assertEqual(append["hint_candidate"]["count"], 1)
        self.assertEqual(append["empty_snapshot"]["count"], 1)
        self.assertEqual(append["unknown"]["count"], 1)
        self.assertEqual(append["entry-bearing"]["count"], 1)
        self.assertEqual(append["entry-bearing"]["total_ms"], 8.0)
        self.assertEqual(append["empty"]["count"], 4)
        self.assertEqual(append["empty"]["total_ms"], 14.0)
        self.assertEqual(append["empty"]["mean_ms"], 3.5)
        self.assertEqual(append["empty"]["p50_ms"], 3.5)
        self.assertEqual(append["empty"]["p95_ms"], 5.0)
        self.assertEqual(append["empty"]["unmatched"], 0)
        self.assertEqual(append["empty"]["submit_sync"], {"true": 2, "false": 2, "unknown": 0})

    def test_retry_keeps_first_submit_and_trace_boundaries_are_unmatched(self):
        lines = [
            log(1_000_000, "raft.append", "event=submit group=1 ready=7 index=4 entries=1 sync=true snapshot=false"),
            log(2_000_000, "raft.append", "event=submit group=1 ready=7 index=4 entries=1 sync=true snapshot=false"),
            log(4_000_000, "raft.append", "event=complete group=1 ready=7 index=4 entries=1 sync=true snapshot=false"),
            log(5_000_000, "raft.append", "event=submit group=1 ready=8 index=0 entries=0 sync=false snapshot=false"),
            log(6_000_000, "raft.append", "event=complete group=2 ready=8 index=0 entries=0 sync=false"),
        ]

        result = MODULE.summarize(lines)
        entries = result["append"]["entry-bearing"]
        empty = result["append"]["hint_candidate"]
        self.assertEqual(entries["count"], 1)
        self.assertEqual(entries["total_ms"], 3.0)
        self.assertEqual(entries["duplicates"], 1)
        self.assertEqual(entries["unmatched_submit"], 0)
        self.assertEqual(entries["unmatched_complete"], 0)
        self.assertEqual(empty["count"], 0)
        self.assertEqual(empty["unmatched_submit"], 1)
        self.assertEqual(empty["unmatched_complete"], 0)
        self.assertEqual(empty["unmatched"], 1)
        self.assertEqual(result["append"]["unknown"]["unmatched_complete"], 1)
        self.assertEqual(result["append"]["empty"]["unmatched"], 2)

    def test_failed_completion_is_not_counted(self):
        lines = [
            log(1_000_000, "raft.append", "event=submit group=1 ready=1 index=1 entries=1 sync=true snapshot=false"),
            log(2_000_000, "raft.append", "event=complete group=1 ready=1 index=1 entries=1 sync=true snapshot=false success=false"),
        ]

        entries = MODULE.summarize(lines)["append"]["entry-bearing"]
        self.assertEqual(entries["count"], 0)
        self.assertEqual(entries["unmatched_submit"], 1)
        self.assertEqual(entries["unmatched_complete"], 0)


if __name__ == "__main__":
    unittest.main()

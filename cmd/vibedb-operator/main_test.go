package main

import "testing"

func TestPodOrdinalRequiresExactRF3StatefulSetOrdinal(t *testing.T) {
	for _, test := range []struct {
		host string
		want int
		ok   bool
	}{
		{"vibedb-shard-0", 0, true}, {"vibedb-shard-2", 2, true},
		{"vibedb-shard-3", 0, false}, {"vibedb-shard", 0, false}, {"-1", 0, false},
	} {
		got, err := podOrdinal(test.host)
		if (err == nil) != test.ok || got != test.want {
			t.Fatalf("host=%q ordinal=%d err=%v", test.host, got, err)
		}
	}
}

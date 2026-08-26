package buildgate

import "testing"

var (
	benchmarkProfile Profile
	benchmarkBytes   []byte
	benchmarkSet     CapabilitySet
	benchmarkPermit  DiskAdoptionPermit
	benchmarkDisk    DiskIdentity
)

func BenchmarkAppendPreface(b *testing.B) {
	profile := testProfile()
	dst := make([]byte, 0, PrefaceBytes)
	b.ReportAllocs()
	b.SetBytes(int64(PrefaceBytes))
	for b.Loop() {
		var err error
		dst, err = AppendPreface(dst[:0], profile)
		if err != nil {
			b.Fatal(err)
		}
	}
	benchmarkBytes = dst
}

func BenchmarkOpenPreface(b *testing.B) {
	raw, err := AppendPreface(nil, testProfile())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(PrefaceBytes))
	for b.Loop() {
		benchmarkProfile, err = OpenPreface(raw)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCheckCompatibility(b *testing.B) {
	local := testProfile()
	remote := local
	remote.Provided, _ = remote.Provided.With(200)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		benchmarkSet, err = CheckCompatibility(local, remote)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAuthorizeDiskAdoption(b *testing.B) {
	profile := testProfile()
	gate, err := NewCurrentDiskGate(profile)
	if err != nil {
		b.Fatal(err)
	}
	identity := DiskIdentity{Grammar: profile.DiskGrammar, Required: profile.Required}
	b.ReportAllocs()
	for b.Loop() {
		benchmarkPermit, err = gate.AuthorizeDiskAdoption(identity)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenDiskIdentity(b *testing.B) {
	raw, err := AppendDiskIdentity(nil, CurrentDiskIdentity())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(DiskIdentityBytes))
	for b.Loop() {
		benchmarkDisk, err = OpenDiskIdentity(raw)
		if err != nil {
			b.Fatal(err)
		}
	}
}

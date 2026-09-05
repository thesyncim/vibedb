package gatewayruntime

import "testing"

func TestRunServeFailsClosedWithoutAuthenticatedProfile(t *testing.T) {
	if status := runServe([]string{"-catalog", "not-opened.vdb"}); status != 2 {
		t.Fatalf("status=%d, want configuration failure", status)
	}
}

func TestRunServePlaintextRequiresExplicitLoopback(t *testing.T) {
	if status := runServe([]string{
		"-catalog", "not-opened.vdb",
		"-listen", "0.0.0.0:0",
		"-dev-plaintext-loopback",
	}); status != 2 {
		t.Fatalf("status=%d, want configuration failure", status)
	}
}

func TestRunServeRejectsMixedPlaintextAndTLSConfiguration(t *testing.T) {
	if status := runServe([]string{
		"-catalog", "not-opened.vdb",
		"-dev-plaintext-loopback",
		"-tls-certificate", "certificate.pem",
	}); status != 2 {
		t.Fatalf("status=%d, want configuration failure", status)
	}
}

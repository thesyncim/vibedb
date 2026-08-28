package rafttransport

import (
	"errors"
	"testing"
	"time"
)

// TestPeerTLSIndependentUTCStepMatrix proves the deliberately narrow place
// where UTC is authoritative. Each peer owns an independent injected clock;
// changing one cannot silently borrow the other peer's time or a process-wide
// wall-clock fallback. RF3 ordering and recovery are covered by the command
// matrix which runs this test beside the shipped process and logical-pulse
// gates.
func TestPeerTLSIndependentUTCStepMatrix(t *testing.T) {
	type clockStep struct {
		name       string
		clientStep time.Duration
		serverStep time.Duration
		accepted   bool
	}
	cases := []clockStep{
		{name: "both inside validity", accepted: true},
		{name: "independent opposite steps inside validity", clientStep: 59 * time.Minute, serverStep: -59 * time.Minute, accepted: true},
		{name: "client stepped past server expiry", clientStep: 61 * time.Minute},
		{name: "server stepped before client validity", serverStep: -61 * time.Minute},
		{name: "both stepped far forward", clientStep: 24 * time.Hour, serverStep: 24 * time.Hour},
		{name: "opposite unsafe steps", clientStep: -24 * time.Hour, serverStep: 24 * time.Hour},
	}

	for ordinal, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			authority := newPeerTLSTestAuthority(t, byte(80+ordinal))
			clientIdentity := peerTLSTestIdentity(byte(90+ordinal), 31)
			serverIdentity := peerTLSTestIdentity(byte(90+ordinal), 51)
			clientNow, serverNow := peerTLSTestNow, peerTLSTestNow
			newProfile := func(identity PeerIdentity, now *time.Time) *PeerTLS {
				profile, err := NewPeerTLS(PeerTLSOptions{
					IdentityOID: peerTLSTestIdentityOID,
					Identity:    identity,
					Certificate: authority.issue(t, identity,
						peerTLSTestNow.Add(-time.Hour), peerTLSTestNow.Add(time.Hour)),
					Roots: authority.roots,
					Now:   func() time.Time { return *now },
				})
				if err != nil {
					t.Fatal(err)
				}
				return profile
			}
			clientTLS := newProfile(clientIdentity, &clientNow)
			serverTLS := newProfile(serverIdentity, &serverNow)
			clientNow = clientNow.Add(test.clientStep)
			serverNow = serverNow.Add(test.serverStep)

			client, server, clientErr, serverErr := peerTLSTestHandshake(
				t, clientTLS, serverTLS, serverIdentity.Node,
				TrafficOrdinary, TrafficOrdinary,
			)
			if client != nil {
				_ = client.Close()
			}
			if server != nil {
				_ = server.Close()
			}
			if test.accepted {
				if clientErr != nil || serverErr != nil {
					t.Fatalf("valid independent clocks rejected: client=%v server=%v", clientErr, serverErr)
				}
				return
			}
			if !errors.Is(clientErr, ErrPeerAuthentication) &&
				!errors.Is(serverErr, ErrPeerAuthentication) {
				t.Fatalf("unsafe UTC step did not fail closed: client=%v server=%v", clientErr, serverErr)
			}
		})
	}
}

//go:build race

package query

// raceEnabled reports whether the test binary was built with -race. The race
// detector instruments every allocation, which inflates both the count and the
// byte total an allocation assertion measures. CI skips the tests that make
// those assertions by name; this constant keeps a plain `go test -race ./...`
// green as well, so the detector stays a one-command tool locally.
const raceEnabled = true

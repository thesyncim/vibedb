// Package serviceerrors preserves real component failures during shutdown.
package serviceerrors

import (
	"errors"
	"reflect"
)

// Without removes only the supplied expected causes from an error tree. In
// particular, errors.Is on a joined error must not discard an independent
// storage or listener failure merely because cancellation is also present.
// Callers pass the observed signal context's cause: recent Go releases give
// signal.NotifyContext a cause distinct from context.Canceled.
func Without(err error, expected ...error) error {
	if err == nil {
		return nil
	}
	for _, cause := range expected {
		if cause != nil && reflect.TypeOf(cause).Comparable() && err == cause {
			return nil
		}
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var remaining []error
		for _, child := range joined.Unwrap() {
			if failure := Without(child, expected...); failure != nil {
				remaining = append(remaining, failure)
			}
		}
		return errors.Join(remaining...)
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok && wrapped.Unwrap() != nil &&
		Without(wrapped.Unwrap(), expected...) == nil {
		return nil
	}
	return err
}

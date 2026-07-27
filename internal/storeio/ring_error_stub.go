//go:build !linux

package storeio

func ringSetupUnavailable(error) bool { return false }

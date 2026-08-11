//go:build linux || darwin

package raftstore

func platformSupported() bool { return true }

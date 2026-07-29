package storeio

import (
	"errors"
	"os"
	"testing"
)

func TestFaultDeviceKeepsDataWritesInsideCommit(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "fault-device-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	pageSize := os.Getpagesize()
	device, err := OpenFaultDevice(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 2, BufferSize: pageSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()

	data := []byte("data")
	root := []byte("root")
	copy(deviceBuffer(t, device, 0), data)
	copy(deviceBuffer(t, device, 1), root)
	page := Write{
		Offset: int64(pageSize), Length: uint32(len(data)), Buffer: 0,
	}
	if err := device.Prewrite([]Write{page}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Prewrite error = %v, want %v", err, ErrUnsupported)
	}
	if info, err := file.Stat(); err != nil {
		t.Fatal(err)
	} else if info.Size() != 0 {
		t.Fatalf("Prewrite grew fault image to %d bytes before Commit", info.Size())
	}

	if err := device.Commit(
		[]Write{page},
		Write{Offset: 0, Length: uint32(len(root)), Buffer: 1},
	); err != nil {
		t.Fatal(err)
	}
	assertFileBytes(t, file, page.Offset, data)
	assertFileBytes(t, file, 0, root)
	records := device.Records()
	if len(records) != 1 || len(records[0].DataPages) != 1 {
		t.Fatalf("recorded commits = %+v, want one commit with one data write", records)
	}
}

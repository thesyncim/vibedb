package replicaaction

import (
	"encoding/binary"
	"net"
	"slices"
	"unicode"
	"unicode/utf8"
)

const MaxRetirementLocators = 3
const MaxRetirementAddressBytes = 4096
const MaxRetirementLocatorBytes = 8 + MaxRetirementLocators*(10+MaxRetirementAddressBytes)

// RetirementLocator supplies only a control address. Its member must be derived
// from the source's retained grant and its connection pinned to the certified
// physical-node identity before an observation can authorize retirement.
type RetirementLocator struct {
	Member  uint64
	Address string
}

func EncodeRetirementLocators(locators []RetirementLocator) ([]byte, error) {
	if len(locators) == 0 || len(locators) > MaxRetirementLocators {
		return nil, ErrControl
	}
	locators = slices.Clone(locators)
	slices.SortFunc(locators, func(a, b RetirementLocator) int {
		if a.Member < b.Member {
			return -1
		}
		if a.Member > b.Member {
			return 1
		}
		return 0
	})
	data := []byte{'V', 'R', 'L', 1, byte(len(locators)), 0, 0, 0}
	var previous uint64
	for _, locator := range locators {
		if locator.Member == 0 || locator.Member <= previous || !validRetirementAddress(locator.Address) {
			return nil, ErrControl
		}
		previous = locator.Member
		data = binary.LittleEndian.AppendUint64(data, locator.Member)
		data = binary.LittleEndian.AppendUint16(data, uint16(len(locator.Address)))
		data = append(data, locator.Address...)
	}
	return data, nil
}
func OpenRetirementLocators(data []byte) ([]RetirementLocator, error) {
	if len(data) < 8 || len(data) > MaxRetirementLocatorBytes || string(data[:4]) != "VRL\x01" || data[4] == 0 || data[4] > MaxRetirementLocators || !zero(data[5:8]) {
		return nil, ErrControl
	}
	count := int(data[4])
	data = data[8:]
	out := make([]RetirementLocator, 0, count)
	var previous uint64
	for range count {
		if len(data) < 10 {
			return nil, ErrControl
		}
		member, size := binary.LittleEndian.Uint64(data), int(binary.LittleEndian.Uint16(data[8:10]))
		data = data[10:]
		if member == 0 || member <= previous || size > len(data) {
			return nil, ErrControl
		}
		address := string(data[:size])
		data = data[size:]
		if !validRetirementAddress(address) {
			return nil, ErrControl
		}
		previous = member
		out = append(out, RetirementLocator{Member: member, Address: address})
	}
	if len(data) != 0 {
		return nil, ErrControl
	}
	return out, nil
}
func validRetirementAddress(address string) bool {
	if len(address) == 0 || len(address) > MaxRetirementAddressBytes || !utf8.ValidString(address) {
		return false
	}
	for _, character := range address {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return false
		}
	}
	host, port, err := net.SplitHostPort(address)
	return err == nil && host != "" && port != ""
}

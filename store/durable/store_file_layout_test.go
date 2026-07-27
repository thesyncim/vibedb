package durable

import "github.com/thesyncim/vibedb/internal/storeio"

func testMutableDataStart(pageSize int) int {
	layout, err := storeio.MutableStoreLayout(uint32(pageSize))
	if err != nil {
		panic(err)
	}
	return int(layout.DataStart)
}

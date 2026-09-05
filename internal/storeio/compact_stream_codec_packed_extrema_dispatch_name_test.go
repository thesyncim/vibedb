//go:build go1.27 && !go1.28 && goexperiment.simd && (arm64 || amd64)

package storeio

import (
	"reflect"
	"runtime"
)

func compactPackedExtremaDispatchName(
	fn func([]byte, int) (uint64, uint64, bool, bool),
) string {
	pc := reflect.ValueOf(fn).Pointer()
	if pc == 0 {
		return ""
	}
	function := runtime.FuncForPC(pc)
	if function == nil {
		return ""
	}
	return function.Name()
}

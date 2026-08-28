//go:build !unix

package gateway

import "os"

func openGatewayIssuerLock(string) (*os.File, error) {
	return nil, ErrCatalogDurabilityUnsupported
}

func closeGatewayIssuerLock(file *os.File) error {
	if file == nil {
		return nil
	}
	return file.Close()
}

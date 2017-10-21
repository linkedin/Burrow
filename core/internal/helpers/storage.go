package helpers

import (
	"time"

	"github.com/linkedin/Burrow/core/protocol"
)

func TimeoutSendStorageRequest(storageChannel chan *protocol.StorageRequest, request *protocol.StorageRequest, maxTime int) {
	timeout := time.After(time.Duration(maxTime) * time.Second)
	select {
	case storageChannel <- request:
	case <-timeout:
	}
}

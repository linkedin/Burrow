package helpers

import (
	"testing"
	"time"
	"github.com/stretchr/testify/assert"

	"github.com/linkedin/Burrow/core/protocol"
)

func TestTimeoutSendStorageRequest(t *testing.T) {
	storageChannel := make(chan *protocol.StorageRequest)
	storageRequest := &protocol.StorageRequest{}

	go TimeoutSendStorageRequest(storageChannel, storageRequest, 1)

	// Sleep for 0.5 seconds before reading. There should be a storage request waiting
	time.Sleep(500 * time.Millisecond)
	readRequest := <- storageChannel

	assert.Equal(t, storageRequest, readRequest, "Expected to receive the same storage request")
}

func TestTimeoutSendStorageRequest_Timeout(t *testing.T) {
	storageChannel := make(chan *protocol.StorageRequest)
	storageRequest := &protocol.StorageRequest{}

	go TimeoutSendStorageRequest(storageChannel, storageRequest, 1)

	// Sleep for 1.5 seconds before reading. There should be nothing waiting
	time.Sleep(1500 * time.Millisecond)

	select {
	case <- storageChannel:
		assert.Fail(t, "Expected to not receive storage request after timeout")
	default:
	}
}

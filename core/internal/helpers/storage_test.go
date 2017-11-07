/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package helpers

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/linkedin/Burrow/core/protocol"
)

func TestTimeoutSendStorageRequest(t *testing.T) {
	storageChannel := make(chan *protocol.StorageRequest)
	storageRequest := &protocol.StorageRequest{}

	go TimeoutSendStorageRequest(storageChannel, storageRequest, 1)

	// Sleep for 0.5 seconds before reading. There should be a storage request waiting
	time.Sleep(500 * time.Millisecond)
	readRequest := <-storageChannel

	assert.Equal(t, storageRequest, readRequest, "Expected to receive the same storage request")
}

func TestTimeoutSendStorageRequest_Timeout(t *testing.T) {
	storageChannel := make(chan *protocol.StorageRequest)
	storageRequest := &protocol.StorageRequest{}

	go TimeoutSendStorageRequest(storageChannel, storageRequest, 1)

	// Sleep for 1.5 seconds before reading. There should be nothing waiting
	time.Sleep(1500 * time.Millisecond)

	select {
	case <-storageChannel:
		assert.Fail(t, "Expected to not receive storage request after timeout")
	default:
	}
}

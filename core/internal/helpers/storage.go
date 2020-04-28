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
	"time"

	"github.com/linkedin/Burrow/core/protocol"
)

// TimeoutSendStorageRequest is a helper func for sending a protocol.StorageRequest to a channel with a timeout,
// specified in seconds. If the request is sent, return true. Otherwise, if the timeout is hit, return false.
func TimeoutSendStorageRequest(storageChannel chan *protocol.StorageRequest, request *protocol.StorageRequest, maxTime int) bool {
	timeout := time.After(time.Duration(maxTime) * time.Second)
	select {
	case storageChannel <- request:
		return true
	case <-timeout:
		return false
	}
}

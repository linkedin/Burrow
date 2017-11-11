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
	"github.com/linkedin/Burrow/core/protocol"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBurrowZookeeperClient_ImplementsZookeeperClient(t *testing.T) {
	assert.Implements(t, (*protocol.ZookeeperClient)(nil), new(BurrowZookeeperClient))
}

func TestMockZookeeperClient_ImplementsZookeeperClient(t *testing.T) {
	assert.Implements(t, (*protocol.ZookeeperClient)(nil), new(MockZookeeperClient))
}

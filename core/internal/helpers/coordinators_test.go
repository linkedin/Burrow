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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/linkedin/Burrow/core/protocol"
)

func TestStartCoordinatorModules(t *testing.T) {
	mock1 := &MockModule{}
	mock2 := &MockModule{}
	modules := map[string]protocol.Module{
		"mock1": mock1,
		"mock2": mock2,
	}

	mock1.On("Start").Return(nil)
	mock2.On("Start").Return(nil)
	err := StartCoordinatorModules(modules)

	assert.Nil(t, err, "Expected error to be nil")
	mock1.AssertExpectations(t)
	mock2.AssertExpectations(t)
}

func TestStartCoordinatorModules_Error(t *testing.T) {
	mock1 := &MockModule{}
	mock2 := &MockModule{}
	modules := map[string]protocol.Module{
		"mock1": mock1,
		"mock2": mock2,
	}

	mock1.On("Start").Return(nil)
	mock2.On("Start").Return(errors.New("bad start"))
	err := StartCoordinatorModules(modules)

	assert.NotNil(t, err, "Expected error to be nil")
	// Can't assert expectations, as it's possible that mock1 won't be called due to non-deterministic ordering of range
}

func TestStopCoordinatorModules(t *testing.T) {
	mock1 := &MockModule{}
	mock2 := &MockModule{}
	modules := map[string]protocol.Module{
		"mock1": mock1,
		"mock2": mock2,
	}

	mock1.On("Stop").Return(nil)
	mock2.On("Stop").Return(nil)
	StopCoordinatorModules(modules)

	mock1.AssertExpectations(t)
	mock2.AssertExpectations(t)
}

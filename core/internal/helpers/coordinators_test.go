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

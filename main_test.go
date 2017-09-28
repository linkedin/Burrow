package main

import (
	"testing"
	"fmt"
)

func assertStringEqual(t *testing.T, expected string, actual string) {
	if expected != actual {
		t.Error(fmt.Sprintf("Expected string \"%s\", got \"%s\"", expected, actual))
	}
}
func assertUInt16Equal(t *testing.T, expected uint16, actual uint16) {
	if expected != actual {
		t.Error(fmt.Sprintf("Expected uint16 \"%d\", got \"%d\"", expected, actual))
	}
}

func assertIntEqual(t *testing.T, expected int, actual int) {
	if expected != actual {
		t.Error(fmt.Sprintf("Expected int \"%d\", got \"%d\"", expected, actual))
	}
}

func assertNoError(t *testing.T, err error) {
	if err != nil {
		t.Error(fmt.Sprintf("Unxpected error %s", err))
	}
}

// This is just a dummy test function to have a base to start with for Travis
func Test_dummy(t *testing.T) {
	t.Log("Dummy test passed")
}

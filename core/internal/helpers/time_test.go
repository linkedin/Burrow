package helpers

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"time"
)

func TestPausableTicker_ImplementsTicker(t *testing.T) {
	assert.Implements(t, (*Ticker)(nil), new(PausableTicker))
}

func TestPausableTicker_New(t *testing.T) {
	ticker := NewPausableTicker(5 * time.Millisecond)
	assert.Implements(t, (*Ticker)(nil), ticker)

	// We shouldn't get any events across the channel
	quitChan := make(chan struct{})
	channel := ticker.GetChannel()
	go func() {
		select {
		case <- channel:
			assert.Fail(t, "Expected to receive no event on ticker channel")
		case <- quitChan:
			break
		}
	}()

	time.Sleep(25 * time.Millisecond)
	close(quitChan)
}

func TestPausableTicker_StartStop(t *testing.T) {
	ticker := NewPausableTicker(20 * time.Millisecond)
	ticker.Start()

	numEvents := 0
	quitChan := make(chan struct{})
	channel := ticker.GetChannel()
	go func() {
		for {
			select {
			case <-channel:
				numEvents += 1
			case <-quitChan:
				return
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	ticker.Stop()
	time.Sleep(50 * time.Millisecond)
	close(quitChan)

	assert.Equalf(t, 2, numEvents, "Expected 2 events, not %v", numEvents)
}

func TestPausableTicker_Restart(t *testing.T) {
	ticker := NewPausableTicker(20 * time.Millisecond)
	ticker.Start()

	numEvents := 0
	quitChan := make(chan struct{})
	channel := ticker.GetChannel()
	go func() {
		for {
			select {
			case <-channel:
				numEvents += 1
			case <-quitChan:
				return
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	ticker.Stop()
	time.Sleep(50 * time.Millisecond)
	ticker.Start()
	time.Sleep(50 * time.Millisecond)
	ticker.Stop()
	close(quitChan)

	assert.Equalf(t, 4, numEvents, "Expected 4 events, not %v", numEvents)
}

package helpers

import (
	"time"
	"github.com/stretchr/testify/mock"
)

// This is an interface for a ticker. It's very simple, with just a start and stop method
type Ticker interface {
	Start()
	Stop()
	GetChannel() <- chan time.Time
}

type PausableTicker struct {
	channel     chan time.Time
	duration    time.Duration
	ticker      *time.Ticker
	quitChannel chan struct{}
}

// Returns a Ticker that has not yet been started, but the channel is ready to use. This ticker can be started and
// stopped multiple times without needing to swap the ticker channel
func NewPausableTicker(d time.Duration) Ticker {
	return &PausableTicker{
		channel:     make(chan time.Time),
		duration:    d,
		ticker:      nil,
	}
}

func (ticker *PausableTicker) Start() {
	if ticker.ticker != nil {
		// Don't restart a ticker that's already running
		return
	}

	// Channel to be able to close the goroutine
	ticker.quitChannel = make(chan struct {})

	// Start the ticker
	ticker.ticker = time.NewTicker(ticker.duration)

	// This goroutine will forward the ticker ticks to our exposed channel
	go func (tickerChan <- chan time.Time, quitChan chan struct {}) {
		for {
			select {
			case tick := <- tickerChan:
				ticker.channel <- tick
			case <- quitChan:
				return
			}
		}
	}(ticker.ticker.C, ticker.quitChannel)

}

func (ticker *PausableTicker) Stop() {
	if ticker.ticker == nil {
		// Don't stop an already stopped ticker
		return
	}

	// Stop the underlying ticker
	ticker.ticker.Stop()
	ticker.ticker = nil

	// Tell our goroutine to quit
	close(ticker.quitChannel)
}

func (ticker *PausableTicker) GetChannel() <- chan time.Time {
	return ticker.channel
}

// Mock Ticker to use for testing
type MockTicker struct {
	mock.Mock
}
func (m *MockTicker) Start() {
	m.Called()
}
func (m *MockTicker) Stop() {
	m.Called()
}
func (m *MockTicker) GetChannel() <- chan time.Time {
	args := m.Called()
	return args.Get(0).(<- chan time.Time)
}

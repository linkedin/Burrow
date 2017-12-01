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
	"sync"
	"testing"
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
		case <-channel:
			assert.Fail(t, "Expected to receive no event on ticker channel")
		case <-quitChan:
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
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-channel:
				numEvents++
			case <-quitChan:
				return
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	ticker.Stop()
	time.Sleep(50 * time.Millisecond)
	close(quitChan)
	wg.Wait()

	assert.Equalf(t, 2, numEvents, "Expected 2 events, not %v", numEvents)
}

func TestPausableTicker_Restart(t *testing.T) {
	ticker := NewPausableTicker(20 * time.Millisecond)
	ticker.Start()

	numEvents := 0
	quitChan := make(chan struct{})
	channel := ticker.GetChannel()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-channel:
				numEvents++
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
	wg.Wait()

	assert.Equalf(t, 4, numEvents, "Expected 4 events, not %v", numEvents)
}

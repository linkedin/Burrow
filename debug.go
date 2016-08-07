/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package main

import (
	"fmt"
	"github.com/linkedin/Burrow/protocol"
)

func printConsumerGroupStatus(status *protocol.ConsumerGroupStatus) {
	fmt.Println("-------------------------------------------------")
	fmt.Println("Group: ", status.Group)
	if status.Status == protocol.StatusOK {
		fmt.Printf("Status: OK      (complete = %t)\n", status.Complete)
	} else {
		if status.Status == protocol.StatusWarning {
			fmt.Printf("Status: WARNING (complete = %t)\n", status.Complete)
		} else {
			fmt.Printf("Status: ERROR   (complete = %t)\n", status.Complete)
		}
		fmt.Println("Partitions:")
		for _, partition := range status.Partitions {
			prefix := "     OK"
			switch {
			case partition.Status == protocol.StatusWarning:
				prefix = "   WARN"
			case partition.Status == protocol.StatusStop:
				prefix = "   STOP"
			case partition.Status == protocol.StatusError:
				prefix = "    ERR"
			case partition.Status == protocol.StatusStall:
				prefix = "  STALL"
			default:
				prefix = "   STOP"
			}
			fmt.Printf("%s %s:%v (%v, %v, %v) -> (%v, %v, %v)\n", prefix, partition.Topic, partition.Partition,
				partition.Start.Timestamp, partition.Start.Offset, partition.Start.Lag,
				partition.End.Timestamp, partition.End.Offset, partition.End.Lag)
		}
	}
}

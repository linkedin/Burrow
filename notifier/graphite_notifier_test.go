package notifier

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/linkedin/Burrow/protocol"
	"github.com/stretchr/testify/assert"
)

func TestGraphiteNotification(t *testing.T) {
	t.Parallel()

	// given
	addr, metrics, metricsWaiter := graphiteServer(t)
	metricsWaiter.Add(1)

	notifier, _ := NewGraphiteNotifier(addr.String(), "stats", 10, 0)

	partitionsCount := 2
	partitions := make([]*protocol.PartitionStatus, partitionsCount)
	for i := 0; i < partitionsCount; i++ {
		partitions[i] = &protocol.PartitionStatus{
			Topic:     "kafka.topic",
			Partition: int32(i),
			Status:    protocol.StatusOK,
			End:       protocol.ConsumerOffset{
				Offset:     100,
				Timestamp:  1024,
				Lag:        20,
				Artificial: false,
				MaxOffset:  100,
			},
		}
	}
	msg := Message{Cluster: "cluster", Group: "consumer.group", Status: protocol.StatusOK, Partitions: partitions}

	// when
	notifier.Notify(msg)
	notifier.sendToGraphite(addr, "stats")

	// then
	metricsWaiter.Wait()
	for i := 0; i < partitionsCount; i++ {
		assert.Equal(t, int64(20), metrics[fmt.Sprintf("stats.cluster.kafka_topic.consumer_group.%d.lag", i)])
		assert.Equal(t, int64(100), metrics[fmt.Sprintf("stats.cluster.kafka_topic.consumer_group.%d.offset", i)])
	}
}

func TestIgnoreThreshold(t *testing.T) {
	t.Parallel()

	// when && then
	assert.False(t, (&GraphiteNotifier{int(protocol.StatusNotFound), nil, sync.Mutex{}}).Ignore(Message{Status: protocol.StatusNotFound}))
	assert.True(t, (&GraphiteNotifier{int(protocol.StatusOK), nil, sync.Mutex{}}).Ignore(Message{Status: protocol.StatusNotFound}))
}

func graphiteServer(t *testing.T) (*net.TCPAddr, map[string]int64, *sync.WaitGroup) {
	metrics := make(map[string]int64)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("Error while listening on localhost", err)
	}

	var waiter sync.WaitGroup
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				t.Fatal("Error while accepting connection", err)
			}

			r := bufio.NewReader(conn)
			line, err := r.ReadString('\n')

			for err == nil {
				parts := strings.Split(line, " ")
				i, _ := strconv.ParseInt(parts[1], 10, 64)

				metrics[parts[0]] = metrics[parts[0]] + i
				line, err = r.ReadString('\n')
			}
			waiter.Done()
			conn.Close()
		}
	}()

	addr, err := net.ResolveTCPAddr("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal("Error while resolving address", err)
	}
	return addr, metrics, &waiter
}

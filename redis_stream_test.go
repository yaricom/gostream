package gostream

import (
	"context"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yaricom/gostream/timestamp"
	"go.uber.org/zap"
	"strconv"

	"os"
	"strings"
	"testing"
	"time"
)

// TestEvent is the test event data structure
type TestEvent struct {
	SKU       string   `json:"sku"`
	Style     string   `json:"style"`
	Color     string   `json:"color"`
	Size      string   `json:"size"`
	Category  string   `json:"category"`
	Hierarchy []string `json:"hierarchy"`
	Timestamp int64    `json:"timestamp"` // nanoseconds
}

func TestRedisStream_AddEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in Unit Test mode.")
	}
	InitDebugForTesting()

	stream, err := initStream(nil, 1)
	require.Nil(t, err, "failed to init stream")

	// add record
	event := DataEvent{Data: TestEvent{Style: uuid.New().String()}}
	err = stream.AddEvent(event)
	require.Nil(t, err, "failed to add event")
}

func TestRedisStream_ReadEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in Unit Test mode.")
	}
	InitDebugForTesting()

	stream, err := initStream(nil, 1)
	require.Nil(t, err, "failed to init stream")

	// create test data
	oEvent := TestEvent{
		SKU:       uuid.New().String(),
		Style:     uuid.New().String(),
		Color:     uuid.New().String(),
		Size:      uuid.New().String(),
		Category:  uuid.New().String(),
		Hierarchy: []string{"one", "two", "three"},
		Timestamp: timestamp.Now(),
	}
	// add event
	err = stream.AddEvent(DataEvent{Data: oEvent})
	require.Nil(t, err, "failed to add event")

	// read event and test
	dEvent, err := stream.ReadEvent(TestEvent{}, 200*time.Millisecond)
	require.Nil(t, err, "failed to read event")
	require.NotNil(t, dEvent.Data, "no data found")
	assert.EqualValues(t, &oEvent, dEvent.Data, "wrong data returned")

	// read again and test that not acknowledged message is still returned
	dEvent, err = stream.ReadEvent(TestEvent{}, 200*time.Millisecond)
	require.Nil(t, err, "failed to read event")
	require.NotNil(t, dEvent.Data, "no data found")
	assert.EqualValues(t, &oEvent, dEvent.Data, "wrong data returned")

	// test max pending len error
	err = stream.AddEvent(DataEvent{Data: oEvent})
	require.EqualError(t, err, ErrStreamIsFull.Error(), "stream full error expected")

	// acknowledge message and
	err = stream.AckEvent(dEvent)
	require.Nil(t, err, "failed to acknowledge event")

	// check that ErrTimeout returned
	dEvent, err = stream.ReadEvent(TestEvent{}, 200*time.Millisecond)
	require.EqualError(t, err, ErrTimeout.Error(), "timeout error expected")
	require.Nil(t, dEvent.Data, "no data expected")

	// add more events and check it
	err = stream.AddEvent(DataEvent{Data: oEvent})
	require.Nil(t, err, "failed to add event")
	dEvent, err = stream.ReadEvent(TestEvent{}, 200*time.Millisecond)
	require.Nil(t, err, "failed to read event")
	require.NotNil(t, dEvent.Data, "no data found")
	assert.EqualValues(t, &oEvent, dEvent.Data, "wrong data returned")
}

const (
	testNamespace = "gostream-est-stream"
)

func initStream(cfg *RedisStreamConfig, maxPendingLen int64) (*redisStream, error) {
	if cfg == nil {
		cfg = &RedisStreamConfig{
			RedisConfig: RedisConfig{
				Addrs:             getTestHosts(),
				DialTimeout:       500 * time.Millisecond,
				ConnRetryInterval: 50 * time.Millisecond,
				Namespace:         testNamespace,
			},
			MaxLength:        10,
			MaxPendingLength: maxPendingLen,
			ConsumerID:       "testConsumer",
			GroupID:          "testGroup",
			StreamName:       "testStream",
		}
	}

	stream := NewRedisStream(cfg).(*redisStream)
	if err := stream.Open(context.Background()); err != nil {
		return nil, err
	}
	// clear namespace
	if keys, err := stream.rdb.Keys(cfg.Namespace + "::*").Result(); err != nil {
		return nil, err
	} else if len(keys) != 0 {
		if _, err := stream.rdb.Del(keys...).Result(); err != nil {
			return nil, err
		}
	}
	if err := stream.initOutputEventsStream(); err != nil {
		return nil, err
	}
	return stream, nil
}

func getTestHosts() []string {
	if h := os.Getenv("ENV_REDIS_TEST_HOSTS"); h != "" {
		return strings.Split(h, ",")
	} else {
		return []string{"localhost:6379"}
	}
}

// InitDebugForTesting is to check if debug environment variable is set and set logging appropriately for tests execution.
func InitDebugForTesting() bool {
	debug, err := strconv.ParseBool(os.Getenv("DEBUG"))
	if err != nil {
		return false
	} else if debug {
		logger, _ := zap.NewDevelopment()
		zap.ReplaceGlobals(logger)
	}
	return debug
}

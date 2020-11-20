package gostream

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	outputEventKey = "event"
)

type redisStream struct {
	rdb        *redis.Client
	config     *RedisStreamConfig
	maxLen     int64  // the soft cap limit on stream length
	consumerID string // the ID of consumer
	groupID    string // the ID of the group
	stream     string // the name of the stream
	lastID     string // the ID of the last read event
	sync.Mutex
	log *zap.Logger
}

// NewRedisStream is to get output events stream based on Redis with soft cap limit set to the  maxLen
func NewRedisStream(config *RedisStreamConfig) DataStream {
	return &redisStream{
		config:     config,
		stream:     fmt.Sprintf("%s::%s", config.Namespace, config.StreamName),
		maxLen:     config.MaxLength,
		consumerID: config.ConsumerID,
		groupID:    config.GroupID,
		lastID:     "0-0",
		log:        zap.L(),
	}
}

func (r *redisStream) initOutputEventsStream() error {
	r.log.Info("Creating subscription group for data stream",
		zap.String("stream", r.stream), zap.String("group_id", r.groupID),
		zap.String("consumer_id", r.consumerID))

	// create subscription groups
	res, err := r.rdb.XGroupCreateMkStream(r.stream, r.groupID, "0").Result()
	if err != nil {
		if strings.Contains(err.Error(), "BUSYGROUP") {
			r.log.Debug("consumer group already exists", zap.String("group", r.groupID))
		} else {
			r.log.Error("failed to create consumer group", zap.String("group", r.groupID), zap.Error(err))
			return errors.Wrapf(err, "failed to create consumer group [%s]", r.groupID)
		}
	} else if res != "OK" {
		return errors.New("failed to create consumer group")
	}
	return nil
}

func (r *redisStream) AddEvent(event DataEvent) error {
	// check that pending list is too big
	res, err := r.rdb.XPending(r.stream, r.groupID).Result()
	if err != nil {
		return errors.Wrap(err, "failed to read number of pending messages")
	}
	var sLen int64
	if count, ok := res.Consumers[r.consumerID]; ok {
		sLen = count
	} else {
		sLen = res.Count
	}
	if sLen >= r.config.MaxPendingLength {
		r.log.Warn("List of pending messages exceeding MAX length.",
			zap.Int64("length", sLen), zap.Int64("max_len", r.config.MaxPendingLength))
		return ErrStreamIsFull
	}

	// marshal to JSON and put
	data, err := json.Marshal(event.Data)
	if err != nil {
		return errors.Wrap(err, "failed to marshal event to JSON")
	}
	// add event to the stream
	args := redis.XAddArgs{
		Stream:       r.stream,
		MaxLenApprox: r.maxLen,
		ID:           "*",
		Values:       map[string]interface{}{outputEventKey: data},
	}
	if _, err := r.rdb.XAdd(&args).Result(); err != nil {
		return errors.Wrap(err, "failed to add output event to the stream")
	}
	return nil
}

func (r *redisStream) ReadEvent(result interface{}, timeout time.Duration) (DataEvent, error) {
	r.Lock()
	defer r.Unlock()

	// read pending messages if any
	//
	res, err := r.readPending()
	if err != nil {
		return DataEvent{}, errors.Wrapf(err, "failed to read data event from stream [%s]", r.stream)
	}
	if len(res[0].Messages) != 0 {
		return r.consumeMessage(result, res[0].Messages[0])
	}
	// If we receive an empty reply, it means we were consuming our history
	// and that the history is now empty. Let's start to consume new messages.

	// read new messages
	//
	id := ">" // last events
	// read event
	args := redis.XReadGroupArgs{
		Group:    r.groupID,
		Consumer: r.consumerID,
		Streams:  []string{r.stream, id},
		Count:    1,
		Block:    timeout,
		NoAck:    false,
	}
	res, err = r.rdb.XReadGroup(&args).Result()
	if err != nil {
		if err == redis.Nil {
			// timeout
			return DataEvent{}, ErrTimeout
		} else {
			return DataEvent{}, errors.Wrapf(err, "failed to read data event from stream [%s]", r.stream)
		}
	}
	// consume message
	return r.consumeMessage(result, res[0].Messages[0])
}

func (r *redisStream) AckEvent(event DataEvent) error {
	r.Lock()
	defer r.Unlock()

	res, err := r.rdb.XAck(r.stream, r.groupID, event.RecordID).Result()
	if err != nil {
		return errors.Wrapf(err, "failed to acknowledge data event, id [%s]", event.RecordID)
	}
	r.lastID = event.RecordID
	r.log.Debug("acknowledge complete", zap.Int64("count", res))
	return nil
}

// consumeMessage is to consume specified message by converting it into data event
func (r *redisStream) consumeMessage(result interface{}, message redis.XMessage) (DataEvent, error) {
	dataEvent := DataEvent{RecordID: message.ID}
	if val, ok := message.Values[outputEventKey]; !ok {
		r.log.Warn("event not found in the message, skipping")
		if err := r.AckEvent(dataEvent); err != nil {
			r.log.Error("failed to acknowledge message", zap.Error(err))
		}
		return DataEvent{}, nil
	} else {
		dataEvent.raw = val
	}
	dataString := dataEvent.raw.(string)

	// unmarshall received event
	value := getPointer(result)
	if err := json.Unmarshal([]byte(dataString), value); err != nil {
		return DataEvent{}, errors.Wrap(err, "failed to unmarshal data event")
	} else {
		dataEvent.Data = value
	}
	return dataEvent, nil
}

// readPending is to read any message from the pending list, i.e. not acknowledged yet
func (r *redisStream) readPending() ([]redis.XStream, error) {
	// read event
	args := redis.XReadGroupArgs{
		Group:    r.groupID,
		Consumer: r.consumerID,
		Streams:  []string{r.stream, r.lastID},
		Count:    1,
	}
	return r.rdb.XReadGroup(&args).Result()
}

// Open is to open the cache by connecting to the Redis for example
func (r *redisStream) Open(openCtx context.Context) error {
	r.rdb = redis.NewClient(&redis.Options{
		Addr:     r.config.Addrs[0],
		Password: r.config.Password,
	})
	// Connect to the Redis server
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(openCtx)
	go func(ctx context.Context) {
		attempt := 1
		for {
			r.log.Info("Trying to connect with Redis server",
				zap.Strings("addresses", r.config.Addrs), zap.Int("attempt", attempt),
				zap.Duration("timeout", r.config.DialTimeout))
			// send status command to Redis and test result
			res := r.rdb.Ping()
			if res.Err() != nil {
				r.log.Warn("failed to connect with Redis server",
					zap.Error(res.Err()), zap.Duration("retry_in", r.config.ConnRetryInterval))
			} else {
				// connected
				close(errCh)
				return
			}
			// sleep for a while
			select {
			case <-time.After(r.config.ConnRetryInterval):
			case <-ctx.Done():
				r.log.Error("canceled due to timeout", zap.Error(ctx.Err()))
				errCh <- ctx.Err()
				return
			}
			attempt++
		}
	}(ctx)
	// wait for connection
	select {
	case <-time.After(r.config.DialTimeout):
		// terminate connection attempts
		cancel()
		return fmt.Errorf("timeout to establish connection with Redis cluster at %v", r.config.Addrs)
	case err := <-errCh:
		if err != nil {
			r.log.Error("failed to connect with Redis cluster",
				zap.Strings("address", r.config.Addrs), zap.Error(err))
			return err
		}
	}
	r.log.Info("Successfully connected to the Redis server")
	return r.initOutputEventsStream()
}

// Close is to close the cache by terminating connection with the Redis for example
func (r *redisStream) Close() error {
	if r.rdb != nil {
		if err := r.rdb.Close(); err != nil {
			return errors.Wrap(err, "failed to close connection to Redis server")
		}
	}
	return nil
}

func getPointer(v interface{}) interface{} {
	vv := reflect.ValueOf(v)
	if vv.Kind() == reflect.Ptr {
		return v
	}
	return reflect.New(vv.Type()).Interface()
}

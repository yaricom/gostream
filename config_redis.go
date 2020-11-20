package gostream

import (
	"fmt"
	"github.com/pkg/errors"
	"time"
)

// RedisConfig is to hold configuration options of the Redis data store
type RedisConfig struct {
	Addrs             []string      // The addresses of Redis instances host:port.
	Password          string        // The password to access server
	DialTimeout       time.Duration // DialTimeout is the timeout for establishing connection with Redis server
	ConnRetryInterval time.Duration // ConnRetryInterval is an interval between connection retries
	Namespace         string        // Namespace is the namespace for the project
}

// Validate is to validate Redis configuration
func (r *RedisConfig) Validate() error {
	if len(r.Addrs) == 0 {
		return errors.New("You need to specify at least one Redis instance address to connect")
	}
	if r.DialTimeout == 0 {
		return errors.New("DialTimeout can not be zero")
	}
	if r.ConnRetryInterval == 0 {
		return errors.New("ConnRetryInterval can not be zero")
	}
	if r.Namespace == "" {
		return errors.New("Namespace is empty.")
	}
	return nil
}

// buildIDKey is to created key including namespace, prefix and ID
func (r *RedisConfig) BuildIDKey(prefix, id string) string {
	return fmt.Sprintf("%s::%s::%s", r.Namespace, prefix, id)
}

// buildKey is to create key comprising of namespace and key
func (r *RedisConfig) BuildKey(key string) string {
	return fmt.Sprintf("%s::%s", r.Namespace, key)
}

// RedisStreamConfig is the configuration of the data stream based on Redis
type RedisStreamConfig struct {
	RedisConfig
	MaxLength        int64  // the maximum allowed length of the data stream. When reached the old messages will be evicted automatically.
	MaxPendingLength int64  // the maximal number of pending messages allowed per group/consumer
	ConsumerID       string // the name of data consumer
	GroupID          string // the name of group of data consumers
	StreamName       string // the data stream name
}

// Validate is to validate Redis configuration
func (r *RedisStreamConfig) Validate() error {
	if err := r.RedisConfig.Validate(); err != nil {
		return err
	}
	if r.MaxLength == 0 {
		return errors.New("MaxLength can not be zero")
	}
	if r.MaxPendingLength == 0 {
		return errors.New("MaxPendingLength can not be zero")
	}
	if r.MaxPendingLength > r.MaxLength {
		return errors.New("MaxPendingLength can not be greater than MaxLength")
	}
	if r.ConsumerID == "" {
		return errors.New("ConsumerID is not specified")
	}
	if r.GroupID == "" {
		return errors.New("GroupID is not specified")
	}
	if r.StreamName == "" {
		return errors.New("StreamName is not specified")
	}
	return nil
}

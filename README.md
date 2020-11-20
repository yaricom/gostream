# gostream
The simple data stream implementation using GO language based on the [Redis stream](https://redis.io/topics/streams-intro)

# Usage
For usage patterns see [redis_stream_test.go](redis_stream_test.go)

# Tests
The unit tests can be started with following command:
```bash
make test DEBUG=true REDIS_ADDR=127.0.0.1:6379
```
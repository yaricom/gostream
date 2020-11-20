// Package timestamp provides conversion utils from int64 timestamp to time.Time and vice-versa.
package timestamp

import (
	"strconv"
	"time"
)

// TimeFromFloatString is to get time from float string representation
func TimeFromFloatString(timeStr string) (time.Time, error) {
	if t, err := strconv.ParseFloat(timeStr, 10); err != nil {
		return time.Time{}, err
	} else {
		return ToTime(int64(t)), nil
	}
}

// FromTime is to get timestamp from given time at current precision
func FromTime(time time.Time) int64 {
	return time.UnixNano()
}

// TimestampToTime is to get time from provided timestamp
func ToTime(timestamp int64) time.Time {
	if timestamp > 1e18 {
		// nanoseconds
		return time.Unix(0, timestamp)
	}
	// seconds
	return time.Unix(timestamp, 0)
}

// TimestampNow is to get timestamp for current time
func Now() int64 {
	return FromTime(time.Now())
}

package gcs

import (
	"log"
	"math/rand"
	"net/url"
	"time"

	"google.golang.org/api/googleapi"
)

const (
	maxSleep      = 15 * time.Second
	maxRetryCount = 20
)

func retry(desc string, f func() (interface{}, error)) (interface{}, error) {
	v, err := f()
	nextSleep := 500 * time.Millisecond
	multiplier := 1.5 + rand.Float32()/2
	retryCount := 0
	for shouldRetry(err) && retryCount < maxRetryCount {
		retryCount++
		log.Printf("Got error when calling %v. Retrying (%v/%v)...", desc, retryCount, maxRetryCount)
		time.Sleep(nextSleep)
		nextSleep = time.Duration(float32(nextSleep) * multiplier)
		if nextSleep > maxSleep {
			nextSleep = maxSleep
		}
		v, err = f()
	}
	return v, err
}

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}

	if _, ok := err.(*url.Error); ok {
		return true
	}

	if ge, ok := err.(*googleapi.Error); ok {
		if ge.Code >= 500 {
			return true
		}

		return false
	}

	log.Printf("Got non-retriable error: %#+v", err)

	return false
}

func isGoogleAPIError(err error, code int) bool {
	if err == nil {
		return false
	}
	if err, ok := err.(*googleapi.Error); ok {
		if err.Code == code {
			return true
		}
	}

	return false
}

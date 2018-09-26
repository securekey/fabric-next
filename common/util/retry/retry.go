/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package retry

import (
	"time"
)

type retryOpts struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	BackoffFactor  float32
	MaxBackoff     time.Duration
	BeforeRetry    BeforeRetryHandler
}

// BeforeRetryHandler is a function that is invoked before a retry attemp.
// Return true to perform the retry; false otherwise.
type BeforeRetryHandler func(err error, attempt int, backoff time.Duration) bool

// Opt is a retry option
type Opt func(opts *retryOpts)

// WithMaxAttempts sets the maximum number of retry attempts
func WithMaxAttempts(value int) Opt {
	return func(opts *retryOpts) {
		opts.MaxAttempts = value
	}
}

// WithInitialBackoff sets the initial backoff
func WithInitialBackoff(value time.Duration) Opt {
	return func(opts *retryOpts) {
		opts.InitialBackoff = value
	}
}

// WithMaxBackoff sets the maximum backoff
func WithMaxBackoff(value time.Duration) Opt {
	return func(opts *retryOpts) {
		opts.MaxBackoff = value
	}
}

// WithBackoffFactor sets the factor by which the backoff is increased on each attempt
func WithBackoffFactor(value float32) Opt {
	return func(opts *retryOpts) {
		opts.BackoffFactor = value
	}
}

// WithBeforeRetry sets the handler to be invoked before a retry
func WithBeforeRetry(value BeforeRetryHandler) Opt {
	return func(opts *retryOpts) {
		opts.BeforeRetry = value
	}
}

// Invocation is the function to invoke on each attempt
type Invocation func() (interface{}, error)

// Invoke invokes the given invocation with the given retry options
func Invoke(invoke Invocation, opts ...Opt) (interface{}, error) {
	retryOpts := &retryOpts{
		MaxAttempts:    5,
		BackoffFactor:  1.5,
		InitialBackoff: 250 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
	}

	// Apply the options
	for _, opt := range opts {
		opt(retryOpts)
	}

	backoff := retryOpts.InitialBackoff
	var lastErr error
	var retVal interface{}
	for i := 1; i <= retryOpts.MaxAttempts; i++ {
		retVal, lastErr = invoke()
		if lastErr == nil {
			return retVal, nil
		}

		if i+1 < retryOpts.MaxAttempts {
			backoff = time.Duration(float32(backoff) * retryOpts.BackoffFactor)
			if backoff > retryOpts.MaxBackoff {
				backoff = retryOpts.MaxBackoff
			}

			if retryOpts.BeforeRetry != nil {
				if !retryOpts.BeforeRetry(lastErr, i, backoff) {
					// No retry for this error
					return nil, lastErr
				}
			}

			time.Sleep(backoff)
		}
	}

	return nil, lastErr
}

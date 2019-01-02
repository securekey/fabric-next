/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package metrics

import (
	"fmt"
	"io"
	"time"

	"sync"

	"runtime"

	"regexp"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/uber-go/tally"
	promreporter "github.com/uber-go/tally/prometheus"
)

const (
	namespace string = "hyperledger_fabric"

	statsdReporterType = "statsd"
	promReporterType   = "prom"

	defaultReporterType = statsdReporterType
	defaultInterval     = 1 * time.Second

	defaultStatsdReporterFlushInterval = 2 * time.Second
	defaultStatsdReporterFlushBytes    = 1432
)

// RootScope tally.NoopScope is a scope that does nothing
var RootScope = tally.NoopScope
var rootScopeMutex = &sync.Mutex{}
var running bool
var debugOn bool

// StatsdReporterOpts ...
type StatsdReporterOpts struct {
	Address       string
	Prefix        string
	FlushInterval time.Duration
	FlushBytes    int
}

// PromReporterOpts ...
type PromReporterOpts struct {
	ListenAddress string
}

// Opts ...
type Opts struct {
	Reporter           string
	Interval           time.Duration
	Enabled            bool
	StatsdReporterOpts StatsdReporterOpts
	PromReporterOpts   PromReporterOpts
}

// IsDebug ...
func IsDebug() bool {
	return debugOn
}

var reg *regexp.Regexp

// Initialize ...
func Initialize() {

	if viper.GetBool("peer.profile.enabled") {
		runtime.SetMutexProfileFraction(5)
	}
	if viper.GetBool("metrics.enabled") {
		debugOn = viper.GetBool("metrics.debug.enabled")
	}

	// start metric server
	opts := NewOpts()
	err := Start(opts)
	if err != nil {
		logger.Errorf("Failed to start metrics collection: %s", err)
	}

	reg = regexp.MustCompile("[^a-zA-Z0-9_]+")

	logger.Info("Fabric Bootstrap filter initialized")
}

func FilterMetricName(name string) string {
	return reg.ReplaceAllString(name, "_")
}

// NewOpts create metrics options based config file.
// TODO: Currently this is only for peer node which uses global viper.
// As for orderer, which uses its local viper, we are unable to get
// metrics options with the function NewOpts()
func NewOpts() Opts {
	opts := Opts{}
	opts.Enabled = viper.GetBool("metrics.enabled")
	if report := viper.GetString("metrics.reporter"); report != "" {
		opts.Reporter = report
	} else {
		opts.Reporter = defaultReporterType
	}
	if interval := viper.GetDuration("metrics.interval"); interval > 0 {
		opts.Interval = interval
	} else {
		opts.Interval = defaultInterval
	}

	if opts.Reporter == statsdReporterType {
		statsdOpts := StatsdReporterOpts{}
		statsdOpts.Address = viper.GetString("metrics.statsdReporter.address")
		statsdOpts.Prefix = viper.GetString("metrics.statsdReporter.prefix")
		if statsdOpts.Prefix == "" && !viper.IsSet("peer.id") {
			statsdOpts.Prefix = viper.GetString("peer.id")
		}
		if flushInterval := viper.GetDuration("metrics.statsdReporter.flushInterval"); flushInterval > 0 {
			statsdOpts.FlushInterval = flushInterval
		} else {
			statsdOpts.FlushInterval = defaultStatsdReporterFlushInterval
		}
		if flushBytes := viper.GetInt("metrics.statsdReporter.flushBytes"); flushBytes > 0 {
			statsdOpts.FlushBytes = flushBytes
		} else {
			statsdOpts.FlushBytes = defaultStatsdReporterFlushBytes
		}
		opts.StatsdReporterOpts = statsdOpts
	}

	if opts.Reporter == promReporterType {
		promOpts := PromReporterOpts{}
		promOpts.ListenAddress = viper.GetString("metrics.fabric.PromReporter.listenAddress")
		opts.PromReporterOpts = promOpts
	}

	return opts
}

// Start starts metrics server
func Start(opts Opts) error {
	if !opts.Enabled {
		return errors.New("Unable to start metrics server because is disbled")
	}
	rootScopeMutex.Lock()
	defer rootScopeMutex.Unlock()
	if !running {
		rootScope, err := create(opts)
		if err == nil {
			running = true
			RootScope = rootScope
		}
		return err
	}
	return errors.New("metrics server was already started")
}

// Shutdown closes underlying resources used by metrics server
func Shutdown() error {
	rootScopeMutex.Lock()
	defer rootScopeMutex.Unlock()
	if running {
		var err error
		if closer, ok := RootScope.(io.Closer); ok {
			if err = closer.Close(); err != nil {
				return err
			}
		}
		running = false
		RootScope = tally.NoopScope
		return err
	}
	return nil
}

func isRunning() bool {
	rootScopeMutex.Lock()
	defer rootScopeMutex.Unlock()
	return running
}

func create(opts Opts) (rootScope tally.Scope, e error) {
	if !opts.Enabled {
		rootScope = tally.NoopScope
	} else {
		if opts.Interval <= 0 {
			e = fmt.Errorf("invalid Interval option %d", opts.Interval)
			return
		}
		var reporter tally.StatsReporter
		var cachedReporter tally.CachedStatsReporter
		switch opts.Reporter {
		case statsdReporterType:
			reporter, e = newStatsdReporter(opts.StatsdReporterOpts)
		case promReporterType:
			cachedReporter, e = newPromReporter(opts.PromReporterOpts)
		default:
			e = fmt.Errorf("not supported Reporter type %s", opts.Reporter)
			return
		}
		if e != nil {
			return
		}
		rootScope = newRootScope(
			tally.ScopeOptions{
				Prefix:         namespace,
				Reporter:       reporter,
				CachedReporter: cachedReporter,
				Separator:      promreporter.DefaultSeparator,
			}, opts.Interval)
	}
	return
}

// StopWatch starts a stopwatch for the given timerName in debug mode.
// Returns a function that is used to stop the stopwatch.
func StopWatch(timerName string) func() {
	if IsDebug() {
		stopWatch := RootScope.Timer(timerName).Start()

		return func() { stopWatch.Stop() }
	}
	return func() {}
}

// IncrementCounter increments the metrics counter in debug mode
func IncrementCounter(counterName string) {
	if IsDebug() {
		RootScope.Counter(counterName).Inc(1)
	}
}

/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package couchdb

import (
	"crypto/tls"
	"math/rand"
	"net/http"
	"net/http/httptrace"
	"sync"
	"time"
)

var initConnMonitor sync.Once
var openTraces sync.Map

type httpTrace struct {
	trace         *httptrace.ClientTrace
	correlationID uint64
	closed        bool
	idle          bool
}

func (t *httpTrace) Closed() {
	if t.closed {
		logger.Warningf("Closed(%d) - Already closed", t.correlationID)
		return
	}
	logger.Debugf("Closed(%d) - Successfully closed", t.correlationID)
	openTraces.Delete(t.correlationID)
	t.closed = true
}

func (t *httpTrace) Trace() *httptrace.ClientTrace {
	return t.trace
}

func (t *httpTrace) log(msg string, args ...interface{}) {
	logger.Infof(msg, args...)
}

func (t *httpTrace) warn(msg string, args ...interface{}) {
	logger.Warningf(msg, args...)
}

func newHTTPTrace() *httpTrace {
	rand.Seed(time.Now().UnixNano())

	initConnMonitor.Do(func() {
		logger.Infof("Starting couchDB connection monitor...")
		go monitorConnections()
	})

	correlationID := rand.Uint64()

	t := &httpTrace{
		correlationID: correlationID,
		idle:          false,
	}

	trace := &httptrace.ClientTrace{
		GetConn: func(hostPort string) {
			t.log("*** GetConn(%d) - [%s]", correlationID, hostPort)
		},
		GotConn: func(info httptrace.GotConnInfo) {
			t.log("*** GotConn(%d) - %+v", correlationID, info)
		},
		PutIdleConn: func(err error) {
			t.idle = true
			if err != nil {
				t.warn("*** PutIdleConn(%d) - Error: %s", correlationID, err)
			} else {
				t.log("*** PutIdleConn(%d) - SUCCESS", correlationID)
			}
		},
		GotFirstResponseByte: func() {
			t.log("*** GotFirstResponseByte(%d)", correlationID)
		},
		Got100Continue: func() {
			t.log("*** Got100Continue(%d)", correlationID)
		},
		DNSStart: func(info httptrace.DNSStartInfo) {
			t.log("*** DNSStart(%d): %+v", correlationID, info)
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			t.log("*** DNSDone(%d): %+v", correlationID, info)
		},
		ConnectStart: func(network, addr string) {
			t.log("*** ConnectStart(%d): network [%s], addr [%s]", correlationID, network, addr)
		},
		ConnectDone: func(network, addr string, err error) {
			if err != nil {
				t.warn("*** ConnectDone(%d): network [%s], addr [%s], err [%s]", correlationID, network, addr, err)
			} else {
				t.log("*** ConnectDone(%d): network [%s], addr [%s]", correlationID, network, addr)
			}
		},
		TLSHandshakeStart: func() {
			t.log("*** TLSHandshakeStart(%d)", correlationID)
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			if err != nil {
				t.warn("*** TLSHandshakeDone(%d): state: %+v, err: %s", correlationID, state, err)
			} else {
				t.log("*** TLSHandshakeDone(%d): state: %+v", correlationID, state)
			}
		},
		WroteHeaders: func() {
			t.log("*** WroteHeaders(%d)", correlationID)
		},
		Wait100Continue: func() {
			t.log("*** Wait100Continue(%d)", correlationID)
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err != nil {
				t.warn("*** WroteRequest(%d): Error: %s", correlationID, info.Err)
			} else {
				t.log("*** WroteRequest(%d)", correlationID)
			}
		},
	}
	t.trace = trace

	openTraces.Store(correlationID, t)

	return t
}

func httpTraceClose(resp *http.Response) {
	t := resp.Request.Context().Value(httpTraceContextKey{})
	if t != nil {
		trace, ok := t.(*httpTrace)
		if !ok {
			logger.Warningf("HTTP trace not found in context")
		} else {
			trace.Closed()
		}
	} else {
		logger.Warningf("HTTP trace not found in context")
	}
}

type httpTraceContextKey struct{}

func monitorConnections() {
	// TODO: Make period configurable
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			checkOpenConnections()
		}
	}
}

func checkOpenConnections() {
	logger.Debugf("Checking for open connections...")
	openTraces.Range(func(key, val interface{}) bool {
		trace := val.(*httpTrace)
		if !trace.idle {
			logger.Infof("Open conn (%d)", trace.correlationID)
		} else {
			logger.Infof("Open conn (%d) - Already put in idle pool", trace.correlationID)
		}
		return true
	})
	logger.Debugf("... done checking for open connections")
}

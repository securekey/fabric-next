/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"sync"

	"github.com/hyperledger/fabric/gossip/util"
	proto "github.com/hyperledger/fabric/protos/gossip"
	"github.com/op/go-logging"
)

// PayloadsBuffer is used to store payloads into which used to
// support payloads with blocks reordering according to the
// sequence numbers. It also will provide the capability
// to signal whenever expected block has arrived.
type PayloadsBuffer interface {
	// Adds new block into the buffer
	Push(payload *proto.Payload) bool

	// Returns next expected sequence number
	Next() uint64

	// Remove and return payload with given sequence number
	Pop() *proto.Payload

	// Get current buffer size
	Size() int

	// Channel to indicate event when new payload pushed with sequence
	// number equal to the next expected value.
	BlockHeightAvailable() (uint64, chan struct{})

	Close()
}

// PayloadsBufferImpl structure to implement PayloadsBuffer interface
// store inner state of available payloads and sequence numbers
type PayloadsBufferImpl struct {
	next       uint64
	height     uint64
	baSignalCh chan struct{}
	buf        map[uint64]*proto.Payload
	mutex      sync.RWMutex
	logger     *logging.Logger
}

// NewPayloadsBuffer is factory function to create new payloads buffer
func NewPayloadsBuffer(next uint64) PayloadsBuffer {
	return &PayloadsBufferImpl{
		buf:        make(map[uint64]*proto.Payload),
		baSignalCh: make(chan struct{}),
		next:       next,
		logger:     util.GetLogger(util.LoggingStateModule, ""),
	}
}

// BlockHeightAvailable function returns the channel which indicates whenever expected
// next block has arrived and one could safely pop out
// next sequence of blocks
func (b *PayloadsBufferImpl) BlockHeightAvailable() (uint64, chan struct{}) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return b.height, b.baSignalCh
}

// Push new payload into the buffer structure in case new arrived payload
// sequence number is below the expected next block number payload will be
// thrown away.
// TODO return bool to indicate if payload was added or not, so that caller can log result.
func (b *PayloadsBufferImpl) Push(payload *proto.Payload) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	seqNum := payload.SeqNum

	if seqNum < b.next || b.buf[seqNum] != nil {
		logger.Debugf("Payload with sequence number = %d has been already processed", payload.SeqNum)
		return false
	}

	b.buf[seqNum] = payload

	// Send notification that next sequence has arrived
	b.height = seqNum + 1
	close(b.baSignalCh)
	b.baSignalCh = make(chan struct{})

	return true
}

// Next function provides the number of the next expected block
func (b *PayloadsBufferImpl) Next() uint64 {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return b.next
}

// Pop function extracts the payload according to the next expected block
// number, if no next block arrived yet, function returns nil.
func (b *PayloadsBufferImpl) Pop() *proto.Payload {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	result := b.buf[b.next]

	if result != nil {
		// If there is such sequence in the buffer need to delete it
		delete(b.buf, b.next)
		// Increment next expect block index
		b.next = b.next + 1
	}

	return result
}

// Size returns current number of payloads stored within buffer
func (b *PayloadsBufferImpl) Size() int {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return len(b.buf)
}

// Close cleanups resources and channels in maintained
func (b *PayloadsBufferImpl) Close() {
}

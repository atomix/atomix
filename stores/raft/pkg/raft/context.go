// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	rsmv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
	raftv1 "github.com/atomix/atomix/stores/raft/pkg/api/v1"
	"sync"
	"sync/atomic"
)

// newContext returns a new protocol context
func newContext() *protocolContext {
	return &protocolContext{
		streams: make(map[protocolStreamID]streams.WriteStream[*rsmv1.ProposalOutput]),
	}
}

// protocolContext stores state shared by servers and the state machine
type protocolContext struct {
	streams     map[protocolStreamID]streams.WriteStream[*rsmv1.ProposalOutput]
	streamsMu   sync.RWMutex
	sequenceNum atomic.Uint64
}

// addStream adds a new stream
func (r *protocolContext) addStream(term raftv1.Term, stream streams.WriteStream[*rsmv1.ProposalOutput]) raftv1.SequenceNum {
	sequenceNum := raftv1.SequenceNum(r.sequenceNum.Add(1))
	streamID := protocolStreamID{
		term:        term,
		sequenceNum: sequenceNum,
	}
	r.streamsMu.Lock()
	r.streams[streamID] = streams.NewCloserStream[*rsmv1.ProposalOutput](stream, func(s streams.WriteStream[*rsmv1.ProposalOutput]) {
		r.removeStream(term, sequenceNum)
	})
	r.streamsMu.Unlock()
	return sequenceNum
}

// removeStream removes a stream by ID
func (r *protocolContext) removeStream(term raftv1.Term, sequenceNum raftv1.SequenceNum) {
	r.streamsMu.Lock()
	defer r.streamsMu.Unlock()
	streamID := protocolStreamID{
		term:        term,
		sequenceNum: sequenceNum,
	}
	delete(r.streams, streamID)
}

// getStream gets a stream by ID
func (r *protocolContext) getStream(term raftv1.Term, sequenceNum raftv1.SequenceNum) streams.WriteStream[*rsmv1.ProposalOutput] {
	r.streamsMu.RLock()
	defer r.streamsMu.RUnlock()
	streamID := protocolStreamID{
		term:        term,
		sequenceNum: sequenceNum,
	}
	if stream, ok := r.streams[streamID]; ok {
		return stream
	}
	return streams.NewNilStream[*rsmv1.ProposalOutput]()
}

type protocolStreamID struct {
	term        raftv1.Term
	sequenceNum raftv1.SequenceNum
}

type protocolQuery struct {
	input  *rsmv1.QueryInput
	stream streams.WriteStream[*rsmv1.QueryOutput]
}

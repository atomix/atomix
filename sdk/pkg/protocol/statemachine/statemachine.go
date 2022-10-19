// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"github.com/atomix/runtime/sdk/pkg/stream"
)

var log = logging.GetLogger()

type Index uint64

type Context interface {
	// Index returns the current service index
	Index() Index
}

type StateMachine interface {
	Propose(input *protocol.ProposalInput, stream stream.WriteStream[*protocol.ProposalOutput])
	Query(input *protocol.QueryInput, stream stream.WriteStream[*protocol.QueryOutput])
}

type CallID interface {
	ProposalID | QueryID
}

// Call is a proposal or query call context
type Call[T CallID, I, O any] interface {
	// ID returns the execution identifier
	ID() T
	// Log returns the operation log
	Log() logging.Logger
	// Input returns the input
	Input() I
	// Output returns the output
	Output(O)
	// Error returns a failure error
	Error(error)
	// Close closes the execution
	Close()
}

type ProposalID uint64

// Proposal is a proposal call
type Proposal[I, O any] interface {
	Call[ProposalID, I, O]
}

type QueryID uint64

// Query is a query call
type Query[I, O any] interface {
	Call[QueryID, I, O]
}

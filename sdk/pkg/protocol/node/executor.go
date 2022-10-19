// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"github.com/atomix/runtime/sdk/pkg/stream"
)

// Executor is the interface for executing operations on the underlying protocol
type Executor interface {
	// Propose proposes a change to the protocol
	Propose(ctx context.Context, proposal *protocol.ProposalInput, stream stream.WriteStream[*protocol.ProposalOutput]) error

	// Query queries the state
	Query(ctx context.Context, query *protocol.QueryInput, stream stream.WriteStream[*protocol.QueryOutput]) error
}

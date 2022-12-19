// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	protocol "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/atomix/atomix/runtime/pkg/stream"
)

// Executor is the interface for executing operations on the underlying protocol
type Executor interface {
	// Propose proposes a change to the protocol
	Propose(ctx context.Context, proposal *protocol.ProposalInput, stream stream.WriteStream[*protocol.ProposalOutput]) error

	// Query queries the state
	Query(ctx context.Context, query *protocol.QueryInput, stream stream.WriteStream[*protocol.QueryOutput]) error
}

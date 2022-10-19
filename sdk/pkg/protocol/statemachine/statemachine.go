// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"github.com/atomix/runtime/sdk/pkg/stream"
)

type StateMachine interface {
	Propose(input *protocol.ProposalInput, stream stream.WriteStream[*protocol.ProposalOutput])
	Query(input *protocol.QueryInput, stream stream.WriteStream[*protocol.QueryOutput])
}

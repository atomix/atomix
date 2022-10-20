// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
)

const Service = "atomix.runtime.counter.v1.Counter"

func RegisterStateMachine(registry *statemachine.PrimitiveTypeRegistry) {
	statemachine.RegisterPrimitiveType[*CounterInput, *CounterOutput](registry)(PrimitiveType)
}

var PrimitiveType = statemachine.NewPrimitiveType[*CounterInput, *CounterOutput](Service, stateMachineCodec,
	func(context statemachine.PrimitiveContext[*CounterInput, *CounterOutput]) statemachine.Executor[*CounterInput, *CounterOutput] {
		return newExecutor(NewCounterStateMachine(context))
	})

type CounterContext interface {
	statemachine.PrimitiveContext[*CounterInput, *CounterOutput]
}

type CounterStateMachine interface {
	statemachine.Context[*CounterInput, *CounterOutput]
	statemachine.Recoverable
	Set(proposal statemachine.Proposal[*SetInput, *SetOutput])
	Update(proposal statemachine.Proposal[*UpdateInput, *UpdateOutput])
	Increment(proposal statemachine.Proposal[*IncrementInput, *IncrementOutput])
	Decrement(proposal statemachine.Proposal[*DecrementInput, *DecrementOutput])
	Get(query statemachine.Query[*GetInput, *GetOutput])
}

func NewCounterStateMachine(ctx statemachine.PrimitiveContext[*CounterInput, *CounterOutput]) CounterStateMachine {
	return &counterStateMachine{
		CounterContext: ctx,
	}
}

type counterStateMachine struct {
	CounterContext
	value int64
}

func (s *counterStateMachine) Snapshot(writer *statemachine.SnapshotWriter) error {
	return writer.WriteVarInt64(s.value)
}

func (s *counterStateMachine) Recover(reader *statemachine.SnapshotReader) error {
	i, err := reader.ReadVarInt64()
	if err != nil {
		return err
	}
	s.value = i
	return nil
}

func (s *counterStateMachine) Set(proposal statemachine.Proposal[*SetInput, *SetOutput]) {
	defer proposal.Close()
	s.value = proposal.Input().Value
	proposal.Output(&SetOutput{
		Value: s.value,
	})
}

func (s *counterStateMachine) Update(proposal statemachine.Proposal[*UpdateInput, *UpdateOutput]) {
	defer proposal.Close()
	if s.value != proposal.Input().Compare {
		proposal.Error(errors.NewConflict("optimistic lock failure"))
	} else {
		s.value = proposal.Input().Update
		proposal.Output(&UpdateOutput{
			Value: s.value,
		})
	}
}

func (s *counterStateMachine) Increment(proposal statemachine.Proposal[*IncrementInput, *IncrementOutput]) {
	defer proposal.Close()
	s.value += proposal.Input().Delta
	proposal.Output(&IncrementOutput{
		Value: s.value,
	})
}

func (s *counterStateMachine) Decrement(proposal statemachine.Proposal[*DecrementInput, *DecrementOutput]) {
	defer proposal.Close()
	s.value -= proposal.Input().Delta
	proposal.Output(&DecrementOutput{
		Value: s.value,
	})
}

func (s *counterStateMachine) Get(query statemachine.Query[*GetInput, *GetOutput]) {
	defer query.Close()
	query.Output(&GetOutput{
		Value: s.value,
	})
}

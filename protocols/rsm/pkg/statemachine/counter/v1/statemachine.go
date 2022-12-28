// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/api/errors"
	counterprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/counter/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
)

const (
	Name       = "Counter"
	APIVersion = "v1"
)

var PrimitiveType = protocol.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterStateMachine(registry *statemachine.PrimitiveTypeRegistry) {
	statemachine.RegisterPrimitiveType[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput](registry)(PrimitiveType,
		func(context statemachine.PrimitiveContext[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput]) statemachine.PrimitiveStateMachine[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput] {
			return newExecutor(NewCounterStateMachine(context))
		}, counterCodec)
}

type CounterContext interface {
	statemachine.PrimitiveContext[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput]
}

type CounterStateMachine interface {
	statemachine.Context[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput]
	statemachine.Recoverable
	Set(proposal statemachine.Proposal[*counterprotocolv1.SetInput, *counterprotocolv1.SetOutput])
	Update(proposal statemachine.Proposal[*counterprotocolv1.UpdateInput, *counterprotocolv1.UpdateOutput])
	Increment(proposal statemachine.Proposal[*counterprotocolv1.IncrementInput, *counterprotocolv1.IncrementOutput])
	Decrement(proposal statemachine.Proposal[*counterprotocolv1.DecrementInput, *counterprotocolv1.DecrementOutput])
	Get(query statemachine.Query[*counterprotocolv1.GetInput, *counterprotocolv1.GetOutput])
}

func NewCounterStateMachine(ctx statemachine.PrimitiveContext[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput]) CounterStateMachine {
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

func (s *counterStateMachine) Set(proposal statemachine.Proposal[*counterprotocolv1.SetInput, *counterprotocolv1.SetOutput]) {
	defer proposal.Close()
	s.value = proposal.Input().Value
	proposal.Output(&counterprotocolv1.SetOutput{
		Value: s.value,
	})
}

func (s *counterStateMachine) Update(proposal statemachine.Proposal[*counterprotocolv1.UpdateInput, *counterprotocolv1.UpdateOutput]) {
	defer proposal.Close()
	if s.value != proposal.Input().Compare {
		proposal.Error(errors.NewConflict("optimistic lock failure"))
	} else {
		s.value = proposal.Input().Update
		proposal.Output(&counterprotocolv1.UpdateOutput{
			Value: s.value,
		})
	}
}

func (s *counterStateMachine) Increment(proposal statemachine.Proposal[*counterprotocolv1.IncrementInput, *counterprotocolv1.IncrementOutput]) {
	defer proposal.Close()
	s.value += proposal.Input().Delta
	proposal.Output(&counterprotocolv1.IncrementOutput{
		Value: s.value,
	})
}

func (s *counterStateMachine) Decrement(proposal statemachine.Proposal[*counterprotocolv1.DecrementInput, *counterprotocolv1.DecrementOutput]) {
	defer proposal.Close()
	s.value -= proposal.Input().Delta
	proposal.Output(&counterprotocolv1.DecrementOutput{
		Value: s.value,
	})
}

func (s *counterStateMachine) Get(query statemachine.Query[*counterprotocolv1.GetInput, *counterprotocolv1.GetOutput]) {
	defer query.Close()
	query.Output(&counterprotocolv1.GetOutput{
		Value: s.value,
	})
}

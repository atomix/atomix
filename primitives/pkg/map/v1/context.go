// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import "github.com/atomix/runtime/sdk/pkg/protocol/statemachine"

type MapContext interface {
	statemachine.PrimitiveContext[*MapInput, *MapOutput]
	Events() statemachine.Proposals[*EventsInput, *EventsOutput]
}

func newContext(context statemachine.PrimitiveContext[*MapInput, *MapOutput]) MapContext {
	return &mapContext{
		PrimitiveContext: context,
		events: statemachine.NewProposals[*MapInput, *MapOutput, *EventsInput, *EventsOutput](context).
			Decoder(func(input *MapInput) (*EventsInput, bool) {
				if events, ok := input.Input.(*MapInput_Events); ok {
					return events.Events, true
				}
				return nil, false
			}).
			Encoder(func(output *EventsOutput) *MapOutput {
				return &MapOutput{
					Output: &MapOutput_Events{
						Events: output,
					},
				}
			}).
			Build(),
	}
}

type mapContext struct {
	statemachine.PrimitiveContext[*MapInput, *MapOutput]
	events statemachine.Proposals[*EventsInput, *EventsOutput]
}

func (c *mapContext) Events() statemachine.Proposals[*EventsInput, *EventsOutput] {
	return c.events
}

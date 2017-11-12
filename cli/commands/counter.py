# -*- coding: utf-8

# Copyright 2017-present Open Networking Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

from . import Command, Action, Resource, command


class CounterResource(Resource):
    def _get_counter_names(self):
        response = self.cli.service.get(
            self.cli.service.url('/v1/primitives/counters'),
            log=False
        )
        if response.status_code == 200:
            return response.json()
        return []

    def suggest(self, prefix):
        counters = self._get_counter_names()
        for counter in counters:
            if counter.lower().startswith(prefix.lower()):
                return counter[len(prefix):]
        return None

    def complete(self, prefix):
        counters = self._get_counter_names()
        for counter in counters:
            if counter.lower().startswith(prefix.lower()):
                yield counter


class GetAction(Action):
    def execute(self, name):
        self.cli.service.output(self.cli.service.get(
            self.cli.service.url('/v1/primitives/counters/{name}', name=name)
        ))


class IncrementAction(Action):
    def execute(self, name):
        self.cli.service.output(self.cli.service.post(
            self.cli.service.url('/v1/primitives/counters/{name}/inc', name=name)
        ))


class SetAction(Action):
    def execute(self, name, value):
        self.cli.service.output(self.cli.service.put(
            self.cli.service.url('/v1/primitives/counters/{name}', name=name),
            data=value,
            headers={'content-type': 'text/plain'}
        ))


class ValueResource(Resource):
    pass


@command(
    'counter {counter} get',
    type=Command.Type.PRIMITIVE,
    counter=CounterResource,
    get=GetAction
)
@command(
    'counter {counter} set {value}',
    type=Command.Type.PRIMITIVE,
    counter=CounterResource,
    set=SetAction,
    value=ValueResource
)
@command(
    'counter {counter} increment',
    type=Command.Type.PRIMITIVE,
    counter=CounterResource,
    increment=IncrementAction
)
class CounterCommand(Command):
    """Counter command"""

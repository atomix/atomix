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


class ValueResource(Resource):
    def _get_value_names(self):
        response = self.cli.service.get(
            self.cli.service.url('/v1/primitives/values', log=False)
        )
        if response.status_code == 200:
            return response.json()
        return []

    def suggest(self, prefix):
        values = self._get_value_names()
        for value in values:
            if value.lower().startswith(prefix.lower()):
                return value[len(prefix):]
        return None

    def complete(self, prefix):
        values = self._get_value_names()
        for value in values:
            if value.lower().startswith(prefix.lower()):
                yield value


class GetAction(Action):
    def execute(self, name):
        self.cli.service.output(self.cli.service.get(
            self.cli.service.url('/v1/primitives/values/{name}', name=name)
        ))


class SetAction(Action):
    def execute(self, name, value):
        self.cli.service.output(self.cli.service.put(
            self.cli.service.url('/v1/primitives/values/{name}', name=name),
            data=value,
            headers={'content-type': 'text/plain'}
        ))


class CompareAndSetAction(Action):
    def execute(self, name, expect, update):
        self.cli.service.output(self.cli.service.post(
            self.cli.service.url('/v1/primitives/values/{name}/cas', name=name),
            data={'expect': expect, 'update': update},
            headers={'content-type': 'application/json'}
        ))


class TextResource(Resource):
    pass


class ExpectResource(Resource):
    def _get_value(self, name):
        response = self.cli.service.get(
            self.cli.service.url('/v1/primitives/values/{name}', name=name),
            log=False
        )
        if response.status_code == 200:
            return response.json()
        return None

    def suggest(self, name, prefix):
        value = self._get_value()
        if value is not None and value.startswith(prefix):
            return value[len(prefix):]
        return None

    def complete(self, name, prefix):
        value = self._get_value()
        if value is not None and value.startswith(prefix):
            yield value


@command(
    'value {value} get',
    type=Command.Type.PRIMITIVE,
    value=ValueResource,
    get=GetAction
)
@command(
    'value {value} set {text}',
    type=Command.Type.PRIMITIVE,
    value=ValueResource,
    set=SetAction,
    text=TextResource
)
@command(
    'value {value} compare-and-set {expect} {update}',
    type=Command.Type.PRIMITIVE,
    value=ValueResource,
    compare_and_set=CompareAndSetAction,
    expect=ExpectResource,
    update=TextResource
)
class ValueCommand(Command):
    """Value command"""

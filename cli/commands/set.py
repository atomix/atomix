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


class SetResource(Resource):
    def _get_set_names(self):
        response = self.cli.service.get(
            self.cli.service.url('/v1/primitives/sets'),
            log=False
        )
        if response.status_code == 200:
            return response.json()
        return []

    def suggest(self, prefix):
        sets = self._get_set_names()
        for set in sets:
            if set.lower().startswith(prefix.lower()):
                return set[len(prefix):]
        return None

    def complete(self, prefix):
        sets = self._get_set_names()
        for set in sets:
            if set.lower().startswith(prefix.lower()):
                yield set

    def execute(self, name):
        self.cli.service.output(self.cli.service.put(
            self.cli.service.url('/v1/primitives/sets/{name}', name=name)
        ))


class AddAction(Action):
    def execute(self, name, value):
        self.cli.service.output(self.cli.service.put(
            self.cli.service.url('/v1/primitives/sets/{name}/{value}', name=name, value=value)
        ))


class RemoveAction(Action):
    def execute(self, name, value):
        self.cli.service.output(self.cli.service.delete(
            self.cli.service.url('/v1/primitives/sets/{name}/{value}', name=name, value=value)
        ))


class ContainsAction(Action):
    def execute(self, name, value):
        self.cli.service.output(self.cli.service.delete(
            self.cli.service.url('/v1/primitives/sets/{name}/{value}', name=name, value=value)
        ))


class SizeAction(Action):
    def execute(self, name):
        self.cli.service.output(self.cli.service.get(
            self.cli.service.url('/v1/primitives/sets/{name}/size', name=name)
        ))


class ClearAction(Action):
    def execute(self, name):
        self.cli.service.output(self.cli.service.delete(
            self.cli.service.url('/v1/primitives/sets/{name}', name=name)
        ))


class TextResource(Resource):
    pass


@command(
    'set {set} add {text}',
    type=Command.Type.PRIMITIVE,
    set=SetResource,
    add=AddAction,
    text=TextResource
)
@command(
    'set {set} remove {text}',
    type=Command.Type.PRIMITIVE,
    set=SetResource,
    remove=RemoveAction,
    text=TextResource
)
@command(
    'set {set} contains {text}',
    type=Command.Type.PRIMITIVE,
    set=SetResource,
    contains=ContainsAction,
    text=TextResource
)
@command(
    'set {set} size',
    type=Command.Type.PRIMITIVE,
    set=SetResource,
    size=SizeAction
)
@command(
    'set {set} clear',
    type=Command.Type.PRIMITIVE,
    set=SetResource,
    clear=ClearAction
)
class SetCommand(Command):
    """Set command"""

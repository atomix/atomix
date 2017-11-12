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


class GeneratorResource(Resource):
    def _get_generator_names(self):
        response = self.cli.service.get(
            self.cli.service.url('/v1/primitives/ids'),
            log=False
        )
        if response.status_code == 200:
            return response.json()
        return []

    def suggest(self, prefix):
        generators = self._get_generator_names()
        for generator in generators:
            if generator.lower().startswith(prefix.lower()):
                return generator[len(prefix):]
        return None

    def complete(self, prefix):
        generators = self._get_generator_names()
        for generator in generators:
            if generator.lower().startswith(prefix.lower()):
                yield generator


class NextAction(Action):
    def execute(self, name):
        self.cli.service.output(self.cli.service.put(
            self.cli.service.url('/v1/primitives/ids/{name}', name=name)
        ))


@command(
    'id {generator} next',
    type=Command.Type.PRIMITIVE,
    generator=GeneratorResource,
    next=NextAction
)
class IdGeneratorCommand(Command):
    """ID generator command"""

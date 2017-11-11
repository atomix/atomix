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

from . import Command, Resource, command, _commands
import sys


class CommandResource(Resource):
    def suggest(self, prefix):
        for command in _commands:
            if command.lower().startswith(prefix.lower()):
                return command[len(prefix):]
        return None

    def complete(self, prefix):
        for command in _commands:
            if command.lower().startswith(prefix.lower()):
                yield command

@command(
    'help [command]',
    type=Command.Type.SYSTEM,
    command=CommandResource
)
class HelpCommand(Command):
    INDENT = '    '

    def _build_help_text(self, command):
        help = [[command.name + ':']]
        help += [['    '] + line for line in self._build_executable(command)]
        return '\n'.join(' '.join(line) for line in help)

    def execute(self, command_name=None):
        if command_name is None:
            for name in sorted(_commands.keys()):
                print(name + ':')
                for definition in _commands[name].definitions:
                    print(self.INDENT + definition)
        else:
            command = _commands.get(command_name)
            if command is None:
                print("Unknown command")
            else:
                print(command.name + ':')
                for definition in command.definitions:
                    print(self.INDENT + definition)


@command(
    'exit',
    type=Command.Type.SYSTEM
)
class ExitCommand(Command):
    def execute(self):
        sys.exit(0)

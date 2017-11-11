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

import shlex
from prompt_toolkit.completion import Completer, Completion

from commands import UnknownCommand


class CommandCompleter(Completer):
    """Command completer."""
    def __init__(self, commands):
        self.commands = commands

    def get_completions(self, document, complete_event):
        in_quote = False
        for word in document.text.split(' '):
            if not in_quote and word.startswith('"'):
                in_quote = True
            elif in_quote and word.endswith('"'):
                in_quote = False

        # If the last word is in a quote, don't suggest anything.
        if in_quote:
            return

        args = shlex.split(document.text)
        if len(args) == 0:
            return

        args = list(args)
        if document.text.endswith(' '):
            args.append('')

        command_name = args.pop(0)
        if len(args) == 0:
            for name in sorted(self.commands.commands):
                if name.startswith(command_name):
                    yield Completion(name, start_position=-len(command_name), display_meta=self.commands.command(name).description)
            return

        try:
            command = self.commands.command(command_name)
        except UnknownCommand:
            return

        for completion in command.complete_tail(*args):
            yield Completion(completion, start_position=-len(args[-1]))

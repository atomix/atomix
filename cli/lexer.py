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

from pygments.lexer import RegexLexer
from pygments.lexer import words
from pygments.token import Keyword, Name, Operator, Generic, Literal
from commands import Commands, Command


class CommandLexer(RegexLexer):
    """Provides highlighting for commands."""
    commands = Commands(None)
    tokens = {
        'root': [
            (words(
                tuple(['atomix']),
                prefix=r'\b',
                suffix=r'\b'),
             Literal.String),
            (words(
                tuple(['docs']),
                prefix=r'\b',
                suffix=r'\b'),
             Literal.Number),
            (words(
                tuple(commands.command_names(Command.Type.CLUSTER) + commands.command_names(Command.Type.PRIMITIVE)),
                prefix=r'\b',
                suffix=r'\b'),
             Name.Class),
            (words(
                tuple(commands.action_names()),
                prefix=r'\b',
                suffix=r'\b'),
             Keyword.Declaration),
            #(words(
            #    tuple([]),
            #    prefix=r'',
            #    suffix=r'\b'),
            # Generic.Output),
            (words(
                tuple(commands.command_names(Command.Type.SYSTEM)),
                prefix=r'',
                suffix=r'\b'),
             Operator.Word),
            #(words(
            #    tuple([]),
            #    prefix=r'',
            #    suffix=r'\b'),
            # Name.Exception),
        ]
    }
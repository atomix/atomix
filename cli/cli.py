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

from __future__ import print_function
from __future__ import unicode_literals

import click
import shlex
import traceback
from prompt_toolkit import AbortAction, Application, CommandLineInterface
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.filters import Always
from prompt_toolkit.history import FileHistory
from prompt_toolkit.interface import AcceptAction
from prompt_toolkit.key_binding.manager import KeyBindingManager
from prompt_toolkit.keys import Keys
from prompt_toolkit.shortcuts import create_default_layout, create_eventloop
from pygments.token import Token

from __init__ import __version__
from auto_suggest import CommandAutoSuggest
from commands import Commands
from completer import CommandCompleter
from lexer import CommandLexer
from style import get_style


class Cli(object):
    """Atomix command line."""

    def __init__(self, host='localhost', port=5678, theme='vim'):
        self.host = host
        self.port = port
        self.commands = Commands(self)
        self.theme = theme
        self._create_cli()

    @property
    def address(self):
        return 'http://%s:%s' % (self.host, self.port)

    def path(self, path, *args, **kwargs):
        return self.address + path.format(*args, **kwargs)

    def _create_cli(self):
        """Creates a new cli"""
        history = FileHistory('.history')

        def toolbar_handler(_):
            """Returns bottom menu items.
            Args:
                * _: An instance of prompt_toolkit's Cli (not used).
            Returns:
                A list of Token.Toolbar.
            """
            return [
                (Token.Toolbar, ' [F5] Refresh '),
                (Token.Toolbar, ' [F9] Docs '),
                (Token.Toolbar, ' [F10] Exit ')
            ]

        layout = create_default_layout(
            message='atomix> ',
            reserve_space_for_menu=8,
            lexer=CommandLexer,
            get_bottom_toolbar_tokens=toolbar_handler
        )
        buffer = Buffer(
            history=history,
            auto_suggest=CommandAutoSuggest(self.commands),
            enable_history_search=True,
            completer=CommandCompleter(self.commands),
            complete_while_typing=Always(),
            accept_action=AcceptAction.RETURN_DOCUMENT
        )
        key_manager = KeyBindingManager(
            enable_search=True,
            enable_abort_and_exit_bindings=True,
            enable_system_bindings=True,
            enable_auto_suggest_bindings=True
        )

        application = Application(
            mouse_support=False,
            style=get_style(self.theme),
            layout=layout,
            buffer=buffer,
            key_bindings_registry=key_manager.registry,
            on_exit=AbortAction.RAISE_EXCEPTION,
            on_abort=AbortAction.RETRY,
            ignore_case=True
        )
        eventloop = create_eventloop()
        self.cli = CommandLineInterface(
            application=application,
            eventloop=eventloop
        )

        @key_manager.registry.add_binding(Keys.F10)
        def handle_f10(_):
            """Quits when the `F10` key is pressed."""
            raise EOFError

        @key_manager.registry.add_binding(Keys.ControlSpace)
        def handle_ctrl_space(event):
            """Initializes autocompletion at the cursor.
            If the autocompletion menu is not showing, display it with the
            appropriate completions for the context.
            If the menu is showing, select the next completion.
            Args:
                * event: An instance of prompt_toolkit's Event.
            Returns:
                None.
            """
            b = event.cli.current_buffer
            if b.complete_state:
                b.complete_next()
            else:
                event.cli.start_completion(select_first=False)

    def run(self):
        """Runs the command line interpreter."""
        print('Version: {version}'.format(version=__version__))
        print('Theme: {theme}'.format(theme=self.theme))
        while True:
            try:
                document = self.cli.run(reset_current_buffer=True)
                self._process_command(document.text)
            except (KeyboardInterrupt, EOFError):
                break;
            else:
                self.cli.request_redraw()

    def _process_command(self, text):
        """Processes the input command."""
        try:
            args = shlex.split(text)
            if len(args) == 0:
                return
            command_name = args.pop(0)
            command = self.commands.command(command_name)
            command.execute_action(*args)
        except Exception as e:
            self.log_exception(e, traceback, echo=True)

    def log_exception(self, e, traceback, echo=False):
        print('Exception: %s' % str(e))
        print("Traceback: %s" % traceback.format_exc())
        if echo:
            click.secho(str(e), fg='red')

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
import webbrowser

from prompt_toolkit.buffer import Buffer, AcceptAction
from prompt_toolkit.enums import DEFAULT_BUFFER, SEARCH_BUFFER
from prompt_toolkit.filters import Always
from prompt_toolkit.filters import IsDone, HasFocus, RendererHeightIsKnown, to_cli_filter, Condition
from prompt_toolkit.history import FileHistory
from prompt_toolkit.interface import CommandLineInterface, Application, AbortAction
from prompt_toolkit.key_binding.manager import KeyBindingManager
from prompt_toolkit.keys import Keys
from prompt_toolkit.layout import Window, HSplit, FloatContainer, Float
from prompt_toolkit.layout.containers import ConditionalContainer, VSplit
from prompt_toolkit.layout.controls import BufferControl, TokenListControl
from prompt_toolkit.layout.dimension import LayoutDimension
from prompt_toolkit.layout.lexers import PygmentsLexer
from prompt_toolkit.layout.margins import PromptMargin, ConditionalMargin
from prompt_toolkit.layout.menus import CompletionsMenu, MultiColumnCompletionsMenu
from prompt_toolkit.layout.processors import PasswordProcessor, ConditionalProcessor, AppendAutoSuggestion, \
    HighlightSearchProcessor, HighlightSelectionProcessor, DisplayMultipleCursors
from prompt_toolkit.layout.prompt import DefaultPrompt
from prompt_toolkit.layout.screen import Char
from prompt_toolkit.layout.toolbars import ValidationToolbar, SystemToolbar, ArgToolbar, SearchToolbar
from prompt_toolkit.shortcuts import _split_multiline_prompt, _RPrompt
from prompt_toolkit.shortcuts import create_eventloop
from prompt_toolkit.token import Token

from __init__ import __version__
from auto_suggest import CommandAutoSuggest
from commands import Commands
from completer import CommandCompleter
from lexer import CommandLexer
from service import AtomixService
from style import get_style

try:
    from pygments.lexer import Lexer as pygments_Lexer
    from pygments.style import Style as pygments_Style
except ImportError:
    pygments_Lexer = None
    pygments_Style = None


class Cli(object):
    """Atomix command line."""

    def __init__(self, host='localhost', port=5678, theme='vim'):
        self.service = AtomixService(host, port)
        self.commands = Commands(self)
        self.theme = theme
        self.cli = self._create_cli()

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
                (Token.Toolbar, ' [F9] Docs '),
                (Token.Toolbar, ' [F10] Exit ')
            ]

        def url_handler(_):
            if self.service.last_request is not None:
                method = ' ' + self.service.last_request['method']
                url = ' ' + self.service.last_request['url']
                headers = ''
                if self.service.last_request['headers'] is not None:
                    headers = ' ' + '; '.join([key + ': ' + value for key, value in self.service.last_request['headers'].items()])
                return [
                    (Token.Method, method),
                    (Token.Url, url),
                    (Token.Headers, headers)
                ]
            return [
                (Token.Method, ''),
                (Token.Url, ''),
                (Token.Headers, '')
            ]

        layout = self._create_layout(
            message='atomix> ',
            reserve_space_for_menu=8,
            lexer=CommandLexer,
            get_bottom_toolbar_tokens=toolbar_handler,
            get_url_tokens=url_handler
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

        @key_manager.registry.add_binding(Keys.F9)
        def handle_f9(_):
            """Inputs the "docs" command when the `F9` key is pressed.
            Args:
                * _: An instance of prompt_toolkit's Event (not used).
            Returns:
                None.
            """
            webbrowser.open('http://atomix.io')

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

        return CommandLineInterface(
            application=application,
            eventloop=eventloop
        )

    def _create_layout(self, message='', lexer=None, is_password=False,
                       reserve_space_for_menu=8,
                       get_prompt_tokens=None, get_continuation_tokens=None,
                       get_rprompt_tokens=None,
                       get_bottom_toolbar_tokens=None,
                       get_url_tokens=None,
                       display_completions_in_columns=False,
                       extra_input_processors=None, multiline=False,
                       wrap_lines=True):
        """
        Create a :class:`.Container` instance for a prompt.
        :param message: Text to be used as prompt.
        :param lexer: :class:`~prompt_toolkit.layout.lexers.Lexer` to be used for
            the highlighting.
        :param is_password: `bool` or :class:`~prompt_toolkit.filters.CLIFilter`.
            When True, display input as '*'.
        :param reserve_space_for_menu: Space to be reserved for the menu. When >0,
            make sure that a minimal height is allocated in the terminal, in order
            to display the completion menu.
        :param get_prompt_tokens: An optional callable that returns the tokens to be
            shown in the menu. (To be used instead of a `message`.)
        :param get_continuation_tokens: An optional callable that takes a
            CommandLineInterface and width as input and returns a list of (Token,
            text) tuples to be used for the continuation.
        :param get_bottom_toolbar_tokens: An optional callable that returns the
            tokens for a toolbar at the bottom.
        :param display_completions_in_columns: `bool` or
            :class:`~prompt_toolkit.filters.CLIFilter`. Display the completions in
            multiple columns.
        :param multiline: `bool` or :class:`~prompt_toolkit.filters.CLIFilter`.
            When True, prefer a layout that is more adapted for multiline input.
            Text after newlines is automatically indented, and search/arg input is
            shown below the input, instead of replacing the prompt.
        :param wrap_lines: `bool` or :class:`~prompt_toolkit.filters.CLIFilter`.
            When True (the default), automatically wrap long lines instead of
            scrolling horizontally.
        """
        assert get_bottom_toolbar_tokens is None or callable(get_bottom_toolbar_tokens)
        assert get_prompt_tokens is None or callable(get_prompt_tokens)
        assert get_rprompt_tokens is None or callable(get_rprompt_tokens)
        assert not (message and get_prompt_tokens)

        display_completions_in_columns = to_cli_filter(display_completions_in_columns)
        multiline = to_cli_filter(multiline)

        if get_prompt_tokens is None:
            get_prompt_tokens = lambda _: [(Token.Prompt, message)]

        has_before_tokens, get_prompt_tokens_1, get_prompt_tokens_2 = \
            _split_multiline_prompt(get_prompt_tokens)

        # `lexer` is supposed to be a `Lexer` instance. But if a Pygments lexer
        # class is given, turn it into a PygmentsLexer. (Important for
        # backwards-compatibility.)
        try:
            if pygments_Lexer and issubclass(lexer, pygments_Lexer):
                lexer = PygmentsLexer(lexer, sync_from_start=True)
        except TypeError:  # Happens when lexer is `None` or an instance of something else.
            pass

        # Create processors list.
        input_processors = [
            ConditionalProcessor(
                # By default, only highlight search when the search
                # input has the focus. (Note that this doesn't mean
                # there is no search: the Vi 'n' binding for instance
                # still allows to jump to the next match in
                # navigation mode.)
                HighlightSearchProcessor(preview_search=True),
                HasFocus(SEARCH_BUFFER)),
            HighlightSelectionProcessor(),
            ConditionalProcessor(AppendAutoSuggestion(), HasFocus(DEFAULT_BUFFER) & ~IsDone()),
            ConditionalProcessor(PasswordProcessor(), is_password),
            DisplayMultipleCursors(DEFAULT_BUFFER),
        ]

        if extra_input_processors:
            input_processors.extend(extra_input_processors)

        # Show the prompt before the input (using the DefaultPrompt processor.
        # This also replaces it with reverse-i-search and 'arg' when required.
        # (Only for single line mode.)
        # (DefaultPrompt should always be at the end of the processors.)
        input_processors.append(ConditionalProcessor(
            DefaultPrompt(get_prompt_tokens_2), ~multiline))

        # Create bottom toolbar.
        if get_bottom_toolbar_tokens:
            toolbars = [ConditionalContainer(VSplit([
                Window(TokenListControl(get_url_tokens, default_char=Char(' ', Token.Toolbar)),
                       height=LayoutDimension.exact(1)),
                Window(TokenListControl(get_bottom_toolbar_tokens, default_char=Char(' ', Token.Toolbar),
                                        align_right=True),
                       height=LayoutDimension.exact(1))]),
                filter=~IsDone() & RendererHeightIsKnown())
            ]
        else:
            toolbars = []

        def get_height(cli):
            # If there is an autocompletion menu to be shown, make sure that our
            # layout has at least a minimal height in order to display it.
            if reserve_space_for_menu and not cli.is_done:
                buff = cli.current_buffer

                # Reserve the space, either when there are completions, or when
                # `complete_while_typing` is true and we expect completions very
                # soon.
                if buff.complete_while_typing() or buff.complete_state is not None:
                    return LayoutDimension(min=reserve_space_for_menu)

            return LayoutDimension()

        # Create and return Container instance.
        return HSplit([
                          # The main input, with completion menus floating on top of it.
                          FloatContainer(
                              HSplit([
                                  ConditionalContainer(
                                      Window(
                                          TokenListControl(get_prompt_tokens_1),
                                          dont_extend_height=True),
                                      Condition(has_before_tokens)
                                  ),
                                  Window(
                                      BufferControl(
                                          input_processors=input_processors,
                                          lexer=lexer,
                                          # Enable preview_search, we want to have immediate feedback
                                          # in reverse-i-search mode.
                                          preview_search=True),
                                      get_height=get_height,
                                      left_margins=[
                                          # In multiline mode, use the window margin to display
                                          # the prompt and continuation tokens.
                                          ConditionalMargin(
                                              PromptMargin(get_prompt_tokens_2, get_continuation_tokens),
                                              filter=multiline
                                          )
                                      ],
                                      wrap_lines=wrap_lines,
                                  ),
                              ]),
                              [
                                  # Completion menus.
                                  Float(xcursor=True,
                                        ycursor=True,
                                        content=CompletionsMenu(
                                            max_height=16,
                                            scroll_offset=1,
                                            extra_filter=HasFocus(DEFAULT_BUFFER) &
                                                         ~display_completions_in_columns)),
                                  Float(xcursor=True,
                                        ycursor=True,
                                        content=MultiColumnCompletionsMenu(
                                            extra_filter=HasFocus(DEFAULT_BUFFER) &
                                                         display_completions_in_columns,
                                            show_meta=True)),

                                  # The right prompt.
                                  Float(right=0, top=0, hide_when_covering_content=True,
                                        content=_RPrompt(get_rprompt_tokens)),
                              ]
                          ),
                          ValidationToolbar(),
                          SystemToolbar(),

                          # In multiline mode, we use two toolbars for 'arg' and 'search'.
                          ConditionalContainer(ArgToolbar(), multiline),
                          ConditionalContainer(SearchToolbar(), multiline),
                      ] + toolbars)

    def run(self):
        """Runs the command line interpreter."""
        print('Version: {version}'.format(version=__version__))
        print('Theme: {theme}'.format(theme=self.theme))
        while True:
            try:
                document = self.cli.run(reset_current_buffer=True)
                args = shlex.split(document.text)
                if len(args) > 0:
                    self._process_command(*args)
            except (KeyboardInterrupt, EOFError):
                break
            else:
                self.cli.request_redraw()

    def _process_command(self, *args):
        """Processes the input command."""
        try:
            args = list(args)
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

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

from collections import OrderedDict

_commands = {}
_type = type

def command(definition, type=None, description=None, **kwargs):
    def wrap(cls):
        params = list(definition.split(' '))
        command = params.pop(0)
        cls.name = command
        if cls.definitions is None:
            cls.definitions = []
        cls.definitions.insert(0, definition)
        cls.type = type
        cls.description = description
        next = cls
        for param in params:
            if param.startswith('{') and param.endswith('}'):
                if next._resource is None:
                    name = param[1:-1]
                    param_cls = _type(str(name), (kwargs[name],), {})
                    param_cls.name = name
                    param_cls.required = True
                    next._resource = param_cls
                    next = next._resource
                else:
                    next = next._resource
            elif param.startswith('[') and param.endswith(']'):
                if next._resource is None:
                    name = param[1:-1]
                    param_cls = _type(str(name), (kwargs[name],), {})
                    param_cls.name = name
                    param_cls.required = False
                    next._resource = param_cls
                    next = next._resource
                else:
                    next = next._resource
            else:
                if next._actions is None:
                    next._actions = OrderedDict()
                action = kwargs[param.replace('-', '_')]
                action.name = param
                next._actions[param] = action
                next = action
        _commands[command] = cls
        return cls
    return wrap

class Commands(object):
    """Atomix commands."""
    def __init__(self, cli):
        self.commands = _commands
        self.cli = cli

    def command_names(self, type=None):
        """Returns the command names."""
        if type is not None:
            return [name for (name, command) in self.commands.items() if command.type == type]
        return self.commands.keys()

    def action_names(self):
        """Returns cluster command names."""
        actions = []
        for command in self.commands.values():
            if command._resource is not None:
                actions += self._flatten_action_names(command._resource._actions)
            actions += self._flatten_action_names(command._actions)
        return actions

    def _flatten_action_names(self, actions):
        results = []
        if actions is not None:
            for name, action in actions.items():
                results.append(name)
                if action._resource is not None:
                    results += self._flatten_action_names(action._resource._actions)
                results += self._flatten_action_names(action._actions)
        return results

    def command(self, name):
        command = self.commands.get(name)
        if command is None:
            raise UnknownCommand(name)
        instance = command()
        instance.cli = self.cli
        return instance

class UnknownExecutable(Exception):
    """Base class for executable exceptions."""
    def __init__(self, name):
        self.name = name

class UnknownCommand(UnknownExecutable):
    """Unknown command exception."""
    def __init__(self, name):
        super(UnknownCommand, self).__init__(name)

    def __str__(self):
        return "Unknown command: {command}".format(command=self.name)

class UnknownAction(UnknownExecutable):
    """Unknown action exception."""
    def __init__(self, name):
        super(UnknownAction, self).__init__(name)

    def __str__(self):
        return "Unknown action: {action}".format(action=self.name)


class Executable(object):
    """Executable."""
    name = None
    _resource = None
    _actions = None
    cli = None

    def _construct(self, executable):
        instance = executable()
        instance.cli = self.cli
        return instance

    def _get_tails_and_args(self, *args):
        args = list(args)
        if len(args) == 0:
            return []

        resources = []
        tail = self
        while len(args) > 1:
            arg = args.pop(0)
            if tail._resource is not None:
                tail = self._construct(tail._resource)
                resources.append(arg)
            elif tail._actions is not None:
                action = tail._actions.get(arg)
                if action is not None:
                    tail = self._construct(action)
                else:
                    break

        prefix = args[0]
        if tail._resource is not None:
            return [(self._construct(tail._resource), list(resources) + [prefix])]
        elif tail._actions is not None:
            actions = []
            for action in tail._actions:
                if action.lower().startswith(prefix):
                    actions.append((self._construct(tail._actions[action]), list(resources) + [prefix]))
            return actions
        return []

    def _get_action_and_args(self, *args):
        args = list(args)
        if len(args) == 0:
            return self, args

        i = 0
        resource = self.__class__
        while True:
            if resource._resource is not None:
                resource = resource._resource
                i += 1
            else:
                break

        if len(args) < i + 1:
            return self, args
        action_name = args.pop(i)
        action = resource._actions.get(action_name)
        if action is None:
            raise UnknownAction(action)
        return self._construct(action), args

    def suggest_tail(self, *args):
        """Returns a suggestion for the given text."""
        for tail, args in self._get_tails_and_args(*args):
            suggestion = tail.suggest(*args)
            if suggestion is not None:
                return suggestion
        return None

    def suggest(self, *args):
        """Returns a suggestion for the given text."""
        return None

    def complete_tail(self, *args):
        """Completes the given text."""
        for tail, args in self._get_tails_and_args(*args):
            for completion in tail.complete(*args):
                yield completion

    def complete(self, *args):
        """Yields a list of completions for the given arguments."""
        return []

    def execute_action(self, *args):
        """Processes the given input."""
        action, args = self._get_action_and_args(*args)
        action.execute(*args)

    def execute(self, *args):
        """Executes the executable."""


class Command(Executable):
    """Command class."""
    class Type(object):
        """Command type."""
        SYSTEM = 'system'
        CLUSTER = 'cluster'
        PRIMITIVE = 'primitive'

    type = None
    definitions = None
    description = None


class Action(Executable):
    """Action"""
    def suggest(self, *args):
        return self.name[len(args[-1]):]

    def complete(self, *args):
        yield self.name


class Resource(Executable):
    """Resource"""

import cluster
import counter
import election
import events
import idgenerator
import lock
import map
import messages
import queue
import set
import system
import tree
import value

__all__ = [
    'cluster',
    'counter',
    'election',
    'events',
    'idgenerator',
    'lock',
    'map',
    'messages',
    'queue',
    'set',
    'system',
    'tree',
    'value'
]

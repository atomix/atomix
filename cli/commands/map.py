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


class MapResource(Resource):
    def _get_map_names(self):
        response = self.cli.service.get(
            self.cli.service.url('/v1/primitives/maps'),
            log=False
        )
        if response.status_code == 200:
            return response.json()
        return []

    def suggest(self, prefix):
        maps = self._get_map_names()
        for map in maps:
            if map.lower().startswith(prefix.lower()):
                return map[len(prefix):]
        return None

    def complete(self, prefix):
        maps = self._get_map_names()
        for map in maps:
            if map.lower().startswith(prefix.lower()):
                yield map


class GetAction(Action):
    def execute(self, name, key):
        self.cli.service.output(self.cli.service.get(
            self.cli.service.url('/v1/primitives/maps/{name}/{key}', name=name, key=key)
        ))


class PutAction(Action):
    def execute(self, name, key, value):
        self.cli.service.output(self.cli.service.put(
            self.cli.service.url('/v1/primitives/maps/{name}/{key}', name=name, key=key, value=value),
            headers={'content-type': 'text/plain'}
        ))


class RemoveAction(Action):
    def execute(self, name, key):
        self.cli.service.output(self.cli.service.delete(
            self.cli.service.url('/v1/primitives/maps/{name}/{key}', name=name, key=key)
        ))


class SizeAction(Action):
    def execute(self, name):
        self.cli.service.output(self.cli.service.get(
            self.cli.service.url('/v1/primitives/maps/{name}/size', name=name)
        ))


class ClearAction(Action):
    def execute(self, name):
        self.cli.service.output(self.cli.service.delete(
            self.cli.service.url('/v1/primitives/maps/{name}', name=name)
        ))


class KeyResource(Resource):
    def _get_map_keys(self, name):
        response = self.cli.service.get(
            self.cli.service.url('/v1/primitives/maps/{name}/keys', name=name),
            log=False
        )
        if response.status_code == 200:
            return response.json()
        return []

    def suggest(self, name, prefix):
        keys = self._get_map_keys(name)
        for key in keys:
            if key.lower().startswith(prefix.lower()):
                return key[len(prefix):]
        return None

    def complete(self, name, prefix):
        keys = self._get_map_keys(name)
        for key in keys:
            if key.lower().startswith(prefix.lower()):
                yield key


class TextResource(Resource):
    pass


@command(
    'map {map} get {key}',
    type=Command.Type.PRIMITIVE,
    map=MapResource,
    get=GetAction,
    key=KeyResource
)
@command(
    'map {map} put {key} {text}',
    type=Command.Type.PRIMITIVE,
    map=MapResource,
    put=PutAction,
    key=KeyResource,
    text=TextResource
)
@command(
    'map {map} remove {key}',
    type=Command.Type.PRIMITIVE,
    map=MapResource,
    remove=RemoveAction,
    key=KeyResource
)
@command(
    'map {map} size',
    type=Command.Type.PRIMITIVE,
    map=MapResource,
    size=SizeAction
)
@command(
    'map {map} clear',
    type=Command.Type.PRIMITIVE,
    map=MapResource,
    clear=ClearAction
)
class MapCommand(Command):
    """Map command"""

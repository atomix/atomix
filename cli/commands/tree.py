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
import requests


class TreeResource(Resource):
    def _get_tree_names(self):
        response = requests.get(self.cli.path('/v1/primitives/trees'))
        if response.status_code == 200:
            return response.json()
        return []

    def suggest(self, prefix):
        trees = self._get_tree_names()
        for tree in trees:
            if tree.lower().startswith(prefix.lower()):
                return tree[len(prefix):]
        return None

    def complete(self, prefix):
        trees = self._get_tree_names()
        for tree in trees:
            if tree.lower().startswith(prefix.lower()):
                yield tree


class GetAction(Action):
    def execute(self, name, path):
        response = requests.get(self.cli.path('/v1/primitives/trees/{name}/{path}', name=name))
        if response.status_code == 200:
            if response.text != '':
                print(response.json())
        else:
            print("Failed to read node")


class CreateAction(Action):
    def execute(self, name, path, value):
        response = requests.post(
            self.cli.path('/v1/primitives/trees/{name}/{path}', name=name, path=path),
            data=value,
            headers={'content-type': 'text/plain'}
        )
        if response.status_code == 200:
            print(response.json())
        else:
            print("Failed to create node")


class SetAction(Action):
    def execute(self, name, path, value):
        response = requests.put(
            self.cli.path('/v1/primitives/trees/{name}/{path}', name=name, path=path),
            data=value,
            headers={'content-type': 'text/plain'}
        )
        if response.status_code == 200:
            print(response.json())
        else:
            print("Failed to update node")


class ReplaceAction(Action):
    def execute(self, name, path, value, version):
        response = requests.put(
            self.cli.path('/v1/primitives/trees/{name}/{path}?version={version}', name=name, path=path, version=version),
            data=value,
            headers={'content-type': 'text/plain'}
        )
        if response.status_code == 200:
            print(response.json())
        else:
            print("Failed to replace node")


class PathResource(Resource):
    def _get_children(self, name, path):
        response = requests.get(self.cli.path('/v1/primitives/trees/{name}/{path}/children', name=name, path=path))
        if response.status_code == 200:
            return response.json()
        return []

    def suggest(self, name, prefix):
        path = prefix.split('/')
        for child in self._get_children(name, '/'.join(path[:-1])):
            if child.lower().startswith(prefix.lower()):
                return child[len(prefix):]
        return None

    def complete(self, name, prefix):
        path = prefix.split('/')
        for child in self._get_children(name, '/'.join(path[:-1])):
            if child.lower().startswith(prefix.lower()):
                yield child


class TextResource(Resource):
    pass


@command(
    'tree {tree} create {path} {text}',
    type=Command.Type.PRIMITIVE,
    tree=TreeResource,
    create=CreateAction,
    path=PathResource,
    text=TextResource
)
@command(
    'tree {tree} set {path} {text}',
    type=Command.Type.PRIMITIVE,
    tree=TreeResource,
    set=SetAction,
    path=PathResource,
    text=TextResource
)
@command(
    'tree {tree} get {path}',
    type=Command.Type.PRIMITIVE,
    tree=TreeResource,
    get=GetAction,
    path=PathResource
)
@command(
    'tree {tree} replace {path} {text} {version}',
    type=Command.Type.PRIMITIVE,
    tree=TreeResource,
    replace=ReplaceAction,
    path=PathResource,
    text=TextResource,
    version=TextResource
)
class TreeCommand(Command):
    """Tree command"""

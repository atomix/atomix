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


class TreeResource(Resource):
    def _get_tree_names(self):
        response = self.cli.service.get(
            self.cli.service.url('/v1/primitives/trees'),
            log=False
        )
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
        self.cli.service.output(self.cli.service.get(
            self.cli.service.url('/v1/primitives/trees/{name}/{path}', name=name, path=path)
        ))


class CreateAction(Action):
    def execute(self, name, path, value):
        self.cli.service.output(self.cli.service.post(
            self.cli.service.url('/v1/primitives/trees/{name}/{path}', name=name, path=path),
            data=value,
            headers={'content-type': 'text/plain'}
        ))


class SetAction(Action):
    def execute(self, name, path, value):
        self.cli.service.output(self.cli.service.put(
            self.cli.service.url('/v1/primitives/trees/{name}/{path}', name=name, path=path),
            data=value,
            headers={'content-type': 'text/plain'}
        ))


class ReplaceAction(Action):
    def execute(self, name, path, value, version):
        self.cli.service.output(self.cli.service.put(
            self.cli.service.url(
                '/v1/primitives/trees/{name}/{path}?version={version}',
                name=name,
                path=path,
                version=version
            ),
            data=value,
            headers={'content-type': 'text/plain'}
        ))


class PathResource(Resource):
    def _get_children(self, name, path):
        if len(path) > 0:
            response = self.cli.service.get(
                self.cli.service.url('/v1/primitives/trees/{name}/children/{path}', name=name, path=path),
                log=False
            )
        else:
            response = self.cli.service.get(
                self.cli.service.url('/v1/primitives/trees/{name}/children', name=name),
                log=False
            )
        if response.status_code == 200:
            return response.json()
        return []

    def suggest(self, name, prefix):
        path = prefix.split('/')
        parent, child_prefix = '/'.join(path[:-1]), path[-1]
        for child in self._get_children(name, parent):
            if child.lower().startswith(child_prefix.lower()):
                return child[len(child_prefix):]
        return None

    def complete(self, name, prefix):
        path = prefix.split('/')
        parent, child_prefix = '/'.join(path[:-1]), path[-1]
        for child in self._get_children(name, parent):
            if child.lower().startswith(child_prefix.lower()):
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

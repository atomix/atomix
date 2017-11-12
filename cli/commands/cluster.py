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


class NodesAction(Action):
    def execute(self):
        self.cli.service.output(self.cli.service.get(self.cli.service.url('/v1/cluster/nodes')))


class NodeAction(Action):
    def execute(self, node=None):
        if node is None:
            self.cli.service.output(self.cli.service.get(
                self.cli.service.url('/v1/cluster/node')
            ))
        else:
            self.cli.service.output(self.cli.service.get(
                self.cli.service.url('/v1/cluster/nodes/{node}', node=node)
            ))


class NodeResource(Resource):
    def _get_nodes(self):
        response = self.cli.service.get(self.cli.service.url('/v1/cluster/nodes'), log=False)
        if response.status_code == 200:
            return response.json()
        return []

    def suggest(self, prefix):
        for node in self._get_nodes():
            if node['id'].lower().startswith(prefix.lower()):
                return node['id'][len(prefix):]
        return None

    def complete(self, prefix):
        for node in self._get_nodes():
            if node['id'].lower().startswith(prefix.lower()):
                yield node['id']


@command(
    'cluster node [id]',
    type=Command.Type.CLUSTER,
    node=NodeAction,
    id=NodeResource
)
@command(
    'cluster nodes',
    type=Command.Type.CLUSTER,
    nodes=NodesAction
)
class ClusterCommand(Command):
    """Cluster command"""
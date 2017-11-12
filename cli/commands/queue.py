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


class QueueResource(Resource):
    def _get_queue_names(self):
        response = self.cli.service.get(
            self.cli.service.url('/v1/primitives/queues'),
            log=False
        )
        if response.status_code == 200:
            return response.json()
        return []

    def suggest(self, prefix):
        queues = self._get_queue_names()
        for queue in queues:
            if queue.lower().startswith(prefix.lower()):
                return queue[len(prefix):]
        return None

    def complete(self, prefix):
        queues = self._get_queue_names()
        for queue in queues:
            if queue.lower().startswith(prefix.lower()):
                yield queue


class AddAction(Action):
    def execute(self, queue, data):
        self.cli.service.output(self.cli.service.post(
            self.cli.service.url('/v1/primitives/queues/{queue}', queue=queue),
            data=data,
            headers={'content-type': 'text/plain'}
        ))


class TakeAction(Action):
    def execute(self, queue):
        self.cli.service.output(self.cli.service.get(
            self.cli.service.url('/v1/primitives/queues/{queue}', queue=queue)
        ))


class ConsumeAction(Action):
    def execute(self, queue):
        try:
            while True:
                response = self.cli.service.get(self.cli.service.url('/v1/primitives/queues/{queue}', queue=queue))
                if response.status_code == 200:
                    self.cli.service.output(response)
                else:
                    break
        except KeyboardInterrupt:
            return


class ItemResource(Resource):
    pass


class CountResource(Resource):
    pass


@command(
    'queue {queue} add {item}',
    type=Command.Type.PRIMITIVE,
    queue=QueueResource,
    add=AddAction,
    item=ItemResource
)
@command(
    'queue {queue} take',
    type=Command.Type.PRIMITIVE,
    queue=QueueResource,
    take=TakeAction
)
@command(
    'queue {queue} consume',
    type=Command.Type.PRIMITIVE,
    queue=QueueResource,
    consume=ConsumeAction
)
class QueueCommand(Command):
    """Queue command"""

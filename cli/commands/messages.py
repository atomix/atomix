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


class SubjectResource(Resource):
    pass


class PublishAction(Action):
    def execute(self, subject, data):
        response = requests.post(self.cli.path('/v1/messages/{subject}', subject=subject), data=data, headers={'content-type': 'text/plain'})
        if response.status_code != 200:
            print("Publish failed: " + response.status_code)


class SendAction(Action):
    def execute(self, subject, node, data):
        response = requests.post(self.cli.path('/v1/messages/{subject}/{node}', subject=subject, node=node), data=data, headers={'content-type': 'text/plain'})
        if response.status_code != 200:
            print("Publish failed: " + response.status_code)


class SubscribeAction(Action):
    def execute(self, subject):
        response = requests.post(self.cli.path('/v1/messages/{subject}/subscribers', subject=subject))
        print(response.json())


class ConsumeAction(Action):
    def execute(self, subject, subscriber):
        response = requests.get(self.cli.path('/v1/messages/{subject}/subscribers/{subscriber}', subject=subject, subscriber=subscriber))
        print(response.json())


class UnsubscribeAction(Action):
    def execute(self, subject, subscriber):
        response = requests.delete(self.cli.path('/v1/messages/{subject}/subscribers/{subscriber}', subject=subject, subscriber=subscriber))
        print(response.json())


class ListenAction(Action):
    def execute(self, subject):
        response = requests.post(self.cli.path('/v1/messages/{subject}/subscribers', subject=subject))
        subscriber = response.text
        try:
            while True:
                response = requests.get(self.cli.path('/v1/messages/{subject}/subscribers/{subscriber}', subject=subject, subscriber=subscriber))
                if response.status_code == 200:
                    print(response.text)
                else:
                    break
        except KeyboardInterrupt:
            requests.delete(self.cli.path('/v1/messages/{subject}/subscribers/{subscriber}', subject=subject, subscriber=subscriber))


class SubscriberResource(Resource):
    def _get_event_subscribers(self, subject):
        response = requests.get(self.cli.path('/v1/messages/{subject}/subscribers', subject=subject))
        if response.status_code == 200:
            return response.json()
        return []

    def suggest(self, subject, prefix):
        subscribers = self._get_event_subscribers(subject)
        for subscriber in subscribers:
            if subscriber.lower().startswith(prefix.lower()):
                return subscriber[len(prefix):]
        return None

    def complete(self, subject, prefix):
        subscribers = self._get_event_subscribers(subject)
        for subscriber in subscribers:
            if subscriber.lower().startswith(prefix.lower()):
                yield subscriber


class NodeResource(Resource):
    def _get_nodes(self):
        response = requests.get(self.cli.path('/v1/cluster/nodes'))
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


class TextResource(Resource):
    pass


@command(
    'messages {subject} publish {text}',
    type=Command.Type.CLUSTER,
    subject=SubjectResource,
    publish=PublishAction,
    text=TextResource
)
@command(
    'messages {subject} send {node} {text}',
    type=Command.Type.CLUSTER,
    subject=SubjectResource,
    send=SendAction,
    node=NodeResource,
    text=TextResource
)
@command(
    'messages {subject} listen',
    type=Command.Type.CLUSTER,
    subject=SubjectResource,
    listen=ListenAction
)
@command(
    'messages {subject} subscribe',
    type=Command.Type.CLUSTER,
    subject=SubjectResource,
    subscribe=SubscribeAction
)
@command(
    'messages {subject} consume {subscriber}',
    type=Command.Type.CLUSTER,
    subject=SubjectResource,
    consume=ConsumeAction,
    subscriber=SubscriberResource
)
@command(
    'messages {subject} unsubscribe {subscriber}',
    type=Command.Type.CLUSTER,
    subject=SubjectResource,
    unsubscribe=UnsubscribeAction,
    subscriber=SubscriberResource
)
class MessagesCommand(Command):
    """Messages command"""

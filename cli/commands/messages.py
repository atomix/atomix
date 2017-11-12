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


class SubjectResource(Resource):
    pass


class PublishAction(Action):
    def execute(self, subject, data):
        self.cli.service.output(self.cli.service.post(
            self.cli.service.url('/v1/messages/{subject}', subject=subject),
            data=data,
            headers={'content-type': 'text/plain'}
        ))


class SendAction(Action):
    def execute(self, subject, node, data):
        self.cli.service.output(self.cli.service.post(
            self.cli.service.url('/v1/messages/{subject}/{node}', subject=subject, node=node),
            data=data,
            headers={'content-type': 'text/plain'}
        ))


class SubscribeAction(Action):
    def execute(self, subject):
        self.cli.service.output(self.cli.service.post(
            self.cli.service.url('/v1/messages/{subject}/subscribers', subject=subject)
        ))


class ConsumeAction(Action):
    def execute(self, subject, subscriber):
        self.cli.service.output(self.cli.service.get(
            self.cli.service.url('/v1/messages/{subject}/subscribers/{subscriber}', subject=subject,
                                 subscriber=subscriber)
        ))


class UnsubscribeAction(Action):
    def execute(self, subject, subscriber):
        self.cli.service.output(self.cli.service.delete(
            self.cli.service.url('/v1/messages/{subject}/subscribers/{subscriber}', subject=subject,
                                 subscriber=subscriber)
        ))


class ListenAction(Action):
    def execute(self, subject):
        response = self.cli.service.post(self.cli.service.url(
            '/v1/messages/{subject}/subscribers',
            subject=subject
        ))
        response.raise_for_status()

        subscriber = response.text
        try:
            while True:
                response = self.cli.service.get(self.cli.service.url(
                    '/v1/messages/{subject}/subscribers/{subscriber}',
                    subject=subject,
                    subscriber=subscriber
                ))
                if response.status_code == 200:
                    self.cli.service.output(response)
                else:
                    break
        except KeyboardInterrupt:
            self.cli.service.delete(self.cli.service.url(
                '/v1/messages/{subject}/subscribers/{subscriber}',
                subject=subject,
                subscriber=subscriber
            ))


class SubscriberResource(Resource):
    def _get_event_subscribers(self, subject):
        response = self.cli.service.get(
            self.cli.service.url('/v1/messages/{subject}/subscribers', subject=subject),
            log=False
        )
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
        response = self.cli.service.get(
            self.cli.service.url('/v1/cluster/nodes'),
            log=False
        )
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

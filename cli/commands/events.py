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


class TopicResource(Resource):
    pass


class PublishAction(Action):
    def execute(self, topic, data):
        self.cli.service.output(self.cli.service.post(
            self.cli.service.url('/v1/events/{topic}', topic=topic),
            data=data,
            headers={'content-type': 'text/plain'}
        ))


class SubscribeAction(Action):
    def execute(self, topic):
        self.cli.service.output(self.cli.service.post(
            self.cli.service.url('/v1/events/{topic}/subscribers', topic=topic)
        ))


class ConsumeAction(Action):
    def execute(self, topic, subscriber):
        self.cli.service.output(self.cli.service.get(
            self.cli.service.url('/v1/events/{topic}/subscribers/{subscriber}', topic=topic, subscriber=subscriber)
        ))


class UnsubscribeAction(Action):
    def execute(self, topic, subscriber):
        self.cli.service.output(self.cli.service.delete(
            self.cli.service.url('/v1/events/{topic}/subscribers/{subscriber}', topic=topic, subscriber=subscriber)
        ))


class ListenAction(Action):
    def execute(self, topic):
        response = self.cli.service.post(self.cli.service.url(
            '/v1/events/{topic}/subscribers',
            topic=topic
        ))
        response.raise_for_status()

        subscriber = response.text
        try:
            while True:
                response = self.cli.service.get(self.cli.service.url(
                    '/v1/events/{topic}/subscribers/{subscriber}',
                    topic=topic,
                    subscriber=subscriber
                ))
                if response.status_code == 200:
                    self.cli.service.output(response)
                else:
                    break
        except KeyboardInterrupt:
            self.cli.service.delete(self.cli.service.url(
                '/v1/events/{topic}/subscribers/{subscriber}',
                topic=topic,
                subscriber=subscriber
            ))


class SubscriberResource(Resource):
    def _get_event_subscribers(self, topic):
        response = self.cli.service.get(
            self.cli.service.url('/v1/events/{topic}/subscribers', topic=topic),
            log=False
        )
        if response.status_code == 200:
            return response.json()
        return []

    def suggest(self, topic, prefix):
        subscribers = self._get_event_subscribers(topic)
        for subscriber in subscribers:
            if subscriber.lower().startswith(prefix.lower()):
                return subscriber[len(prefix):]
        return None

    def complete(self, topic, prefix):
        subscribers = self._get_event_subscribers(topic)
        for subscriber in subscribers:
            if subscriber.lower().startswith(prefix.lower()):
                yield subscriber


class TextResource(Resource):
    pass


@command(
    'events {topic} publish {text}',
    type=Command.Type.CLUSTER,
    topic=TopicResource,
    publish=PublishAction,
    text=TextResource
)
@command(
    'events {topic} listen',
    type=Command.Type.CLUSTER,
    topic=TopicResource,
    listen=ListenAction
)
@command(
    'events {topic} subscribe',
    type=Command.Type.CLUSTER,
    topic=TopicResource,
    subscribe=SubscribeAction
)
@command(
    'events {topic} consume {subscriber}',
    type=Command.Type.CLUSTER,
    topic=TopicResource,
    consume=ConsumeAction,
    subscriber=SubscriberResource
)
@command(
    'events {topic} unsubscribe {subscriber}',
    type=Command.Type.CLUSTER,
    topic=TopicResource,
    unsubscribe=UnsubscribeAction,
    subscriber=SubscriberResource
)
class EventsCommand(Command):
    """Events command"""

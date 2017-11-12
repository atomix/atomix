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


class ElectionResource(Resource):
    def _get_election_names(self):
        response = self.cli.service.get(self.cli.service.url('/v1/primitives/elections'), log=False)
        if response.status_code == 200:
            return response.json()
        return []

    def suggest(self, prefix):
        elections = self._get_election_names()
        for election in elections:
            if election.lower().startswith(prefix.lower()):
                return election[len(prefix):]
        return None

    def complete(self, prefix):
        elections = self._get_election_names()
        for election in elections:
            if election.lower().startswith(prefix.lower()):
                yield election


class RunAction(Action):
    def execute(self, name):
        self.cli.service.output(self.cli.service.post(
            self.cli.service.url('/v1/primitives/elections/{name}', name=name)
        ))


class LeaderAction(Action):
    def execute(self, name):
        self.cli.service.output(self.cli.service.get(
            self.cli.service.url('/v1/primitives/elections/{name}', name=name)
        ))


class ListenAction(Action):
    def execute(self, name, id):
        self.cli.service.output(self.cli.service.get(
            self.cli.service.url('/v1/primitives/elections/{name}/{id}', name=name, id=id)
        ))


class WithdrawAction(Action):
    def execute(self, name, id):
        self.cli.service.output(self.cli.service.delete(
            self.cli.service.url('/v1/primitives/elections/{name}/{id}', name=name, id=id)
        ))


class AnointAction(Action):
    def execute(self, name, id):
        self.cli.service.output(self.cli.service.post(
            self.cli.service.url('/v1/primitives/elections/{name}/{id}/anoint', name=name, id=id)
        ))


class PromoteAction(Action):
    def execute(self, name, id):
        self.cli.service.output(self.cli.service.post(
            self.cli.service.url('/v1/primitives/elections/{name}/{id}/promote', name=name, id=id)
        ))


class EvictAction(Action):
    def execute(self, name, id):
        self.cli.service.output(self.cli.service.post(
            self.cli.service.url('/v1/primitives/elections/{name}/{id}/evict', name=name, id=id)
        ))


class CandidateResource(Resource):
    def _get_candidates(self, name):
        response = self.cli.service.get(
            self.cli.service.url('/v1/primitives/elections/{name}', name=name),
            log=False
        )
        if response.status_code == 200:
            return response.json()
        return []

    def suggest(self, name, prefix):
        for candidate in self._get_candidates(name):
            if candidate.lower().startswith(prefix.lower()):
                return candidate[len(prefix):]
        return None

    def complete(self, name, prefix):
        for node in self._get_candidates(name):
            if node.lower().startswith(prefix.lower()):
                yield node


@command(
    'election {election} run',
    type=Command.Type.PRIMITIVE,
    election=ElectionResource,
    run=RunAction
)
@command(
    'election {election} leader',
    type=Command.Type.PRIMITIVE,
    election=ElectionResource,
    leader=LeaderAction
)
@command(
    'election {election} listen {candidate}',
    type=Command.Type.PRIMITIVE,
    election=ElectionResource,
    listen=ListenAction,
    candidate=CandidateResource
)
@command(
    'election {election} anoint {candidate}',
    type=Command.Type.PRIMITIVE,
    election=ElectionResource,
    anoint=AnointAction,
    candidate=CandidateResource
)
@command(
    'election {election} evict {candidate}',
    type=Command.Type.PRIMITIVE,
    election=ElectionResource,
    evict=EvictAction,
    candidate=CandidateResource
)
@command(
    'election {election} promote {candidate}',
    type=Command.Type.PRIMITIVE,
    election=ElectionResource,
    promote=PromoteAction,
    candidate=CandidateResource
)
@command(
    'election {election} withdraw {candidate}',
    type=Command.Type.PRIMITIVE,
    election=ElectionResource,
    withdraw=WithdrawAction,
    candidate=CandidateResource
)
class ElectionCommand(Command):
    """Election command"""

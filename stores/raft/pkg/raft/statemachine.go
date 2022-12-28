// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	rsmv1 "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	raftv1 "github.com/atomix/atomix/stores/raft/api/v1"
	"github.com/gogo/protobuf/proto"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
	"io"
)

func newStateMachine(protocol *protocolContext, types *statemachine.PrimitiveTypeRegistry) dbsm.IStateMachine {
	return &stateMachine{
		protocol: protocol,
		sm:       statemachine.NewStateMachine(types),
	}
}

type stateMachine struct {
	protocol *protocolContext
	sm       statemachine.StateMachine
}

func (s *stateMachine) Update(bytes []byte) (dbsm.Result, error) {
	proposal := &raftv1.RaftProposal{}
	if err := proto.Unmarshal(bytes, proposal); err != nil {
		return dbsm.Result{}, err
	}
	var input rsmv1.ProposalInput
	if err := proto.Unmarshal(proposal.Data, &input); err != nil {
		return dbsm.Result{}, err
	}
	stream := s.protocol.getStream(proposal.Term, proposal.SequenceNum)
	s.sm.Propose(&input, stream)
	return dbsm.Result{}, nil
}

func (s *stateMachine) Lookup(value interface{}) (interface{}, error) {
	query := value.(*protocolQuery)
	s.sm.Query(query.input, query.stream)
	return nil, nil
}

func (s *stateMachine) SaveSnapshot(w io.Writer, collection dbsm.ISnapshotFileCollection, i <-chan struct{}) error {
	log.Infow("Persisting state to snapshot")
	if err := s.sm.Snapshot(statemachine.NewSnapshotWriter(w)); err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (s *stateMachine) RecoverFromSnapshot(r io.Reader, files []dbsm.SnapshotFile, i <-chan struct{}) error {
	log.Infow("Recovering state from snapshot")
	if err := s.sm.Recover(statemachine.NewSnapshotReader(r)); err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (s *stateMachine) Close() error {
	return nil
}

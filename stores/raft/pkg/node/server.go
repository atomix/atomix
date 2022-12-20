// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	raftv1 "github.com/atomix/atomix/stores/raft/pkg/api/v1"
)

func NewNodeServer(protocol *Protocol) raftv1.NodeServer {
	return &nodeServer{
		protocol: protocol,
	}
}

type nodeServer struct {
	protocol *Protocol
}

func (s *nodeServer) Bootstrap(ctx context.Context, request *raftv1.BootstrapRequest) (*raftv1.BootstrapResponse, error) {
	log.Debugw("Bootstrap",
		logging.Stringer("BootstrapRequest", request))
	if err := s.protocol.Bootstrap(request.Group); err != nil {
		log.Warnw("Bootstrap",
			logging.Stringer("BootstrapRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &raftv1.BootstrapResponse{}
	log.Debugw("Bootstrap",
		logging.Stringer("BootstrapRequest", request),
		logging.Stringer("BootstrapResponse", response))
	return response, nil
}

func (s *nodeServer) Join(ctx context.Context, request *raftv1.JoinRequest) (*raftv1.JoinResponse, error) {
	log.Debugw("Join",
		logging.Stringer("JoinRequest", request))
	if err := s.protocol.Join(request.Group); err != nil {
		log.Warnw("Join",
			logging.Stringer("JoinRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &raftv1.JoinResponse{}
	log.Debugw("Join",
		logging.Stringer("JoinRequest", request),
		logging.Stringer("JoinResponse", response))
	return response, nil
}

func (s *nodeServer) Leave(ctx context.Context, request *raftv1.LeaveRequest) (*raftv1.LeaveResponse, error) {
	log.Debugw("Leave",
		logging.Stringer("LeaveRequest", request))
	if err := s.protocol.Leave(request.GroupID); err != nil {
		log.Warnw("Leave",
			logging.Stringer("LeaveRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &raftv1.LeaveResponse{}
	log.Debugw("Leave",
		logging.Stringer("LeaveRequest", request),
		logging.Stringer("LeaveResponse", response))
	return response, nil
}

func (s *nodeServer) Watch(request *raftv1.WatchRequest, server raftv1.Node_WatchServer) error {
	log.Debugw("Watch",
		logging.Stringer("WatchRequest", request))
	ch := make(chan raftv1.Event, 100)
	go s.protocol.Watch(server.Context(), ch)
	for event := range ch {
		log.Debugw("Watch",
			logging.Stringer("WatchRequest", request),
			logging.Stringer("NodeEvent", &event))
		err := server.Send(&event)
		if err != nil {
			log.Warnw("Watch",
				logging.Stringer("WatchRequest", request),
				logging.Stringer("NodeEvent", &event),
				logging.Error("Error", err))
			return errors.ToProto(err)
		}
	}
	return nil
}

// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package multiraft

import (
	"context"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	multiraftv1 "github.com/atomix/atomix/stores/multi-raft/pkg/api/v1"
)

func NewNodeServer(protocol *Protocol) multiraftv1.NodeServer {
	return &nodeServer{
		protocol: protocol,
	}
}

type nodeServer struct {
	protocol *Protocol
}

func (s *nodeServer) Bootstrap(ctx context.Context, request *multiraftv1.BootstrapRequest) (*multiraftv1.BootstrapResponse, error) {
	log.Debugw("Bootstrap",
		logging.Stringer("BootstrapRequest", request))
	if err := s.protocol.Bootstrap(request.Group); err != nil {
		log.Warnw("Bootstrap",
			logging.Stringer("BootstrapRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &multiraftv1.BootstrapResponse{}
	log.Debugw("Bootstrap",
		logging.Stringer("BootstrapRequest", request),
		logging.Stringer("BootstrapResponse", response))
	return response, nil
}

func (s *nodeServer) Join(ctx context.Context, request *multiraftv1.JoinRequest) (*multiraftv1.JoinResponse, error) {
	log.Debugw("Join",
		logging.Stringer("JoinRequest", request))
	if err := s.protocol.Join(request.Group); err != nil {
		log.Warnw("Join",
			logging.Stringer("JoinRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &multiraftv1.JoinResponse{}
	log.Debugw("Join",
		logging.Stringer("JoinRequest", request),
		logging.Stringer("JoinResponse", response))
	return response, nil
}

func (s *nodeServer) Leave(ctx context.Context, request *multiraftv1.LeaveRequest) (*multiraftv1.LeaveResponse, error) {
	log.Debugw("Leave",
		logging.Stringer("LeaveRequest", request))
	if err := s.protocol.Leave(request.GroupID); err != nil {
		log.Warnw("Leave",
			logging.Stringer("LeaveRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &multiraftv1.LeaveResponse{}
	log.Debugw("Leave",
		logging.Stringer("LeaveRequest", request),
		logging.Stringer("LeaveResponse", response))
	return response, nil
}

func (s *nodeServer) Watch(request *multiraftv1.WatchRequest, server multiraftv1.Node_WatchServer) error {
	log.Debugw("Watch",
		logging.Stringer("WatchRequest", request))
	ch := make(chan multiraftv1.Event, 100)
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

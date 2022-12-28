// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	raftv1 "github.com/atomix/atomix/stores/raft/api/v1"
)

func NewNodeServer(protocol *Protocol) raftv1.NodeServer {
	return &nodeServer{
		protocol: protocol,
	}
}

type nodeServer struct {
	protocol *Protocol
}

func (s *nodeServer) GetConfig(ctx context.Context, request *raftv1.GetConfigRequest) (*raftv1.GetConfigResponse, error) {
	log.Debugw("GetConfig",
		logging.Stringer("GetConfigRequest", request))
	config, err := s.protocol.GetConfig(request.GroupID)
	if err != nil {
		log.Warnw("GetConfig",
			logging.Stringer("GetConfigRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &raftv1.GetConfigResponse{
		Group: config,
	}
	log.Debugw("GetConfig",
		logging.Stringer("GetConfigRequest", request),
		logging.Stringer("GetConfigResponse", response))
	return response, nil
}

func (s *nodeServer) BootstrapGroup(ctx context.Context, request *raftv1.BootstrapGroupRequest) (*raftv1.BootstrapGroupResponse, error) {
	log.Debugw("BootstrapGroup",
		logging.Stringer("BootstrapGroupRequest", request))
	if request.GroupID == 0 {
		err := errors.NewInvalid("group_id is required")
		log.Warnw("BootstrapGroup",
			logging.Stringer("BootstrapGroupRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	if request.MemberID == 0 {
		err := errors.NewInvalid("member_id is required")
		log.Warnw("BootstrapGroup",
			logging.Stringer("BootstrapGroupRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	if err := s.protocol.BootstrapGroup(ctx, request.GroupID, request.MemberID, request.Config, request.Members...); err != nil {
		log.Warnw("BootstrapGroup",
			logging.Stringer("BootstrapGroupRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &raftv1.BootstrapGroupResponse{}
	log.Debugw("BootstrapGroup",
		logging.Stringer("BootstrapGroupRequest", request),
		logging.Stringer("BootstrapGroupResponse", response))
	return response, nil
}

func (s *nodeServer) AddMember(ctx context.Context, request *raftv1.AddMemberRequest) (*raftv1.AddMemberResponse, error) {
	log.Debugw("AddMember",
		logging.Stringer("AddMemberRequest", request))
	if request.GroupID == 0 {
		err := errors.NewInvalid("group_id is required")
		log.Warnw("AddMember",
			logging.Stringer("AddMemberRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	if request.Version == 0 {
		err := errors.NewInvalid("version is required")
		log.Warnw("AddMember",
			logging.Stringer("AddMemberRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	if err := s.protocol.AddMember(ctx, request.GroupID, request.Member, request.Version); err != nil {
		log.Warnw("AddMember",
			logging.Stringer("AddMemberRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &raftv1.AddMemberResponse{}
	log.Debugw("AddMember",
		logging.Stringer("AddMemberRequest", request),
		logging.Stringer("AddMemberResponse", response))
	return response, nil
}

func (s *nodeServer) RemoveMember(ctx context.Context, request *raftv1.RemoveMemberRequest) (*raftv1.RemoveMemberResponse, error) {
	log.Debugw("RemoveMember",
		logging.Stringer("RemoveMemberRequest", request))
	if request.GroupID == 0 {
		err := errors.NewInvalid("group_id is required")
		log.Warnw("RemoveMember",
			logging.Stringer("RemoveMemberRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	if request.MemberID == 0 {
		err := errors.NewInvalid("member_id is required")
		log.Warnw("RemoveMember",
			logging.Stringer("RemoveMemberRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	if request.Version == 0 {
		err := errors.NewInvalid("version is required")
		log.Warnw("RemoveMember",
			logging.Stringer("RemoveMemberRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	if err := s.protocol.RemoveMember(ctx, request.GroupID, request.MemberID, request.Version); err != nil {
		log.Warnw("RemoveMember",
			logging.Stringer("RemoveMemberRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &raftv1.RemoveMemberResponse{}
	log.Debugw("RemoveMember",
		logging.Stringer("RemoveMemberRequest", request),
		logging.Stringer("RemoveMemberResponse", response))
	return response, nil
}

func (s *nodeServer) JoinGroup(ctx context.Context, request *raftv1.JoinGroupRequest) (*raftv1.JoinGroupResponse, error) {
	log.Debugw("JoinGroup",
		logging.Stringer("JoinGroupRequest", request))
	if request.GroupID == 0 {
		err := errors.NewInvalid("group_id is required")
		log.Warnw("JoinGroup",
			logging.Stringer("JoinGroupRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	if request.MemberID == 0 {
		err := errors.NewInvalid("member_id is required")
		log.Warnw("JoinGroup",
			logging.Stringer("JoinGroupRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	if err := s.protocol.JoinGroup(ctx, request.GroupID, request.MemberID, request.Config); err != nil {
		log.Warnw("JoinGroup",
			logging.Stringer("JoinGroupRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &raftv1.JoinGroupResponse{}
	log.Debugw("JoinGroup",
		logging.Stringer("JoinGroupRequest", request),
		logging.Stringer("JoinGroupResponse", response))
	return response, nil
}

func (s *nodeServer) LeaveGroup(ctx context.Context, request *raftv1.LeaveGroupRequest) (*raftv1.LeaveGroupResponse, error) {
	log.Debugw("LeaveGroup",
		logging.Stringer("LeaveGroupRequest", request))
	if request.GroupID == 0 {
		err := errors.NewInvalid("group_id is required")
		log.Warnw("LeaveGroup",
			logging.Stringer("LeaveGroupRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	if err := s.protocol.LeaveGroup(ctx, request.GroupID); err != nil {
		log.Warnw("LeaveGroup",
			logging.Stringer("LeaveGroupRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &raftv1.LeaveGroupResponse{}
	log.Debugw("LeaveGroup",
		logging.Stringer("LeaveGroupRequest", request),
		logging.Stringer("LeaveGroupResponse", response))
	return response, nil
}

func (s *nodeServer) DeleteData(ctx context.Context, request *raftv1.DeleteDataRequest) (*raftv1.DeleteDataResponse, error) {
	log.Debugw("DeleteData",
		logging.Stringer("DeleteDataRequest", request))
	if request.GroupID == 0 {
		err := errors.NewInvalid("group_id is required")
		log.Warnw("DeleteData",
			logging.Stringer("DeleteDataRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	if request.MemberID == 0 {
		err := errors.NewInvalid("member_id is required")
		log.Warnw("DeleteData",
			logging.Stringer("DeleteDataRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	if err := s.protocol.DeleteData(ctx, request.GroupID, request.MemberID); err != nil {
		log.Warnw("DeleteData",
			logging.Stringer("DeleteDataRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &raftv1.DeleteDataResponse{}
	log.Debugw("DeleteData",
		logging.Stringer("DeleteDataRequest", request),
		logging.Stringer("DeleteDataResponse", response))
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
			return err
		}
	}
	return nil
}

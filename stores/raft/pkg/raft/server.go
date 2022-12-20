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

func (s *nodeServer) GetConfig(ctx context.Context, request *raftv1.GetConfigRequest) (*raftv1.GetConfigResponse, error) {
	log.Debugw("GetConfig",
		logging.Stringer("GetConfigRequest", request))
	config, err := s.protocol.GetConfig(request.ShardID)
	if err != nil {
		log.Warnw("GetConfig",
			logging.Stringer("GetConfigRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &raftv1.GetConfigResponse{
		Shard: config,
	}
	log.Debugw("GetConfig",
		logging.Stringer("GetConfigRequest", request),
		logging.Stringer("GetConfigResponse", response))
	return response, nil
}

func (s *nodeServer) BootstrapShard(ctx context.Context, request *raftv1.BootstrapShardRequest) (*raftv1.BootstrapShardResponse, error) {
	log.Debugw("BootstrapShard",
		logging.Stringer("BootstrapShardRequest", request))
	if request.ShardID == 0 {
		err := errors.NewInvalid("shard_id is required")
		log.Warnw("BootstrapShard",
			logging.Stringer("BootstrapShardRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if request.ReplicaID == 0 {
		err := errors.NewInvalid("member_id is required")
		log.Warnw("BootstrapShard",
			logging.Stringer("BootstrapShardRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if err := s.protocol.BootstrapShard(ctx, request.ShardID, request.ReplicaID, request.Config, request.Replicas...); err != nil {
		log.Warnw("BootstrapShard",
			logging.Stringer("BootstrapShardRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &raftv1.BootstrapShardResponse{}
	log.Debugw("BootstrapShard",
		logging.Stringer("BootstrapShardRequest", request),
		logging.Stringer("BootstrapShardResponse", response))
	return response, nil
}

func (s *nodeServer) AddReplica(ctx context.Context, request *raftv1.AddReplicaRequest) (*raftv1.AddReplicaResponse, error) {
	log.Debugw("AddReplica",
		logging.Stringer("AddReplicaRequest", request))
	if request.ShardID == 0 {
		err := errors.NewInvalid("shard_id is required")
		log.Warnw("AddReplica",
			logging.Stringer("AddReplicaRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if request.Version == 0 {
		err := errors.NewInvalid("version is required")
		log.Warnw("AddReplica",
			logging.Stringer("AddReplicaRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if err := s.protocol.AddReplica(ctx, request.ShardID, request.Replica, request.Version); err != nil {
		log.Warnw("AddReplica",
			logging.Stringer("AddReplicaRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &raftv1.AddReplicaResponse{}
	log.Debugw("AddReplica",
		logging.Stringer("AddReplicaRequest", request),
		logging.Stringer("AddReplicaResponse", response))
	return response, nil
}

func (s *nodeServer) RemoveReplica(ctx context.Context, request *raftv1.RemoveReplicaRequest) (*raftv1.RemoveReplicaResponse, error) {
	log.Debugw("RemoveReplica",
		logging.Stringer("RemoveReplicaRequest", request))
	if request.ShardID == 0 {
		err := errors.NewInvalid("shard_id is required")
		log.Warnw("RemoveReplica",
			logging.Stringer("RemoveReplicaRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if request.ReplicaID == 0 {
		err := errors.NewInvalid("member_id is required")
		log.Warnw("RemoveReplica",
			logging.Stringer("RemoveReplicaRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if request.Version == 0 {
		err := errors.NewInvalid("version is required")
		log.Warnw("RemoveReplica",
			logging.Stringer("RemoveReplicaRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if err := s.protocol.RemoveReplica(ctx, request.ShardID, request.ReplicaID, request.Version); err != nil {
		log.Warnw("RemoveReplica",
			logging.Stringer("RemoveReplicaRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &raftv1.RemoveReplicaResponse{}
	log.Debugw("RemoveReplica",
		logging.Stringer("RemoveReplicaRequest", request),
		logging.Stringer("RemoveReplicaResponse", response))
	return response, nil
}

func (s *nodeServer) JoinShard(ctx context.Context, request *raftv1.JoinShardRequest) (*raftv1.JoinShardResponse, error) {
	log.Debugw("JoinShard",
		logging.Stringer("JoinShardRequest", request))
	if request.ShardID == 0 {
		err := errors.NewInvalid("shard_id is required")
		log.Warnw("JoinShard",
			logging.Stringer("JoinShardRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if request.ReplicaID == 0 {
		err := errors.NewInvalid("member_id is required")
		log.Warnw("JoinShard",
			logging.Stringer("JoinShardRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if err := s.protocol.JoinShard(ctx, request.ShardID, request.ReplicaID, request.Config); err != nil {
		log.Warnw("JoinShard",
			logging.Stringer("JoinShardRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &raftv1.JoinShardResponse{}
	log.Debugw("JoinShard",
		logging.Stringer("JoinShardRequest", request),
		logging.Stringer("JoinShardResponse", response))
	return response, nil
}

func (s *nodeServer) LeaveShard(ctx context.Context, request *raftv1.LeaveShardRequest) (*raftv1.LeaveShardResponse, error) {
	log.Debugw("LeaveShard",
		logging.Stringer("LeaveShardRequest", request))
	if request.ShardID == 0 {
		err := errors.NewInvalid("shard_id is required")
		log.Warnw("LeaveShard",
			logging.Stringer("LeaveShardRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if err := s.protocol.LeaveShard(ctx, request.ShardID); err != nil {
		log.Warnw("LeaveShard",
			logging.Stringer("LeaveShardRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &raftv1.LeaveShardResponse{}
	log.Debugw("LeaveShard",
		logging.Stringer("LeaveShardRequest", request),
		logging.Stringer("LeaveShardResponse", response))
	return response, nil
}

func (s *nodeServer) DeleteData(ctx context.Context, request *raftv1.DeleteDataRequest) (*raftv1.DeleteDataResponse, error) {
	log.Debugw("DeleteData",
		logging.Stringer("DeleteDataRequest", request))
	if request.ShardID == 0 {
		err := errors.NewInvalid("shard_id is required")
		log.Warnw("DeleteData",
			logging.Stringer("DeleteDataRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if request.ReplicaID == 0 {
		err := errors.NewInvalid("member_id is required")
		log.Warnw("DeleteData",
			logging.Stringer("DeleteDataRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if err := s.protocol.DeleteData(ctx, request.ShardID, request.ReplicaID); err != nil {
		log.Warnw("DeleteData",
			logging.Stringer("DeleteDataRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
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
			return errors.ToProto(err)
		}
	}
	return nil
}

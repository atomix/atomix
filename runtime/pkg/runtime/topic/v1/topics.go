// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	topicv1 "github.com/atomix/atomix/api/runtime/topic/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

type TopicProvider interface {
	NewTopicV1(ctx context.Context, id runtimev1.PrimitiveID) (TopicProxy, error)
}

func resolve(ctx context.Context, conn driver.Conn, id runtimev1.PrimitiveID) (TopicProxy, bool, error) {
	if provider, ok := conn.(TopicProvider); ok {
		topic, err := provider.NewTopicV1(ctx, id)
		if err != nil {
			return nil, false, err
		}
		return topic, true, nil
	}
	return nil, false, nil
}

func NewTopicsServer(rt *runtime.Runtime) topicv1.TopicsServer {
	return &topicsServer{
		manager: runtime.NewPrimitiveManager[TopicProxy, *topicv1.Config](topicv1.PrimitiveType, resolve, rt),
	}
}

type topicsServer struct {
	manager runtime.PrimitiveManager[*topicv1.Config]
}

func (s *topicsServer) Create(ctx context.Context, request *topicv1.CreateRequest) (*topicv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	config, err := s.manager.Create(ctx, request.ID, request.Tags)
	if err != nil {
		log.Warnw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &topicv1.CreateResponse{
		Config: *config,
	}
	log.Debugw("Create",
		logging.Trunc64("CreateResponse", response))
	return response, nil
}

func (s *topicsServer) Close(ctx context.Context, request *topicv1.CloseRequest) (*topicv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	err := s.manager.Close(ctx, request.ID)
	if err != nil {
		log.Warnw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &topicv1.CloseResponse{}
	log.Debugw("Close",
		logging.Trunc64("CloseResponse", response))
	return response, nil
}

var _ topicv1.TopicsServer = (*topicsServer)(nil)

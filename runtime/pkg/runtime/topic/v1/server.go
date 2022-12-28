// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	topicv1 "github.com/atomix/atomix/api/runtime/topic/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/runtime"
)

var log = logging.GetLogger()

const truncLen = 250

func newTopicServer(manager *runtime.PrimitiveManager[Topic]) topicv1.TopicServer {
	return &topicServer{
		manager: manager,
	}
}

type topicServer struct {
	manager *runtime.PrimitiveManager[Topic]
}

func (s *topicServer) Create(ctx context.Context, request *topicv1.CreateRequest) (*topicv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	client, err := s.manager.Create(ctx, request.ID, request.Tags)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Create(ctx, request)
	if err != nil {
		log.Debugw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Create",
		logging.Trunc64("CreateResponse", response))
	return response, nil
}

func (s *topicServer) Close(ctx context.Context, request *topicv1.CloseRequest) (*topicv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Close(ctx, request)
	if err != nil {
		log.Debugw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Close",
		logging.Trunc64("CloseResponse", response))
	return response, nil
}

func (s *topicServer) Publish(ctx context.Context, request *topicv1.PublishRequest) (*topicv1.PublishResponse, error) {
	log.Debugw("Publish",
		logging.Trunc64("PublishRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Publish",
			logging.Trunc64("PublishRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Publish(ctx, request)
	if err != nil {
		log.Debugw("Publish",
			logging.Trunc64("PublishRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Publish",
		logging.Trunc64("PublishResponse", response))
	return response, nil
}

func (s *topicServer) Subscribe(request *topicv1.SubscribeRequest, server topicv1.Topic_SubscribeServer) error {
	log.Debugw("Subscribe",
		logging.Trunc64("SubscribeRequest", request),
		logging.String("State", "started"))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Subscribe",
			logging.Trunc64("SubscribeRequest", request),
			logging.Error("Error", err))
		return err
	}
	err = client.Subscribe(request, server)
	if err != nil {
		log.Debugw("Subscribe",
			logging.Trunc64("SubscribeRequest", request),
			logging.Error("Error", err))
		return err
	}
	return nil
}

var _ topicv1.TopicServer = (*topicServer)(nil)

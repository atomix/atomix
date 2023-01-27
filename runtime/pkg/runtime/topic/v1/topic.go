// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	topicv1 "github.com/atomix/atomix/api/runtime/topic/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

var log = logging.GetLogger()

type TopicProxy interface {
	runtime.PrimitiveProxy
	topicv1.TopicServer
}

func NewTopicServer(rt *runtime.Runtime) topicv1.TopicServer {
	return &topicServer{
		manager: runtime.NewPrimitiveRegistry[TopicProxy](topicv1.PrimitiveType, rt),
	}
}

type topicServer struct {
	manager runtime.PrimitiveRegistry[TopicProxy]
}

func (s *topicServer) Publish(ctx context.Context, request *topicv1.PublishRequest) (*topicv1.PublishResponse, error) {
	log.Debugw("Publish",
		logging.Trunc64("PublishRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
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

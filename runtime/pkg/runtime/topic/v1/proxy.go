// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	topicv1 "github.com/atomix/atomix/api/pkg/runtime/topic/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	"github.com/atomix/atomix/runtime/pkg/utils/stringer"
)

var log = logging.GetLogger()

const truncLen = 250

func newTopicServer(client *runtime.PrimitiveClient[Topic]) topicv1.TopicServer {
	return &topicServer{
		client: client,
	}
}

type topicServer struct {
	client *runtime.PrimitiveClient[Topic]
}

func (s *topicServer) Create(ctx context.Context, request *topicv1.CreateRequest) (*topicv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)))
	client, err := s.client.Create(ctx, request.ID, request.Tags)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Create(ctx, request)
	if err != nil {
		log.Debugw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Create",
		logging.Stringer("CreateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *topicServer) Close(ctx context.Context, request *topicv1.CloseRequest) (*topicv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Close(ctx, request)
	if err != nil {
		log.Debugw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Close",
		logging.Stringer("CloseResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *topicServer) Publish(ctx context.Context, request *topicv1.PublishRequest) (*topicv1.PublishResponse, error) {
	log.Debugw("Publish",
		logging.Stringer("PublishRequest", stringer.Truncate(request, truncLen)))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Publish",
			logging.Stringer("PublishRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Publish(ctx, request)
	if err != nil {
		log.Debugw("Publish",
			logging.Stringer("PublishRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Publish",
		logging.Stringer("PublishResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *topicServer) Subscribe(request *topicv1.SubscribeRequest, server topicv1.Topic_SubscribeServer) error {
	log.Debugw("Subscribe",
		logging.Stringer("SubscribeRequest", stringer.Truncate(request, truncLen)),
		logging.String("State", "started"))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Subscribe",
			logging.Stringer("SubscribeRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return err
	}
	err = client.Subscribe(request, server)
	if err != nil {
		log.Debugw("Subscribe",
			logging.Stringer("SubscribeRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return err
	}
	return nil
}

var _ topicv1.TopicServer = (*topicServer)(nil)

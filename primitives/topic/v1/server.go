// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	topicv1 "github.com/atomix/runtime/api/atomix/topic/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/runtime/pkg/runtime"
	"io"
)

var log = logging.GetLogger()

func newTopicServer(delegate *runtime.Delegate[topicv1.TopicClient]) topicv1.TopicServer {
	return &topicServer{
		delegate: delegate,
	}
}

type topicServer struct {
	delegate *runtime.Delegate[topicv1.TopicClient]
}

func (s *topicServer) Create(ctx context.Context, request *topicv1.CreateRequest) (*topicv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", request))
	client, err := s.delegate.Create(request.ID.Name, request.Tags)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Create(ctx, request)
	if err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Create",
		logging.Stringer("CreateResponse", response))
	return response, nil
}

func (s *topicServer) Close(ctx context.Context, request *topicv1.CloseRequest) (*topicv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", request))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Close",
			logging.Stringer("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Close(ctx, request)
	if err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Close",
		logging.Stringer("CloseResponse", response))
	return response, nil
}

func (s *topicServer) Publish(ctx context.Context, request *topicv1.PublishRequest) (*topicv1.PublishResponse, error) {
	log.Debugw("Publish",
		logging.Stringer("PublishRequest", request))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Publish",
			logging.Stringer("PublishRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Publish(ctx, request)
	if err != nil {
		log.Warnw("Publish",
			logging.Stringer("PublishRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Publish",
		logging.Stringer("PublishResponse", response))
	return response, nil
}

func (s *topicServer) Subscribe(request *topicv1.SubscribeRequest, server topicv1.Topic_SubscribeServer) error {
	log.Debugw("Subscribe",
		logging.Stringer("SubscribeRequest", request),
		logging.String("State", "started"))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Subscribe",
			logging.Stringer("SubscribeRequest", request),
			logging.Error("Error", err))
		return err
	}
	stream, err := client.Subscribe(server.Context(), request)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Subscribe",
			logging.Stringer("SubscribeRequest", request),
			logging.Error("Error", err))
		return err
	}
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			log.Debugw("Subscribe",
				logging.Stringer("SubscribeRequest", request),
				logging.String("State", "complete"))
			return nil
		}
		if err != nil {
			err = errors.ToProto(err)
			log.Warnw("Subscribe",
				logging.Stringer("SubscribeRequest", request),
				logging.Error("Error", err))
			return err
		}
		log.Warnw("Subscribe",
			logging.Stringer("SubscribeResponse", response))
		err = server.Send(response)
		if err != nil {
			err = errors.ToProto(err)
			log.Warnw("Subscribe",
				logging.Stringer("SubscribeRequest", request),
				logging.Error("Error", err))
			return err
		}
	}
}

var _ topicv1.TopicServer = (*topicServer)(nil)

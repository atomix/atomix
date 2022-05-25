// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/topic/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/primitive"
)

func newTopicServer(sessions *primitive.SessionManager[Topic]) v1.TopicServer {
	return &topicServer{
		sessions: sessions,
	}
}

type topicServer struct {
	sessions *primitive.SessionManager[Topic]
}

func (s *topicServer) Publish(ctx context.Context, request *v1.PublishRequest) (*v1.PublishResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Publish(ctx, request)
}

func (s *topicServer) Subscribe(request *v1.SubscribeRequest, server v1.Topic_SubscribeServer) error {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return errors.ToProto(err)
	}
	return session.Subscribe(request, server)
}

var _ v1.TopicServer = (*topicServer)(nil)

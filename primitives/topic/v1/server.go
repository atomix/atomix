// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/topic/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/primitive"
)

func newTopicServer(proxies *primitive.ProxyManager[Topic]) v1.TopicServer {
	return &topicServer{
		proxies: proxies,
	}
}

type topicServer struct {
	proxies *primitive.ProxyManager[Topic]
}

func (s *topicServer) Publish(ctx context.Context, request *v1.PublishRequest) (*v1.PublishResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Publish(ctx, request)
}

func (s *topicServer) Subscribe(request *v1.SubscribeRequest, server v1.Topic_SubscribeServer) error {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return errors.ToProto(err)
	}
	return proxy.Subscribe(request, server)
}

var _ v1.TopicServer = (*topicServer)(nil)

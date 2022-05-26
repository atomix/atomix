// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	topicv1 "github.com/atomix/runtime/api/atomix/topic/v1"
	atomixv1 "github.com/atomix/runtime/api/atomix/v1"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.topic.v1.Topic"

var Kind = primitive.NewKind[TopicClient, Topic, *topicv1.TopicConfig](serviceName, register, create)

func register(server *grpc.Server, proxies *primitive.ProxyManager[Topic]) {
	topicv1.RegisterTopicServer(server, newTopicServer(proxies))
}

func create(client TopicClient) func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *topicv1.TopicConfig) (Topic, error) {
	return func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *topicv1.TopicConfig) (Topic, error) {
		return client.GetTopic(ctx, primitiveID)
	}
}

type TopicClient interface {
	GetTopic(ctx context.Context, primitiveID atomixv1.PrimitiveId) (Topic, error)
}

type Topic interface {
	primitive.Proxy
	topicv1.TopicServer
}

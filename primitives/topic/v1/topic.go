// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/api/atomix/topic/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var Primitive = primitive.New[Topic](clientFactory, func(server *grpc.Server, service *primitive.Service[Topic], registry *primitive.Registry[Topic]) {
	v1.RegisterTopicManagerServer(server, newTopicV1ManagerServer(service))
	v1.RegisterTopicServer(server, newTopicV1Server(registry))
})

// clientFactory is the topic/v1 client factory
var clientFactory = func(client driver.Client) (*primitive.Client[Topic], bool) {
	if topicClient, ok := client.(TopicClient); ok {
		return primitive.NewClient[Topic](topicClient.GetTopic), true
	}
	return nil, false
}

type TopicClient interface {
	GetTopic(ctx context.Context, primitiveID runtimev1.PrimitiveId) (Topic, error)
}

type Topic interface {
	primitive.Primitive
	v1.TopicServer
}

// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	topicv1 "github.com/atomix/runtime/api/atomix/topic/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

const serviceName = "atomix.topic.v1.Topic"

var Primitive = primitive.NewType[Topic](serviceName, resolve, register)

func resolve(client driver.Client) (*primitive.Client[Topic], bool) {
	if topicClient, ok := client.(TopicClient); ok {
		return primitive.NewClient[Topic](topicClient.GetTopic), true
	}
	return nil, false
}

func register(server *grpc.Server, service *primitive.Service[Topic], registry *primitive.Registry[Topic]) {
	topicv1.RegisterTopicServer(server, newTopicV1Server(registry))
}

type TopicClient interface {
	GetTopic(ctx context.Context, primitiveID runtimev1.ObjectId) (Topic, error)
}

type Topic interface {
	primitive.Primitive
	topicv1.TopicServer
}

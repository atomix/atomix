// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	topicv1 "github.com/atomix/runtime/api/atomix/topic/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.topic.v1.Topic"

var Primitive = primitive.NewType[TopicClient, Topic, *topicv1.TopicConfig](serviceName, register, create)

func register(server *grpc.Server, sessions *primitive.SessionManager[Topic]) {
	topicv1.RegisterTopicServer(server, newTopicServer(sessions))
}

func create(client TopicClient) func(ctx context.Context, sessionID primitivev1.SessionId, config *topicv1.TopicConfig) (Topic, error) {
	return func(ctx context.Context, sessionID primitivev1.SessionId, config *topicv1.TopicConfig) (Topic, error) {
		return client.GetTopic(ctx, sessionID)
	}
}

type TopicClient interface {
	GetTopic(ctx context.Context, sessionID primitivev1.SessionId) (Topic, error)
}

type Topic interface {
	primitive.Primitive
	topicv1.TopicServer
}

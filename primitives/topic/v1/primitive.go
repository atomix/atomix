// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	topicv1 "github.com/atomix/runtime/api/atomix/topic/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"github.com/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const (
	Name       = "Topic"
	APIVersion = "v1"
)

var Type = primitive.NewType[topicv1.TopicClient](Name, APIVersion, register, resolve)

func register(server *grpc.Server, manager *primitive.Manager[topicv1.TopicClient]) {
	topicv1.RegisterTopicServer(server, newTopicServer(manager))
}

func resolve(client runtime.Client) (primitive.Factory[topicv1.TopicClient], bool) {
	if topic, ok := client.(TopicProvider); ok {
		return topic.GetTopic, true
	}
	return nil, false
}

type TopicProvider interface {
	GetTopic(string) topicv1.TopicClient
}

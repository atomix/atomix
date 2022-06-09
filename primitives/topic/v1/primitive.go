// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	topicv1 "github.com/atomix/runtime/api/atomix/topic/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.topic.v1.Topic"

var Kind = primitive.NewKind[topicv1.TopicClient](serviceName, register, resolve)

func register(server *grpc.Server, proxies *primitive.Manager[topicv1.TopicClient]) {
	topicv1.RegisterTopicServer(server, newTopicServer(proxies))
}

func resolve(client driver.Client) (primitive.Factory[topicv1.TopicClient], bool) {
	if topic, ok := client.(TopicProvider); ok {
		return topic.GetTopic, true
	}
	return nil, false
}

type TopicProvider interface {
	GetTopic(primitive.ID) topicv1.TopicClient
}

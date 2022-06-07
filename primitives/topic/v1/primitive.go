// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	topicv1 "github.com/atomix/runtime/api/atomix/topic/v1"
	"github.com/atomix/runtime/pkg/atomix/driver"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

var Kind = primitive.NewKind[topicv1.TopicServer](register, resolve)

func register(server *grpc.Server, proxies *primitive.Manager[topicv1.TopicServer]) {
	topicv1.RegisterTopicServer(server, newTopicServer(proxies))
}

func resolve(client driver.Client) (primitive.Factory[topicv1.TopicServer], bool) {
	if counter, ok := client.(TopicProvider); ok {
		return counter.GetTopic, true
	}
	return nil, false
}

type TopicProvider interface {
	GetTopic(primitive.ID) topicv1.TopicServer
}

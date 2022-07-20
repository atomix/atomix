// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	topicv1 "github.com/atomix/runtime/api/atomix/runtime/topic/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "Topic"
	APIVersion = "v1"
)

var Type = runtime.NewType[topicv1.TopicServer](Name, APIVersion, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[topicv1.TopicServer]) {
	topicv1.RegisterTopicServer(server, newTopicServer(delegate))
}

func resolve(client runtime.Client) (topicv1.TopicServer, bool) {
	if provider, ok := client.(TopicProvider); ok {
		return provider.Topic(), true
	}
	return nil, false
}

type TopicProvider interface {
	Topic() topicv1.TopicServer
}

// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	topicv1 "github.com/atomix/atomix/api/pkg/runtime/topic/v1"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "Topic"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterServer(server *grpc.Server, rt runtime.Runtime) {
	topicv1.RegisterTopicServer(server, newTopicServer(runtime.NewPrimitiveClient[Topic](PrimitiveType, rt, resolve)))
}

func resolve(conn runtime.Conn) (runtime.PrimitiveProvider[Topic], bool) {
	if provider, ok := conn.(TopicProvider); ok {
		return provider.NewTopic, true
	}
	return nil, false
}

type Topic topicv1.TopicServer

type TopicProvider interface {
	NewTopic(config []byte) (Topic, error)
}

// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	topicv1 "github.com/atomix/atomix/api/pkg/topic/v1"
	"github.com/atomix/atomix/driver/pkg/driver"
	topicdriverv1 "github.com/atomix/atomix/driver/pkg/driver/topic/v1"
	"github.com/atomix/atomix/proxy/pkg/proxy"
	"google.golang.org/grpc"
)

const Service = "atomix.counter.v1.Topic"

var Type = proxy.NewType[topicv1.TopicServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[topicv1.TopicServer]) {
	topicv1.RegisterTopicServer(server, newTopicServer(delegate))
}

func resolve(conn driver.Conn, spec proxy.PrimitiveSpec) (topicv1.TopicServer, bool, error) {
	if provider, ok := conn.(topicdriverv1.TopicProvider); ok {
		topic, err := provider.NewTopic(spec)
		return topic, true, err
	}
	return nil, false, nil
}

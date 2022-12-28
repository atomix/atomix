// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	topicv1 "github.com/atomix/atomix/api/runtime/topic/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
)

const (
	Name       = "Topic"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

type Topic topicv1.TopicServer

type TopicProvider interface {
	NewTopicV1(primitiveID runtimev1.PrimitiveID) (Topic, error)
}

type ConfigurableTopicProvider[S any] interface {
	NewTopicV1(primitiveID runtimev1.PrimitiveID, spec S) (Topic, error)
}

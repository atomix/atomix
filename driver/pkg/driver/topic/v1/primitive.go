// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/driver/pkg/driver"
	topicv1 "github.com/atomix/atomix/runtime/api/atomix/runtime/topic/v1"
)

type TopicProxy topicv1.TopicServer

type TopicProvider interface {
	NewTopic(spec driver.PrimitiveSpec) (TopicProxy, error)
}

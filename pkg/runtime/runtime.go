// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/version"
)

var log = logging.GetLogger()

type Version string

type Runtime interface {
	Version() Version
	Clusters() Store[*runtimev1.Cluster]
	Primitives() Store[*runtimev1.Primitive]
	Bindings() Store[*runtimev1.Binding]
	Drivers() *DriverCache
}

func New(opts ...Option) Runtime {
	var options Options
	options.apply(opts...)
	return &atomixRuntime{
		clusters:   NewStore[*runtimev1.Cluster](),
		primitives: NewStore[*runtimev1.Primitive](),
		bindings:   NewStore[*runtimev1.Binding](),
		drivers:    NewDriverCache(options.CacheDir),
	}
}

type atomixRuntime struct {
	clusters   Store[*runtimev1.Cluster]
	primitives Store[*runtimev1.Primitive]
	bindings   Store[*runtimev1.Binding]
	drivers    *DriverCache
}

func (r *atomixRuntime) Version() Version {
	return Version(version.Version())
}

func (r *atomixRuntime) Clusters() Store[*runtimev1.Cluster] {
	return r.clusters
}

func (r *atomixRuntime) Primitives() Store[*runtimev1.Primitive] {
	return r.primitives
}

func (r *atomixRuntime) Bindings() Store[*runtimev1.Binding] {
	return r.bindings
}

func (r *atomixRuntime) Drivers() *DriverCache {
	return r.drivers
}

var _ Runtime = (*atomixRuntime)(nil)

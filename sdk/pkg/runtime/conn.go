// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"encoding/json"
	atomiccounterv1 "github.com/atomix/runtime/api/atomix/runtime/atomic/counter/v1"
	atomiccountermapv1 "github.com/atomix/runtime/api/atomix/runtime/atomic/countermap/v1"
	atomicindexedmapv1 "github.com/atomix/runtime/api/atomix/runtime/atomic/indexedmap/v1"
	atomiclockv1 "github.com/atomix/runtime/api/atomix/runtime/atomic/lock/v1"
	atomicmapv1 "github.com/atomix/runtime/api/atomix/runtime/atomic/map/v1"
	atomicvaluev1 "github.com/atomix/runtime/api/atomix/runtime/atomic/value/v1"
	counterv1 "github.com/atomix/runtime/api/atomix/runtime/counter/v1"
	electionv1 "github.com/atomix/runtime/api/atomix/runtime/election/v1"
	listv1 "github.com/atomix/runtime/api/atomix/runtime/list/v1"
	mapv1 "github.com/atomix/runtime/api/atomix/runtime/map/v1"
	multimapv1 "github.com/atomix/runtime/api/atomix/runtime/multimap/v1"
	setv1 "github.com/atomix/runtime/api/atomix/runtime/set/v1"
	topicv1 "github.com/atomix/runtime/api/atomix/runtime/topic/v1"
	valuev1 "github.com/atomix/runtime/api/atomix/runtime/value/v1"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/gogo/protobuf/types"
)

type Conn interface {
	Context() context.Context
	Configurator[[]byte]
	AtomicCounter(config []byte) (atomiccounterv1.AtomicCounterServer, error)
	AtomicCounterMap(config []byte) (atomiccountermapv1.AtomicCounterMapServer, error)
	AtomicIndexedMap(config []byte) (atomicindexedmapv1.AtomicIndexedMapServer, error)
	AtomicMap(config []byte) (atomicmapv1.AtomicMapServer, error)
	AtomicValue(config []byte) (atomicvaluev1.AtomicValueServer, error)
	Counter(config []byte) (counterv1.CounterServer, error)
	LeaderElection(config []byte) (electionv1.LeaderElectionServer, error)
	List(config []byte) (listv1.ListServer, error)
	Lock(config []byte) (atomiclockv1.LockServer, error)
	Map(config []byte) (mapv1.MapServer, error)
	MultiMap(config []byte) (multimapv1.MultiMapServer, error)
	Set(config []byte) (setv1.SetServer, error)
	Topic(config []byte) (topicv1.TopicServer, error)
	Value(config []byte) (valuev1.ValueServer, error)
	Closer
}

type Connector[C any] func(ctx context.Context, config C) (Conn, error)

type ConnOptions struct {
	AtomicCounterFactory    func([]byte) (atomiccounterv1.AtomicCounterServer, error)
	AtomicCounterMapFactory func([]byte) (atomiccountermapv1.AtomicCounterMapServer, error)
	AtomicIndexedMapFactory func([]byte) (atomicindexedmapv1.AtomicIndexedMapServer, error)
	AtomicMapFactory        func([]byte) (atomicmapv1.AtomicMapServer, error)
	AtomicValueFactory      func([]byte) (atomicvaluev1.AtomicValueServer, error)
	CounterFactory          func([]byte) (counterv1.CounterServer, error)
	LeaderElectionFactory   func([]byte) (electionv1.LeaderElectionServer, error)
	ListFactory             func([]byte) (listv1.ListServer, error)
	LockFactory             func([]byte) (atomiclockv1.LockServer, error)
	MapFactory              func([]byte) (mapv1.MapServer, error)
	MultiMapFactory         func([]byte) (multimapv1.MultiMapServer, error)
	SetFactory              func([]byte) (setv1.SetServer, error)
	TopicFactory            func([]byte) (topicv1.TopicServer, error)
	ValueFactory            func([]byte) (valuev1.ValueServer, error)
}

func (o *ConnOptions) apply(opts ...ConnOption) {
	for _, opt := range opts {
		opt(o)
	}
}

type ConnOption func(*ConnOptions)

func WithAtomicCounterFactory[C any](f func(C) (atomiccounterv1.AtomicCounterServer, error)) ConnOption {
	return func(options *ConnOptions) {
		options.AtomicCounterFactory = func(data []byte) (atomiccounterv1.AtomicCounterServer, error) {
			var config C
			if data != nil {
				if err := json.Unmarshal(data, &config); err != nil {
					return nil, err
				}
			}
			return f(config)
		}
	}
}

func WithAtomicCounterMapFactory[C any](f func(C) (atomiccountermapv1.AtomicCounterMapServer, error)) ConnOption {
	return func(options *ConnOptions) {
		options.AtomicCounterMapFactory = func(data []byte) (atomiccountermapv1.AtomicCounterMapServer, error) {
			var config C
			if data != nil {
				if err := json.Unmarshal(data, &config); err != nil {
					return nil, err
				}
			}
			return f(config)
		}
	}
}

func WithAtomicIndexedMapFactory[C any](f func(C) (atomicindexedmapv1.AtomicIndexedMapServer, error)) ConnOption {
	return func(options *ConnOptions) {
		options.AtomicIndexedMapFactory = func(data []byte) (atomicindexedmapv1.AtomicIndexedMapServer, error) {
			var config C
			if data != nil {
				if err := json.Unmarshal(data, &config); err != nil {
					return nil, err
				}
			}
			return f(config)
		}
	}
}

func WithAtomicMapFactory[C any](f func(C) (atomicmapv1.AtomicMapServer, error)) ConnOption {
	return func(options *ConnOptions) {
		options.AtomicMapFactory = func(data []byte) (atomicmapv1.AtomicMapServer, error) {
			var config C
			if data != nil {
				if err := json.Unmarshal(data, &config); err != nil {
					return nil, err
				}
			}
			return f(config)
		}
	}
}

func WithAtomicValueFactory[C any](f func(C) (atomicvaluev1.AtomicValueServer, error)) ConnOption {
	return func(options *ConnOptions) {
		options.AtomicValueFactory = func(data []byte) (atomicvaluev1.AtomicValueServer, error) {
			var config C
			if data != nil {
				if err := json.Unmarshal(data, &config); err != nil {
					return nil, err
				}
			}
			return f(config)
		}
	}
}

func WithCounterFactory[C any](f func(C) (counterv1.CounterServer, error)) ConnOption {
	return func(options *ConnOptions) {
		options.CounterFactory = func(data []byte) (counterv1.CounterServer, error) {
			var config C
			if data != nil {
				if err := json.Unmarshal(data, &config); err != nil {
					return nil, err
				}
			}
			return f(config)
		}
	}
}

func WithLeaderElectionFactory[C any](f func(C) (electionv1.LeaderElectionServer, error)) ConnOption {
	return func(options *ConnOptions) {
		options.LeaderElectionFactory = func(data []byte) (electionv1.LeaderElectionServer, error) {
			var config C
			if data != nil {
				if err := json.Unmarshal(data, &config); err != nil {
					return nil, err
				}
			}
			return f(config)
		}
	}
}

func WithListFactory[C any](f func(C) (listv1.ListServer, error)) ConnOption {
	return func(options *ConnOptions) {
		options.ListFactory = func(data []byte) (listv1.ListServer, error) {
			var config C
			if data != nil {
				if err := json.Unmarshal(data, &config); err != nil {
					return nil, err
				}
			}
			return f(config)
		}
	}
}

func WithLockFactory[C any](f func(C) (atomiclockv1.LockServer, error)) ConnOption {
	return func(options *ConnOptions) {
		options.LockFactory = func(data []byte) (atomiclockv1.LockServer, error) {
			var config C
			if data != nil {
				if err := json.Unmarshal(data, &config); err != nil {
					return nil, err
				}
			}
			return f(config)
		}
	}
}

func WithMapFactory[C any](f func(C) (mapv1.MapServer, error)) ConnOption {
	return func(options *ConnOptions) {
		options.MapFactory = func(data []byte) (mapv1.MapServer, error) {
			var config C
			if data != nil {
				if err := json.Unmarshal(data, &config); err != nil {
					return nil, err
				}
			}
			return f(config)
		}
	}
}

func WithMultiMapFactory[C any](f func(C) (multimapv1.MultiMapServer, error)) ConnOption {
	return func(options *ConnOptions) {
		options.MultiMapFactory = func(data []byte) (multimapv1.MultiMapServer, error) {
			var config C
			if data != nil {
				if err := json.Unmarshal(data, &config); err != nil {
					return nil, err
				}
			}
			return f(config)
		}
	}
}

func WithSetFactory[C any](f func(C) (setv1.SetServer, error)) ConnOption {
	return func(options *ConnOptions) {
		options.SetFactory = func(data []byte) (setv1.SetServer, error) {
			var config C
			if data != nil {
				if err := json.Unmarshal(data, &config); err != nil {
					return nil, err
				}
			}
			return f(config)
		}
	}
}

func WithTopicFactory[C any](f func(C) (topicv1.TopicServer, error)) ConnOption {
	return func(options *ConnOptions) {
		options.TopicFactory = func(data []byte) (topicv1.TopicServer, error) {
			var config C
			if data != nil {
				if err := json.Unmarshal(data, &config); err != nil {
					return nil, err
				}
			}
			return f(config)
		}
	}
}

func WithValueFactory[C any](f func(C) (valuev1.ValueServer, error)) ConnOption {
	return func(options *ConnOptions) {
		options.ValueFactory = func(data []byte) (valuev1.ValueServer, error) {
			var config C
			if data != nil {
				if err := json.Unmarshal(data, &config); err != nil {
					return nil, err
				}
			}
			return f(config)
		}
	}
}

func NewConn[C any](client Client, opts ...ConnOption) Conn {
	var options ConnOptions
	options.apply(opts...)
	ctx, cancel := context.WithCancel(context.Background())
	return &configurableConn[C]{
		options: options,
		client:  client,
		ctx:     ctx,
		cancel:  cancel,
	}
}

type configurableConn[C any] struct {
	options ConnOptions
	client  Client
	ctx     context.Context
	cancel  context.CancelFunc
}

func (c *configurableConn[C]) Context() context.Context {
	return c.ctx
}

func (c *configurableConn[C]) AtomicCounter(config []byte) (atomiccounterv1.AtomicCounterServer, error) {
	if c.options.AtomicCounterFactory == nil {
		return nil, errors.NewNotSupported("primitive type not supported by driver")
	}
	return c.options.AtomicCounterFactory(config)
}

func (c *configurableConn[C]) AtomicCounterMap(config []byte) (atomiccountermapv1.AtomicCounterMapServer, error) {
	if c.options.AtomicCounterMapFactory == nil {
		return nil, errors.NewNotSupported("primitive type not supported by driver")
	}
	return c.options.AtomicCounterMapFactory(config)
}

func (c *configurableConn[C]) AtomicIndexedMap(config []byte) (atomicindexedmapv1.AtomicIndexedMapServer, error) {
	if c.options.AtomicIndexedMapFactory == nil {
		return nil, errors.NewNotSupported("primitive type not supported by driver")
	}
	return c.options.AtomicIndexedMapFactory(config)
}

func (c *configurableConn[C]) AtomicMap(config []byte) (atomicmapv1.AtomicMapServer, error) {
	if c.options.AtomicMapFactory == nil {
		return nil, errors.NewNotSupported("primitive type not supported by driver")
	}
	return c.options.AtomicMapFactory(config)
}

func (c *configurableConn[C]) AtomicValue(config []byte) (atomicvaluev1.AtomicValueServer, error) {
	if c.options.AtomicValueFactory == nil {
		return nil, errors.NewNotSupported("primitive type not supported by driver")
	}
	return c.options.AtomicValueFactory(config)
}

func (c *configurableConn[C]) Counter(config []byte) (counterv1.CounterServer, error) {
	if c.options.CounterFactory == nil {
		return nil, errors.NewNotSupported("primitive type not supported by driver")
	}
	return c.options.CounterFactory(config)
}

func (c *configurableConn[C]) LeaderElection(config []byte) (electionv1.LeaderElectionServer, error) {
	if c.options.LeaderElectionFactory == nil {
		return nil, errors.NewNotSupported("primitive type not supported by driver")
	}
	return c.options.LeaderElectionFactory(config)
}

func (c *configurableConn[C]) List(config []byte) (listv1.ListServer, error) {
	if c.options.ListFactory == nil {
		return nil, errors.NewNotSupported("primitive type not supported by driver")
	}
	return c.options.ListFactory(config)
}

func (c *configurableConn[C]) Lock(config []byte) (atomiclockv1.LockServer, error) {
	if c.options.LockFactory == nil {
		return nil, errors.NewNotSupported("primitive type not supported by driver")
	}
	return c.options.LockFactory(config)
}

func (c *configurableConn[C]) Map(config []byte) (mapv1.MapServer, error) {
	if c.options.MapFactory == nil {
		return nil, errors.NewNotSupported("primitive type not supported by driver")
	}
	return c.options.MapFactory(config)
}

func (c *configurableConn[C]) MultiMap(config []byte) (multimapv1.MultiMapServer, error) {
	if c.options.MultiMapFactory == nil {
		return nil, errors.NewNotSupported("primitive type not supported by driver")
	}
	return c.options.MultiMapFactory(config)
}

func (c *configurableConn[C]) Set(config []byte) (setv1.SetServer, error) {
	if c.options.SetFactory == nil {
		return nil, errors.NewNotSupported("primitive type not supported by driver")
	}
	return c.options.SetFactory(config)
}

func (c *configurableConn[C]) Topic(config []byte) (topicv1.TopicServer, error) {
	if c.options.TopicFactory == nil {
		return nil, errors.NewNotSupported("primitive type not supported by driver")
	}
	return c.options.TopicFactory(config)
}

func (c *configurableConn[C]) Value(config []byte) (valuev1.ValueServer, error) {
	if c.options.ValueFactory == nil {
		return nil, errors.NewNotSupported("primitive type not supported by driver")
	}
	return c.options.ValueFactory(config)
}

func (c *configurableConn[C]) Configure(ctx context.Context, data []byte) error {
	if configurator, ok := c.client.(Configurator[C]); ok {
		var config C
		if data != nil {
			if err := json.Unmarshal(data, &config); err != nil {
				return err
			}
		}
		return configurator.Configure(ctx, config)
	}
	return nil
}

func (c *configurableConn[C]) Close(ctx context.Context) error {
	defer c.cancel()
	return c.client.Close(ctx)
}

var _ Conn = (*configurableConn[*types.Any])(nil)

// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package atomix

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	atomixv1 "github.com/atomix/runtime/api/atomix/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"github.com/atomix/runtime/pkg/atomix/runtime"
	"sync"
)

func newServer(runtime runtime.Runtime, registry *primitive.Registry, kinds ...primitive.Kind) atomixv1.AtomixServer {
	primitiveKinds := make(map[string]primitive.Kind)
	for _, kind := range kinds {
		primitiveKinds[kind.Name()] = kind
	}
	return &atomixServer{
		runtime:  runtime,
		registry: registry,
		kinds:    primitiveKinds,
	}
}

type atomixServer struct {
	runtime  runtime.Runtime
	registry *primitive.Registry
	kinds    map[string]primitive.Kind
	mu       sync.Mutex
}

func (s *atomixServer) CreatePrimitive(ctx context.Context, request *atomixv1.CreatePrimitiveRequest) (*atomixv1.CreatePrimitiveResponse, error) {
	log.Debugw("CreatePrimitive",
		logging.Stringer("CreatePrimitiveRequest", request))

	s.mu.Lock()
	defer s.mu.Unlock()

	session, ok := s.registry.Get(request.PrimitiveId)
	if ok {
		response := &atomixv1.CreatePrimitiveResponse{}
		log.Debugw("CreatePrimitive",
			logging.Stringer("CreatePrimitiveResponse", response))
		return response, nil
	}

	kind, ok := s.kinds[request.Kind]
	if !ok {
		err := errors.NewForbidden("unknown primitive kind '%s'", request.Kind)
		log.Warnw("CreatePrimitive",
			logging.Stringer("CreatePrimitiveRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	proxy := &runtimev1.Proxy{
		ProxyMeta: runtimev1.ProxyMeta{
			ID: runtimev1.ProxyId{
				ID: request.Session.ID.String(),
			},
			Labels: request.Session.Labels,
		},
	}

	conn, err := s.runtime.Connect(ctx, proxy)
	if err != nil {
		return nil, errors.ToProto(err)
	}

	session, err = kind.GetProxy(conn.Client())(ctx, request.PrimitiveId, request.Session, request.Config)
	if err != nil {
		return nil, errors.ToProto(err)
	}

	s.registry.Register(request.PrimitiveId, request.Session, session)

	response := &atomixv1.CreatePrimitiveResponse{}
	log.Debugw("CreatePrimitive",
		logging.Stringer("CreatePrimitiveResponse", response))
	return response, nil
}

func (s *atomixServer) ClosePrimitive(ctx context.Context, request *atomixv1.ClosePrimitiveRequest) (*atomixv1.ClosePrimitiveResponse, error) {
	log.Debugw("ClosePrimitive",
		logging.Stringer("ClosePrimitiveRequest", request))

	s.mu.Lock()
	defer s.mu.Unlock()

	session, ok := s.registry.Unregister(request.PrimitiveId)
	if !ok {
		response := &atomixv1.ClosePrimitiveResponse{}
		log.Debugw("ClosePrimitive",
			logging.Stringer("ClosePrimitiveResponse", response))
		return response, nil
	}

	if err := session.Close(ctx); err != nil {
		log.Warnw("ClosePrimitive",
			logging.Stringer("ClosePrimitiveRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &atomixv1.ClosePrimitiveResponse{}
	log.Debugw("ClosePrimitive",
		logging.Stringer("ClosePrimitiveResponse", response))
	return response, nil
}

var _ atomixv1.AtomixServer = (*atomixServer)(nil)

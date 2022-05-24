// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"github.com/atomix/runtime/pkg/runtime"
	"sync"
)

func newSessionServiceServer(runtime runtime.Runtime, registry *primitive.SessionRegistry, types ...primitive.Type) primitivev1.SessionServiceServer {
	primitiveTypes := make(map[string]primitive.Type)
	for _, t := range types {
		primitiveTypes[t.Name()] = t
	}
	return &sessionServiceServer{
		runtime:  runtime,
		registry: registry,
		types:    primitiveTypes,
	}
}

type sessionServiceServer struct {
	runtime  runtime.Runtime
	registry *primitive.SessionRegistry
	types    map[string]primitive.Type
	mu       sync.Mutex
}

func (s *sessionServiceServer) OpenSession(ctx context.Context, request *primitivev1.OpenSessionRequest) (*primitivev1.OpenSessionResponse, error) {
	log.Debugw("OpenSession",
		logging.Stringer("OpenSessionRequest", request))

	s.mu.Lock()
	defer s.mu.Unlock()

	session, ok := s.registry.Get(request.Session.ID)
	if ok {
		response := &primitivev1.OpenSessionResponse{}
		log.Debugw("OpenSession",
			logging.Stringer("OpenSessionResponse", response))
		return response, nil
	}

	primitiveType, ok := s.types[request.Session.PrimitiveType]
	if !ok {
		err := errors.NewForbidden("unknown primitive type '%s'", request.Session.PrimitiveType)
		log.Warnw("OpenSession",
			logging.Stringer("OpenSessionRequest", request),
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

	session, err = primitiveType.GetProxy(conn.Client())(ctx, request.Session.ID, request.Session.Config)
	if err != nil {
		return nil, errors.ToProto(err)
	}

	s.registry.Register(request.Session.ID, session)

	response := &primitivev1.OpenSessionResponse{}
	log.Debugw("OpenSession",
		logging.Stringer("OpenSessionResponse", response))
	return response, nil
}

func (s *sessionServiceServer) CloseSession(ctx context.Context, request *primitivev1.CloseSessionRequest) (*primitivev1.CloseSessionResponse, error) {
	log.Debugw("CloseSession",
		logging.Stringer("ClosePrimitiveRequest", request))

	s.mu.Lock()
	defer s.mu.Unlock()

	session, ok := s.registry.Unregister(request.SessionID)
	if !ok {
		response := &primitivev1.CloseSessionResponse{}
		log.Debugw("CloseSession",
			logging.Stringer("CloseSessionResponse", response))
		return response, nil
	}

	if err := session.Close(ctx); err != nil {
		log.Warnw("CloseSession",
			logging.Stringer("ClosePrimitiveRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &primitivev1.CloseSessionResponse{}
	log.Debugw("CloseSession",
		logging.Stringer("CloseSessionResponse", response))
	return response, nil
}

var _ primitivev1.SessionServiceServer = (*sessionServiceServer)(nil)

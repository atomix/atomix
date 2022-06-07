// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	counterv1 "github.com/atomix/runtime/api/atomix/counter/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
)

func newCounterServer(proxies *primitive.Manager[counterv1.CounterServer]) counterv1.CounterServer {
	return &counterServer{
		proxies: proxies,
	}
}

type counterServer struct {
	proxies *primitive.Manager[counterv1.CounterServer]
}

func (s *counterServer) Create(ctx context.Context, request *counterv1.CreateRequest) (*counterv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", request))
	proxy, err := s.proxies.Connect(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Create(ctx, request)
	if err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Create",
		logging.Stringer("CreateResponse", response))
	return response, nil
}

func (s *counterServer) Close(ctx context.Context, request *counterv1.CloseRequest) (*counterv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", request))
	proxy, err := s.proxies.Close(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Close",
			logging.Stringer("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Close(ctx, request)
	if err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Close",
		logging.Stringer("CloseResponse", response))
	return response, nil
}

func (s *counterServer) Set(ctx context.Context, request *counterv1.SetRequest) (*counterv1.SetResponse, error) {
	log.Debugw("Set",
		logging.Stringer("SetRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Set",
			logging.Stringer("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Set(ctx, request)
	if err != nil {
		log.Warnw("Set",
			logging.Stringer("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Set",
		logging.Stringer("SetResponse", response))
	return response, nil
}

func (s *counterServer) Get(ctx context.Context, request *counterv1.GetRequest) (*counterv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Get",
			logging.Stringer("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Get(ctx, request)
	if err != nil {
		log.Warnw("Get",
			logging.Stringer("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Get",
		logging.Stringer("GetResponse", response))
	return response, nil
}

func (s *counterServer) Increment(ctx context.Context, request *counterv1.IncrementRequest) (*counterv1.IncrementResponse, error) {
	log.Debugw("Increment",
		logging.Stringer("IncrementRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Increment",
			logging.Stringer("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Increment(ctx, request)
	if err != nil {
		log.Warnw("Increment",
			logging.Stringer("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Increment",
		logging.Stringer("IncrementResponse", response))
	return response, nil
}

func (s *counterServer) Decrement(ctx context.Context, request *counterv1.DecrementRequest) (*counterv1.DecrementResponse, error) {
	log.Debugw("Decrement",
		logging.Stringer("DecrementRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Decrement",
			logging.Stringer("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Decrement(ctx, request)
	if err != nil {
		log.Warnw("Decrement",
			logging.Stringer("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Decrement",
		logging.Stringer("DecrementResponse", response))
	return response, nil
}

var _ counterv1.CounterServer = (*counterServer)(nil)

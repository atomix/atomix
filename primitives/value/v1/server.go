// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	valuev1 "github.com/atomix/runtime/api/atomix/value/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
)

func newValueServer(proxies *primitive.Manager[valuev1.ValueServer]) valuev1.ValueServer {
	return &valueServer{
		proxies: proxies,
	}
}

type valueServer struct {
	proxies *primitive.Manager[valuev1.ValueServer]
}

func (s *valueServer) Create(ctx context.Context, request *valuev1.CreateRequest) (*valuev1.CreateResponse, error) {
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

func (s *valueServer) Close(ctx context.Context, request *valuev1.CloseRequest) (*valuev1.CloseResponse, error) {
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

func (s *valueServer) Set(ctx context.Context, request *valuev1.SetRequest) (*valuev1.SetResponse, error) {
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

func (s *valueServer) Get(ctx context.Context, request *valuev1.GetRequest) (*valuev1.GetResponse, error) {
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

func (s *valueServer) Events(request *valuev1.EventsRequest, server valuev1.Value_EventsServer) error {
	log.Debugw("Events",
		logging.Stringer("EventsRequest", request))
	proxy, err := s.proxies.Get(server.Context())
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Events",
			logging.Stringer("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	err = proxy.Events(request, server)
	if err != nil {
		log.Warnw("Events",
			logging.Stringer("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	return nil
}

var _ valuev1.ValueServer = (*valueServer)(nil)

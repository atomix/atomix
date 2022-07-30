// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	mapv1 "github.com/atomix/runtime/api/atomix/runtime/map/v1"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	runtime "github.com/atomix/runtime/sdk/pkg/runtime"
)

var log = logging.GetLogger()

func newMapServer(delegate *runtime.Delegate[mapv1.MapServer]) mapv1.MapServer {
	return &mapServer{
		delegate: delegate,
	}
}

type mapServer struct {
	delegate *runtime.Delegate[mapv1.MapServer]
}

func (s *mapServer) Create(ctx context.Context, request *mapv1.CreateRequest) (*mapv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", request))
	client, err := s.delegate.Create(request.ID.Name, request.Tags)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Create(ctx, request)
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

func (s *mapServer) Close(ctx context.Context, request *mapv1.CloseRequest) (*mapv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", request))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Close",
			logging.Stringer("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Close(ctx, request)
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

func (s *mapServer) Size(ctx context.Context, request *mapv1.SizeRequest) (*mapv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Stringer("SizeRequest", request))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Size",
			logging.Stringer("SizeRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Size(ctx, request)
	if err != nil {
		log.Warnw("Size",
			logging.Stringer("SizeRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Size",
		logging.Stringer("SizeResponse", response))
	return response, nil
}

func (s *mapServer) Put(ctx context.Context, request *mapv1.PutRequest) (*mapv1.PutResponse, error) {
	log.Debugw("Put",
		logging.Stringer("PutRequest", request))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Put(ctx, request)
	if err != nil {
		log.Warnw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Put",
		logging.Stringer("PutResponse", response))
	return response, nil
}

func (s *mapServer) Get(ctx context.Context, request *mapv1.GetRequest) (*mapv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", request))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Get",
			logging.Stringer("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Get(ctx, request)
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

func (s *mapServer) Remove(ctx context.Context, request *mapv1.RemoveRequest) (*mapv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", request))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Remove(ctx, request)
	if err != nil {
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Remove",
		logging.Stringer("RemoveResponse", response))
	return response, nil
}

func (s *mapServer) Clear(ctx context.Context, request *mapv1.ClearRequest) (*mapv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Stringer("ClearRequest", request))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Clear",
			logging.Stringer("ClearRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Clear(ctx, request)
	if err != nil {
		log.Warnw("Clear",
			logging.Stringer("ClearRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Clear",
		logging.Stringer("ClearResponse", response))
	return response, nil
}

func (s *mapServer) Events(request *mapv1.EventsRequest, server mapv1.Map_EventsServer) error {
	log.Debugw("Events",
		logging.Stringer("EventsRequest", request),
		logging.String("State", "started"))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Events",
			logging.Stringer("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	err = client.Events(request, server)
	if err != nil {
		log.Warnw("Events",
			logging.Stringer("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	return nil
}

func (s *mapServer) Entries(request *mapv1.EntriesRequest, server mapv1.Map_EntriesServer) error {
	log.Debugw("Entries",
		logging.Stringer("EntriesRequest", request),
		logging.String("State", "started"))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Entries",
			logging.Stringer("EntriesRequest", request),
			logging.Error("Error", err))
		return err
	}
	err = client.Entries(request, server)
	if err != nil {
		log.Warnw("Entries",
			logging.Stringer("EntriesRequest", request),
			logging.Error("Error", err))
		return err
	}
	return nil
}

var _ mapv1.MapServer = (*mapServer)(nil)

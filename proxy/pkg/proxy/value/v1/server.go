// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/atomix/proxy/pkg/proxy"
	valuev1 "github.com/atomix/atomix/runtime/api/atomix/runtime/value/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/stringer"
)

var log = logging.GetLogger()

const truncLen = 250

func newValueServer(delegate *proxy.Delegate[valuev1.ValueServer]) valuev1.ValueServer {
	return &valueServer{
		delegate: delegate,
	}
}

type valueServer struct {
	delegate *proxy.Delegate[valuev1.ValueServer]
}

func (s *valueServer) Create(ctx context.Context, request *valuev1.CreateRequest) (*valuev1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Create(request.ID.Name, request.Tags)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Create(ctx, request)
	if err != nil {
		log.Debugw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Create",
		logging.Stringer("CreateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *valueServer) Close(ctx context.Context, request *valuev1.CloseRequest) (*valuev1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Close(ctx, request)
	if err != nil {
		log.Debugw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Close",
		logging.Stringer("CloseResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *valueServer) Set(ctx context.Context, request *valuev1.SetRequest) (*valuev1.SetResponse, error) {
	log.Debugw("Set",
		logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Set",
			logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Set(ctx, request)
	if err != nil {
		log.Debugw("Set",
			logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Set",
		logging.Stringer("SetResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *valueServer) Insert(ctx context.Context, request *valuev1.InsertRequest) (*valuev1.InsertResponse, error) {
	log.Debugw("Insert",
		logging.Stringer("InsertRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Insert",
			logging.Stringer("InsertRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Insert(ctx, request)
	if err != nil {
		log.Debugw("Insert",
			logging.Stringer("InsertRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Insert",
		logging.Stringer("InsertResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *valueServer) Update(ctx context.Context, request *valuev1.UpdateRequest) (*valuev1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Update(ctx, request)
	if err != nil {
		log.Debugw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Update",
		logging.Stringer("UpdateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *valueServer) Get(ctx context.Context, request *valuev1.GetRequest) (*valuev1.GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Get(ctx, request)
	if err != nil {
		log.Debugw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Get",
		logging.Stringer("GetResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *valueServer) Delete(ctx context.Context, request *valuev1.DeleteRequest) (*valuev1.DeleteResponse, error) {
	log.Debugw("Delete",
		logging.Stringer("DeleteRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Delete",
			logging.Stringer("DeleteRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Delete(ctx, request)
	if err != nil {
		log.Debugw("Delete",
			logging.Stringer("DeleteRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Delete",
		logging.Stringer("DeleteResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *valueServer) Watch(request *valuev1.WatchRequest, server valuev1.Value_WatchServer) error {
	log.Debugw("Watch",
		logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
		logging.String("State", "started"))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Watch",
			logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return err
	}
	err = client.Watch(request, server)
	if err != nil {
		log.Debugw("Watch",
			logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return err
	}
	return nil
}

func (s *valueServer) Events(request *valuev1.EventsRequest, server valuev1.Value_EventsServer) error {
	log.Debugw("Events",
		logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
		logging.String("State", "started"))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Events",
			logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return err
	}
	err = client.Events(request, server)
	if err != nil {
		log.Debugw("Events",
			logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return err
	}
	return nil
}

var _ valuev1.ValueServer = (*valueServer)(nil)

// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	listv1 "github.com/atomix/runtime/api/atomix/list/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/runtime/pkg/runtime"
	"io"
)

var log = logging.GetLogger()

func newListServer(delegate *runtime.Delegate[listv1.ListClient]) listv1.ListServer {
	return &listServer{
		delegate: delegate,
	}
}

type listServer struct {
	delegate *runtime.Delegate[listv1.ListClient]
}

func (s *listServer) Create(ctx context.Context, request *listv1.CreateRequest) (*listv1.CreateResponse, error) {
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

func (s *listServer) Close(ctx context.Context, request *listv1.CloseRequest) (*listv1.CloseResponse, error) {
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

func (s *listServer) Size(ctx context.Context, request *listv1.SizeRequest) (*listv1.SizeResponse, error) {
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

func (s *listServer) Append(ctx context.Context, request *listv1.AppendRequest) (*listv1.AppendResponse, error) {
	log.Debugw("Append",
		logging.Stringer("AppendRequest", request))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Append",
			logging.Stringer("AppendRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Append(ctx, request)
	if err != nil {
		log.Warnw("Append",
			logging.Stringer("AppendRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Append",
		logging.Stringer("AppendResponse", response))
	return response, nil
}

func (s *listServer) Insert(ctx context.Context, request *listv1.InsertRequest) (*listv1.InsertResponse, error) {
	log.Debugw("Insert",
		logging.Stringer("InsertRequest", request))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Insert",
			logging.Stringer("InsertRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Insert(ctx, request)
	if err != nil {
		log.Warnw("Insert",
			logging.Stringer("InsertRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Insert",
		logging.Stringer("InsertResponse", response))
	return response, nil
}

func (s *listServer) Get(ctx context.Context, request *listv1.GetRequest) (*listv1.GetResponse, error) {
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

func (s *listServer) Set(ctx context.Context, request *listv1.SetRequest) (*listv1.SetResponse, error) {
	log.Debugw("Set",
		logging.Stringer("SetRequest", request))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Set",
			logging.Stringer("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Set(ctx, request)
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

func (s *listServer) Remove(ctx context.Context, request *listv1.RemoveRequest) (*listv1.RemoveResponse, error) {
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

func (s *listServer) Clear(ctx context.Context, request *listv1.ClearRequest) (*listv1.ClearResponse, error) {
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

func (s *listServer) Events(request *listv1.EventsRequest, server listv1.List_EventsServer) error {
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
	stream, err := client.Events(server.Context(), request)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Events",
			logging.Stringer("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			log.Debugw("Events",
				logging.Stringer("EventsRequest", request),
				logging.String("State", "complete"))
			return nil
		}
		if err != nil {
			err = errors.ToProto(err)
			log.Warnw("Events",
				logging.Stringer("EventsRequest", request),
				logging.Error("Error", err))
			return err
		}
		log.Warnw("Events",
			logging.Stringer("EventsResponse", response))
		err = server.Send(response)
		if err != nil {
			err = errors.ToProto(err)
			log.Warnw("Events",
				logging.Stringer("EventsRequest", request),
				logging.Error("Error", err))
			return err
		}
	}
}

func (s *listServer) Elements(request *listv1.ElementsRequest, server listv1.List_ElementsServer) error {
	log.Debugw("Elements",
		logging.Stringer("ElementsRequest", request),
		logging.String("State", "started"))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Elements",
			logging.Stringer("ElementsRequest", request),
			logging.Error("Error", err))
		return err
	}
	stream, err := client.Elements(server.Context(), request)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Elements",
			logging.Stringer("ElementsRequest", request),
			logging.Error("Error", err))
		return err
	}
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			log.Debugw("Elements",
				logging.Stringer("ElementsRequest", request),
				logging.String("State", "complete"))
			return nil
		}
		if err != nil {
			err = errors.ToProto(err)
			log.Warnw("Elements",
				logging.Stringer("ElementsRequest", request),
				logging.Error("Error", err))
			return err
		}
		log.Warnw("Elements",
			logging.Stringer("ElementsResponse", response))
		err = server.Send(response)
		if err != nil {
			err = errors.ToProto(err)
			log.Warnw("Elements",
				logging.Stringer("ElementsRequest", request),
				logging.Error("Error", err))
			return err
		}
	}
}

var _ listv1.ListServer = (*listServer)(nil)

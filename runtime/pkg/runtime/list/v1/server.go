// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	listv1 "github.com/atomix/atomix/api/runtime/list/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/runtime"
)

var log = logging.GetLogger()

const truncLen = 250

func newListServer(client *runtime.PrimitiveClient[List]) listv1.ListServer {
	return &listServer{
		client: client,
	}
}

type listServer struct {
	client *runtime.PrimitiveClient[List]
}

func (s *listServer) Create(ctx context.Context, request *listv1.CreateRequest) (*listv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	client, err := s.client.Create(ctx, request.ID, request.Tags)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Create(ctx, request)
	if err != nil {
		log.Debugw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Create",
		logging.Trunc64("CreateResponse", response))
	return response, nil
}

func (s *listServer) Close(ctx context.Context, request *listv1.CloseRequest) (*listv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Close(ctx, request)
	if err != nil {
		log.Debugw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Close",
		logging.Trunc64("CloseResponse", response))
	return response, nil
}

func (s *listServer) Size(ctx context.Context, request *listv1.SizeRequest) (*listv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Trunc64("SizeRequest", request))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Size",
			logging.Trunc64("SizeRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Size(ctx, request)
	if err != nil {
		log.Debugw("Size",
			logging.Trunc64("SizeRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Size",
		logging.Trunc64("SizeResponse", response))
	return response, nil
}

func (s *listServer) Append(ctx context.Context, request *listv1.AppendRequest) (*listv1.AppendResponse, error) {
	log.Debugw("Append",
		logging.Trunc64("AppendRequest", request))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Append",
			logging.Trunc64("AppendRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Append(ctx, request)
	if err != nil {
		log.Debugw("Append",
			logging.Trunc64("AppendRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Append",
		logging.Trunc64("AppendResponse", response))
	return response, nil
}

func (s *listServer) Insert(ctx context.Context, request *listv1.InsertRequest) (*listv1.InsertResponse, error) {
	log.Debugw("Insert",
		logging.Trunc64("InsertRequest", request))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Insert",
			logging.Trunc64("InsertRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Insert(ctx, request)
	if err != nil {
		log.Debugw("Insert",
			logging.Trunc64("InsertRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Insert",
		logging.Trunc64("InsertResponse", response))
	return response, nil
}

func (s *listServer) Get(ctx context.Context, request *listv1.GetRequest) (*listv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Trunc64("GetRequest", request))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Get",
			logging.Trunc64("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Get(ctx, request)
	if err != nil {
		log.Debugw("Get",
			logging.Trunc64("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Get",
		logging.Trunc64("GetResponse", response))
	return response, nil
}

func (s *listServer) Set(ctx context.Context, request *listv1.SetRequest) (*listv1.SetResponse, error) {
	log.Debugw("Set",
		logging.Trunc64("SetRequest", request))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Set",
			logging.Trunc64("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Set(ctx, request)
	if err != nil {
		log.Debugw("Set",
			logging.Trunc64("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Set",
		logging.Trunc64("SetResponse", response))
	return response, nil
}

func (s *listServer) Remove(ctx context.Context, request *listv1.RemoveRequest) (*listv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Trunc64("RemoveRequest", request))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Remove",
			logging.Trunc64("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Remove(ctx, request)
	if err != nil {
		log.Debugw("Remove",
			logging.Trunc64("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Remove",
		logging.Trunc64("RemoveResponse", response))
	return response, nil
}

func (s *listServer) Clear(ctx context.Context, request *listv1.ClearRequest) (*listv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Trunc64("ClearRequest", request))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Clear",
			logging.Trunc64("ClearRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Clear(ctx, request)
	if err != nil {
		log.Debugw("Clear",
			logging.Trunc64("ClearRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Clear",
		logging.Trunc64("ClearResponse", response))
	return response, nil
}

func (s *listServer) Events(request *listv1.EventsRequest, server listv1.List_EventsServer) error {
	log.Debugw("Events",
		logging.Trunc64("EventsRequest", request),
		logging.String("State", "started"))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Events",
			logging.Trunc64("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	err = client.Events(request, server)
	if err != nil {
		log.Debugw("Events",
			logging.Trunc64("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	return nil
}

func (s *listServer) Items(request *listv1.ItemsRequest, server listv1.List_ItemsServer) error {
	log.Debugw("Items",
		logging.Trunc64("ItemsRequest", request),
		logging.String("State", "started"))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Items",
			logging.Trunc64("ItemsRequest", request),
			logging.Error("Error", err))
		return err
	}
	err = client.Items(request, server)
	if err != nil {
		log.Debugw("Items",
			logging.Trunc64("ItemsRequest", request),
			logging.Error("Error", err))
		return err
	}
	return nil
}

var _ listv1.ListServer = (*listServer)(nil)

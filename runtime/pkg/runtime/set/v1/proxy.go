// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	setv1 "github.com/atomix/atomix/api/pkg/runtime/set/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	"github.com/atomix/atomix/runtime/pkg/utils/stringer"
)

var log = logging.GetLogger()

const truncLen = 250

func newSetServer(client *runtime.PrimitiveClient[Set]) setv1.SetServer {
	return &setServer{
		client: client,
	}
}

type setServer struct {
	client *runtime.PrimitiveClient[Set]
}

func (s *setServer) Create(ctx context.Context, request *setv1.CreateRequest) (*setv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)))
	client, err := s.client.Create(ctx, request.ID, request.Tags)
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

func (s *setServer) Close(ctx context.Context, request *setv1.CloseRequest) (*setv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)))
	client, err := s.client.Get(request.ID)
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

func (s *setServer) Size(ctx context.Context, request *setv1.SizeRequest) (*setv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Size",
			logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Size(ctx, request)
	if err != nil {
		log.Debugw("Size",
			logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Size",
		logging.Stringer("SizeResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *setServer) Contains(ctx context.Context, request *setv1.ContainsRequest) (*setv1.ContainsResponse, error) {
	log.Debugw("Contains",
		logging.Stringer("ContainsRequest", stringer.Truncate(request, truncLen)))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Contains",
			logging.Stringer("ContainsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Contains(ctx, request)
	if err != nil {
		log.Debugw("Contains",
			logging.Stringer("ContainsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Contains",
		logging.Stringer("ContainsResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *setServer) Add(ctx context.Context, request *setv1.AddRequest) (*setv1.AddResponse, error) {
	log.Debugw("Add",
		logging.Stringer("AddRequest", stringer.Truncate(request, truncLen)))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Add",
			logging.Stringer("AddRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Add(ctx, request)
	if err != nil {
		log.Debugw("Add",
			logging.Stringer("AddRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Add",
		logging.Stringer("AddResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *setServer) Remove(ctx context.Context, request *setv1.RemoveRequest) (*setv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Remove(ctx, request)
	if err != nil {
		log.Debugw("Remove",
			logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Remove",
		logging.Stringer("RemoveResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *setServer) Clear(ctx context.Context, request *setv1.ClearRequest) (*setv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Clear",
			logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Clear(ctx, request)
	if err != nil {
		log.Debugw("Clear",
			logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Clear",
		logging.Stringer("ClearResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *setServer) Events(request *setv1.EventsRequest, server setv1.Set_EventsServer) error {
	log.Debugw("Events",
		logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
		logging.String("State", "started"))
	client, err := s.client.Get(request.ID)
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

func (s *setServer) Elements(request *setv1.ElementsRequest, server setv1.Set_ElementsServer) error {
	log.Debugw("Elements",
		logging.Stringer("ElementsRequest", stringer.Truncate(request, truncLen)),
		logging.String("State", "started"))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Elements",
			logging.Stringer("ElementsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return err
	}
	err = client.Elements(request, server)
	if err != nil {
		log.Debugw("Elements",
			logging.Stringer("ElementsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return err
	}
	return nil
}

var _ setv1.SetServer = (*setServer)(nil)

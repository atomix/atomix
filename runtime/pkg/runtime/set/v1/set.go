// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	setv1 "github.com/atomix/atomix/api/runtime/set/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

var log = logging.GetLogger()

type SetProxy interface {
	runtime.PrimitiveProxy
	// Size gets the number of elements in the set
	Size(context.Context, *setv1.SizeRequest) (*setv1.SizeResponse, error)
	// Contains returns whether the set contains a value
	Contains(context.Context, *setv1.ContainsRequest) (*setv1.ContainsResponse, error)
	// Add adds a value to the set
	Add(context.Context, *setv1.AddRequest) (*setv1.AddResponse, error)
	// Remove removes a value from the set
	Remove(context.Context, *setv1.RemoveRequest) (*setv1.RemoveResponse, error)
	// Clear removes all values from the set
	Clear(context.Context, *setv1.ClearRequest) (*setv1.ClearResponse, error)
	// Events listens for set change events
	Events(*setv1.EventsRequest, setv1.Set_EventsServer) error
	// Elements lists all elements in the set
	Elements(*setv1.ElementsRequest, setv1.Set_ElementsServer) error
}

func NewSetServer(rt *runtime.Runtime) setv1.SetServer {
	return &setServer{
		SetsServer: NewSetsServer(rt),
		primitives: runtime.NewPrimitiveRegistry[SetProxy](setv1.PrimitiveType, rt),
	}
}

type setServer struct {
	setv1.SetsServer
	primitives runtime.PrimitiveRegistry[SetProxy]
}

func (s *setServer) Size(ctx context.Context, request *setv1.SizeRequest) (*setv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Trunc64("SizeRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
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

func (s *setServer) Contains(ctx context.Context, request *setv1.ContainsRequest) (*setv1.ContainsResponse, error) {
	log.Debugw("Contains",
		logging.Trunc64("ContainsRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Contains",
			logging.Trunc64("ContainsRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Contains(ctx, request)
	if err != nil {
		log.Debugw("Contains",
			logging.Trunc64("ContainsRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Contains",
		logging.Trunc64("ContainsResponse", response))
	return response, nil
}

func (s *setServer) Add(ctx context.Context, request *setv1.AddRequest) (*setv1.AddResponse, error) {
	log.Debugw("Add",
		logging.Trunc64("AddRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Add",
			logging.Trunc64("AddRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Add(ctx, request)
	if err != nil {
		log.Debugw("Add",
			logging.Trunc64("AddRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Add",
		logging.Trunc64("AddResponse", response))
	return response, nil
}

func (s *setServer) Remove(ctx context.Context, request *setv1.RemoveRequest) (*setv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Trunc64("RemoveRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
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

func (s *setServer) Clear(ctx context.Context, request *setv1.ClearRequest) (*setv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Trunc64("ClearRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
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

func (s *setServer) Events(request *setv1.EventsRequest, server setv1.Set_EventsServer) error {
	log.Debugw("Events",
		logging.Trunc64("EventsRequest", request),
		logging.String("State", "started"))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
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

func (s *setServer) Elements(request *setv1.ElementsRequest, server setv1.Set_ElementsServer) error {
	log.Debugw("Elements",
		logging.Trunc64("ElementsRequest", request),
		logging.String("State", "started"))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Elements",
			logging.Trunc64("ElementsRequest", request),
			logging.Error("Error", err))
		return err
	}
	err = client.Elements(request, server)
	if err != nil {
		log.Debugw("Elements",
			logging.Trunc64("ElementsRequest", request),
			logging.Error("Error", err))
		return err
	}
	return nil
}

var _ setv1.SetServer = (*setServer)(nil)

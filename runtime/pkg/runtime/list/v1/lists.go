// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	listv1 "github.com/atomix/atomix/api/runtime/list/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

type ListProvider interface {
	NewListV1(ctx context.Context, id runtimev1.PrimitiveID) (ListProxy, error)
}

func resolve(ctx context.Context, conn driver.Conn, id runtimev1.PrimitiveID) (ListProxy, bool, error) {
	if provider, ok := conn.(ListProvider); ok {
		list, err := provider.NewListV1(ctx, id)
		if err != nil {
			return nil, false, err
		}
		return list, true, nil
	}
	return nil, false, nil
}

func NewListsServer(rt *runtime.Runtime) listv1.ListsServer {
	return &listsServer{
		manager: runtime.NewPrimitiveManager[ListProxy, *listv1.ListConfig](listv1.PrimitiveType, resolve, rt),
	}
}

type listsServer struct {
	manager runtime.PrimitiveManager[*listv1.ListConfig]
}

func (s *listsServer) Create(ctx context.Context, request *listv1.CreateRequest) (*listv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	config, err := s.manager.Create(ctx, request.ID, request.Tags)
	if err != nil {
		log.Warnw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &listv1.CreateResponse{
		Config: *config,
	}
	log.Debugw("Create",
		logging.Trunc64("CreateResponse", response))
	return response, nil
}

func (s *listsServer) Close(ctx context.Context, request *listv1.CloseRequest) (*listv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	err := s.manager.Close(ctx, request.ID)
	if err != nil {
		log.Warnw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &listv1.CloseResponse{}
	log.Debugw("Close",
		logging.Trunc64("CloseResponse", response))
	return response, nil
}

var _ listv1.ListsServer = (*listsServer)(nil)

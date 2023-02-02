// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	multimapv1 "github.com/atomix/atomix/api/runtime/multimap/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

type MultiMapProvider interface {
	NewMultiMapV1(ctx context.Context, id runtimev1.PrimitiveID) (MultiMapProxy, error)
}

func resolve(ctx context.Context, conn driver.Conn, id runtimev1.PrimitiveID) (MultiMapProxy, bool, error) {
	if provider, ok := conn.(MultiMapProvider); ok {
		multiMap, err := provider.NewMultiMapV1(ctx, id)
		if err != nil {
			return nil, false, err
		}
		return multiMap, true, nil
	}
	return nil, false, nil
}

func NewMultiMapsServer(rt *runtime.Runtime) multimapv1.MultiMapsServer {
	return &multiMapsServer{
		manager: runtime.NewPrimitiveManager[MultiMapProxy, *multimapv1.Config](multimapv1.PrimitiveType, resolve, rt),
	}
}

type multiMapsServer struct {
	manager runtime.PrimitiveManager[*multimapv1.Config]
}

func (s *multiMapsServer) Create(ctx context.Context, request *multimapv1.CreateRequest) (*multimapv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	config, err := s.manager.Create(ctx, request.ID, request.Tags)
	if err != nil {
		log.Warnw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &multimapv1.CreateResponse{
		Config: *config,
	}
	log.Debugw("Create",
		logging.Trunc64("CreateResponse", response))
	return response, nil
}

func (s *multiMapsServer) Close(ctx context.Context, request *multimapv1.CloseRequest) (*multimapv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	err := s.manager.Close(ctx, request.ID)
	if err != nil {
		log.Warnw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &multimapv1.CloseResponse{}
	log.Debugw("Close",
		logging.Trunc64("CloseResponse", response))
	return response, nil
}

var _ multimapv1.MultiMapsServer = (*multiMapsServer)(nil)

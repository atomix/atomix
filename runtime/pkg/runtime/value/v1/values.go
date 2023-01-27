// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	valuev1 "github.com/atomix/atomix/api/runtime/value/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

type ValueProvider interface {
	NewValueV1(ctx context.Context, id runtimev1.PrimitiveID) (ValueProxy, error)
}

func resolve(ctx context.Context, conn driver.Conn, id runtimev1.PrimitiveID) (ValueProxy, bool, error) {
	if provider, ok := conn.(ValueProvider); ok {
		value, err := provider.NewValueV1(ctx, id)
		if err != nil {
			return nil, false, err
		}
		return value, true, nil
	}
	return nil, false, nil
}

func NewValuesServer(rt *runtime.Runtime) valuev1.ValuesServer {
	return &valuesServer{
		manager: runtime.NewPrimitiveManager[ValueProxy, *valuev1.ValueConfig](valuev1.PrimitiveType, resolve, rt),
	}
}

type valuesServer struct {
	manager runtime.PrimitiveManager[*valuev1.ValueConfig]
}

func (s *valuesServer) Create(ctx context.Context, request *valuev1.CreateRequest) (*valuev1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	config, err := s.manager.Create(ctx, request.ID, request.Tags)
	if err != nil {
		log.Warnw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &valuev1.CreateResponse{
		Config: *config,
	}
	log.Debugw("Create",
		logging.Trunc64("CreateResponse", response))
	return response, nil
}

func (s *valuesServer) Close(ctx context.Context, request *valuev1.CloseRequest) (*valuev1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	err := s.manager.Close(ctx, request.ID)
	if err != nil {
		log.Warnw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &valuev1.CloseResponse{}
	log.Debugw("Close",
		logging.Trunc64("CloseResponse", response))
	return response, nil
}

var _ valuev1.ValuesServer = (*valuesServer)(nil)

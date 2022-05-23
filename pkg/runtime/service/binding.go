// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/runtime/store"
)

func newBindingServiceServer(store store.Store[*runtimev1.BindingId, *runtimev1.Binding]) runtimev1.BindingServiceServer {
	return &bindingServiceServer{
		store: store,
	}
}

type bindingServiceServer struct {
	store store.Store[*runtimev1.BindingId, *runtimev1.Binding]
}

func (s *bindingServiceServer) GetBinding(ctx context.Context, request *runtimev1.GetBindingRequest) (*runtimev1.GetBindingResponse, error) {
	log.Debugw("GetBinding",
		logging.Stringer("GetBindingRequest", request))

	binding, ok := s.store.Get(&request.BindingID)
	if !ok {
		err := errors.NewNotFound("binding '%s' not found", request.BindingID)
		log.Warnw("GetBinding",
			logging.Stringer("GetBindingRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &runtimev1.GetBindingResponse{
		Binding: binding,
	}
	log.Debugw("GetBinding",
		logging.Stringer("GetBindingResponse", response))
	return response, nil
}

func (s *bindingServiceServer) ListBindings(ctx context.Context, request *runtimev1.ListBindingsRequest) (*runtimev1.ListBindingsResponse, error) {
	log.Debugw("ListBindings",
		logging.Stringer("ListBindingsRequest", request))

	bindings := s.store.List()
	response := &runtimev1.ListBindingsResponse{
		Bindings: bindings,
	}
	log.Debugw("ListBindings",
		logging.Stringer("ListBindingsResponse", response))
	return response, nil
}

func (s *bindingServiceServer) CreateBinding(ctx context.Context, request *runtimev1.CreateBindingRequest) (*runtimev1.CreateBindingResponse, error) {
	log.Debugw("CreateBinding",
		logging.Stringer("CreateBindingRequest", request))

	binding := request.Binding
	err := s.store.Create(binding)
	if err != nil {
		log.Warnw("CreateBinding",
			logging.Stringer("CreateBindingRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &runtimev1.CreateBindingResponse{
		Binding: binding,
	}
	log.Debugw("CreateBinding",
		logging.Stringer("CreateBindingResponse", response))
	return response, nil
}

func (s *bindingServiceServer) UpdateBinding(ctx context.Context, request *runtimev1.UpdateBindingRequest) (*runtimev1.UpdateBindingResponse, error) {
	log.Debugw("UpdateBinding",
		logging.Stringer("UpdateBindingRequest", request))

	binding := request.Binding
	err := s.store.Update(binding)
	if err != nil {
		log.Warnw("UpdateBinding",
			logging.Stringer("UpdateBindingRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &runtimev1.UpdateBindingResponse{
		Binding: binding,
	}
	log.Debugw("UpdateBinding",
		logging.Stringer("UpdateBindingResponse", response))
	return response, nil
}

func (s *bindingServiceServer) DeleteBinding(ctx context.Context, request *runtimev1.DeleteBindingRequest) (*runtimev1.DeleteBindingResponse, error) {
	log.Debugw("DeleteBinding",
		logging.Stringer("DeleteBindingRequest", request))

	binding := request.Binding
	err := s.store.Delete(binding)
	if err != nil {
		log.Warnw("DeleteBinding",
			logging.Stringer("DeleteBindingRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &runtimev1.DeleteBindingResponse{}
	log.Debugw("DeleteBinding",
		logging.Stringer("DeleteBindingResponse", response))
	return response, nil
}

var _ runtimev1.BindingServiceServer = (*bindingServiceServer)(nil)

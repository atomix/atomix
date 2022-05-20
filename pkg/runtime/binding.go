// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
)

func newBindingStore() Store[runtimev1.BindingId, *runtimev1.Binding] {
	return NewStore[runtimev1.BindingId, *runtimev1.Binding](func(binding *runtimev1.Binding) runtimev1.BindingId {
		return binding.BindingID
	})
}

func newBindingManagerServer(bindings Store[runtimev1.BindingId, *runtimev1.Binding]) runtimev1.BindingManagerServer {
	return &bindingManagerServer{
		bindings: bindings,
	}
}

type bindingManagerServer struct {
	bindings Store[runtimev1.BindingId, *runtimev1.Binding]
}

func (s *bindingManagerServer) GetBinding(ctx context.Context, request *runtimev1.GetBindingRequest) (*runtimev1.GetBindingResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (s *bindingManagerServer) CreateBinding(ctx context.Context, request *runtimev1.CreateBindingRequest) (*runtimev1.CreateBindingResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (s *bindingManagerServer) UpdateBinding(ctx context.Context, request *runtimev1.UpdateBindingRequest) (*runtimev1.UpdateBindingResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (s *bindingManagerServer) DeleteBinding(ctx context.Context, request *runtimev1.DeleteBindingRequest) (*runtimev1.DeleteBindingResponse, error) {
	//TODO implement me
	panic("implement me")
}

var _ runtimev1.BindingManagerServer = (*bindingManagerServer)(nil)

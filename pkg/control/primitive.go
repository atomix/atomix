// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/runtime"
)

func newPrimitiveServiceServer(primitives runtime.Store[*runtimev1.Primitive]) runtimev1.PrimitiveServiceServer {
	return &primitiveServiceServer{
		primitives: primitives,
	}
}

type primitiveServiceServer struct {
	primitives runtime.Store[*runtimev1.Primitive]
}

func (s *primitiveServiceServer) GetPrimitive(ctx context.Context, request *runtimev1.GetPrimitiveRequest) (*runtimev1.GetPrimitiveResponse, error) {
	log.Debugw("GetPrimitive",
		logging.Stringer("GetPrimitiveRequest", request))
	primitive, err := s.primitives.Get(request.PrimitiveID)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	response := &runtimev1.GetPrimitiveResponse{
		Primitive: primitive,
	}
	log.Debugw("GetPrimitive",
		logging.Stringer("GetPrimitiveRequest", response))
	return response, nil
}

func (s *primitiveServiceServer) ListPrimitives(ctx context.Context, request *runtimev1.ListPrimitivesRequest) (*runtimev1.ListPrimitivesResponse, error) {
	log.Debugw("ListPrimitives",
		logging.Stringer("ListPrimitivesRequest", request))
	primitives, err := s.primitives.List()
	if err != nil {
		return nil, errors.ToProto(err)
	}
	response := &runtimev1.ListPrimitivesResponse{
		Primitives: primitives,
	}
	log.Debugw("ListPrimitives",
		logging.Stringer("ListPrimitivesRequest", response))
	return response, nil
}

var _ runtimev1.PrimitiveServiceServer = (*primitiveServiceServer)(nil)

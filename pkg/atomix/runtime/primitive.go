// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/runtime/store"
)

func newPrimitiveServiceServer(store *store.Store[*runtimev1.PrimitiveId, *runtimev1.Primitive]) runtimev1.PrimitiveServiceServer {
	return &primitiveServiceServer{
		store: store,
	}
}

type primitiveServiceServer struct {
	store *store.Store[*runtimev1.PrimitiveId, *runtimev1.Primitive]
}

func (s *primitiveServiceServer) GetPrimitive(ctx context.Context, request *runtimev1.GetPrimitiveRequest) (*runtimev1.GetPrimitiveResponse, error) {
	log.Debugw("GetPrimitive",
		logging.Stringer("GetPrimitiveRequest", request))

	primitive, ok := s.store.Get(&request.PrimitiveID)
	if !ok {
		err := errors.NewNotFound("primitive '%s' not found", request.PrimitiveID)
		log.Warnw("GetPrimitive",
			logging.Stringer("GetPrimitiveRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &runtimev1.GetPrimitiveResponse{
		Primitive: primitive,
	}
	log.Debugw("GetPrimitive",
		logging.Stringer("GetPrimitiveResponse", response))
	return response, nil
}

func (s *primitiveServiceServer) ListPrimitives(ctx context.Context, request *runtimev1.ListPrimitivesRequest) (*runtimev1.ListPrimitivesResponse, error) {
	log.Debugw("ListPrimitives",
		logging.Stringer("ListPrimitivesRequest", request))

	primitives := s.store.List()
	response := &runtimev1.ListPrimitivesResponse{
		Primitives: primitives,
	}
	log.Debugw("ListPrimitives",
		logging.Stringer("ListPrimitivesResponse", response))
	return response, nil
}

var _ runtimev1.PrimitiveServiceServer = (*primitiveServiceServer)(nil)

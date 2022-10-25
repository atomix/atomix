// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	counterv1 "github.com/atomix/runtime/api/atomix/runtime/counter/v1"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"github.com/atomix/runtime/sdk/pkg/protocol/client"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"github.com/atomix/runtime/sdk/pkg/stringer"
	"google.golang.org/grpc"
)

func NewCounterProxy(protocol *client.Protocol, spec runtime.PrimitiveSpec) (counterv1.CounterServer, error) {
	return &counterProxy{
		Protocol:      protocol,
		PrimitiveSpec: spec,
	}, nil
}

type counterProxy struct {
	*client.Protocol
	runtime.PrimitiveSpec
}

func (s *counterProxy) Create(ctx context.Context, request *counterv1.CreateRequest) (*counterv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if err := session.CreatePrimitive(ctx, s.PrimitiveSpec); err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &counterv1.CreateResponse{}
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("CreateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *counterProxy) Close(ctx context.Context, request *counterv1.CloseRequest) (*counterv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if err := session.ClosePrimitive(ctx, request.ID.Name); err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &counterv1.CloseResponse{}
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("CloseResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *counterProxy) Set(ctx context.Context, request *counterv1.SetRequest) (*counterv1.SetResponse, error) {
	log.Debugw("Set",
		logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Set",
			logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Set",
			logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	proposal := client.Proposal[*SetResponse](primitive)
	output, ok, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*SetResponse, error) {
		return NewCounterClient(conn).Set(ctx, &SetRequest{
			Headers: headers,
			SetInput: &SetInput{
				Value: request.Value,
			},
		})
	})
	if !ok {
		log.Warnw("Set",
			logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Set",
			logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &counterv1.SetResponse{
		Value: output.Value,
	}
	log.Debugw("Set",
		logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("SetResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *counterProxy) Get(ctx context.Context, request *counterv1.GetRequest) (*counterv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	proposal := client.Query[*GetResponse](primitive)
	output, ok, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*GetResponse, error) {
		return NewCounterClient(conn).Get(ctx, &GetRequest{
			Headers:  headers,
			GetInput: &GetInput{},
		})
	})
	if !ok {
		log.Warnw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &counterv1.GetResponse{
		Value: output.Value,
	}
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("GetResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *counterProxy) Increment(ctx context.Context, request *counterv1.IncrementRequest) (*counterv1.IncrementResponse, error) {
	log.Debugw("Increment",
		logging.Stringer("IncrementRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Increment",
			logging.Stringer("IncrementRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Increment",
			logging.Stringer("IncrementRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	proposal := client.Proposal[*IncrementResponse](primitive)
	output, ok, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*IncrementResponse, error) {
		return NewCounterClient(conn).Increment(ctx, &IncrementRequest{
			Headers: headers,
			IncrementInput: &IncrementInput{
				Delta: request.Delta,
			},
		})
	})
	if !ok {
		log.Warnw("Increment",
			logging.Stringer("IncrementRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Increment",
			logging.Stringer("IncrementRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &counterv1.IncrementResponse{
		Value: output.Value,
	}
	log.Debugw("Increment",
		logging.Stringer("IncrementRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("IncrementResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *counterProxy) Decrement(ctx context.Context, request *counterv1.DecrementRequest) (*counterv1.DecrementResponse, error) {
	log.Debugw("Decrement",
		logging.Stringer("DecrementRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Decrement",
			logging.Stringer("DecrementRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Decrement",
			logging.Stringer("DecrementRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	proposal := client.Proposal[*DecrementResponse](primitive)
	output, ok, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*DecrementResponse, error) {
		return NewCounterClient(conn).Decrement(ctx, &DecrementRequest{
			Headers: headers,
			DecrementInput: &DecrementInput{
				Delta: request.Delta,
			},
		})
	})
	if !ok {
		log.Warnw("Decrement",
			logging.Stringer("DecrementRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Decrement",
			logging.Stringer("DecrementRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &counterv1.DecrementResponse{
		Value: output.Value,
	}
	log.Debugw("Decrement",
		logging.Stringer("DecrementRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("DecrementResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *counterProxy) Update(ctx context.Context, request *counterv1.UpdateRequest) (*counterv1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	proposal := client.Proposal[*UpdateResponse](primitive)
	output, ok, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*UpdateResponse, error) {
		return NewCounterClient(conn).Update(ctx, &UpdateRequest{
			Headers: headers,
			UpdateInput: &UpdateInput{
				Compare: request.Check,
				Update:  request.Update,
			},
		})
	})
	if !ok {
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &counterv1.UpdateResponse{
		Value: output.Value,
	}
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("UpdateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

var _ counterv1.CounterServer = (*counterProxy)(nil)

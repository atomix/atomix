// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	counterv1 "github.com/atomix/atomix/api/runtime/counter/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	counterprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/counter/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/client"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

func NewCounter(protocol *client.Protocol) counterv1.CounterServer {
	return &counterClient{
		Protocol: protocol,
	}
}

type counterClient struct {
	*client.Protocol
}

func (s *counterClient) Create(ctx context.Context, request *counterv1.CreateRequest) (*counterv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if err := session.CreatePrimitive(ctx, runtimev1.PrimitiveMeta{
		Type:        counterv1.PrimitiveType,
		PrimitiveID: request.ID,
		Tags:        request.Tags,
	}); err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &counterv1.CreateResponse{}
	log.Debugw("Create",
		logging.Stringer("CreateRequest", request),
		logging.Stringer("CreateResponse", response))
	return response, nil
}

func (s *counterClient) Close(ctx context.Context, request *counterv1.CloseRequest) (*counterv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if err := session.ClosePrimitive(ctx, request.ID.Name); err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &counterv1.CloseResponse{}
	log.Debugw("Close",
		logging.Stringer("CloseRequest", request),
		logging.Stringer("CloseResponse", response))
	return response, nil
}

func (s *counterClient) Set(ctx context.Context, request *counterv1.SetRequest) (*counterv1.SetResponse, error) {
	log.Debugw("Set",
		logging.Stringer("SetRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Set",
			logging.Stringer("SetRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Set",
			logging.Stringer("SetRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	proposal := client.Proposal[*counterprotocolv1.SetResponse](primitive)
	output, ok, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*counterprotocolv1.SetResponse, error) {
		return counterprotocolv1.NewCounterClient(conn).Set(ctx, &counterprotocolv1.SetRequest{
			Headers: headers,
			SetInput: &counterprotocolv1.SetInput{
				Value: request.Value,
			},
		})
	})
	if !ok {
		log.Warnw("Set",
			logging.Stringer("SetRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Set",
			logging.Stringer("SetRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &counterv1.SetResponse{
		Value: output.Value,
	}
	log.Debugw("Set",
		logging.Stringer("SetRequest", request),
		logging.Stringer("SetResponse", response))
	return response, nil
}

func (s *counterClient) Get(ctx context.Context, request *counterv1.GetRequest) (*counterv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Get",
			logging.Stringer("GetRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Get",
			logging.Stringer("GetRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	proposal := client.Query[*counterprotocolv1.GetResponse](primitive)
	output, ok, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*counterprotocolv1.GetResponse, error) {
		return counterprotocolv1.NewCounterClient(conn).Get(ctx, &counterprotocolv1.GetRequest{
			Headers:  headers,
			GetInput: &counterprotocolv1.GetInput{},
		})
	})
	if !ok {
		log.Warnw("Get",
			logging.Stringer("GetRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Get",
			logging.Stringer("GetRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &counterv1.GetResponse{
		Value: output.Value,
	}
	log.Debugw("Get",
		logging.Stringer("GetRequest", request),
		logging.Stringer("GetResponse", response))
	return response, nil
}

func (s *counterClient) Increment(ctx context.Context, request *counterv1.IncrementRequest) (*counterv1.IncrementResponse, error) {
	log.Debugw("Increment",
		logging.Stringer("IncrementRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Increment",
			logging.Stringer("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Increment",
			logging.Stringer("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	proposal := client.Proposal[*counterprotocolv1.IncrementResponse](primitive)
	output, ok, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*counterprotocolv1.IncrementResponse, error) {
		return counterprotocolv1.NewCounterClient(conn).Increment(ctx, &counterprotocolv1.IncrementRequest{
			Headers: headers,
			IncrementInput: &counterprotocolv1.IncrementInput{
				Delta: request.Delta,
			},
		})
	})
	if !ok {
		log.Warnw("Increment",
			logging.Stringer("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Increment",
			logging.Stringer("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &counterv1.IncrementResponse{
		Value: output.Value,
	}
	log.Debugw("Increment",
		logging.Stringer("IncrementRequest", request),
		logging.Stringer("IncrementResponse", response))
	return response, nil
}

func (s *counterClient) Decrement(ctx context.Context, request *counterv1.DecrementRequest) (*counterv1.DecrementResponse, error) {
	log.Debugw("Decrement",
		logging.Stringer("DecrementRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Decrement",
			logging.Stringer("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Decrement",
			logging.Stringer("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	proposal := client.Proposal[*counterprotocolv1.DecrementResponse](primitive)
	output, ok, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*counterprotocolv1.DecrementResponse, error) {
		return counterprotocolv1.NewCounterClient(conn).Decrement(ctx, &counterprotocolv1.DecrementRequest{
			Headers: headers,
			DecrementInput: &counterprotocolv1.DecrementInput{
				Delta: request.Delta,
			},
		})
	})
	if !ok {
		log.Warnw("Decrement",
			logging.Stringer("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Decrement",
			logging.Stringer("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &counterv1.DecrementResponse{
		Value: output.Value,
	}
	log.Debugw("Decrement",
		logging.Stringer("DecrementRequest", request),
		logging.Stringer("DecrementResponse", response))
	return response, nil
}

func (s *counterClient) Update(ctx context.Context, request *counterv1.UpdateRequest) (*counterv1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	proposal := client.Proposal[*counterprotocolv1.UpdateResponse](primitive)
	output, ok, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*counterprotocolv1.UpdateResponse, error) {
		return counterprotocolv1.NewCounterClient(conn).Update(ctx, &counterprotocolv1.UpdateRequest{
			Headers: headers,
			UpdateInput: &counterprotocolv1.UpdateInput{
				Compare: request.Check,
				Update:  request.Update,
			},
		})
	})
	if !ok {
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Update",
			logging.Stringer("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &counterv1.UpdateResponse{
		Value: output.Value,
	}
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", request),
		logging.Stringer("UpdateResponse", response))
	return response, nil
}

var _ counterv1.CounterServer = (*counterClient)(nil)

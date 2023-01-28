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
	"github.com/atomix/atomix/runtime/pkg/logging"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

func NewCounter(protocol *client.Protocol, id runtimev1.PrimitiveID) *CounterSession {
	return &CounterSession{
		Protocol: protocol,
		id:       id,
	}
}

type CounterSession struct {
	*client.Protocol
	id runtimev1.PrimitiveID
}

func (s *CounterSession) Open(ctx context.Context) error {
	log.Debugw("Create",
		logging.String("Name", s.id.Name))
	partition := s.PartitionBy([]byte(s.id.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Create",
			logging.String("Name", s.id.Name),
			logging.Error("Error", err))
		return err
	}
	meta := runtimev1.PrimitiveMeta{
		Type:        counterv1.PrimitiveType,
		PrimitiveID: s.id,
	}
	if err := session.CreatePrimitive(ctx, meta); err != nil {
		log.Warnw("Create",
			logging.String("Name", s.id.Name),
			logging.Error("Error", err))
		return err
	}
	return nil
}

func (s *CounterSession) Close(ctx context.Context) error {
	log.Debugw("Close",
		logging.String("Name", s.id.Name))
	partition := s.PartitionBy([]byte(s.id.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Create",
			logging.String("Name", s.id.Name),
			logging.Error("Error", err))
		return err
	}
	if err := session.ClosePrimitive(ctx, s.id.Name); err != nil {
		log.Warnw("Close",
			logging.String("Name", s.id.Name),
			logging.Error("Error", err))
		return err
	}
	return nil
}

func (s *CounterSession) Set(ctx context.Context, request *counterv1.SetRequest) (*counterv1.SetResponse, error) {
	log.Debugw("Set",
		logging.Trunc128("SetRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Set",
			logging.Trunc128("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Set",
			logging.Trunc128("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
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
			logging.Trunc128("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Set",
			logging.Trunc128("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &counterv1.SetResponse{
		Value: output.Value,
	}
	log.Debugw("Set",
		logging.Trunc128("SetRequest", request),
		logging.Trunc128("SetResponse", response))
	return response, nil
}

func (s *CounterSession) Get(ctx context.Context, request *counterv1.GetRequest) (*counterv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Get",
			logging.Trunc128("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Get",
			logging.Trunc128("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
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
			logging.Trunc128("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Get",
			logging.Trunc128("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &counterv1.GetResponse{
		Value: output.Value,
	}
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request),
		logging.Trunc128("GetResponse", response))
	return response, nil
}

func (s *CounterSession) Increment(ctx context.Context, request *counterv1.IncrementRequest) (*counterv1.IncrementResponse, error) {
	log.Debugw("Increment",
		logging.Trunc128("IncrementRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Increment",
			logging.Trunc128("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Increment",
			logging.Trunc128("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, err
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
			logging.Trunc128("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Increment",
			logging.Trunc128("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &counterv1.IncrementResponse{
		Value: output.Value,
	}
	log.Debugw("Increment",
		logging.Trunc128("IncrementRequest", request),
		logging.Trunc128("IncrementResponse", response))
	return response, nil
}

func (s *CounterSession) Decrement(ctx context.Context, request *counterv1.DecrementRequest) (*counterv1.DecrementResponse, error) {
	log.Debugw("Decrement",
		logging.Trunc128("DecrementRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Decrement",
			logging.Trunc128("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Decrement",
			logging.Trunc128("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, err
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
			logging.Trunc128("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Decrement",
			logging.Trunc128("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &counterv1.DecrementResponse{
		Value: output.Value,
	}
	log.Debugw("Decrement",
		logging.Trunc128("DecrementRequest", request),
		logging.Trunc128("DecrementResponse", response))
	return response, nil
}

func (s *CounterSession) Update(ctx context.Context, request *counterv1.UpdateRequest) (*counterv1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Trunc128("UpdateRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Update",
			logging.Trunc128("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Update",
			logging.Trunc128("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, err
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
			logging.Trunc128("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Update",
			logging.Trunc128("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &counterv1.UpdateResponse{
		Value: output.Value,
	}
	log.Debugw("Update",
		logging.Trunc128("UpdateRequest", request),
		logging.Trunc128("UpdateResponse", response))
	return response, nil
}

var _ counterv1.CounterServer = (*CounterSession)(nil)

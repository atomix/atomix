// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	lockv1 "github.com/atomix/atomix/api/runtime/lock/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	lockprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/lock/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/client"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

func NewLock(protocol *client.Protocol) (lockv1.LockServer, error) {
	return &lockClient{
		Protocol: protocol,
	}, nil
}

type lockClient struct {
	*client.Protocol
}

func (s *lockClient) Create(ctx context.Context, request *lockv1.CreateRequest) (*lockv1.CreateResponse, error) {
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
		Type:        lockv1.PrimitiveType,
		PrimitiveID: request.ID,
		Tags:        request.Tags,
	}); err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &lockv1.CreateResponse{}
	log.Debugw("Create",
		logging.Stringer("CreateRequest", request),
		logging.Stringer("CreateResponse", response))
	return response, nil
}

func (s *lockClient) Close(ctx context.Context, request *lockv1.CloseRequest) (*lockv1.CloseResponse, error) {
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
	response := &lockv1.CloseResponse{}
	log.Debugw("Close",
		logging.Stringer("CloseRequest", request),
		logging.Stringer("CloseResponse", response))
	return response, nil
}

func (s *lockClient) Lock(ctx context.Context, request *lockv1.LockRequest) (*lockv1.LockResponse, error) {
	log.Debugw("Lock",
		logging.Stringer("LockRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Lock",
			logging.Stringer("LockRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Lock",
			logging.Stringer("LockRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	query := client.Proposal[*lockprotocolv1.AcquireResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*lockprotocolv1.AcquireResponse, error) {
		return lockprotocolv1.NewLockClient(conn).Acquire(ctx, &lockprotocolv1.AcquireRequest{
			Headers: headers,
			AcquireInput: &lockprotocolv1.AcquireInput{
				Timeout: request.Timeout,
			},
		})
	})
	if !ok {
		log.Warnw("Lock",
			logging.Stringer("LockRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Lock",
			logging.Stringer("LockRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &lockv1.LockResponse{
		Version: uint64(output.Index),
	}
	log.Debugw("Lock",
		logging.Stringer("LockRequest", request),
		logging.Stringer("LockResponse", response))
	return response, nil
}

func (s *lockClient) Unlock(ctx context.Context, request *lockv1.UnlockRequest) (*lockv1.UnlockResponse, error) {
	log.Debugw("Unlock",
		logging.Stringer("UnlockRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Unlock",
			logging.Stringer("UnlockRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Unlock",
			logging.Stringer("UnlockRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	query := client.Proposal[*lockprotocolv1.ReleaseResponse](primitive)
	_, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*lockprotocolv1.ReleaseResponse, error) {
		return lockprotocolv1.NewLockClient(conn).Release(ctx, &lockprotocolv1.ReleaseRequest{
			Headers:      headers,
			ReleaseInput: &lockprotocolv1.ReleaseInput{},
		})
	})
	if !ok {
		log.Warnw("Unlock",
			logging.Stringer("UnlockRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Unlock",
			logging.Stringer("UnlockRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &lockv1.UnlockResponse{}
	log.Debugw("Unlock",
		logging.Stringer("UnlockRequest", request),
		logging.Stringer("UnlockResponse", response))
	return response, nil
}

func (s *lockClient) GetLock(ctx context.Context, request *lockv1.GetLockRequest) (*lockv1.GetLockResponse, error) {
	log.Debugw("GetLock",
		logging.Stringer("GetLockRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("GetLock",
			logging.Stringer("GetLockRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("GetLock",
			logging.Stringer("GetLockRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	query := client.Query[*lockprotocolv1.GetResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*lockprotocolv1.GetResponse, error) {
		return lockprotocolv1.NewLockClient(conn).Get(ctx, &lockprotocolv1.GetRequest{
			Headers:  headers,
			GetInput: &lockprotocolv1.GetInput{},
		})
	})
	if !ok {
		log.Warnw("GetLock",
			logging.Stringer("GetLockRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("GetLock",
			logging.Stringer("GetLockRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &lockv1.GetLockResponse{
		Version: uint64(output.Index),
	}
	log.Debugw("GetLock",
		logging.Stringer("GetLockRequest", request),
		logging.Stringer("GetLockResponse", response))
	return response, nil
}

var _ lockv1.LockServer = (*lockClient)(nil)

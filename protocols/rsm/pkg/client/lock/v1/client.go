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
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtimelockv1 "github.com/atomix/atomix/runtime/pkg/runtime/lock/v1"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

func NewLock(protocol *client.Protocol, id runtimev1.PrimitiveID) *LockSession {
	return &LockSession{
		Protocol: protocol,
		id:       id,
	}
}

type LockSession struct {
	*client.Protocol
	id runtimev1.PrimitiveID
}

func (s *LockSession) Open(ctx context.Context) error {
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
		Type:        lockv1.PrimitiveType,
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

func (s *LockSession) Close(ctx context.Context) error {
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

func (s *LockSession) Lock(ctx context.Context, request *lockv1.LockRequest) (*lockv1.LockResponse, error) {
	log.Debugw("Lock",
		logging.Trunc128("LockRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Lock",
			logging.Trunc128("LockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Lock",
			logging.Trunc128("LockRequest", request),
			logging.Error("Error", err))
		return nil, err
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
			logging.Trunc128("LockRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Lock",
			logging.Trunc128("LockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &lockv1.LockResponse{
		Version: uint64(output.Index),
	}
	log.Debugw("Lock",
		logging.Trunc128("LockRequest", request),
		logging.Trunc128("LockResponse", response))
	return response, nil
}

func (s *LockSession) Unlock(ctx context.Context, request *lockv1.UnlockRequest) (*lockv1.UnlockResponse, error) {
	log.Debugw("Unlock",
		logging.Trunc128("UnlockRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Unlock",
			logging.Trunc128("UnlockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Unlock",
			logging.Trunc128("UnlockRequest", request),
			logging.Error("Error", err))
		return nil, err
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
			logging.Trunc128("UnlockRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Unlock",
			logging.Trunc128("UnlockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &lockv1.UnlockResponse{}
	log.Debugw("Unlock",
		logging.Trunc128("UnlockRequest", request),
		logging.Trunc128("UnlockResponse", response))
	return response, nil
}

func (s *LockSession) GetLock(ctx context.Context, request *lockv1.GetLockRequest) (*lockv1.GetLockResponse, error) {
	log.Debugw("GetLock",
		logging.Trunc128("GetLockRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("GetLock",
			logging.Trunc128("GetLockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("GetLock",
			logging.Trunc128("GetLockRequest", request),
			logging.Error("Error", err))
		return nil, err
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
			logging.Trunc128("GetLockRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("GetLock",
			logging.Trunc128("GetLockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &lockv1.GetLockResponse{
		Version: uint64(output.Index),
	}
	log.Debugw("GetLock",
		logging.Trunc128("GetLockRequest", request),
		logging.Trunc128("GetLockResponse", response))
	return response, nil
}

var _ runtimelockv1.LockProxy = (*LockSession)(nil)

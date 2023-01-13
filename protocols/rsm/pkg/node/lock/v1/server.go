// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	lockprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/lock/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/node"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

func RegisterServer(node *node.Node) {
	node.RegisterService(func(server *grpc.Server) {
		lockprotocolv1.RegisterLockServer(server, NewLockServer(node))
	})
}

var serverCodec = node.NewCodec[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput](
	func(input *lockprotocolv1.LockInput) ([]byte, error) {
		return proto.Marshal(input)
	},
	func(bytes []byte) (*lockprotocolv1.LockOutput, error) {
		output := &lockprotocolv1.LockOutput{}
		if err := proto.Unmarshal(bytes, output); err != nil {
			return nil, err
		}
		return output, nil
	})

func NewLockServer(protocol node.Protocol) lockprotocolv1.LockServer {
	return &lockServer{
		handler: node.NewHandler[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput](protocol, serverCodec),
	}
}

type lockServer struct {
	handler node.Handler[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput]
}

func (s *lockServer) Acquire(ctx context.Context, request *lockprotocolv1.AcquireRequest) (*lockprotocolv1.AcquireResponse, error) {
	log.Debugw("Acquire",
		logging.Trunc128("AcquireRequest", request))
	input := &lockprotocolv1.LockInput{
		Input: &lockprotocolv1.LockInput_Acquire{
			Acquire: request.AcquireInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Acquire",
			logging.Trunc128("AcquireRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &lockprotocolv1.AcquireResponse{
		Headers:       headers,
		AcquireOutput: output.GetAcquire(),
	}
	log.Debugw("Acquire",
		logging.Trunc128("AcquireRequest", request),
		logging.Trunc128("AcquireResponse", response))
	return response, nil
}

func (s *lockServer) Release(ctx context.Context, request *lockprotocolv1.ReleaseRequest) (*lockprotocolv1.ReleaseResponse, error) {
	log.Debugw("Release",
		logging.Trunc128("ReleaseRequest", request))
	input := &lockprotocolv1.LockInput{
		Input: &lockprotocolv1.LockInput_Release{
			Release: request.ReleaseInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Release",
			logging.Trunc128("ReleaseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &lockprotocolv1.ReleaseResponse{
		Headers:       headers,
		ReleaseOutput: output.GetRelease(),
	}
	log.Debugw("Release",
		logging.Trunc128("ReleaseRequest", request),
		logging.Trunc128("ReleaseResponse", response))
	return response, nil
}

func (s *lockServer) Get(ctx context.Context, request *lockprotocolv1.GetRequest) (*lockprotocolv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request))
	input := &lockprotocolv1.LockInput{
		Input: &lockprotocolv1.LockInput_Get{
			Get: request.GetInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Get",
			logging.Trunc128("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &lockprotocolv1.GetResponse{
		Headers:   headers,
		GetOutput: output.GetGet(),
	}
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request),
		logging.Trunc128("GetResponse", response))
	return response, nil
}

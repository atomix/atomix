// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/protocol/node"
	"github.com/atomix/runtime/sdk/pkg/stringer"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const truncLen = 200

func RegisterServer(node *node.Node) {
	node.RegisterService(func(server *grpc.Server) {
		RegisterLockServer(server, NewLockServer(node))
	})
}

var serverCodec = node.NewCodec[*LockInput, *LockOutput](
	func(input *LockInput) ([]byte, error) {
		return proto.Marshal(input)
	},
	func(bytes []byte) (*LockOutput, error) {
		output := &LockOutput{}
		if err := proto.Unmarshal(bytes, output); err != nil {
			return nil, err
		}
		return output, nil
	})

func NewLockServer(protocol node.Protocol) LockServer {
	return &lockServer{
		handler: node.NewHandler[*LockInput, *LockOutput](protocol, serverCodec),
	}
}

type lockServer struct {
	handler node.Handler[*LockInput, *LockOutput]
}

func (s *lockServer) Acquire(ctx context.Context, request *AcquireRequest) (*AcquireResponse, error) {
	log.Debugw("Acquire",
		logging.Stringer("AcquireRequest", stringer.Truncate(request, truncLen)))
	input := &LockInput{
		Input: &LockInput_Acquire{
			Acquire: request.AcquireInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Acquire",
			logging.Stringer("AcquireRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &AcquireResponse{
		Headers:       headers,
		AcquireOutput: output.GetAcquire(),
	}
	log.Debugw("Acquire",
		logging.Stringer("AcquireRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("AcquireResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *lockServer) Release(ctx context.Context, request *ReleaseRequest) (*ReleaseResponse, error) {
	log.Debugw("Release",
		logging.Stringer("ReleaseRequest", stringer.Truncate(request, truncLen)))
	input := &LockInput{
		Input: &LockInput_Release{
			Release: request.ReleaseInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Release",
			logging.Stringer("ReleaseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &ReleaseResponse{
		Headers:       headers,
		ReleaseOutput: output.GetRelease(),
	}
	log.Debugw("Release",
		logging.Stringer("ReleaseRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("ReleaseResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *lockServer) Get(ctx context.Context, request *GetRequest) (*GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)))
	input := &LockInput{
		Input: &LockInput_Get{
			Get: request.GetInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &GetResponse{
		Headers:   headers,
		GetOutput: output.GetGet(),
	}
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("GetResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

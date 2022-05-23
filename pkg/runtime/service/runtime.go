// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/runtime"
)

func newRuntimeServer(runtime runtime.Runtime) runtimev1.RuntimeServer {
	return &runtimeServer{
		runtime: runtime,
	}
}

type runtimeServer struct {
	runtime runtime.Runtime
}

func (s *runtimeServer) GetRuntimeInfo(ctx context.Context, request *runtimev1.GetRuntimeInfoRequest) (*runtimev1.GetRuntimeInfoResponse, error) {
	log.Debugw("GetRuntimeInfo",
		logging.Stringer("GetRuntimeInfoRequest", request))
	response := &runtimev1.GetRuntimeInfoResponse{
		RuntimeInfo: runtimev1.RuntimeInfo{
			Version: string(s.runtime.Version()),
		},
	}
	log.Debugw("GetRuntimeInfo",
		logging.Stringer("GetRuntimeInfoResponse", response))
	return response, nil
}

var _ runtimev1.RuntimeServer = (*runtimeServer)(nil)

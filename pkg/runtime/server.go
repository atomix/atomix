// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/version"
	"io"
	"os"
)

func newRuntimeServer(runtime *Runtime) runtimev1.RuntimeServer {
	return &runtimeServer{
		runtime: runtime,
	}
}

type runtimeServer struct {
	runtime *Runtime
}

func (s *runtimeServer) GetRuntimeInfo(ctx context.Context, request *runtimev1.GetRuntimeInfoRequest) (*runtimev1.GetRuntimeInfoResponse, error) {
	log.Debugw("GetRuntimeInfo",
		logging.Stringer("GetRuntimeInfoRequest", request))
	response := &runtimev1.GetRuntimeInfoResponse{
		RuntimeInfo: runtimev1.RuntimeInfo{
			Version: version.Version(),
		},
	}
	log.Debugw("GetRuntimeInfo",
		logging.Stringer("GetRuntimeInfoResponse", response))
	return response, nil
}

func (s *runtimeServer) ConnectCluster(ctx context.Context, request *runtimev1.ConnectClusterRequest) (*runtimev1.ConnectClusterResponse, error) {
	log.Debugw("ConnectCluster",
		logging.Stringer("ConnectClusterRequest", request))
	err := s.runtime.addConnection(request.Cluster, request.Connection)
	if err != nil {
		log.Errorw("ConnectCluster",
			logging.Stringer("ConnectClusterRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &runtimev1.ConnectClusterResponse{}
	log.Debugw("ConnectCluster",
		logging.Stringer("ConnectClusterResponse", response))
	return response, nil
}

func (s *runtimeServer) ConfigureCluster(ctx context.Context, request *runtimev1.ConfigureClusterRequest) (*runtimev1.ConfigureClusterResponse, error) {
	log.Debugw("ConfigureCluster",
		logging.Stringer("ConfigureClusterRequest", request))
	err := s.runtime.configureConnection(ctx, request.Cluster, request.Connection)
	if err != nil {
		log.Errorw("ConfigureCluster",
			logging.Stringer("ConfigureClusterRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &runtimev1.ConfigureClusterResponse{}
	log.Debugw("ConfigureCluster",
		logging.Stringer("ConfigureClusterResponse", response))
	return response, nil
}

func (s *runtimeServer) DisconnectCluster(ctx context.Context, request *runtimev1.DisconnectClusterRequest) (*runtimev1.DisconnectClusterResponse, error) {
	log.Debugw("DisconnectCluster",
		logging.Stringer("DisconnectClusterRequest", request))
	err := s.runtime.closeConnection(ctx, request.Cluster)
	if err != nil {
		log.Errorw("DisconnectCluster",
			logging.Stringer("DisconnectClusterRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &runtimev1.DisconnectClusterResponse{}
	log.Debugw("DisconnectCluster",
		logging.Stringer("DisconnectClusterResponse", response))
	return response, nil
}

func (s *runtimeServer) InstallDriver(server runtimev1.Runtime_InstallDriverServer) error {
	initRequest, err := server.Recv()
	if err != nil {
		log.Errorw("InstallDriver",
			logging.Stringer("InstallDriverRequest", initRequest),
			logging.Error("Error", err))
		return err
	}

	log.Debugw("InstallDriver",
		logging.Stringer("InstallDriverRequest", initRequest))

	var plugin *DriverPlugin
	switch r := initRequest.Driver.(type) {
	case *runtimev1.InstallDriverRequest_Header:
		plugin = s.runtime.drivers.Get(r.Header.Driver.Name, r.Header.Driver.Version)
	default:
		return errors.ToProto(errors.NewInvalid("expected header request"))
	}

	writer, err := plugin.Create()
	if err != nil {
		if !os.IsExist(err) {
			err := errors.NewInternal(err.Error())
			log.Errorw("InstallDriver",
				logging.Stringer("InstallDriverRequest", initRequest),
				logging.Error("Error", err))
			return errors.ToProto(err)
		}
		response := &runtimev1.InstallDriverResponse{}
		log.Debugw("InstallDriver",
			logging.Stringer("InstallDriverResponse", response))
		return server.SendAndClose(response)
	}

	for {
		request, err := server.Recv()
		if err == io.EOF {
			err := errors.NewInvalid("expected trailer request")
			log.Errorw("InstallDriver",
				logging.Stringer("InstallDriverRequest", initRequest),
				logging.Error("Error", err))
			return errors.ToProto(err)
		}
		if err != nil {
			log.Errorw("InstallDriver",
				logging.Stringer("InstallDriverRequest", initRequest),
				logging.Error("Error", err))
			return err
		}

		log.Debugw("InstallDriver",
			logging.Stringer("InstallDriverRequest", request))

		switch r := request.Driver.(type) {
		case *runtimev1.InstallDriverRequest_Chunk:
			if i, err := writer.Write(r.Chunk.Data); err != nil {
				return errors.ToProto(errors.NewFault(err.Error()))
			} else {
				log.Debugf("Wrote %d bytes", i)
			}
		case *runtimev1.InstallDriverRequest_Trailer:
			log.Debugw("InstallDriver",
				logging.Stringer("InstallDriverRequest", request))
			if err := writer.Close(r.Trailer.Checksum); err != nil {
				return errors.ToProto(err)
			}
			response := &runtimev1.InstallDriverResponse{}
			log.Debugw("InstallDriver",
				logging.Stringer("InstallDriverResponse", response))
			return server.SendAndClose(response)
		default:
			err := errors.NewInvalid("expected chunk or trailer request")
			log.Errorw("InstallDriver",
				logging.Stringer("InstallDriverRequest", initRequest),
				logging.Error("Error", err))
			return errors.ToProto(err)
		}
	}
}

var _ runtimev1.RuntimeServer = (*runtimeServer)(nil)

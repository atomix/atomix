// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package service

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/driver"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/runtime/plugin"
	"github.com/atomix/runtime/pkg/atomix/store"
	"io"
	"os"
)

func newDriverServiceServer(drivers store.Store[*runtimev1.DriverId, *runtimev1.Driver], plugins *plugin.Cache[driver.Driver]) runtimev1.DriverServiceServer {
	return &driverServiceServer{
		drivers: drivers,
		plugins: plugins,
	}
}

type driverServiceServer struct {
	drivers store.Store[*runtimev1.DriverId, *runtimev1.Driver]
	plugins *plugin.Cache[driver.Driver]
}

func (s *driverServiceServer) InstallDriver(server runtimev1.DriverService_InstallDriverServer) error {
	initRequest, err := server.Recv()
	if err != nil {
		log.Errorw("InstallDriver",
			logging.Stringer("InstallDriverRequest", initRequest),
			logging.Error("Error", err))
		return err
	}

	log.Debugw("InstallDriver",
		logging.Stringer("InstallDriverRequest", initRequest))

	var info *runtimev1.Driver
	var plugin *plugin.Plugin[driver.Driver]
	switch r := initRequest.Driver.(type) {
	case *runtimev1.InstallDriverRequest_Header:
		info = r.Header.Driver
		plugin = s.plugins.Get(r.Header.Driver.ID.Name, r.Header.Driver.ID.Version)
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
				log.Errorw("InstallDriver",
					logging.Stringer("InstallDriverRequest", initRequest),
					logging.Error("Error", err))
				return errors.ToProto(err)
			}
			if err := s.drivers.Create(info); err != nil {
				log.Errorw("InstallDriver",
					logging.Stringer("InstallDriverRequest", initRequest),
					logging.Error("Error", err))
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

var _ runtimev1.DriverServiceServer = (*driverServiceServer)(nil)

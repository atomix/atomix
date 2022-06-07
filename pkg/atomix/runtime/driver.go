// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/driver"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/runtime/plugin"
	"io"
	"os"
	"sync"
)

func newDriverRepository(cacheDir string, drivers ...driver.Driver) *driverRepository {
	driversMap := make(map[string]map[string]driver.Driver)
	for _, d := range drivers {
		versionsMap, ok := driversMap[d.Name()]
		if !ok {
			versionsMap = make(map[string]driver.Driver)
			driversMap[d.Name()] = versionsMap
		}
		versionsMap[d.Version()] = d
	}
	return &driverRepository{
		plugins: plugin.NewCache[driver.Driver](cacheDir),
		drivers: driversMap,
	}
}

type driverRepository struct {
	plugins *plugin.Cache[driver.Driver]
	drivers map[string]map[string]driver.Driver
	mu      sync.Mutex
}

func (r *driverRepository) get(name, version string) (driver.Driver, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	versions, ok := r.drivers[name]
	if ok {
		driver, ok := versions[version]
		if ok {
			return driver, nil
		}
	}

	d, err := r.plugins.Get(name, version).Load()
	if err != nil {
		return nil, err
	}

	versions, ok = r.drivers[name]
	if !ok {
		versions = make(map[string]driver.Driver)
		r.drivers[name] = versions
	}
	versions[version] = d
	return d, nil
}

func newDriverServiceServer(plugins *plugin.Cache[driver.Driver]) runtimev1.DriverServiceServer {
	return &driverServiceServer{
		plugins: plugins,
	}
}

type driverServiceServer struct {
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

	var plugin *plugin.Plugin[driver.Driver]
	switch r := initRequest.Driver.(type) {
	case *runtimev1.InstallDriverRequest_Header:
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

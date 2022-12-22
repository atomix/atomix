// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"encoding/json"
	"fmt"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var log = logging.GetLogger()

var driverID = runtimev1.DriverID{
	Name:    "Etcd",
	Version: "v3",
}

func New() runtime.Driver {
	return &etcdDriver{}
}

type etcdDriver struct{}

func (d *etcdDriver) ID() runtimev1.DriverID {
	return driverID
}

func (d *etcdDriver) Connect(ctx context.Context, spec runtimev1.ConnSpec) (runtime.Conn, error) {
	var config clientv3.Config
	if err := json.Unmarshal(spec.Config, &config); err != nil {
		log.Error(err)
		return nil, err
	}
	client, err := clientv3.New(config)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return newConn(client)
}

func (d *etcdDriver) String() string {
	return fmt.Sprintf("%s/%s", driverID.Name, driverID.Version)
}

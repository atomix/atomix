// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"fmt"
	runtimev1 "github.com/atomix/api/pkg/atomix/runtime/v1"
	"github.com/atomix/sdk/pkg/atom"
	"github.com/atomix/sdk/pkg/controller"
	"github.com/atomix/sdk/pkg/driver"
	"github.com/atomix/sdk/pkg/logging"
	"google.golang.org/grpc"
	"net"
	"sync"
)

var log = logging.GetLogger()

func New(controller *controller.Client, atoms *atom.Repository, drivers *driver.Repository, opts ...Option) *Runtime {
	var options Options
	options.apply(opts...)
	return &Runtime{
		options:    options,
		controller: controller,
		atoms:      atoms,
		drivers:    drivers,
		server:     grpc.NewServer(),
		conns:      make(map[string]driver.Conn),
	}
}

type Runtime struct {
	options    Options
	controller *controller.Client
	atoms      *atom.Repository
	drivers    *driver.Repository
	server     *grpc.Server
	conns      map[string]driver.Conn
	mu         sync.RWMutex
}

func (r *Runtime) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := r.registerAtoms(ctx); err != nil {
		return err
	}

	address := fmt.Sprintf("%s:%d", r.options.Host, r.options.Port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	go func() {
		if err := r.server.Serve(lis); err != nil {
			panic(err)
		}
	}()
	return nil
}

func (r *Runtime) registerAtoms(ctx context.Context) error {
	wg := &sync.WaitGroup{}
	errCh := make(chan error, len(r.options.Atoms))
	for _, atom := range r.options.Atoms {
		wg.Add(1)
		go func(name, version string) {
			defer wg.Done()
			atom, err := r.atoms.Load(ctx, name, version)
			if err != nil {
				errCh <- err
			} else {
				atom.Register(r.server, r.connect)
			}
		}(atom.Name, atom.Version)
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()
	return <-errCh
}

func (r *Runtime) connect(ctx context.Context, name string) (driver.Conn, error) {
	r.mu.RLock()
	conn, ok := r.conns[name]
	r.mu.RUnlock()
	if ok {
		return conn, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok = r.conns[name]
	if ok {
		return conn, nil
	}

	watchCh := make(chan *runtimev1.ConnectionInfo)
	if err := r.controller.Connect(context.Background(), name, watchCh); err != nil {
		return nil, err
	}

	select {
	case info, ok := <-watchCh:
		if !ok {
			return nil, context.Canceled
		}

		driver, err := r.drivers.Load(ctx, info.Driver.Name, info.Driver.Version)
		if err != nil {
			return nil, err
		}

		conn, err := driver.Connect(ctx, info.Config)
		if err != nil {
			return nil, err
		}

		go func() {
			for configuration := range watchCh {
				if err := conn.Configure(context.Background(), configuration.Config); err != nil {
					log.Error(err)
				}
			}

			r.mu.Lock()
			delete(r.conns, name)
			r.mu.Unlock()

			if err := conn.Close(context.Background()); err != nil {
				log.Error(err)
			}
		}()

		r.conns[name] = conn
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *Runtime) Shutdown() error {
	if err := r.controller.Close(); err != nil {
		return err
	}
	return nil
}

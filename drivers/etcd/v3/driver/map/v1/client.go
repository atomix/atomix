// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/api/errors"
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	runtimemapv1 "github.com/atomix/atomix/runtime/pkg/runtime/map/v1"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/client/v3/namespace"
	"sync/atomic"
)

func NewMap(session *concurrency.Session, id runtimev1.PrimitiveID) (runtimemapv1.MapProxy, error) {
	prefix := fmt.Sprintf("%s/", id.Name)
	return &etcdMap{
		session:  session,
		kv:       namespace.NewKV(session.Client(), prefix),
		lease:    namespace.NewLease(session.Client(), prefix),
		watcher:  namespace.NewWatcher(session.Client(), prefix),
		revision: &etcdRevision{},
	}, nil
}

type etcdMap struct {
	session  *concurrency.Session
	kv       clientv3.KV
	lease    clientv3.Lease
	watcher  clientv3.Watcher
	revision *etcdRevision
}

func (c *etcdMap) Create(ctx context.Context, request *mapv1.CreateRequest) (*mapv1.CreateResponse, error) {
	prefix := fmt.Sprintf("%s/", request.ID.Name)
	c.kv = namespace.NewKV(c.session.Client(), prefix)
	c.lease = namespace.NewLease(c.session.Client(), prefix)
	c.watcher = namespace.NewWatcher(c.session.Client(), prefix)
	return &mapv1.CreateResponse{}, nil
}

func (c *etcdMap) Size(ctx context.Context, request *mapv1.SizeRequest) (*mapv1.SizeResponse, error) {
	response, err := c.kv.Get(ctx, "", clientv3.WithCountOnly())
	if err != nil {
		return nil, convertError(err)
	}
	return &mapv1.SizeResponse{
		Size_: uint32(response.Count),
	}, nil
}

func (c *etcdMap) Put(ctx context.Context, request *mapv1.PutRequest) (*mapv1.PutResponse, error) {
	var opts []clientv3.OpOption
	opts = append(opts, clientv3.WithPrevKV())
	if request.TTL != nil {
		response, err := c.lease.Grant(ctx, int64(request.TTL.Seconds()))
		if err != nil {
			return nil, convertError(err)
		}
		opts = append(opts, clientv3.WithLease(response.ID))
		c.revision.Update(response.Revision)
	}

	if request.PrevVersion == 0 {
		response, err := c.kv.Put(ctx, request.Key, string(request.Value), opts...)
		if err != nil {
			return nil, convertError(err)
		}
		c.revision.Update(response.Header.Revision)

		if response.PrevKv == nil {
			return &mapv1.PutResponse{
				Version: uint64(response.Header.Revision),
			}, nil
		}

		return &mapv1.PutResponse{
			Version: uint64(response.Header.Revision),
			PrevValue: &mapv1.VersionedValue{
				Value:   response.PrevKv.Value,
				Version: uint64(response.PrevKv.ModRevision),
			},
		}, nil
	}

	response, err := c.kv.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(request.Key), "=", int64(request.PrevVersion))).
		Then(clientv3.OpPut(request.Key, string(request.Value), opts...)).
		Commit()
	if err != nil {
		return nil, convertError(err)
	}

	if !response.Succeeded {
		return nil, errors.NewConflict("version has changed")
	}

	prev := response.Responses[0].GetResponsePut().PrevKv
	return &mapv1.PutResponse{
		Version: uint64(response.Header.Revision),
		PrevValue: &mapv1.VersionedValue{
			Value:   prev.Value,
			Version: uint64(prev.ModRevision),
		},
	}, nil
}

func (c *etcdMap) Insert(ctx context.Context, request *mapv1.InsertRequest) (*mapv1.InsertResponse, error) {
	var opts []clientv3.OpOption
	opts = append(opts, clientv3.WithPrevKV())
	if request.TTL != nil {
		response, err := c.lease.Grant(ctx, int64(request.TTL.Seconds()))
		if err != nil {
			return nil, convertError(err)
		}
		opts = append(opts, clientv3.WithLease(response.ID))
		c.revision.Update(response.Revision)
	}

	response, err := c.kv.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(request.Key), "=", int64(0))).
		Then(clientv3.OpPut(request.Key, string(request.Value), opts...)).
		Commit()
	if err != nil {
		return nil, convertError(err)
	}
	c.revision.Update(response.Header.Revision)

	if !response.Succeeded {
		return nil, errors.NewAlreadyExists("key %s already exists", request.Key)
	}

	return &mapv1.InsertResponse{
		Version: uint64(response.Header.Revision),
	}, nil
}

func (c *etcdMap) Update(ctx context.Context, request *mapv1.UpdateRequest) (*mapv1.UpdateResponse, error) {
	txn := c.kv.Txn(ctx)
	if request.PrevVersion == 0 {
		txn = txn.If(clientv3.Compare(clientv3.CreateRevision(request.Key), ">", int64(0)))
	} else {
		txn = txn.If(clientv3.Compare(clientv3.ModRevision(request.Key), "=", int64(request.PrevVersion)))
	}

	var opts []clientv3.OpOption
	opts = append(opts, clientv3.WithPrevKV())
	if request.TTL != nil {
		response, err := c.lease.Grant(ctx, int64(request.TTL.Seconds()))
		if err != nil {
			return nil, convertError(err)
		}
		opts = append(opts, clientv3.WithLease(response.ID))
		c.revision.Update(response.Revision)
	}

	response, err := txn.Then(clientv3.OpPut(request.Key, string(request.Value), opts...)).Commit()
	if err != nil {
		return nil, convertError(err)
	}

	if !response.Succeeded {
		return nil, errors.NewConflict("version has changed")
	}

	prev := response.Responses[0].GetResponsePut().PrevKv
	return &mapv1.UpdateResponse{
		Version: uint64(response.Header.Revision),
		PrevValue: mapv1.VersionedValue{
			Value:   prev.Value,
			Version: uint64(prev.ModRevision),
		},
	}, nil
}

func (c *etcdMap) Get(ctx context.Context, request *mapv1.GetRequest) (*mapv1.GetResponse, error) {
	response, err := c.kv.Get(ctx, request.Key, clientv3.WithRev(c.revision.Get()))
	if err != nil {
		return nil, err
	}
	c.revision.Update(response.Header.Revision)

	if len(response.Kvs) == 0 {
		return nil, errors.NewNotFound("key %s not found", request.Key)
	}

	kv := response.Kvs[0]
	return &mapv1.GetResponse{
		Value: mapv1.VersionedValue{
			Value:   kv.Value,
			Version: uint64(kv.ModRevision),
		},
	}, nil
}

func (c *etcdMap) Remove(ctx context.Context, request *mapv1.RemoveRequest) (*mapv1.RemoveResponse, error) {
	if request.PrevVersion == 0 {
		response, err := c.kv.Delete(ctx, request.Key)
		if err != nil {
			return nil, convertError(err)
		}
		c.revision.Update(response.Header.Revision)

		if len(response.PrevKvs) == 0 {
			return nil, errors.NewNotFound("key %s not found", request.Key)
		}

		prev := response.PrevKvs[0]
		return &mapv1.RemoveResponse{
			Value: mapv1.VersionedValue{
				Value:   prev.Value,
				Version: uint64(prev.ModRevision),
			},
		}, nil
	}

	response, err := c.kv.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(request.Key), "=", int64(request.PrevVersion))).
		Then(clientv3.OpDelete(request.Key)).
		Commit()
	if err != nil {
		return nil, convertError(err)
	}

	if !response.Succeeded {
		return nil, errors.NewConflict("version has changed")
	}

	prev := response.Responses[0].GetResponseDeleteRange().PrevKvs[0]
	return &mapv1.RemoveResponse{
		Value: mapv1.VersionedValue{
			Value:   prev.Value,
			Version: uint64(prev.ModRevision),
		},
	}, nil
}

func (c *etcdMap) Clear(ctx context.Context, request *mapv1.ClearRequest) (*mapv1.ClearResponse, error) {
	response, err := c.kv.Delete(ctx, "", clientv3.WithPrefix())
	if err != nil {
		return nil, convertError(err)
	}
	c.revision.Update(response.Header.Revision)
	return &mapv1.ClearResponse{}, nil
}

func (c *etcdMap) Lock(ctx context.Context, request *mapv1.LockRequest) (*mapv1.LockResponse, error) {
	return nil, errors.NewNotSupported("Lock not supported by etcd driver")
}

func (c *etcdMap) Unlock(ctx context.Context, request *mapv1.UnlockRequest) (*mapv1.UnlockResponse, error) {
	return nil, errors.NewNotSupported("Unlock not supported by etcd driver")
}

func (c *etcdMap) Events(request *mapv1.EventsRequest, server mapv1.Map_EventsServer) error {
	ch := c.watcher.Watch(server.Context(), request.Key)
	for response := range ch {
		c.revision.Update(response.Header.Revision)
		for _, event := range response.Events {
			var mapEvent mapv1.Event
			switch event.Type {
			case mvccpb.PUT:
				if event.PrevKv == nil {
					mapEvent = mapv1.Event{
						Key: string(event.Kv.Key),
						Event: &mapv1.Event_Inserted_{
							Inserted: &mapv1.Event_Inserted{
								Value: mapv1.VersionedValue{
									Version: uint64(event.Kv.CreateRevision),
									Value:   event.Kv.Value,
								},
							},
						},
					}
				} else {
					mapEvent = mapv1.Event{
						Key: string(event.Kv.Key),
						Event: &mapv1.Event_Updated_{
							Updated: &mapv1.Event_Updated{
								Value: mapv1.VersionedValue{
									Version: uint64(event.Kv.ModRevision),
									Value:   event.Kv.Value,
								},
								PrevValue: mapv1.VersionedValue{
									Version: uint64(event.PrevKv.ModRevision),
									Value:   event.PrevKv.Value,
								},
							},
						},
					}
				}
			case mvccpb.DELETE:
				mapEvent = mapv1.Event{
					Key: string(event.Kv.Key),
					Event: &mapv1.Event_Removed_{
						Removed: &mapv1.Event_Removed{
							Value: mapv1.VersionedValue{
								Version: uint64(event.PrevKv.ModRevision),
								Value:   event.PrevKv.Value,
							},
							Expired: event.PrevKv.Lease != 0,
						},
					},
				}
			}

			mapResponse := &mapv1.EventsResponse{
				Event: mapEvent,
			}
			if err := server.Send(mapResponse); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *etcdMap) Entries(request *mapv1.EntriesRequest, server mapv1.Map_EntriesServer) error {
	if request.Watch {
		return errors.NewNotSupported("watch flag not supported by etcd driver")
	}

	response, err := c.kv.Get(server.Context(), "", clientv3.WithPrefix())
	if err != nil {
		return convertError(err)
	}
	c.revision.Update(response.Header.Revision)
	for _, kv := range response.Kvs {
		err := server.Send(&mapv1.EntriesResponse{
			Entry: mapv1.Entry{
				Key: string(kv.Key),
				Value: &mapv1.VersionedValue{
					Value:   kv.Value,
					Version: uint64(kv.ModRevision),
				},
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *etcdMap) Close(ctx context.Context) error {
	return nil
}

func convertError(err error) error {
	if err == nil {
		return nil
	}
	switch err {
	case rpctypes.ErrAuthFailed:
		return errors.NewUnauthorized(err.Error())
	case rpctypes.ErrKeyNotFound:
		return errors.NewNotFound(err.Error())
	case rpctypes.ErrDuplicateKey:
		return errors.NewAlreadyExists(err.Error())
	default:
		return errors.NewUnknown(err.Error())
	}
}

type etcdRevision struct {
	revision atomic.Int64
}

func (i *etcdRevision) Update(update int64) {
	for {
		current := i.revision.Load()
		if update <= current {
			return
		}
		if i.revision.CompareAndSwap(current, update) {
			return
		}
	}
}

func (i *etcdRevision) Get() int64 {
	return i.revision.Load()
}

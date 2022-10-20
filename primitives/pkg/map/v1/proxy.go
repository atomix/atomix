// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"container/list"
	"context"
	mapv1 "github.com/atomix/runtime/api/atomix/runtime/map/v1"
	"github.com/atomix/runtime/sdk/pkg/async"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"github.com/atomix/runtime/sdk/pkg/protocol/client"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"github.com/atomix/runtime/sdk/pkg/stringer"
	"google.golang.org/grpc"
	"io"
	"sync"
	"time"
)

func NewMapProxy(protocol *client.Protocol, spec runtime.PrimitiveSpec) (mapv1.MapServer, error) {
	proxy := newMapProxy(protocol, spec)
	var config MapConfig
	if err := spec.UnmarshalConfig(&config); err != nil {
		return nil, err
	}
	if config.Cache.Enabled {
		proxy = newCachingMapProxy(proxy, config.Cache)
	}
	return proxy, nil
}

func newMapProxy(protocol *client.Protocol, spec runtime.PrimitiveSpec) mapv1.MapServer {
	return &mapProxy{
		Protocol:      protocol,
		PrimitiveSpec: spec,
	}
}

type mapProxy struct {
	*client.Protocol
	runtime.PrimitiveSpec
}

func (s *mapProxy) Create(ctx context.Context, request *mapv1.CreateRequest) (*mapv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)))
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		session, err := partition.GetSession(ctx)
		if err != nil {
			return err
		}
		return session.CreatePrimitive(ctx, s.PrimitiveSpec)
	})
	if err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &mapv1.CreateResponse{}
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("CreateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *mapProxy) Close(ctx context.Context, request *mapv1.CloseRequest) (*mapv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)))
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		session, err := partition.GetSession(ctx)
		if err != nil {
			return err
		}
		return session.ClosePrimitive(ctx, request.ID.Name)
	})
	if err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &mapv1.CloseResponse{}
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("CloseResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *mapProxy) Size(ctx context.Context, request *mapv1.SizeRequest) (*mapv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)))
	partitions := s.Partitions()
	sizes, err := async.ExecuteAsync[int](len(partitions), func(i int) (int, error) {
		partition := partitions[i]
		session, err := partition.GetSession(ctx)
		if err != nil {
			log.Warnw("Size",
				logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return 0, err
		}
		primitive, err := session.GetPrimitive(request.ID.Name)
		if err != nil {
			log.Warnw("Size",
				logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return 0, err
		}
		query := client.Query[*SizeResponse](primitive)
		output, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*SizeResponse, error) {
			return NewMapClient(conn).Size(ctx, &SizeRequest{
				Headers:   headers,
				SizeInput: &SizeInput{},
			})
		})
		if err != nil {
			log.Warnw("Size",
				logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return 0, err
		}
		return int(output.Size_), nil
	})
	if err != nil {
		return nil, errors.ToProto(err)
	}
	var size int
	for _, s := range sizes {
		size += s
	}
	response := &mapv1.SizeResponse{
		Size_: uint32(size),
	}
	log.Debugw("Size",
		logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("SizeResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *mapProxy) Put(ctx context.Context, request *mapv1.PutRequest) (*mapv1.PutResponse, error) {
	log.Debugw("Put",
		logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*PutResponse](primitive)
	output, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*PutResponse, error) {
		input := &PutRequest{
			Headers: headers,
			PutInput: &PutInput{
				Key:       request.Key,
				Value:     request.Value,
				TTL:       request.TTL,
				PrevIndex: protocol.Index(request.PrevVersion),
			},
		}
		return NewMapClient(conn).Put(ctx, input)
	})
	if err != nil {
		log.Warnw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &mapv1.PutResponse{
		Version: uint64(output.Index),
	}
	if output.PrevValue != nil {
		response.PrevValue = &mapv1.VersionedValue{
			Value:   output.PrevValue.Value,
			Version: uint64(output.PrevValue.Index),
		}
	}
	log.Debugw("Put",
		logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("PutResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *mapProxy) Insert(ctx context.Context, request *mapv1.InsertRequest) (*mapv1.InsertResponse, error) {
	log.Debugw("Insert",
		logging.Stringer("InsertRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Insert",
			logging.Stringer("InsertRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Insert",
			logging.Stringer("InsertRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*InsertResponse](primitive)
	output, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*InsertResponse, error) {
		return NewMapClient(conn).Insert(ctx, &InsertRequest{
			Headers: headers,
			InsertInput: &InsertInput{
				Key:   request.Key,
				Value: request.Value,
				TTL:   request.TTL,
			},
		})
	})
	if err != nil {
		log.Warnw("Insert",
			logging.Stringer("InsertRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &mapv1.InsertResponse{
		Version: uint64(output.Index),
	}
	log.Debugw("Insert",
		logging.Stringer("InsertRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("InsertResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *mapProxy) Update(ctx context.Context, request *mapv1.UpdateRequest) (*mapv1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*UpdateResponse](primitive)
	output, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*UpdateResponse, error) {
		input := &UpdateRequest{
			Headers: headers,
			UpdateInput: &UpdateInput{
				Key:       request.Key,
				Value:     request.Value,
				TTL:       request.TTL,
				PrevIndex: protocol.Index(request.PrevVersion),
			},
		}
		return NewMapClient(conn).Update(ctx, input)
	})
	if err != nil {
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &mapv1.UpdateResponse{
		Version: uint64(output.Index),
		PrevValue: mapv1.VersionedValue{
			Value:   output.PrevValue.Value,
			Version: uint64(output.PrevValue.Index),
		},
	}
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("UpdateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *mapProxy) Get(ctx context.Context, request *mapv1.GetRequest) (*mapv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	query := client.Query[*GetResponse](primitive)
	output, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*GetResponse, error) {
		return NewMapClient(conn).Get(ctx, &GetRequest{
			Headers: headers,
			GetInput: &GetInput{
				Key: request.Key,
			},
		})
	})
	if err != nil {
		log.Warnw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &mapv1.GetResponse{
		Value: mapv1.VersionedValue{
			Value:   output.Value.Value,
			Version: uint64(output.Value.Index),
		},
	}
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("GetResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *mapProxy) Remove(ctx context.Context, request *mapv1.RemoveRequest) (*mapv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*RemoveResponse](primitive)
	output, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*RemoveResponse, error) {
		input := &RemoveRequest{
			Headers: headers,
			RemoveInput: &RemoveInput{
				Key:       request.Key,
				PrevIndex: protocol.Index(request.PrevVersion),
			},
		}
		return NewMapClient(conn).Remove(ctx, input)
	})
	if err != nil {
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &mapv1.RemoveResponse{
		Value: mapv1.VersionedValue{
			Value:   output.Value.Value,
			Version: uint64(output.Value.Index),
		},
	}
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("RemoveResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *mapProxy) Clear(ctx context.Context, request *mapv1.ClearRequest) (*mapv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)))
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		session, err := partition.GetSession(ctx)
		if err != nil {
			log.Warnw("Clear",
				logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}
		primitive, err := session.GetPrimitive(request.ID.Name)
		if err != nil {
			log.Warnw("Clear",
				logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}
		command := client.Proposal[*ClearResponse](primitive)
		_, err = command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*ClearResponse, error) {
			return NewMapClient(conn).Clear(ctx, &ClearRequest{
				Headers:    headers,
				ClearInput: &ClearInput{},
			})
		})
		if err != nil {
			log.Warnw("Clear",
				logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}
		return nil
	})
	if err != nil {
		return nil, errors.ToProto(err)
	}
	response := &mapv1.ClearResponse{}
	log.Debugw("Clear",
		logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("ClearResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *mapProxy) Lock(ctx context.Context, request *mapv1.LockRequest) (*mapv1.LockResponse, error) {
	log.Debugw("Lock",
		logging.Stringer("LockRequest", stringer.Truncate(request, truncLen)))

	partitions := s.Partitions()
	indexKeys := make(map[int][]string)
	for _, key := range request.Keys {
		index := s.PartitionIndex([]byte(key))
		indexKeys[index] = append(indexKeys[index], key)
	}

	partitionIndexes := make([]int, 0, len(indexKeys))
	for index := range indexKeys {
		partitionIndexes = append(partitionIndexes, index)
	}

	err := async.IterAsync(len(partitionIndexes), func(i int) error {
		index := partitionIndexes[i]
		partition := partitions[index]
		keys := indexKeys[index]

		session, err := partition.GetSession(ctx)
		if err != nil {
			log.Warnw("Lock",
				logging.Stringer("LockRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}
		primitive, err := session.GetPrimitive(request.ID.Name)
		if err != nil {
			log.Warnw("Lock",
				logging.Stringer("LockRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}
		command := client.Proposal[*LockResponse](primitive)
		_, err = command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*LockResponse, error) {
			return NewMapClient(conn).Lock(ctx, &LockRequest{
				Headers: headers,
				LockInput: &LockInput{
					Keys:    keys,
					Timeout: request.Timeout,
				},
			})
		})
		if err != nil {
			log.Warnw("Lock",
				logging.Stringer("LockRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}
		return nil
	})
	if err != nil {
		return nil, errors.ToProto(err)
	}
	response := &mapv1.LockResponse{}
	log.Debugw("Lock",
		logging.Stringer("LockRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("LockResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *mapProxy) Unlock(ctx context.Context, request *mapv1.UnlockRequest) (*mapv1.UnlockResponse, error) {
	log.Debugw("Unlock",
		logging.Stringer("UnlockRequest", stringer.Truncate(request, truncLen)))
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		session, err := partition.GetSession(ctx)
		if err != nil {
			log.Warnw("Unlock",
				logging.Stringer("UnlockRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}
		primitive, err := session.GetPrimitive(request.ID.Name)
		if err != nil {
			log.Warnw("Unlock",
				logging.Stringer("UnlockRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}
		command := client.Proposal[*UnlockResponse](primitive)
		_, err = command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*UnlockResponse, error) {
			return NewMapClient(conn).Unlock(ctx, &UnlockRequest{
				Headers:     headers,
				UnlockInput: &UnlockInput{},
			})
		})
		if err != nil {
			log.Warnw("Unlock",
				logging.Stringer("UnlockRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}
		return nil
	})
	if err != nil {
		return nil, errors.ToProto(err)
	}
	response := &mapv1.UnlockResponse{}
	log.Debugw("Unlock",
		logging.Stringer("UnlockRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("UnlockResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *mapProxy) Events(request *mapv1.EventsRequest, server mapv1.Map_EventsServer) error {
	log.Debugw("Events",
		logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)))
	partitions := s.Partitions()
	return async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		session, err := partition.GetSession(server.Context())
		if err != nil {
			err = errors.ToProto(err)
			log.Warnw("Events",
				logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}
		primitive, err := session.GetPrimitive(request.ID.Name)
		if err != nil {
			err = errors.ToProto(err)
			log.Warnw("Events",
				logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}
		command := client.StreamProposal[*EventsResponse](primitive)
		stream, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (client.ProposalStream[*EventsResponse], error) {
			return NewMapClient(conn).Events(server.Context(), &EventsRequest{
				Headers: headers,
				EventsInput: &EventsInput{
					Key: request.Key,
				},
			})
		})
		if err != nil {
			err = errors.ToProto(err)
			log.Warnw("Events",
				logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}
		for {
			output, err := stream.Recv()
			if err == io.EOF {
				log.Debugw("Events",
					logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
					logging.String("State", "Done"))
				return nil
			}
			if err != nil {
				log.Warnw("Events",
					logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
					logging.Error("Error", err))
				return errors.ToProto(err)
			}
			var response *mapv1.EventsResponse
			switch e := output.Event.Event.(type) {
			case *Event_Inserted_:
				response = &mapv1.EventsResponse{
					Event: mapv1.Event{
						Key: output.Event.Key,
						Event: &mapv1.Event_Inserted_{
							Inserted: &mapv1.Event_Inserted{
								Value: mapv1.VersionedValue{
									Value:   e.Inserted.Value.Value,
									Version: uint64(e.Inserted.Value.Index),
								},
							},
						},
					},
				}
			case *Event_Updated_:
				response = &mapv1.EventsResponse{
					Event: mapv1.Event{
						Key: output.Event.Key,
						Event: &mapv1.Event_Updated_{
							Updated: &mapv1.Event_Updated{
								Value: mapv1.VersionedValue{
									Value:   e.Updated.Value.Value,
									Version: uint64(e.Updated.Value.Index),
								},
								PrevValue: mapv1.VersionedValue{
									Value:   e.Updated.PrevValue.Value,
									Version: uint64(e.Updated.PrevValue.Index),
								},
							},
						},
					},
				}
			case *Event_Removed_:
				response = &mapv1.EventsResponse{
					Event: mapv1.Event{
						Key: output.Event.Key,
						Event: &mapv1.Event_Removed_{
							Removed: &mapv1.Event_Removed{
								Value: mapv1.VersionedValue{
									Value:   e.Removed.Value.Value,
									Version: uint64(e.Removed.Value.Index),
								},
								Expired: e.Removed.Expired,
							},
						},
					},
				}
			}
			log.Debugw("Events",
				logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
				logging.Stringer("EventsResponse", stringer.Truncate(response, truncLen)))
			if err := server.Send(response); err != nil {
				log.Warnw("Events",
					logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
					logging.Stringer("EventsResponse", stringer.Truncate(response, truncLen)),
					logging.Error("Error", err))
				return err
			}
		}
	})
}

func (s *mapProxy) Entries(request *mapv1.EntriesRequest, server mapv1.Map_EntriesServer) error {
	log.Debugw("Entries",
		logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)))
	partitions := s.Partitions()
	return async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		session, err := partition.GetSession(server.Context())
		if err != nil {
			log.Warnw("Entries",
				logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return errors.ToProto(err)
		}
		primitive, err := session.GetPrimitive(request.ID.Name)
		if err != nil {
			log.Warnw("Entries",
				logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return errors.ToProto(err)
		}
		query := client.StreamQuery[*EntriesResponse](primitive)
		stream, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (client.QueryStream[*EntriesResponse], error) {
			return NewMapClient(conn).Entries(server.Context(), &EntriesRequest{
				Headers: headers,
				EntriesInput: &EntriesInput{
					Watch: request.Watch,
				},
			})
		})
		if err != nil {
			log.Warnw("Entries",
				logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return errors.ToProto(err)
		}
		for {
			output, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				log.Warnw("Entries",
					logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
					logging.Error("Error", err))
				return errors.ToProto(err)
			}
			response := &mapv1.EntriesResponse{
				Entry: mapv1.Entry{
					Key: output.Entry.Key,
				},
			}
			if output.Entry.Value != nil {
				response.Entry.Value = &mapv1.VersionedValue{
					Value:   output.Entry.Value.Value,
					Version: uint64(output.Entry.Value.Index),
				}
			}
			log.Debugw("Entries",
				logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Stringer("EntriesResponse", stringer.Truncate(response, truncLen)))
			if err := server.Send(response); err != nil {
				log.Warnw("Entries",
					logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
					logging.Stringer("EntriesResponse", stringer.Truncate(response, truncLen)),
					logging.Error("Error", err))
				return err
			}
		}
	})
}

var _ mapv1.MapServer = (*mapProxy)(nil)

func newCachingMapProxy(m mapv1.MapServer, config CacheConfig) mapv1.MapServer {
	return &cachingMapServer{
		MapServer: m,
		config:    config,
		entries:   make(map[string]*list.Element),
		aged:      list.New(),
	}
}

type cachingMapServer struct {
	mapv1.MapServer
	config  CacheConfig
	entries map[string]*list.Element
	aged    *list.List
	mu      sync.RWMutex
}

func (s *cachingMapServer) Create(ctx context.Context, request *mapv1.CreateRequest) (*mapv1.CreateResponse, error) {
	response, err := s.MapServer.Create(ctx, request)
	if err != nil {
		return nil, err
	}
	err = s.MapServer.Events(&mapv1.EventsRequest{
		ID: request.ID,
	}, newCachingEventsServer(s))
	go func() {
		interval := s.config.EvictionInterval
		if interval == nil {
			i := time.Minute
			interval = &i
		}
		ticker := time.NewTicker(*interval)
		for range ticker.C {
			s.evict()
		}
	}()
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (s *cachingMapServer) Put(ctx context.Context, request *mapv1.PutRequest) (*mapv1.PutResponse, error) {
	response, err := s.MapServer.Put(ctx, request)
	if err != nil {
		return nil, err
	}
	s.update(&mapv1.Entry{
		Key: request.Key,
		Value: &mapv1.VersionedValue{
			Value:   request.Value,
			Version: response.Version,
		},
	})
	return response, nil
}

func (s *cachingMapServer) Insert(ctx context.Context, request *mapv1.InsertRequest) (*mapv1.InsertResponse, error) {
	response, err := s.MapServer.Insert(ctx, request)
	if err != nil {
		return nil, err
	}
	s.update(&mapv1.Entry{
		Key: request.Key,
		Value: &mapv1.VersionedValue{
			Value:   request.Value,
			Version: response.Version,
		},
	})
	return response, nil
}

func (s *cachingMapServer) Update(ctx context.Context, request *mapv1.UpdateRequest) (*mapv1.UpdateResponse, error) {
	response, err := s.MapServer.Update(ctx, request)
	if err != nil {
		return nil, err
	}
	s.update(&mapv1.Entry{
		Key: request.Key,
		Value: &mapv1.VersionedValue{
			Value:   request.Value,
			Version: response.Version,
		},
	})
	return response, nil
}

func (s *cachingMapServer) Get(ctx context.Context, request *mapv1.GetRequest) (*mapv1.GetResponse, error) {
	s.mu.RLock()
	elem, ok := s.entries[request.Key]
	s.mu.RUnlock()
	if ok {
		return &mapv1.GetResponse{
			Value: *elem.Value.(*cachedEntry).entry.Value,
		}, nil
	}
	return s.MapServer.Get(ctx, request)
}

func (s *cachingMapServer) Remove(ctx context.Context, request *mapv1.RemoveRequest) (*mapv1.RemoveResponse, error) {
	response, err := s.MapServer.Remove(ctx, request)
	if err != nil {
		return nil, err
	}
	s.delete(request.Key)
	return response, nil
}

func (s *cachingMapServer) Clear(ctx context.Context, request *mapv1.ClearRequest) (*mapv1.ClearResponse, error) {
	response, err := s.MapServer.Clear(ctx, request)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.entries = make(map[string]*list.Element)
	s.aged = list.New()
	s.mu.Unlock()
	return response, nil
}

func (s *cachingMapServer) update(update *mapv1.Entry) {
	s.mu.RLock()
	check, ok := s.entries[update.Key]
	s.mu.RUnlock()
	if ok && check.Value.(*cachedEntry).entry.Value.Version >= update.Value.Version {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	check, ok = s.entries[update.Key]
	if ok && check.Value.(*cachedEntry).entry.Value.Version >= update.Value.Version {
		return
	}

	entry := newCachedEntry(update)
	if elem, ok := s.entries[update.Key]; ok {
		s.aged.Remove(elem)
	}
	s.entries[update.Key] = s.aged.PushBack(entry)
}

func (s *cachingMapServer) delete(key string) {
	s.mu.RLock()
	_, ok := s.entries[key]
	s.mu.RUnlock()
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if elem, ok := s.entries[key]; ok {
		delete(s.entries, key)
		s.aged.Remove(elem)
	}
}

func (s *cachingMapServer) evict() {
	t := time.Now()
	evictionDuration := time.Hour
	if s.config.EvictAfter != nil {
		evictionDuration = *s.config.EvictAfter
	}

	s.mu.RLock()
	size := uint64(len(s.entries))
	entry := s.aged.Front()
	s.mu.RUnlock()
	if (entry == nil || t.Sub(entry.Value.(*cachedEntry).timestamp) < evictionDuration) && size <= s.config.Size_ {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	size = uint64(len(s.entries))
	entry = s.aged.Front()
	if (entry == nil || t.Sub(entry.Value.(*cachedEntry).timestamp) < evictionDuration) && size <= s.config.Size_ {
		return
	}

	for i := s.aged.Len(); i > int(s.config.Size_); i-- {
		if entry := s.aged.Front(); entry != nil {
			s.aged.Remove(entry)
		}
	}

	entry = s.aged.Front()
	for entry != nil {
		if t.Sub(entry.Value.(*cachedEntry).timestamp) < evictionDuration {
			break
		}
		s.aged.Remove(entry)
		entry = s.aged.Front()
	}
}

var _ mapv1.MapServer = (*cachingMapServer)(nil)

func newCachingEventsServer(server *cachingMapServer) mapv1.Map_EventsServer {
	return &cachingEventsServer{
		server: server,
	}
}

type cachingEventsServer struct {
	grpc.ServerStream
	server *cachingMapServer
}

func (s *cachingEventsServer) Send(response *mapv1.EventsResponse) error {
	switch e := response.Event.Event.(type) {
	case *mapv1.Event_Inserted_:
		s.server.update(&mapv1.Entry{
			Key: response.Event.Key,
			Value: &mapv1.VersionedValue{
				Value:   e.Inserted.Value.Value,
				Version: e.Inserted.Value.Version,
			},
		})
	case *mapv1.Event_Updated_:
		s.server.update(&mapv1.Entry{
			Key: response.Event.Key,
			Value: &mapv1.VersionedValue{
				Value:   e.Updated.Value.Value,
				Version: e.Updated.Value.Version,
			},
		})
	case *mapv1.Event_Removed_:
		s.server.delete(response.Event.Key)
	}
	return nil
}

func newCachedEntry(entry *mapv1.Entry) *cachedEntry {
	return &cachedEntry{
		entry:     entry,
		timestamp: time.Now(),
	}
}

type cachedEntry struct {
	entry     *mapv1.Entry
	timestamp time.Time
}

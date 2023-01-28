// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	countermapv1 "github.com/atomix/atomix/api/runtime/countermap/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	countermapprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/countermap/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/client"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtimecountermapv1 "github.com/atomix/atomix/runtime/pkg/runtime/countermap/v1"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
	"github.com/atomix/atomix/runtime/pkg/utils/async"
	"google.golang.org/grpc"
	"io"
	"sync"
)

var log = logging.GetLogger()

func NewCounterMap(protocol *client.Protocol, id runtimev1.PrimitiveID) runtimecountermapv1.CounterMapProxy {
	return &counterMapProxy{
		Protocol: protocol,
		id:       id,
	}
}

type counterMapProxy struct {
	*client.Protocol
	id runtimev1.PrimitiveID
}

func (s *counterMapProxy) Open(ctx context.Context) error {
	log.Debugw("Create",
		logging.String("Name", s.id.Name))
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		session, err := partition.GetSession(ctx)
		if err != nil {
			return err
		}
		return session.CreatePrimitive(ctx, runtimev1.PrimitiveMeta{
			Type:        countermapv1.PrimitiveType,
			PrimitiveID: s.id,
		})
	})
	if err != nil {
		log.Warnw("Create",
			logging.String("Name", s.id.Name),
			logging.Error("Error", err))
		return err
	}
	return nil
}

func (s *counterMapProxy) Close(ctx context.Context) error {
	log.Debugw("Close",
		logging.String("Name", s.id.Name))
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		session, err := partition.GetSession(ctx)
		if err != nil {
			return err
		}
		return session.ClosePrimitive(ctx, s.id.Name)
	})
	if err != nil {
		log.Warnw("Close",
			logging.String("Name", s.id.Name),
			logging.Error("Error", err))
		return err
	}
	return nil
}

func (s *counterMapProxy) Size(ctx context.Context, request *countermapv1.SizeRequest) (*countermapv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Trunc128("SizeRequest", request))
	partitions := s.Partitions()
	sizes, err := async.ExecuteAsync[int](len(partitions), func(i int) (int, error) {
		partition := partitions[i]
		session, err := partition.GetSession(ctx)
		if err != nil {
			log.Warnw("Size",
				logging.Trunc128("SizeRequest", request),
				logging.Error("Error", err))
			return 0, err
		}
		primitive, err := session.GetPrimitive(request.ID.Name)
		if err != nil {
			log.Warnw("Size",
				logging.Trunc128("SizeRequest", request),
				logging.Error("Error", err))
			return 0, err
		}
		query := client.Query[*countermapprotocolv1.SizeResponse](primitive)
		output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*countermapprotocolv1.SizeResponse, error) {
			return countermapprotocolv1.NewCounterMapClient(conn).Size(ctx, &countermapprotocolv1.SizeRequest{
				Headers:   headers,
				SizeInput: &countermapprotocolv1.SizeInput{},
			})
		})
		if !ok {
			log.Warnw("Size",
				logging.Trunc128("SizeRequest", request),
				logging.Error("Error", err))
			return 0, err
		} else if err != nil {
			log.Debugw("Size",
				logging.Trunc128("SizeRequest", request),
				logging.Error("Error", err))
			return 0, err
		}
		return int(output.Size_), nil
	})
	if err != nil {
		return nil, err
	}
	var size int
	for _, s := range sizes {
		size += s
	}
	response := &countermapv1.SizeResponse{
		Size_: uint32(size),
	}
	log.Debugw("Size",
		logging.Trunc128("SizeRequest", request),
		logging.Trunc128("SizeResponse", response))
	return response, nil
}

func (s *counterMapProxy) Set(ctx context.Context, request *countermapv1.SetRequest) (*countermapv1.SetResponse, error) {
	log.Debugw("Set",
		logging.Trunc128("SetRequest", request))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Set",
			logging.Trunc128("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Set",
			logging.Trunc128("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*countermapprotocolv1.SetResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*countermapprotocolv1.SetResponse, error) {
		input := &countermapprotocolv1.SetRequest{
			Headers: headers,
			SetInput: &countermapprotocolv1.SetInput{
				Key:   request.Key,
				Value: request.Value,
			},
		}
		return countermapprotocolv1.NewCounterMapClient(conn).Set(ctx, input)
	})
	if !ok {
		log.Warnw("Set",
			logging.Trunc128("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Set",
			logging.Trunc128("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapv1.SetResponse{
		PrevValue: output.PrevValue,
	}
	log.Debugw("Set",
		logging.Trunc128("SetRequest", request),
		logging.Trunc128("SetResponse", response))
	return response, nil
}

func (s *counterMapProxy) Insert(ctx context.Context, request *countermapv1.InsertRequest) (*countermapv1.InsertResponse, error) {
	log.Debugw("Insert",
		logging.Trunc128("InsertRequest", request))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Insert",
			logging.Trunc128("InsertRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Insert",
			logging.Trunc128("InsertRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*countermapprotocolv1.InsertResponse](primitive)
	_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*countermapprotocolv1.InsertResponse, error) {
		return countermapprotocolv1.NewCounterMapClient(conn).Insert(ctx, &countermapprotocolv1.InsertRequest{
			Headers: headers,
			InsertInput: &countermapprotocolv1.InsertInput{
				Key:   request.Key,
				Value: request.Value,
			},
		})
	})
	if !ok {
		log.Warnw("Insert",
			logging.Trunc128("InsertRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Insert",
			logging.Trunc128("InsertRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapv1.InsertResponse{}
	log.Debugw("Insert",
		logging.Trunc128("InsertRequest", request),
		logging.Trunc128("InsertResponse", response))
	return response, nil
}

func (s *counterMapProxy) Update(ctx context.Context, request *countermapv1.UpdateRequest) (*countermapv1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Trunc128("UpdateRequest", request))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Update",
			logging.Trunc128("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Update",
			logging.Trunc128("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*countermapprotocolv1.UpdateResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*countermapprotocolv1.UpdateResponse, error) {
		input := &countermapprotocolv1.UpdateRequest{
			Headers: headers,
			UpdateInput: &countermapprotocolv1.UpdateInput{
				Key:   request.Key,
				Value: request.Value,
			},
		}
		return countermapprotocolv1.NewCounterMapClient(conn).Update(ctx, input)
	})
	if !ok {
		log.Warnw("Update",
			logging.Trunc128("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Update",
			logging.Trunc128("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapv1.UpdateResponse{
		PrevValue: output.PrevValue,
	}
	log.Debugw("Update",
		logging.Trunc128("UpdateRequest", request),
		logging.Trunc128("UpdateResponse", response))
	return response, nil
}

func (s *counterMapProxy) Increment(ctx context.Context, request *countermapv1.IncrementRequest) (*countermapv1.IncrementResponse, error) {
	log.Debugw("Increment",
		logging.Trunc128("IncrementRequest", request))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Increment",
			logging.Trunc128("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Increment",
			logging.Trunc128("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*countermapprotocolv1.IncrementResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*countermapprotocolv1.IncrementResponse, error) {
		input := &countermapprotocolv1.IncrementRequest{
			Headers: headers,
			IncrementInput: &countermapprotocolv1.IncrementInput{
				Key:   request.Key,
				Delta: request.Delta,
			},
		}
		return countermapprotocolv1.NewCounterMapClient(conn).Increment(ctx, input)
	})
	if !ok {
		log.Warnw("Increment",
			logging.Trunc128("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Increment",
			logging.Trunc128("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapv1.IncrementResponse{
		PrevValue: output.PrevValue,
	}
	log.Debugw("Increment",
		logging.Trunc128("IncrementRequest", request),
		logging.Trunc128("IncrementResponse", response))
	return response, nil
}

func (s *counterMapProxy) Decrement(ctx context.Context, request *countermapv1.DecrementRequest) (*countermapv1.DecrementResponse, error) {
	log.Debugw("Decrement",
		logging.Trunc128("DecrementRequest", request))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Decrement",
			logging.Trunc128("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Decrement",
			logging.Trunc128("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*countermapprotocolv1.DecrementResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*countermapprotocolv1.DecrementResponse, error) {
		input := &countermapprotocolv1.DecrementRequest{
			Headers: headers,
			DecrementInput: &countermapprotocolv1.DecrementInput{
				Key:   request.Key,
				Delta: request.Delta,
			},
		}
		return countermapprotocolv1.NewCounterMapClient(conn).Decrement(ctx, input)
	})
	if !ok {
		log.Warnw("Decrement",
			logging.Trunc128("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Decrement",
			logging.Trunc128("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapv1.DecrementResponse{
		PrevValue: output.PrevValue,
	}
	log.Debugw("Decrement",
		logging.Trunc128("DecrementRequest", request),
		logging.Trunc128("DecrementResponse", response))
	return response, nil
}

func (s *counterMapProxy) Get(ctx context.Context, request *countermapv1.GetRequest) (*countermapv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Get",
			logging.Trunc128("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Get",
			logging.Trunc128("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	query := client.Query[*countermapprotocolv1.GetResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*countermapprotocolv1.GetResponse, error) {
		return countermapprotocolv1.NewCounterMapClient(conn).Get(ctx, &countermapprotocolv1.GetRequest{
			Headers: headers,
			GetInput: &countermapprotocolv1.GetInput{
				Key: request.Key,
			},
		})
	})
	if !ok {
		log.Warnw("Get",
			logging.Trunc128("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Get",
			logging.Trunc128("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapv1.GetResponse{
		Value: output.Value,
	}
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request),
		logging.Trunc128("GetResponse", response))
	return response, nil
}

func (s *counterMapProxy) Remove(ctx context.Context, request *countermapv1.RemoveRequest) (*countermapv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Trunc128("RemoveRequest", request))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Remove",
			logging.Trunc128("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Remove",
			logging.Trunc128("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*countermapprotocolv1.RemoveResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*countermapprotocolv1.RemoveResponse, error) {
		input := &countermapprotocolv1.RemoveRequest{
			Headers: headers,
			RemoveInput: &countermapprotocolv1.RemoveInput{
				Key:       request.Key,
				PrevValue: request.PrevValue,
			},
		}
		return countermapprotocolv1.NewCounterMapClient(conn).Remove(ctx, input)
	})
	if !ok {
		log.Warnw("Remove",
			logging.Trunc128("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Remove",
			logging.Trunc128("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapv1.RemoveResponse{
		Value: output.Value,
	}
	log.Debugw("Remove",
		logging.Trunc128("RemoveRequest", request),
		logging.Trunc128("RemoveResponse", response))
	return response, nil
}

func (s *counterMapProxy) Clear(ctx context.Context, request *countermapv1.ClearRequest) (*countermapv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Trunc128("ClearRequest", request))
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		session, err := partition.GetSession(ctx)
		if err != nil {
			log.Warnw("Clear",
				logging.Trunc128("ClearRequest", request),
				logging.Error("Error", err))
			return err
		}
		primitive, err := session.GetPrimitive(request.ID.Name)
		if err != nil {
			log.Warnw("Clear",
				logging.Trunc128("ClearRequest", request),
				logging.Error("Error", err))
			return err
		}
		command := client.Proposal[*countermapprotocolv1.ClearResponse](primitive)
		_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*countermapprotocolv1.ClearResponse, error) {
			return countermapprotocolv1.NewCounterMapClient(conn).Clear(ctx, &countermapprotocolv1.ClearRequest{
				Headers:    headers,
				ClearInput: &countermapprotocolv1.ClearInput{},
			})
		})
		if !ok {
			log.Warnw("Clear",
				logging.Trunc128("ClearRequest", request),
				logging.Error("Error", err))
			return err
		} else if err != nil {
			log.Debugw("Clear",
				logging.Trunc128("ClearRequest", request),
				logging.Error("Error", err))
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	response := &countermapv1.ClearResponse{}
	log.Debugw("Clear",
		logging.Trunc128("ClearRequest", request),
		logging.Trunc128("ClearResponse", response))
	return response, nil
}

func (s *counterMapProxy) Lock(ctx context.Context, request *countermapv1.LockRequest) (*countermapv1.LockResponse, error) {
	log.Debugw("Lock",
		logging.Trunc128("LockRequest", request))

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
				logging.Trunc128("LockRequest", request),
				logging.Error("Error", err))
			return err
		}
		primitive, err := session.GetPrimitive(request.ID.Name)
		if err != nil {
			log.Warnw("Lock",
				logging.Trunc128("LockRequest", request),
				logging.Error("Error", err))
			return err
		}
		command := client.Proposal[*countermapprotocolv1.LockResponse](primitive)
		_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*countermapprotocolv1.LockResponse, error) {
			return countermapprotocolv1.NewCounterMapClient(conn).Lock(ctx, &countermapprotocolv1.LockRequest{
				Headers: headers,
				LockInput: &countermapprotocolv1.LockInput{
					Keys:    keys,
					Timeout: request.Timeout,
				},
			})
		})
		if !ok {
			log.Warnw("Lock",
				logging.Trunc128("LockRequest", request),
				logging.Error("Error", err))
			return err
		} else if err != nil {
			log.Debugw("Lock",
				logging.Trunc128("LockRequest", request),
				logging.Error("Error", err))
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	response := &countermapv1.LockResponse{}
	log.Debugw("Lock",
		logging.Trunc128("LockRequest", request),
		logging.Trunc128("LockResponse", response))
	return response, nil
}

func (s *counterMapProxy) Unlock(ctx context.Context, request *countermapv1.UnlockRequest) (*countermapv1.UnlockResponse, error) {
	log.Debugw("Unlock",
		logging.Trunc128("UnlockRequest", request))
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		session, err := partition.GetSession(ctx)
		if err != nil {
			log.Warnw("Unlock",
				logging.Trunc128("UnlockRequest", request),
				logging.Error("Error", err))
			return err
		}
		primitive, err := session.GetPrimitive(request.ID.Name)
		if err != nil {
			log.Warnw("Unlock",
				logging.Trunc128("UnlockRequest", request),
				logging.Error("Error", err))
			return err
		}
		command := client.Proposal[*countermapprotocolv1.UnlockResponse](primitive)
		_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*countermapprotocolv1.UnlockResponse, error) {
			return countermapprotocolv1.NewCounterMapClient(conn).Unlock(ctx, &countermapprotocolv1.UnlockRequest{
				Headers:     headers,
				UnlockInput: &countermapprotocolv1.UnlockInput{},
			})
		})
		if !ok {
			log.Warnw("Unlock",
				logging.Trunc128("UnlockRequest", request),
				logging.Error("Error", err))
			return err
		} else if err != nil {
			log.Debugw("Unlock",
				logging.Trunc128("UnlockRequest", request),
				logging.Error("Error", err))
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	response := &countermapv1.UnlockResponse{}
	log.Debugw("Unlock",
		logging.Trunc128("UnlockRequest", request),
		logging.Trunc128("UnlockResponse", response))
	return response, nil
}

func (s *counterMapProxy) Events(request *countermapv1.EventsRequest, server countermapv1.CounterMap_EventsServer) error {
	log.Debugw("Events received",
		logging.Trunc128("EventsRequest", request))
	partitions := s.Partitions()
	ch := make(chan streams.Result[*countermapv1.EventsResponse])
	wg := &sync.WaitGroup{}
	for i := 0; i < len(partitions); i++ {
		wg.Add(1)
		go func(partition *client.PartitionClient) {
			defer wg.Done()
			session, err := partition.GetSession(server.Context())
			if err != nil {
				log.Warnw("Events",
					logging.Trunc128("EventsRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*countermapv1.EventsResponse]{
					Error: err,
				}
				return
			}
			primitive, err := session.GetPrimitive(request.ID.Name)
			if err != nil {
				log.Warnw("Events",
					logging.Trunc128("EventsRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*countermapv1.EventsResponse]{
					Error: err,
				}
				return
			}
			proposal := client.StreamProposal[*countermapprotocolv1.EventsResponse](primitive)
			stream, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (client.ProposalStream[*countermapprotocolv1.EventsResponse], error) {
				return countermapprotocolv1.NewCounterMapClient(conn).Events(server.Context(), &countermapprotocolv1.EventsRequest{
					Headers: headers,
					EventsInput: &countermapprotocolv1.EventsInput{
						Key: request.Key,
					},
				})
			})
			if err != nil {
				log.Warnw("Events",
					logging.Trunc128("EventsRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*countermapv1.EventsResponse]{
					Error: err,
				}
				return
			}
			for {
				output, ok, err := stream.Recv()
				if !ok {
					if err != io.EOF {
						log.Warnw("Events",
							logging.Trunc128("EventsRequest", request),
							logging.Error("Error", err))
						ch <- streams.Result[*countermapv1.EventsResponse]{
							Error: err,
						}
					}
					return
				}
				if err != nil {
					log.Debugw("Events",
						logging.Trunc128("EventsRequest", request),
						logging.Error("Error", err))
					ch <- streams.Result[*countermapv1.EventsResponse]{
						Error: err,
					}
				} else {
					response := &countermapv1.EventsResponse{
						Event: countermapv1.Event{
							Key: output.Event.Key,
						},
					}
					switch e := output.Event.Event.(type) {
					case *countermapprotocolv1.Event_Inserted_:
						response.Event.Event = &countermapv1.Event_Inserted_{
							Inserted: &countermapv1.Event_Inserted{
								Value: e.Inserted.Value,
							},
						}
					case *countermapprotocolv1.Event_Updated_:
						response.Event.Event = &countermapv1.Event_Updated_{
							Updated: &countermapv1.Event_Updated{
								Value:     e.Updated.Value,
								PrevValue: e.Updated.PrevValue,
							},
						}
					case *countermapprotocolv1.Event_Removed_:
						response.Event.Event = &countermapv1.Event_Removed_{
							Removed: &countermapv1.Event_Removed{
								Value: e.Removed.Value,
							},
						}
					}
					log.Debugw("Events",
						logging.Trunc128("EventsRequest", request),
						logging.Trunc128("EventsResponse", response))
					ch <- streams.Result[*countermapv1.EventsResponse]{
						Value: response,
					}
				}
			}
		}(partitions[i])
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for result := range ch {
		if result.Failed() {
			return result.Error
		}
		if err := server.Send(result.Value); err != nil {
			return err
		}
	}
	log.Debugw("Events complete",
		logging.Trunc128("EventsRequest", request))
	return nil
}

func (s *counterMapProxy) Entries(request *countermapv1.EntriesRequest, server countermapv1.CounterMap_EntriesServer) error {
	log.Debugw("Entries received",
		logging.Trunc128("EntriesRequest", request))
	partitions := s.Partitions()
	ch := make(chan streams.Result[*countermapv1.EntriesResponse])
	wg := &sync.WaitGroup{}
	for i := 0; i < len(partitions); i++ {
		wg.Add(1)
		go func(partition *client.PartitionClient) {
			defer wg.Done()
			session, err := partition.GetSession(server.Context())
			if err != nil {
				log.Warnw("Entries",
					logging.Trunc128("EntriesRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*countermapv1.EntriesResponse]{
					Error: err,
				}
				return
			}
			primitive, err := session.GetPrimitive(request.ID.Name)
			if err != nil {
				log.Warnw("Entries",
					logging.Trunc128("EntriesRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*countermapv1.EntriesResponse]{
					Error: err,
				}
				return
			}
			query := client.StreamQuery[*countermapprotocolv1.EntriesResponse](primitive)
			stream, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (client.QueryStream[*countermapprotocolv1.EntriesResponse], error) {
				return countermapprotocolv1.NewCounterMapClient(conn).Entries(server.Context(), &countermapprotocolv1.EntriesRequest{
					Headers: headers,
					EntriesInput: &countermapprotocolv1.EntriesInput{
						Watch: request.Watch,
					},
				})
			})
			if err != nil {
				log.Warnw("Entries",
					logging.Trunc128("EntriesRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*countermapv1.EntriesResponse]{
					Error: err,
				}
				return
			}
			for {
				output, ok, err := stream.Recv()
				if !ok {
					if err != io.EOF {
						log.Warnw("Entries",
							logging.Trunc128("EntriesRequest", request),
							logging.Error("Error", err))
						ch <- streams.Result[*countermapv1.EntriesResponse]{
							Error: err,
						}
					}
					return
				}
				if err != nil {
					log.Debugw("Entries",
						logging.Trunc128("EntriesRequest", request),
						logging.Error("Error", err))
					ch <- streams.Result[*countermapv1.EntriesResponse]{
						Error: err,
					}
				} else {
					response := &countermapv1.EntriesResponse{
						Entry: countermapv1.Entry{
							Key:   output.Entry.Key,
							Value: output.Entry.Value,
						},
					}
					log.Debugw("Entries",
						logging.Trunc128("EntriesRequest", request),
						logging.Trunc128("EntriesResponse", response))
					ch <- streams.Result[*countermapv1.EntriesResponse]{
						Value: response,
					}
				}
			}
		}(partitions[i])
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for result := range ch {
		if result.Failed() {
			return result.Error
		}
		if err := server.Send(result.Value); err != nil {
			return err
		}
	}
	log.Debugw("Entries complete",
		logging.Trunc128("EntriesRequest", request))
	return nil
}

var _ countermapv1.CounterMapServer = (*counterMapProxy)(nil)

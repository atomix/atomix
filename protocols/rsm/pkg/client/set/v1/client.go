// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	setv1 "github.com/atomix/atomix/api/runtime/set/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	setprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/set/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/client"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtimesetv1 "github.com/atomix/atomix/runtime/pkg/runtime/set/v1"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
	"github.com/atomix/atomix/runtime/pkg/utils/async"
	"google.golang.org/grpc"
	"io"
	"sync"
)

var log = logging.GetLogger()

func NewSet(protocol *client.Protocol, id runtimev1.PrimitiveID) runtimesetv1.SetProxy {
	return &setProxy{
		Protocol: protocol,
		id:       id,
	}
}

type setProxy struct {
	*client.Protocol
	id runtimev1.PrimitiveID
}

func (s *setProxy) Open(ctx context.Context) error {
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
			Type:        setv1.PrimitiveType,
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

func (s *setProxy) Close(ctx context.Context) error {
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

func (s *setProxy) Size(ctx context.Context, request *setv1.SizeRequest) (*setv1.SizeResponse, error) {
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
		query := client.Query[*setprotocolv1.SizeResponse](primitive)
		output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*setprotocolv1.SizeResponse, error) {
			return setprotocolv1.NewSetClient(conn).Size(ctx, &setprotocolv1.SizeRequest{
				Headers:   headers,
				SizeInput: &setprotocolv1.SizeInput{},
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
	response := &setv1.SizeResponse{
		Size_: uint32(size),
	}
	log.Debugw("Size",
		logging.Trunc128("SizeRequest", request),
		logging.Trunc128("SizeResponse", response))
	return response, nil
}

func (s *setProxy) Add(ctx context.Context, request *setv1.AddRequest) (*setv1.AddResponse, error) {
	log.Debugw("Add",
		logging.Trunc128("AddRequest", request))
	partition := s.PartitionBy([]byte(request.Element.Value))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Add",
			logging.Trunc128("AddRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Add",
			logging.Trunc128("AddRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*setprotocolv1.AddResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*setprotocolv1.AddResponse, error) {
		input := &setprotocolv1.AddRequest{
			Headers: headers,
			AddInput: &setprotocolv1.AddInput{
				Element: setprotocolv1.Element{
					Value: request.Element.Value,
				},
				TTL: request.TTL,
			},
		}
		return setprotocolv1.NewSetClient(conn).Add(ctx, input)
	})
	if !ok {
		log.Warnw("Add",
			logging.Trunc128("AddRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Add",
			logging.Trunc128("AddRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &setv1.AddResponse{
		Added: output.Added,
	}
	log.Debugw("Add",
		logging.Trunc128("AddRequest", request),
		logging.Trunc128("AddResponse", response))
	return response, nil
}

func (s *setProxy) Contains(ctx context.Context, request *setv1.ContainsRequest) (*setv1.ContainsResponse, error) {
	log.Debugw("Contains",
		logging.Trunc128("ContainsRequest", request))
	partition := s.PartitionBy([]byte(request.Element.Value))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Contains",
			logging.Trunc128("ContainsRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Contains",
			logging.Trunc128("ContainsRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Query[*setprotocolv1.ContainsResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*setprotocolv1.ContainsResponse, error) {
		input := &setprotocolv1.ContainsRequest{
			Headers: headers,
			ContainsInput: &setprotocolv1.ContainsInput{
				Element: setprotocolv1.Element{
					Value: request.Element.Value,
				},
			},
		}
		return setprotocolv1.NewSetClient(conn).Contains(ctx, input)
	})
	if !ok {
		log.Warnw("Contains",
			logging.Trunc128("ContainsRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Contains",
			logging.Trunc128("ContainsRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &setv1.ContainsResponse{
		Contains: output.Contains,
	}
	log.Debugw("Contains",
		logging.Trunc128("ContainsRequest", request),
		logging.Trunc128("ContainsResponse", response))
	return response, nil
}

func (s *setProxy) Remove(ctx context.Context, request *setv1.RemoveRequest) (*setv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Trunc128("RemoveRequest", request))
	partition := s.PartitionBy([]byte(request.Element.Value))
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
	command := client.Proposal[*setprotocolv1.RemoveResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*setprotocolv1.RemoveResponse, error) {
		input := &setprotocolv1.RemoveRequest{
			Headers: headers,
			RemoveInput: &setprotocolv1.RemoveInput{
				Element: setprotocolv1.Element{
					Value: request.Element.Value,
				},
			},
		}
		return setprotocolv1.NewSetClient(conn).Remove(ctx, input)
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
	response := &setv1.RemoveResponse{
		Removed: output.Removed,
	}
	log.Debugw("Remove",
		logging.Trunc128("RemoveRequest", request),
		logging.Trunc128("RemoveResponse", response))
	return response, nil
}

func (s *setProxy) Clear(ctx context.Context, request *setv1.ClearRequest) (*setv1.ClearResponse, error) {
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
		command := client.Proposal[*setprotocolv1.ClearResponse](primitive)
		_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*setprotocolv1.ClearResponse, error) {
			return setprotocolv1.NewSetClient(conn).Clear(ctx, &setprotocolv1.ClearRequest{
				Headers:    headers,
				ClearInput: &setprotocolv1.ClearInput{},
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
	response := &setv1.ClearResponse{}
	log.Debugw("Clear",
		logging.Trunc128("ClearRequest", request),
		logging.Trunc128("ClearResponse", response))
	return response, nil
}

func (s *setProxy) Events(request *setv1.EventsRequest, server setv1.Set_EventsServer) error {
	log.Debugw("Events received",
		logging.Trunc128("EventsRequest", request))
	partitions := s.Partitions()
	ch := make(chan streams.Result[*setv1.EventsResponse])
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
				ch <- streams.Result[*setv1.EventsResponse]{
					Error: err,
				}
				return
			}
			primitive, err := session.GetPrimitive(request.ID.Name)
			if err != nil {
				log.Warnw("Events",
					logging.Trunc128("EventsRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*setv1.EventsResponse]{
					Error: err,
				}
				return
			}
			proposal := client.StreamProposal[*setprotocolv1.EventsResponse](primitive)
			stream, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (client.ProposalStream[*setprotocolv1.EventsResponse], error) {
				return setprotocolv1.NewSetClient(conn).Events(server.Context(), &setprotocolv1.EventsRequest{
					Headers:     headers,
					EventsInput: &setprotocolv1.EventsInput{},
				})
			})
			if err != nil {
				log.Warnw("Events",
					logging.Trunc128("EventsRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*setv1.EventsResponse]{
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
						ch <- streams.Result[*setv1.EventsResponse]{
							Error: err,
						}
					}
					return
				}
				if err != nil {
					log.Debugw("Events",
						logging.Trunc128("EventsRequest", request),
						logging.Error("Error", err))
					ch <- streams.Result[*setv1.EventsResponse]{
						Error: err,
					}
				} else {
					response := &setv1.EventsResponse{
						Event: setv1.Event{},
					}
					switch e := output.Event.Event.(type) {
					case *setprotocolv1.Event_Added_:
						response.Event.Event = &setv1.Event_Added_{
							Added: &setv1.Event_Added{
								Element: setv1.Element{
									Value: e.Added.Element.Value,
								},
							},
						}
					case *setprotocolv1.Event_Removed_:
						response.Event.Event = &setv1.Event_Removed_{
							Removed: &setv1.Event_Removed{
								Element: setv1.Element{
									Value: e.Removed.Element.Value,
								},
								Expired: e.Removed.Expired,
							},
						}
					}
					log.Debugw("Events",
						logging.Trunc128("EventsRequest", request),
						logging.Trunc128("EventsResponse", response))
					ch <- streams.Result[*setv1.EventsResponse]{
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

func (s *setProxy) Elements(request *setv1.ElementsRequest, server setv1.Set_ElementsServer) error {
	log.Debugw("Elements received",
		logging.Trunc128("ElementsRequest", request))
	partitions := s.Partitions()
	ch := make(chan streams.Result[*setv1.ElementsResponse])
	wg := &sync.WaitGroup{}
	for i := 0; i < len(partitions); i++ {
		wg.Add(1)
		go func(partition *client.PartitionClient) {
			defer wg.Done()
			session, err := partition.GetSession(server.Context())
			if err != nil {
				log.Warnw("Elements",
					logging.Trunc128("ElementsRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*setv1.ElementsResponse]{
					Error: err,
				}
				return
			}
			primitive, err := session.GetPrimitive(request.ID.Name)
			if err != nil {
				log.Warnw("Elements",
					logging.Trunc128("ElementsRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*setv1.ElementsResponse]{
					Error: err,
				}
				return
			}
			query := client.StreamQuery[*setprotocolv1.ElementsResponse](primitive)
			stream, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (client.QueryStream[*setprotocolv1.ElementsResponse], error) {
				return setprotocolv1.NewSetClient(conn).Elements(server.Context(), &setprotocolv1.ElementsRequest{
					Headers: headers,
					ElementsInput: &setprotocolv1.ElementsInput{
						Watch: request.Watch,
					},
				})
			})
			if err != nil {
				log.Warnw("Elements",
					logging.Trunc128("ElementsRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*setv1.ElementsResponse]{
					Error: err,
				}
				return
			}
			for {
				output, ok, err := stream.Recv()
				if !ok {
					if err != io.EOF {
						log.Warnw("Elements",
							logging.Trunc128("ElementsRequest", request),
							logging.Error("Error", err))
						ch <- streams.Result[*setv1.ElementsResponse]{
							Error: err,
						}
					}
					return
				}
				if err != nil {
					log.Debugw("Elements",
						logging.Trunc128("ElementsRequest", request),
						logging.Error("Error", err))
					ch <- streams.Result[*setv1.ElementsResponse]{
						Error: err,
					}
				} else {
					response := &setv1.ElementsResponse{
						Element: setv1.Element{
							Value: output.Element.Value,
						},
					}
					log.Debugw("Elements",
						logging.Trunc128("ElementsRequest", request),
						logging.Trunc128("ElementsResponse", response))
					ch <- streams.Result[*setv1.ElementsResponse]{
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
	log.Debugw("Elements complete",
		logging.Trunc128("ElementsRequest", request))
	return nil
}

var _ setv1.SetServer = (*setProxy)(nil)

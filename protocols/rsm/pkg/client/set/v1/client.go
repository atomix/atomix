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
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
	"github.com/atomix/atomix/runtime/pkg/utils/async"
	"google.golang.org/grpc"
	"io"
	"sync"
)

var log = logging.GetLogger()

func NewSet(protocol *client.Protocol) setv1.SetServer {
	return &setClient{
		Protocol: protocol,
	}
}

type setClient struct {
	*client.Protocol
}

func (s *setClient) Create(ctx context.Context, request *setv1.CreateRequest) (*setv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", request))
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		session, err := partition.GetSession(ctx)
		if err != nil {
			return err
		}
		return session.CreatePrimitive(ctx, runtimev1.PrimitiveMeta{
			Type:        setv1.PrimitiveType,
			PrimitiveID: request.ID,
			Tags:        request.Tags,
		})
	})
	if err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &setv1.CreateResponse{}
	log.Debugw("Create",
		logging.Stringer("CreateRequest", request),
		logging.Stringer("CreateResponse", response))
	return response, nil
}

func (s *setClient) Close(ctx context.Context, request *setv1.CloseRequest) (*setv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", request))
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
			logging.Stringer("CloseRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &setv1.CloseResponse{}
	log.Debugw("Close",
		logging.Stringer("CloseRequest", request),
		logging.Stringer("CloseResponse", response))
	return response, nil
}

func (s *setClient) Size(ctx context.Context, request *setv1.SizeRequest) (*setv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Stringer("SizeRequest", request))
	partitions := s.Partitions()
	sizes, err := async.ExecuteAsync[int](len(partitions), func(i int) (int, error) {
		partition := partitions[i]
		session, err := partition.GetSession(ctx)
		if err != nil {
			log.Warnw("Size",
				logging.Stringer("SizeRequest", request),
				logging.Error("Error", err))
			return 0, err
		}
		primitive, err := session.GetPrimitive(request.ID.Name)
		if err != nil {
			log.Warnw("Size",
				logging.Stringer("SizeRequest", request),
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
				logging.Stringer("SizeRequest", request),
				logging.Error("Error", err))
			return 0, err
		} else if err != nil {
			log.Debugw("Size",
				logging.Stringer("SizeRequest", request),
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
	response := &setv1.SizeResponse{
		Size_: uint32(size),
	}
	log.Debugw("Size",
		logging.Stringer("SizeRequest", request),
		logging.Stringer("SizeResponse", response))
	return response, nil
}

func (s *setClient) Add(ctx context.Context, request *setv1.AddRequest) (*setv1.AddResponse, error) {
	log.Debugw("Add",
		logging.Stringer("AddRequest", request))
	partition := s.PartitionBy([]byte(request.Element.Value))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Add",
			logging.Stringer("AddRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Add",
			logging.Stringer("AddRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*setprotocolv1.AddResponse](primitive)
	_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*setprotocolv1.AddResponse, error) {
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
			logging.Stringer("AddRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Add",
			logging.Stringer("AddRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &setv1.AddResponse{}
	log.Debugw("Add",
		logging.Stringer("AddRequest", request),
		logging.Stringer("AddResponse", response))
	return response, nil
}

func (s *setClient) Contains(ctx context.Context, request *setv1.ContainsRequest) (*setv1.ContainsResponse, error) {
	log.Debugw("Contains",
		logging.Stringer("ContainsRequest", request))
	partition := s.PartitionBy([]byte(request.Element.Value))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Contains",
			logging.Stringer("ContainsRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Contains",
			logging.Stringer("ContainsRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
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
			logging.Stringer("ContainsRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Contains",
			logging.Stringer("ContainsRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &setv1.ContainsResponse{
		Contains: output.Contains,
	}
	log.Debugw("Contains",
		logging.Stringer("ContainsRequest", request),
		logging.Stringer("ContainsResponse", response))
	return response, nil
}

func (s *setClient) Remove(ctx context.Context, request *setv1.RemoveRequest) (*setv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", request))
	partition := s.PartitionBy([]byte(request.Element.Value))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*setprotocolv1.RemoveResponse](primitive)
	_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*setprotocolv1.RemoveResponse, error) {
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
			logging.Stringer("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Remove",
			logging.Stringer("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &setv1.RemoveResponse{}
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", request),
		logging.Stringer("RemoveResponse", response))
	return response, nil
}

func (s *setClient) Clear(ctx context.Context, request *setv1.ClearRequest) (*setv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Stringer("ClearRequest", request))
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		session, err := partition.GetSession(ctx)
		if err != nil {
			log.Warnw("Clear",
				logging.Stringer("ClearRequest", request),
				logging.Error("Error", err))
			return err
		}
		primitive, err := session.GetPrimitive(request.ID.Name)
		if err != nil {
			log.Warnw("Clear",
				logging.Stringer("ClearRequest", request),
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
				logging.Stringer("ClearRequest", request),
				logging.Error("Error", err))
			return err
		} else if err != nil {
			log.Debugw("Clear",
				logging.Stringer("ClearRequest", request),
				logging.Error("Error", err))
			return err
		}
		return nil
	})
	if err != nil {
		return nil, errors.ToProto(err)
	}
	response := &setv1.ClearResponse{}
	log.Debugw("Clear",
		logging.Stringer("ClearRequest", request),
		logging.Stringer("ClearResponse", response))
	return response, nil
}

func (s *setClient) Events(request *setv1.EventsRequest, server setv1.Set_EventsServer) error {
	log.Debugw("Events received",
		logging.Stringer("EventsRequest", request))
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
					logging.Stringer("EventsRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*setv1.EventsResponse]{
					Error: err,
				}
				return
			}
			primitive, err := session.GetPrimitive(request.ID.Name)
			if err != nil {
				log.Warnw("Events",
					logging.Stringer("EventsRequest", request),
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
					logging.Stringer("EventsRequest", request),
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
							logging.Stringer("EventsRequest", request),
							logging.Error("Error", err))
						ch <- streams.Result[*setv1.EventsResponse]{
							Error: err,
						}
					}
					return
				}
				if err != nil {
					log.Debugw("Events",
						logging.Stringer("EventsRequest", request),
						logging.Error("Error", err))
					ch <- streams.Result[*setv1.EventsResponse]{
						Error: errors.ToProto(err),
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
						logging.Stringer("EventsRequest", request),
						logging.Stringer("EventsResponse", response))
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
		logging.Stringer("EventsRequest", request))
	return nil
}

func (s *setClient) Elements(request *setv1.ElementsRequest, server setv1.Set_ElementsServer) error {
	log.Debugw("Elements received",
		logging.Stringer("ElementsRequest", request))
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
					logging.Stringer("ElementsRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*setv1.ElementsResponse]{
					Error: err,
				}
				return
			}
			primitive, err := session.GetPrimitive(request.ID.Name)
			if err != nil {
				log.Warnw("Elements",
					logging.Stringer("ElementsRequest", request),
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
					logging.Stringer("ElementsRequest", request),
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
							logging.Stringer("ElementsRequest", request),
							logging.Error("Error", err))
						ch <- streams.Result[*setv1.ElementsResponse]{
							Error: err,
						}
					}
					return
				}
				if err != nil {
					log.Debugw("Elements",
						logging.Stringer("ElementsRequest", request),
						logging.Error("Error", err))
					ch <- streams.Result[*setv1.ElementsResponse]{
						Error: errors.ToProto(err),
					}
				} else {
					response := &setv1.ElementsResponse{
						Element: setv1.Element{
							Value: output.Element.Value,
						},
					}
					log.Debugw("Elements",
						logging.Stringer("ElementsRequest", request),
						logging.Stringer("ElementsResponse", response))
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
		logging.Stringer("ElementsRequest", request))
	return nil
}

var _ setv1.SetServer = (*setClient)(nil)

// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	setv1 "github.com/atomix/runtime/api/atomix/runtime/set/v1"
	"github.com/atomix/runtime/sdk/pkg/async"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"github.com/atomix/runtime/sdk/pkg/protocol/client"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	streams "github.com/atomix/runtime/sdk/pkg/stream"
	"github.com/atomix/runtime/sdk/pkg/stringer"
	"google.golang.org/grpc"
	"io"
	"sync"
)

func NewSetProxy(protocol *client.Protocol, spec runtime.PrimitiveSpec) (setv1.SetServer, error) {
	return &setProxy{
		Protocol:      protocol,
		PrimitiveSpec: spec,
	}, nil
}

type setProxy struct {
	*client.Protocol
	runtime.PrimitiveSpec
}

func (s *setProxy) Create(ctx context.Context, request *setv1.CreateRequest) (*setv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if err := session.CreatePrimitive(ctx, s.PrimitiveSpec); err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &setv1.CreateResponse{}
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("CreateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *setProxy) Close(ctx context.Context, request *setv1.CloseRequest) (*setv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if err := session.ClosePrimitive(ctx, request.ID.Name); err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &setv1.CloseResponse{}
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("CloseResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *setProxy) Size(ctx context.Context, request *setv1.SizeRequest) (*setv1.SizeResponse, error) {
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
		output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*SizeResponse, error) {
			return NewSetClient(conn).Size(ctx, &SizeRequest{
				Headers:   headers,
				SizeInput: &SizeInput{},
			})
		})
		if !ok {
			log.Warnw("Size",
				logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return 0, err
		} else if err != nil {
			log.Debugw("Size",
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
	response := &setv1.SizeResponse{
		Size_: uint32(size),
	}
	log.Debugw("Size",
		logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("SizeResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *setProxy) Add(ctx context.Context, request *setv1.AddRequest) (*setv1.AddResponse, error) {
	log.Debugw("Add",
		logging.Stringer("AddRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.Element.Value))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Add",
			logging.Stringer("AddRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Add",
			logging.Stringer("AddRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*AddResponse](primitive)
	_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*AddResponse, error) {
		input := &AddRequest{
			Headers: headers,
			AddInput: &AddInput{
				Element: Element{
					Value: request.Element.Value,
				},
				TTL: request.TTL,
			},
		}
		return NewSetClient(conn).Add(ctx, input)
	})
	if !ok {
		log.Warnw("Add",
			logging.Stringer("AddRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Add",
			logging.Stringer("AddRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &setv1.AddResponse{}
	log.Debugw("Add",
		logging.Stringer("AddRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("AddResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *setProxy) Contains(ctx context.Context, request *setv1.ContainsRequest) (*setv1.ContainsResponse, error) {
	log.Debugw("Contains",
		logging.Stringer("ContainsRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.Element.Value))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Contains",
			logging.Stringer("ContainsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Contains",
			logging.Stringer("ContainsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Query[*ContainsResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*ContainsResponse, error) {
		input := &ContainsRequest{
			Headers: headers,
			ContainsInput: &ContainsInput{
				Element: Element{
					Value: request.Element.Value,
				},
			},
		}
		return NewSetClient(conn).Contains(ctx, input)
	})
	if !ok {
		log.Warnw("Contains",
			logging.Stringer("ContainsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Contains",
			logging.Stringer("ContainsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &setv1.ContainsResponse{
		Contains: output.Contains,
	}
	log.Debugw("Contains",
		logging.Stringer("ContainsRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("ContainsResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *setProxy) Remove(ctx context.Context, request *setv1.RemoveRequest) (*setv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.Element.Value))
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
	_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*RemoveResponse, error) {
		input := &RemoveRequest{
			Headers: headers,
			RemoveInput: &RemoveInput{
				Element: Element{
					Value: request.Element.Value,
				},
			},
		}
		return NewSetClient(conn).Remove(ctx, input)
	})
	if !ok {
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Remove",
			logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &setv1.RemoveResponse{}
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("RemoveResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *setProxy) Clear(ctx context.Context, request *setv1.ClearRequest) (*setv1.ClearResponse, error) {
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
		_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*ClearResponse, error) {
			return NewSetClient(conn).Clear(ctx, &ClearRequest{
				Headers:    headers,
				ClearInput: &ClearInput{},
			})
		})
		if !ok {
			log.Warnw("Clear",
				logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		} else if err != nil {
			log.Debugw("Clear",
				logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
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
		logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("ClearResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *setProxy) Events(request *setv1.EventsRequest, server setv1.Set_EventsServer) error {
	log.Debugw("Events received",
		logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)))
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
					logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
					logging.Error("Error", err))
				ch <- streams.Result[*setv1.EventsResponse]{
					Error: err,
				}
				return
			}
			primitive, err := session.GetPrimitive(request.ID.Name)
			if err != nil {
				log.Warnw("Events",
					logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
					logging.Error("Error", err))
				ch <- streams.Result[*setv1.EventsResponse]{
					Error: err,
				}
				return
			}
			proposal := client.StreamProposal[*EventsResponse](primitive)
			stream, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (client.ProposalStream[*EventsResponse], error) {
				return NewSetClient(conn).Events(server.Context(), &EventsRequest{
					Headers:     headers,
					EventsInput: &EventsInput{},
				})
			})
			if err != nil {
				log.Warnw("Events",
					logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
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
							logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
							logging.Error("Error", err))
						ch <- streams.Result[*setv1.EventsResponse]{
							Error: err,
						}
					}
					return
				}
				if err != nil {
					log.Debugw("Events",
						logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
						logging.Error("Error", err))
					ch <- streams.Result[*setv1.EventsResponse]{
						Error: errors.ToProto(err),
					}
				} else {
					response := &setv1.EventsResponse{
						Event: setv1.Event{},
					}
					switch e := output.Event.Event.(type) {
					case *Event_Added_:
						response.Event.Event = &setv1.Event_Added_{
							Added: &setv1.Event_Added{
								Element: setv1.Element{
									Value: e.Added.Element.Value,
								},
							},
						}
					case *Event_Removed_:
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
						logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
						logging.Stringer("EventsResponse", stringer.Truncate(response, truncLen)))
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
		logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)))
	return nil
}

func (s *setProxy) Elements(request *setv1.ElementsRequest, server setv1.Set_ElementsServer) error {
	log.Debugw("Elements received",
		logging.Stringer("ElementsRequest", stringer.Truncate(request, truncLen)))
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
					logging.Stringer("ElementsRequest", stringer.Truncate(request, truncLen)),
					logging.Error("Error", err))
				ch <- streams.Result[*setv1.ElementsResponse]{
					Error: err,
				}
				return
			}
			primitive, err := session.GetPrimitive(request.ID.Name)
			if err != nil {
				log.Warnw("Elements",
					logging.Stringer("ElementsRequest", stringer.Truncate(request, truncLen)),
					logging.Error("Error", err))
				ch <- streams.Result[*setv1.ElementsResponse]{
					Error: err,
				}
				return
			}
			query := client.StreamQuery[*ElementsResponse](primitive)
			stream, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (client.QueryStream[*ElementsResponse], error) {
				return NewSetClient(conn).Elements(server.Context(), &ElementsRequest{
					Headers: headers,
					ElementsInput: &ElementsInput{
						Watch: request.Watch,
					},
				})
			})
			if err != nil {
				log.Warnw("Elements",
					logging.Stringer("ElementsRequest", stringer.Truncate(request, truncLen)),
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
							logging.Stringer("ElementsRequest", stringer.Truncate(request, truncLen)),
							logging.Error("Error", err))
						ch <- streams.Result[*setv1.ElementsResponse]{
							Error: err,
						}
					}
					return
				}
				if err != nil {
					log.Debugw("Elements",
						logging.Stringer("ElementsRequest", stringer.Truncate(request, truncLen)),
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
						logging.Stringer("ElementsRequest", stringer.Truncate(request, truncLen)),
						logging.Stringer("ElementsResponse", stringer.Truncate(response, truncLen)))
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
		logging.Stringer("ElementsRequest", stringer.Truncate(request, truncLen)))
	return nil
}

var _ setv1.SetServer = (*setProxy)(nil)

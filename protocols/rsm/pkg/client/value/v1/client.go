// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	valuev1 "github.com/atomix/atomix/api/runtime/value/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	valueprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/value/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/client"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"google.golang.org/grpc"
	"io"
)

var log = logging.GetLogger()

func NewValue(protocol *client.Protocol) valuev1.ValueServer {
	return &valueClient{
		Protocol: protocol,
	}
}

type valueClient struct {
	*client.Protocol
}

func (s *valueClient) Create(ctx context.Context, request *valuev1.CreateRequest) (*valuev1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc128("CreateRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Create",
			logging.Trunc128("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	if err := session.CreatePrimitive(ctx, runtimev1.PrimitiveMeta{
		Type:        valuev1.PrimitiveType,
		PrimitiveID: request.ID,
		Tags:        request.Tags,
	}); err != nil {
		log.Warnw("Create",
			logging.Trunc128("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &valuev1.CreateResponse{}
	log.Debugw("Create",
		logging.Trunc128("CreateRequest", request),
		logging.Trunc128("CreateResponse", response))
	return response, nil
}

func (s *valueClient) Close(ctx context.Context, request *valuev1.CloseRequest) (*valuev1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc128("CloseRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Close",
			logging.Trunc128("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	if err := session.ClosePrimitive(ctx, request.ID.Name); err != nil {
		log.Warnw("Close",
			logging.Trunc128("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &valuev1.CloseResponse{}
	log.Debugw("Close",
		logging.Trunc128("CloseRequest", request),
		logging.Trunc128("CloseResponse", response))
	return response, nil
}

func (s *valueClient) Set(ctx context.Context, request *valuev1.SetRequest) (*valuev1.SetResponse, error) {
	log.Debugw("Set",
		logging.Trunc128("SetRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
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
	command := client.Proposal[*valueprotocolv1.SetResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*valueprotocolv1.SetResponse, error) {
		return valueprotocolv1.NewValueClient(conn).Set(ctx, &valueprotocolv1.SetRequest{
			Headers: headers,
			SetInput: &valueprotocolv1.SetInput{
				Value: request.Value,
			},
		})
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
	response := &valuev1.SetResponse{
		Version: uint64(output.Index),
	}
	log.Debugw("Set",
		logging.Trunc128("SetRequest", request),
		logging.Trunc128("SetResponse", response))
	return response, nil
}

func (s *valueClient) Insert(ctx context.Context, request *valuev1.InsertRequest) (*valuev1.InsertResponse, error) {
	log.Debugw("Insert",
		logging.Trunc128("InsertRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
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
	command := client.Proposal[*valueprotocolv1.InsertResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*valueprotocolv1.InsertResponse, error) {
		return valueprotocolv1.NewValueClient(conn).Insert(ctx, &valueprotocolv1.InsertRequest{
			Headers: headers,
			InsertInput: &valueprotocolv1.InsertInput{
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
	response := &valuev1.InsertResponse{
		Version: uint64(output.Index),
	}
	log.Debugw("Insert",
		logging.Trunc128("InsertRequest", request),
		logging.Trunc128("InsertResponse", response))
	return response, nil
}

func (s *valueClient) Get(ctx context.Context, request *valuev1.GetRequest) (*valuev1.GetResponse, error) {
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
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
	command := client.Query[*valueprotocolv1.GetResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*valueprotocolv1.GetResponse, error) {
		return valueprotocolv1.NewValueClient(conn).Get(ctx, &valueprotocolv1.GetRequest{
			Headers:  headers,
			GetInput: &valueprotocolv1.GetInput{},
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
	response := &valuev1.GetResponse{
		Value: &valuev1.VersionedValue{
			Value:   output.Value.Value,
			Version: uint64(output.Value.Index),
		},
	}
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request),
		logging.Trunc128("GetResponse", response))
	return response, nil
}

func (s *valueClient) Update(ctx context.Context, request *valuev1.UpdateRequest) (*valuev1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Trunc128("UpdateRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
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
	command := client.Proposal[*valueprotocolv1.UpdateResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*valueprotocolv1.UpdateResponse, error) {
		return valueprotocolv1.NewValueClient(conn).Update(ctx, &valueprotocolv1.UpdateRequest{
			Headers: headers,
			UpdateInput: &valueprotocolv1.UpdateInput{
				Value:     request.Value,
				PrevIndex: protocol.Index(request.PrevVersion),
				TTL:       request.TTL,
			},
		})
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
	response := &valuev1.UpdateResponse{
		Version: uint64(output.Index),
		PrevValue: valuev1.VersionedValue{
			Value:   output.PrevValue.Value,
			Version: uint64(output.PrevValue.Index),
		},
	}
	log.Debugw("Update",
		logging.Trunc128("UpdateRequest", request),
		logging.Trunc128("UpdateResponse", response))
	return response, nil
}

func (s *valueClient) Delete(ctx context.Context, request *valuev1.DeleteRequest) (*valuev1.DeleteResponse, error) {
	log.Debugw("Delete",
		logging.Trunc128("DeleteRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Delete",
			logging.Trunc128("DeleteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Delete",
			logging.Trunc128("DeleteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*valueprotocolv1.DeleteResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*valueprotocolv1.DeleteResponse, error) {
		return valueprotocolv1.NewValueClient(conn).Delete(ctx, &valueprotocolv1.DeleteRequest{
			Headers: headers,
			DeleteInput: &valueprotocolv1.DeleteInput{
				PrevIndex: protocol.Index(request.PrevVersion),
			},
		})
	})
	if !ok {
		log.Warnw("Delete",
			logging.Trunc128("DeleteRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Delete",
			logging.Trunc128("DeleteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &valuev1.DeleteResponse{
		Value: &valuev1.VersionedValue{
			Value:   output.Value.Value,
			Version: uint64(output.Value.Index),
		},
	}
	log.Debugw("Delete",
		logging.Trunc128("DeleteRequest", request),
		logging.Trunc128("DeleteResponse", response))
	return response, nil
}

func (s *valueClient) Events(request *valuev1.EventsRequest, server valuev1.Value_EventsServer) error {
	log.Debugw("Events",
		logging.Trunc128("EventsRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(server.Context())
	if err != nil {
		log.Warnw("Events",
			logging.Trunc128("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Events",
			logging.Trunc128("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	command := client.StreamProposal[*valueprotocolv1.EventsResponse](primitive)
	stream, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (client.ProposalStream[*valueprotocolv1.EventsResponse], error) {
		return valueprotocolv1.NewValueClient(conn).Events(server.Context(), &valueprotocolv1.EventsRequest{
			Headers:     headers,
			EventsInput: &valueprotocolv1.EventsInput{},
		})
	})
	if err != nil {
		log.Warnw("Events",
			logging.Trunc128("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	for {
		output, ok, err := stream.Recv()
		if err == io.EOF {
			log.Debugw("Events",
				logging.Trunc128("EventsRequest", request),
				logging.String("State", "Done"))
			return nil
		}
		if !ok {
			log.Warnw("Events",
				logging.Trunc128("EventsRequest", request),
				logging.Error("Error", err))
			return err
		} else if err != nil {
			log.Debugw("Events",
				logging.Trunc128("EventsRequest", request),
				logging.Error("Error", err))
			return err
		}
		response := &valuev1.EventsResponse{
			Event: valuev1.Event{},
		}
		switch e := output.Event.Event.(type) {
		case *valueprotocolv1.Event_Created_:
			response.Event.Event = &valuev1.Event_Created_{
				Created: &valuev1.Event_Created{
					Value: valuev1.VersionedValue{
						Value:   e.Created.Value.Value,
						Version: uint64(e.Created.Value.Index),
					},
				},
			}
		case *valueprotocolv1.Event_Updated_:
			response.Event.Event = &valuev1.Event_Updated_{
				Updated: &valuev1.Event_Updated{
					Value: valuev1.VersionedValue{
						Value:   e.Updated.Value.Value,
						Version: uint64(e.Updated.Value.Index),
					},
					PrevValue: valuev1.VersionedValue{
						Value:   e.Updated.PrevValue.Value,
						Version: uint64(e.Updated.PrevValue.Index),
					},
				},
			}
		case *valueprotocolv1.Event_Deleted_:
			response.Event.Event = &valuev1.Event_Deleted_{
				Deleted: &valuev1.Event_Deleted{
					Value: valuev1.VersionedValue{
						Value:   e.Deleted.Value.Value,
						Version: uint64(e.Deleted.Value.Index),
					},
					Expired: e.Deleted.Expired,
				},
			}
		}
		log.Debugw("Events",
			logging.Trunc128("EventsRequest", request),
			logging.Trunc128("EventsResponse", response))
		if err := server.Send(response); err != nil {
			log.Warnw("Events",
				logging.Trunc128("EventsRequest", request),
				logging.Trunc128("EventsResponse", response),
				logging.Error("Error", err))
			return err
		}
	}
}

func (s *valueClient) Watch(request *valuev1.WatchRequest, server valuev1.Value_WatchServer) error {
	log.Debugw("Events",
		logging.Trunc128("EventsRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(server.Context())
	if err != nil {
		log.Warnw("Events",
			logging.Trunc128("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Events",
			logging.Trunc128("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	query := client.StreamQuery[*valueprotocolv1.WatchResponse](primitive)
	stream, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (client.QueryStream[*valueprotocolv1.WatchResponse], error) {
		return valueprotocolv1.NewValueClient(conn).Watch(server.Context(), &valueprotocolv1.WatchRequest{
			Headers:    headers,
			WatchInput: &valueprotocolv1.WatchInput{},
		})
	})
	if err != nil {
		log.Warnw("Watch",
			logging.Trunc128("WatchRequest", request),
			logging.Error("Error", err))
		return err
	}
	for {
		output, ok, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if !ok {
			log.Warnw("Watch",
				logging.Trunc128("WatchRequest", request),
				logging.Error("Error", err))
			return err
		} else if err != nil {
			log.Debugw("Watch",
				logging.Trunc128("WatchRequest", request),
				logging.Error("Error", err))
			return err
		}
		response := &valuev1.WatchResponse{
			Value: &valuev1.VersionedValue{
				Value:   output.Value.Value,
				Version: uint64(output.Value.Index),
			},
		}
		log.Debugw("Watch",
			logging.Trunc128("WatchRequest", request),
			logging.Trunc128("WatchResponse", response))
		if err := server.Send(response); err != nil {
			log.Warnw("Watch",
				logging.Trunc128("WatchRequest", request),
				logging.Trunc128("WatchResponse", response),
				logging.Error("Error", err))
			return err
		}
	}
}

var _ valuev1.ValueServer = (*valueClient)(nil)

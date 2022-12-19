// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	valuev1 "github.com/atomix/atomix/api/pkg/runtime/value/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	valueprotocolv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/value/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/client"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	valueruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/value/v1"
	"github.com/atomix/atomix/runtime/pkg/utils/stringer"
	"google.golang.org/grpc"
	"io"
)

var log = logging.GetLogger()

const truncLen = 200

func NewValueProxy(protocol *client.Protocol, spec runtimev1.PrimitiveSpec) (valueruntimev1.Value, error) {
	return &valueProxy{
		Protocol:      protocol,
		PrimitiveSpec: spec,
	}, nil
}

type valueProxy struct {
	*client.Protocol
	runtimev1.PrimitiveSpec
}

func (s *valueProxy) Create(ctx context.Context, request *valuev1.CreateRequest) (*valuev1.CreateResponse, error) {
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
	if err := session.CreatePrimitive(ctx, s.PrimitiveMeta); err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &valuev1.CreateResponse{}
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("CreateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *valueProxy) Close(ctx context.Context, request *valuev1.CloseRequest) (*valuev1.CloseResponse, error) {
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
	response := &valuev1.CloseResponse{}
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("CloseResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *valueProxy) Set(ctx context.Context, request *valuev1.SetRequest) (*valuev1.SetResponse, error) {
	log.Debugw("Set",
		logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Set",
			logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Set",
			logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
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
			logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Set",
			logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &valuev1.SetResponse{
		Version: uint64(output.Index),
	}
	log.Debugw("Set",
		logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("SetResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *valueProxy) Insert(ctx context.Context, request *valuev1.InsertRequest) (*valuev1.InsertResponse, error) {
	log.Debugw("Insert",
		logging.Stringer("InsertRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
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
			logging.Stringer("InsertRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Insert",
			logging.Stringer("InsertRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &valuev1.InsertResponse{
		Version: uint64(output.Index),
	}
	log.Debugw("Insert",
		logging.Stringer("InsertRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("InsertResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *valueProxy) Get(ctx context.Context, request *valuev1.GetRequest) (*valuev1.GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
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
	command := client.Query[*valueprotocolv1.GetResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*valueprotocolv1.GetResponse, error) {
		return valueprotocolv1.NewValueClient(conn).Get(ctx, &valueprotocolv1.GetRequest{
			Headers:  headers,
			GetInput: &valueprotocolv1.GetInput{},
		})
	})
	if !ok {
		log.Warnw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &valuev1.GetResponse{
		Value: &valuev1.VersionedValue{
			Value:   output.Value.Value,
			Version: uint64(output.Value.Index),
		},
	}
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("GetResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *valueProxy) Update(ctx context.Context, request *valuev1.UpdateRequest) (*valuev1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
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
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &valuev1.UpdateResponse{
		Version: uint64(output.Index),
		PrevValue: valuev1.VersionedValue{
			Value:   output.PrevValue.Value,
			Version: uint64(output.PrevValue.Index),
		},
	}
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("UpdateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *valueProxy) Delete(ctx context.Context, request *valuev1.DeleteRequest) (*valuev1.DeleteResponse, error) {
	log.Debugw("Delete",
		logging.Stringer("DeleteRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Delete",
			logging.Stringer("DeleteRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Delete",
			logging.Stringer("DeleteRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
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
			logging.Stringer("DeleteRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Delete",
			logging.Stringer("DeleteRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &valuev1.DeleteResponse{
		Value: &valuev1.VersionedValue{
			Value:   output.Value.Value,
			Version: uint64(output.Value.Index),
		},
	}
	log.Debugw("Delete",
		logging.Stringer("DeleteRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("DeleteResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *valueProxy) Events(request *valuev1.EventsRequest, server valuev1.Value_EventsServer) error {
	log.Debugw("Events",
		logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(server.Context())
	if err != nil {
		log.Warnw("Events",
			logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Events",
			logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return errors.ToProto(err)
	}
	command := client.StreamProposal[*valueprotocolv1.EventsResponse](primitive)
	stream, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (client.ProposalStream[*valueprotocolv1.EventsResponse], error) {
		return valueprotocolv1.NewValueClient(conn).Events(server.Context(), &valueprotocolv1.EventsRequest{
			Headers:     headers,
			EventsInput: &valueprotocolv1.EventsInput{},
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
		output, ok, err := stream.Recv()
		if err == io.EOF {
			log.Debugw("Events",
				logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
				logging.String("State", "Done"))
			return nil
		}
		if !ok {
			log.Warnw("Events",
				logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return errors.ToProto(err)
		} else if err != nil {
			log.Debugw("Events",
				logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return errors.ToProto(err)
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
}

func (s *valueProxy) Watch(request *valuev1.WatchRequest, server valuev1.Value_WatchServer) error {
	log.Debugw("Events",
		logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(server.Context())
	if err != nil {
		log.Warnw("Events",
			logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Events",
			logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return errors.ToProto(err)
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
			logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return errors.ToProto(err)
	}
	for {
		output, ok, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if !ok {
			log.Warnw("Watch",
				logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return errors.ToProto(err)
		} else if err != nil {
			log.Debugw("Watch",
				logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return errors.ToProto(err)
		}
		response := &valuev1.WatchResponse{
			Value: &valuev1.VersionedValue{
				Value:   output.Value.Value,
				Version: uint64(output.Value.Index),
			},
		}
		log.Debugw("Watch",
			logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
			logging.Stringer("WatchResponse", stringer.Truncate(response, truncLen)))
		if err := server.Send(response); err != nil {
			log.Warnw("Watch",
				logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
				logging.Stringer("WatchResponse", stringer.Truncate(response, truncLen)),
				logging.Error("Error", err))
			return err
		}
	}
}

var _ valuev1.ValueServer = (*valueProxy)(nil)

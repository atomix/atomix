// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	indexedmapv1 "github.com/atomix/atomix/api/runtime/indexedmap/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	indexedmapprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/indexedmap/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/client"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/utils/async"
	"google.golang.org/grpc"
	"io"
)

var log = logging.GetLogger()

func NewIndexedMap(protocol *client.Protocol, id runtimev1.PrimitiveID) *IndexedMapSession {
	return &IndexedMapSession{
		Protocol: protocol,
		id:       id,
	}
}

type IndexedMapSession struct {
	*client.Protocol
	id runtimev1.PrimitiveID
}

func (s *IndexedMapSession) Open(ctx context.Context) error {
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
			Type:        indexedmapv1.PrimitiveType,
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

func (s *IndexedMapSession) Close(ctx context.Context) error {
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

func (s *IndexedMapSession) Size(ctx context.Context, request *indexedmapv1.SizeRequest) (*indexedmapv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Trunc128("SizeRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Size",
			logging.Trunc128("SizeRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Size",
			logging.Trunc128("SizeRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	query := client.Query[*indexedmapprotocolv1.SizeResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*indexedmapprotocolv1.SizeResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).Size(ctx, &indexedmapprotocolv1.SizeRequest{
			Headers:   headers,
			SizeInput: &indexedmapprotocolv1.SizeInput{},
		})
	})
	if !ok {
		log.Warnw("Size",
			logging.Trunc128("SizeRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Size",
			logging.Trunc128("SizeRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &indexedmapv1.SizeResponse{
		Size_: output.Size_,
	}
	log.Debugw("Size",
		logging.Trunc128("SizeRequest", request),
		logging.Trunc128("SizeResponse", response))
	return response, nil
}

func (s *IndexedMapSession) Append(ctx context.Context, request *indexedmapv1.AppendRequest) (*indexedmapv1.AppendResponse, error) {
	log.Debugw("Append",
		logging.Trunc128("AppendRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Append",
			logging.Trunc128("AppendRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Append",
			logging.Trunc128("AppendRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*indexedmapprotocolv1.AppendResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*indexedmapprotocolv1.AppendResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).Append(ctx, &indexedmapprotocolv1.AppendRequest{
			Headers: headers,
			AppendInput: &indexedmapprotocolv1.AppendInput{
				Key:   request.Key,
				Value: request.Value,
				TTL:   request.TTL,
			},
		})
	})
	if !ok {
		log.Warnw("Append",
			logging.Trunc128("AppendRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Append",
			logging.Trunc128("AppendRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &indexedmapv1.AppendResponse{
		Entry: newClientEntry(output.Entry),
	}
	log.Debugw("Append",
		logging.Trunc128("AppendRequest", request),
		logging.Trunc128("AppendResponse", response))
	return response, nil
}

func (s *IndexedMapSession) Update(ctx context.Context, request *indexedmapv1.UpdateRequest) (*indexedmapv1.UpdateResponse, error) {
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
	command := client.Proposal[*indexedmapprotocolv1.UpdateResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*indexedmapprotocolv1.UpdateResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).Update(ctx, &indexedmapprotocolv1.UpdateRequest{
			Headers: headers,
			UpdateInput: &indexedmapprotocolv1.UpdateInput{
				Key:         request.Key,
				Index:       request.Index,
				Value:       request.Value,
				TTL:         request.TTL,
				PrevVersion: request.PrevVersion,
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
	response := &indexedmapv1.UpdateResponse{
		Entry: newClientEntry(output.Entry),
	}
	log.Debugw("Update",
		logging.Trunc128("UpdateRequest", request),
		logging.Trunc128("UpdateResponse", response))
	return response, nil
}

func (s *IndexedMapSession) Get(ctx context.Context, request *indexedmapv1.GetRequest) (*indexedmapv1.GetResponse, error) {
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
	query := client.Query[*indexedmapprotocolv1.GetResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*indexedmapprotocolv1.GetResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).Get(ctx, &indexedmapprotocolv1.GetRequest{
			Headers: headers,
			GetInput: &indexedmapprotocolv1.GetInput{
				Key:   request.Key,
				Index: request.Index,
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
	response := &indexedmapv1.GetResponse{
		Entry: newClientEntry(output.Entry),
	}
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request),
		logging.Trunc128("GetResponse", response))
	return response, nil
}

func (s *IndexedMapSession) FirstEntry(ctx context.Context, request *indexedmapv1.FirstEntryRequest) (*indexedmapv1.FirstEntryResponse, error) {
	log.Debugw("FirstEntry",
		logging.Trunc128("FirstEntryRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("FirstEntry",
			logging.Trunc128("FirstEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("FirstEntry",
			logging.Trunc128("FirstEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	query := client.Query[*indexedmapprotocolv1.FirstEntryResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*indexedmapprotocolv1.FirstEntryResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).FirstEntry(ctx, &indexedmapprotocolv1.FirstEntryRequest{
			Headers:         headers,
			FirstEntryInput: &indexedmapprotocolv1.FirstEntryInput{},
		})
	})
	if !ok {
		log.Warnw("FirstEntry",
			logging.Trunc128("FirstEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("FirstEntry",
			logging.Trunc128("FirstEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &indexedmapv1.FirstEntryResponse{
		Entry: newClientEntry(output.Entry),
	}
	log.Debugw("FirstEntry",
		logging.Trunc128("FirstEntryRequest", request),
		logging.Trunc128("FirstEntryResponse", response))
	return response, nil
}

func (s *IndexedMapSession) LastEntry(ctx context.Context, request *indexedmapv1.LastEntryRequest) (*indexedmapv1.LastEntryResponse, error) {
	log.Debugw("LastEntry",
		logging.Trunc128("LastEntryRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("LastEntry",
			logging.Trunc128("LastEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("LastEntry",
			logging.Trunc128("LastEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	query := client.Query[*indexedmapprotocolv1.LastEntryResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*indexedmapprotocolv1.LastEntryResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).LastEntry(ctx, &indexedmapprotocolv1.LastEntryRequest{
			Headers:        headers,
			LastEntryInput: &indexedmapprotocolv1.LastEntryInput{},
		})
	})
	if !ok {
		log.Warnw("LastEntry",
			logging.Trunc128("LastEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("LastEntry",
			logging.Trunc128("LastEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &indexedmapv1.LastEntryResponse{
		Entry: newClientEntry(output.Entry),
	}
	log.Debugw("LastEntry",
		logging.Trunc128("LastEntryRequest", request),
		logging.Trunc128("LastEntryResponse", response))
	return response, nil
}

func (s *IndexedMapSession) NextEntry(ctx context.Context, request *indexedmapv1.NextEntryRequest) (*indexedmapv1.NextEntryResponse, error) {
	log.Debugw("NextEntry",
		logging.Trunc128("NextEntryRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("NextEntry",
			logging.Trunc128("NextEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("NextEntry",
			logging.Trunc128("NextEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	query := client.Query[*indexedmapprotocolv1.NextEntryResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*indexedmapprotocolv1.NextEntryResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).NextEntry(ctx, &indexedmapprotocolv1.NextEntryRequest{
			Headers: headers,
			NextEntryInput: &indexedmapprotocolv1.NextEntryInput{
				Index: request.Index,
			},
		})
	})
	if !ok {
		log.Warnw("NextEntry",
			logging.Trunc128("NextEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("NextEntry",
			logging.Trunc128("NextEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &indexedmapv1.NextEntryResponse{
		Entry: newClientEntry(output.Entry),
	}
	log.Debugw("NextEntry",
		logging.Trunc128("NextEntryRequest", request),
		logging.Trunc128("NextEntryResponse", response))
	return response, nil
}

func (s *IndexedMapSession) PrevEntry(ctx context.Context, request *indexedmapv1.PrevEntryRequest) (*indexedmapv1.PrevEntryResponse, error) {
	log.Debugw("PrevEntry",
		logging.Trunc128("PrevEntryRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("PrevEntry",
			logging.Trunc128("PrevEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("PrevEntry",
			logging.Trunc128("PrevEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	query := client.Query[*indexedmapprotocolv1.PrevEntryResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*indexedmapprotocolv1.PrevEntryResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).PrevEntry(ctx, &indexedmapprotocolv1.PrevEntryRequest{
			Headers: headers,
			PrevEntryInput: &indexedmapprotocolv1.PrevEntryInput{
				Index: request.Index,
			},
		})
	})
	if !ok {
		log.Warnw("PrevEntry",
			logging.Trunc128("PrevEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("PrevEntry",
			logging.Trunc128("PrevEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &indexedmapv1.PrevEntryResponse{
		Entry: newClientEntry(output.Entry),
	}
	log.Debugw("PrevEntry",
		logging.Trunc128("PrevEntryRequest", request),
		logging.Trunc128("PrevEntryResponse", response))
	return response, nil
}

func (s *IndexedMapSession) Remove(ctx context.Context, request *indexedmapv1.RemoveRequest) (*indexedmapv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Trunc128("RemoveRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
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
	command := client.Proposal[*indexedmapprotocolv1.RemoveResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*indexedmapprotocolv1.RemoveResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).Remove(ctx, &indexedmapprotocolv1.RemoveRequest{
			Headers: headers,
			RemoveInput: &indexedmapprotocolv1.RemoveInput{
				Key:         request.Key,
				Index:       request.Index,
				PrevVersion: request.PrevVersion,
			},
		})
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
	response := &indexedmapv1.RemoveResponse{
		Entry: newClientEntry(output.Entry),
	}
	log.Debugw("Remove",
		logging.Trunc128("RemoveRequest", request),
		logging.Trunc128("RemoveResponse", response))
	return response, nil
}

func (s *IndexedMapSession) Clear(ctx context.Context, request *indexedmapv1.ClearRequest) (*indexedmapv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Trunc128("ClearRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Clear",
			logging.Trunc128("ClearRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Clear",
			logging.Trunc128("ClearRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*indexedmapprotocolv1.ClearResponse](primitive)
	_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*indexedmapprotocolv1.ClearResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).Clear(ctx, &indexedmapprotocolv1.ClearRequest{
			Headers:    headers,
			ClearInput: &indexedmapprotocolv1.ClearInput{},
		})
	})
	if !ok {
		log.Warnw("Clear",
			logging.Trunc128("ClearRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Clear",
			logging.Trunc128("ClearRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &indexedmapv1.ClearResponse{}
	log.Debugw("Clear",
		logging.Trunc128("ClearRequest", request),
		logging.Trunc128("ClearResponse", response))
	return response, nil
}

func (s *IndexedMapSession) Entries(request *indexedmapv1.EntriesRequest, server indexedmapv1.IndexedMap_EntriesServer) error {
	log.Debugw("Entries",
		logging.Trunc128("EntriesRequest", request))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(server.Context())
	if err != nil {
		log.Warnw("Entries",
			logging.Trunc128("EntriesRequest", request),
			logging.Error("Error", err))
		return err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Entries",
			logging.Trunc128("EntriesRequest", request),
			logging.Error("Error", err))
		return err
	}
	query := client.StreamQuery[*indexedmapprotocolv1.EntriesResponse](primitive)
	stream, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (client.QueryStream[*indexedmapprotocolv1.EntriesResponse], error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).Entries(server.Context(), &indexedmapprotocolv1.EntriesRequest{
			Headers: headers,
			EntriesInput: &indexedmapprotocolv1.EntriesInput{
				Watch: request.Watch,
			},
		})
	})
	if err != nil {
		log.Warnw("Entries",
			logging.Trunc128("EntriesRequest", request),
			logging.Error("Error", err))
		return err
	}
	for {
		output, ok, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if !ok {
			log.Warnw("Entries",
				logging.Trunc128("EntriesRequest", request),
				logging.Error("Error", err))
			return err
		} else if err != nil {
			log.Debugw("Entries",
				logging.Trunc128("EntriesRequest", request),
				logging.Error("Error", err))
			return err
		}
		response := &indexedmapv1.EntriesResponse{
			Entry: *newClientEntry(&output.Entry),
		}
		log.Debugw("Entries",
			logging.Trunc128("EntriesRequest", request),
			logging.Trunc128("EntriesResponse", response))
		if err := server.Send(response); err != nil {
			log.Warnw("Entries",
				logging.Trunc128("EntriesRequest", request),
				logging.Trunc128("EntriesResponse", response),
				logging.Error("Error", err))
			return err
		}
	}
}

func (s *IndexedMapSession) Events(request *indexedmapv1.EventsRequest, server indexedmapv1.IndexedMap_EventsServer) error {
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
	command := client.StreamProposal[*indexedmapprotocolv1.EventsResponse](primitive)
	stream, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (client.ProposalStream[*indexedmapprotocolv1.EventsResponse], error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).Events(server.Context(), &indexedmapprotocolv1.EventsRequest{
			Headers: headers,
			EventsInput: &indexedmapprotocolv1.EventsInput{
				Key: request.Key,
			},
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
		response := &indexedmapv1.EventsResponse{
			Event: indexedmapv1.Event{
				Key:   output.Event.Key,
				Index: output.Event.Index,
			},
		}
		switch e := output.Event.Event.(type) {
		case *indexedmapprotocolv1.Event_Inserted_:
			response.Event.Event = &indexedmapv1.Event_Inserted_{
				Inserted: &indexedmapv1.Event_Inserted{
					Value: *newClientValue(&e.Inserted.Value),
				},
			}
		case *indexedmapprotocolv1.Event_Updated_:
			response.Event.Event = &indexedmapv1.Event_Updated_{
				Updated: &indexedmapv1.Event_Updated{
					Value:     *newClientValue(&e.Updated.Value),
					PrevValue: *newClientValue(&e.Updated.PrevValue),
				},
			}
		case *indexedmapprotocolv1.Event_Removed_:
			response.Event.Event = &indexedmapv1.Event_Removed_{
				Removed: &indexedmapv1.Event_Removed{
					Value:   *newClientValue(&e.Removed.Value),
					Expired: e.Removed.Expired,
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

func newClientEntry(entry *indexedmapprotocolv1.Entry) *indexedmapv1.Entry {
	if entry == nil {
		return nil
	}
	return &indexedmapv1.Entry{
		Key:   entry.Key,
		Index: entry.Index,
		Value: newClientValue(entry.Value),
	}
}

func newClientValue(value *indexedmapprotocolv1.Value) *indexedmapv1.VersionedValue {
	if value == nil {
		return nil
	}
	return &indexedmapv1.VersionedValue{
		Value:   value.Value,
		Version: value.Version,
	}
}

var _ indexedmapv1.IndexedMapServer = (*IndexedMapSession)(nil)

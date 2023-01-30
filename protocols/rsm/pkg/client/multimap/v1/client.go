// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	multimapv1 "github.com/atomix/atomix/api/runtime/multimap/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	multimapprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/multimap/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/client"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtimemultimapv1 "github.com/atomix/atomix/runtime/pkg/runtime/multimap/v1"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
	"github.com/atomix/atomix/runtime/pkg/utils/async"
	"google.golang.org/grpc"
	"io"
	"sync"
)

var log = logging.GetLogger()

func NewMultiMap(protocol *client.Protocol, id runtimev1.PrimitiveID) *MultiMapSession {
	return &MultiMapSession{
		Protocol: protocol,
		id:       id,
	}
}

type MultiMapSession struct {
	*client.Protocol
	id runtimev1.PrimitiveID
}

func (s *MultiMapSession) Open(ctx context.Context) error {
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
			Type:        multimapv1.PrimitiveType,
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

func (s *MultiMapSession) Close(ctx context.Context) error {
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

func (s *MultiMapSession) Size(ctx context.Context, request *multimapv1.SizeRequest) (*multimapv1.SizeResponse, error) {
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
		query := client.Query[*multimapprotocolv1.SizeResponse](primitive)
		output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*multimapprotocolv1.SizeResponse, error) {
			return multimapprotocolv1.NewMultiMapClient(conn).Size(ctx, &multimapprotocolv1.SizeRequest{
				Headers:   headers,
				SizeInput: &multimapprotocolv1.SizeInput{},
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
	response := &multimapv1.SizeResponse{
		Size_: uint32(size),
	}
	log.Debugw("Size",
		logging.Trunc128("SizeRequest", request),
		logging.Trunc128("SizeResponse", response))
	return response, nil
}

func (s *MultiMapSession) Put(ctx context.Context, request *multimapv1.PutRequest) (*multimapv1.PutResponse, error) {
	log.Debugw("Put",
		logging.Trunc128("PutRequest", request))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Put",
			logging.Trunc128("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Put",
			logging.Trunc128("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*multimapprotocolv1.PutResponse](primitive)
	_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*multimapprotocolv1.PutResponse, error) {
		input := &multimapprotocolv1.PutRequest{
			Headers: headers,
			PutInput: &multimapprotocolv1.PutInput{
				Key:   request.Key,
				Value: request.Value,
			},
		}
		return multimapprotocolv1.NewMultiMapClient(conn).Put(ctx, input)
	})
	if !ok {
		log.Warnw("Put",
			logging.Trunc128("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Put",
			logging.Trunc128("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &multimapv1.PutResponse{}
	log.Debugw("Put",
		logging.Trunc128("PutRequest", request),
		logging.Trunc128("PutResponse", response))
	return response, nil
}

func (s *MultiMapSession) PutAll(ctx context.Context, request *multimapv1.PutAllRequest) (*multimapv1.PutAllResponse, error) {
	log.Debugw("PutAll",
		logging.Trunc128("PutAllRequest", request))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("PutAll",
			logging.Trunc128("PutAllRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("PutAll",
			logging.Trunc128("PutAllRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*multimapprotocolv1.PutAllResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*multimapprotocolv1.PutAllResponse, error) {
		input := &multimapprotocolv1.PutAllRequest{
			Headers: headers,
			PutAllInput: &multimapprotocolv1.PutAllInput{
				Key:    request.Key,
				Values: request.Values,
			},
		}
		return multimapprotocolv1.NewMultiMapClient(conn).PutAll(ctx, input)
	})
	if !ok {
		log.Warnw("PutAll",
			logging.Trunc128("PutAllRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("PutAll",
			logging.Trunc128("PutAllRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &multimapv1.PutAllResponse{
		Updated: output.Updated,
	}
	log.Debugw("PutAll",
		logging.Trunc128("PutAllRequest", request),
		logging.Trunc128("PutAllResponse", response))
	return response, nil
}

func (s *MultiMapSession) PutEntries(ctx context.Context, request *multimapv1.PutEntriesRequest) (*multimapv1.PutEntriesResponse, error) {
	log.Debugw("PutEntries",
		logging.Trunc128("PutEntriesRequest", request))
	entries := make(map[int][]multimapprotocolv1.Entry)
	for _, entry := range request.Entries {
		index := s.PartitionIndex([]byte(entry.Key))
		entries[index] = append(entries[index], multimapprotocolv1.Entry{
			Key:    entry.Key,
			Values: entry.Values,
		})
	}

	indexes := make([]int, 0, len(entries))
	for index := range entries {
		indexes = append(indexes, index)
	}

	partitions := s.Partitions()
	results, err := async.ExecuteAsync[bool](len(indexes), func(i int) (bool, error) {
		index := indexes[i]
		partition := partitions[index]

		session, err := partition.GetSession(ctx)
		if err != nil {
			log.Warnw("PutEntries",
				logging.Trunc128("PutEntriesRequest", request),
				logging.Error("Error", err))
			return false, err
		}
		primitive, err := session.GetPrimitive(request.ID.Name)
		if err != nil {
			log.Warnw("PutEntries",
				logging.Trunc128("PutEntriesRequest", request),
				logging.Error("Error", err))
			return false, err
		}
		command := client.Proposal[*multimapprotocolv1.PutEntriesResponse](primitive)
		input := &multimapprotocolv1.PutEntriesInput{
			Entries: entries[index],
		}
		output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*multimapprotocolv1.PutEntriesResponse, error) {
			return multimapprotocolv1.NewMultiMapClient(conn).PutEntries(ctx, &multimapprotocolv1.PutEntriesRequest{
				Headers:         headers,
				PutEntriesInput: input,
			})
		})
		if !ok {
			log.Warnw("PutEntries",
				logging.Trunc128("PutEntriesRequest", request),
				logging.Error("Error", err))
			return false, err
		} else if err != nil {
			log.Debugw("PutEntries",
				logging.Trunc128("PutEntriesRequest", request),
				logging.Error("Error", err))
			return false, err
		}
		return output.Updated, nil
	})
	if err != nil {
		return nil, err
	}
	response := &multimapv1.PutEntriesResponse{}
	for _, updated := range results {
		if updated {
			response.Updated = true
		}
	}
	log.Debugw("PutEntries",
		logging.Trunc128("PutEntriesRequest", request),
		logging.Trunc128("PutEntriesResponse", response))
	return response, nil
}

func (s *MultiMapSession) Replace(ctx context.Context, request *multimapv1.ReplaceRequest) (*multimapv1.ReplaceResponse, error) {
	log.Debugw("Replace",
		logging.Trunc128("ReplaceRequest", request))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Replace",
			logging.Trunc128("ReplaceRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Replace",
			logging.Trunc128("ReplaceRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*multimapprotocolv1.ReplaceResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*multimapprotocolv1.ReplaceResponse, error) {
		input := &multimapprotocolv1.ReplaceRequest{
			Headers: headers,
			ReplaceInput: &multimapprotocolv1.ReplaceInput{
				Key:    request.Key,
				Values: request.Values,
			},
		}
		return multimapprotocolv1.NewMultiMapClient(conn).Replace(ctx, input)
	})
	if !ok {
		log.Warnw("Replace",
			logging.Trunc128("ReplaceRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("Replace",
			logging.Trunc128("ReplaceRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &multimapv1.ReplaceResponse{
		PrevValues: output.PrevValues,
	}
	log.Debugw("Replace",
		logging.Trunc128("ReplaceRequest", request),
		logging.Trunc128("ReplaceResponse", response))
	return response, nil
}

func (s *MultiMapSession) Contains(ctx context.Context, request *multimapv1.ContainsRequest) (*multimapv1.ContainsResponse, error) {
	log.Debugw("Contains",
		logging.Trunc128("ContainsRequest", request))
	partition := s.PartitionBy([]byte(request.Key))
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
	query := client.Query[*multimapprotocolv1.ContainsResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*multimapprotocolv1.ContainsResponse, error) {
		return multimapprotocolv1.NewMultiMapClient(conn).Contains(ctx, &multimapprotocolv1.ContainsRequest{
			Headers: headers,
			ContainsInput: &multimapprotocolv1.ContainsInput{
				Key: request.Key,
			},
		})
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
	response := &multimapv1.ContainsResponse{
		Result: output.Result,
	}
	log.Debugw("Contains",
		logging.Trunc128("ContainsRequest", request),
		logging.Trunc128("ContainsResponse", response))
	return response, nil
}

func (s *MultiMapSession) Get(ctx context.Context, request *multimapv1.GetRequest) (*multimapv1.GetResponse, error) {
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
	query := client.Query[*multimapprotocolv1.GetResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*multimapprotocolv1.GetResponse, error) {
		return multimapprotocolv1.NewMultiMapClient(conn).Get(ctx, &multimapprotocolv1.GetRequest{
			Headers: headers,
			GetInput: &multimapprotocolv1.GetInput{
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
	response := &multimapv1.GetResponse{
		Values: output.Values,
	}
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request),
		logging.Trunc128("GetResponse", response))
	return response, nil
}

func (s *MultiMapSession) Remove(ctx context.Context, request *multimapv1.RemoveRequest) (*multimapv1.RemoveResponse, error) {
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
	command := client.Proposal[*multimapprotocolv1.RemoveResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*multimapprotocolv1.RemoveResponse, error) {
		input := &multimapprotocolv1.RemoveRequest{
			Headers: headers,
			RemoveInput: &multimapprotocolv1.RemoveInput{
				Key:   request.Key,
				Value: request.Value,
			},
		}
		return multimapprotocolv1.NewMultiMapClient(conn).Remove(ctx, input)
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
	response := &multimapv1.RemoveResponse{
		Values: output.Values,
	}
	log.Debugw("Remove",
		logging.Trunc128("RemoveRequest", request),
		logging.Trunc128("RemoveResponse", response))
	return response, nil
}

func (s *MultiMapSession) RemoveAll(ctx context.Context, request *multimapv1.RemoveAllRequest) (*multimapv1.RemoveAllResponse, error) {
	log.Debugw("RemoveAll",
		logging.Trunc128("RemoveAllRequest", request))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("RemoveAll",
			logging.Trunc128("RemoveAllRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("RemoveAll",
			logging.Trunc128("RemoveAllRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	command := client.Proposal[*multimapprotocolv1.RemoveAllResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*multimapprotocolv1.RemoveAllResponse, error) {
		input := &multimapprotocolv1.RemoveAllRequest{
			Headers: headers,
			RemoveAllInput: &multimapprotocolv1.RemoveAllInput{
				Key:    request.Key,
				Values: request.Values,
			},
		}
		return multimapprotocolv1.NewMultiMapClient(conn).RemoveAll(ctx, input)
	})
	if !ok {
		log.Warnw("RemoveAll",
			logging.Trunc128("RemoveAllRequest", request),
			logging.Error("Error", err))
		return nil, err
	} else if err != nil {
		log.Debugw("RemoveAll",
			logging.Trunc128("RemoveAllRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &multimapv1.RemoveAllResponse{
		Updated: output.Updated,
	}
	log.Debugw("RemoveAll",
		logging.Trunc128("RemoveAllRequest", request),
		logging.Trunc128("RemoveAllResponse", response))
	return response, nil
}

func (s *MultiMapSession) RemoveEntries(ctx context.Context, request *multimapv1.RemoveEntriesRequest) (*multimapv1.RemoveEntriesResponse, error) {
	log.Debugw("RemoveEntries",
		logging.Trunc128("RemoveEntriesRequest", request))
	entries := make(map[int][]multimapprotocolv1.Entry)
	for _, entry := range request.Entries {
		index := s.PartitionIndex([]byte(entry.Key))
		entries[index] = append(entries[index], multimapprotocolv1.Entry{
			Key:    entry.Key,
			Values: entry.Values,
		})
	}

	indexes := make([]int, 0, len(entries))
	for index := range entries {
		indexes = append(indexes, index)
	}

	partitions := s.Partitions()
	results, err := async.ExecuteAsync[bool](len(indexes), func(i int) (bool, error) {
		index := indexes[i]
		partition := partitions[index]

		session, err := partition.GetSession(ctx)
		if err != nil {
			log.Warnw("RemoveEntries",
				logging.Trunc128("RemoveEntriesRequest", request),
				logging.Error("Error", err))
			return false, err
		}
		primitive, err := session.GetPrimitive(request.ID.Name)
		if err != nil {
			log.Warnw("RemoveEntries",
				logging.Trunc128("RemoveEntriesRequest", request),
				logging.Error("Error", err))
			return false, err
		}
		command := client.Proposal[*multimapprotocolv1.RemoveEntriesResponse](primitive)
		input := &multimapprotocolv1.RemoveEntriesInput{
			Entries: entries[index],
		}
		output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*multimapprotocolv1.RemoveEntriesResponse, error) {
			return multimapprotocolv1.NewMultiMapClient(conn).RemoveEntries(ctx, &multimapprotocolv1.RemoveEntriesRequest{
				Headers:            headers,
				RemoveEntriesInput: input,
			})
		})
		if !ok {
			log.Warnw("RemoveEntries",
				logging.Trunc128("RemoveEntriesRequest", request),
				logging.Error("Error", err))
			return false, err
		} else if err != nil {
			log.Debugw("RemoveEntries",
				logging.Trunc128("RemoveEntriesRequest", request),
				logging.Error("Error", err))
			return false, err
		}
		return output.Updated, nil
	})
	if err != nil {
		return nil, err
	}
	response := &multimapv1.RemoveEntriesResponse{}
	for _, updated := range results {
		if updated {
			response.Updated = true
		}
	}
	log.Debugw("RemoveEntries",
		logging.Trunc128("RemoveEntriesRequest", request),
		logging.Trunc128("RemoveEntriesResponse", response))
	return response, nil
}

func (s *MultiMapSession) Clear(ctx context.Context, request *multimapv1.ClearRequest) (*multimapv1.ClearResponse, error) {
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
		command := client.Proposal[*multimapprotocolv1.ClearResponse](primitive)
		_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*multimapprotocolv1.ClearResponse, error) {
			return multimapprotocolv1.NewMultiMapClient(conn).Clear(ctx, &multimapprotocolv1.ClearRequest{
				Headers:    headers,
				ClearInput: &multimapprotocolv1.ClearInput{},
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
	response := &multimapv1.ClearResponse{}
	log.Debugw("Clear",
		logging.Trunc128("ClearRequest", request),
		logging.Trunc128("ClearResponse", response))
	return response, nil
}

func (s *MultiMapSession) Events(request *multimapv1.EventsRequest, server multimapv1.MultiMap_EventsServer) error {
	log.Debugw("Events received",
		logging.Trunc128("EventsRequest", request))
	partitions := s.Partitions()
	ch := make(chan streams.Result[*multimapv1.EventsResponse])
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
				ch <- streams.Result[*multimapv1.EventsResponse]{
					Error: err,
				}
				return
			}
			primitive, err := session.GetPrimitive(request.ID.Name)
			if err != nil {
				log.Warnw("Events",
					logging.Trunc128("EventsRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*multimapv1.EventsResponse]{
					Error: err,
				}
				return
			}
			proposal := client.StreamProposal[*multimapprotocolv1.EventsResponse](primitive)
			stream, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (client.ProposalStream[*multimapprotocolv1.EventsResponse], error) {
				return multimapprotocolv1.NewMultiMapClient(conn).Events(server.Context(), &multimapprotocolv1.EventsRequest{
					Headers: headers,
					EventsInput: &multimapprotocolv1.EventsInput{
						Key: request.Key,
					},
				})
			})
			if err != nil {
				log.Warnw("Events",
					logging.Trunc128("EventsRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*multimapv1.EventsResponse]{
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
						ch <- streams.Result[*multimapv1.EventsResponse]{
							Error: err,
						}
					}
					return
				}
				if err != nil {
					log.Debugw("Events",
						logging.Trunc128("EventsRequest", request),
						logging.Error("Error", err))
					ch <- streams.Result[*multimapv1.EventsResponse]{
						Error: err,
					}
				} else {
					response := &multimapv1.EventsResponse{
						Event: multimapv1.Event{
							Key: output.Event.Key,
						},
					}
					switch e := output.Event.Event.(type) {
					case *multimapprotocolv1.Event_Added_:
						response.Event.Event = &multimapv1.Event_Added_{
							Added: &multimapv1.Event_Added{
								Value: e.Added.Value,
							},
						}
					case *multimapprotocolv1.Event_Removed_:
						response.Event.Event = &multimapv1.Event_Removed_{
							Removed: &multimapv1.Event_Removed{
								Value: e.Removed.Value,
							},
						}
					}
					log.Debugw("Events",
						logging.Trunc128("EventsRequest", request),
						logging.Trunc128("EventsResponse", response))
					ch <- streams.Result[*multimapv1.EventsResponse]{
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

func (s *MultiMapSession) Entries(request *multimapv1.EntriesRequest, server multimapv1.MultiMap_EntriesServer) error {
	log.Debugw("Entries received",
		logging.Trunc128("EntriesRequest", request))
	partitions := s.Partitions()
	ch := make(chan streams.Result[*multimapv1.EntriesResponse])
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
				ch <- streams.Result[*multimapv1.EntriesResponse]{
					Error: err,
				}
				return
			}
			primitive, err := session.GetPrimitive(request.ID.Name)
			if err != nil {
				log.Warnw("Entries",
					logging.Trunc128("EntriesRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*multimapv1.EntriesResponse]{
					Error: err,
				}
				return
			}
			query := client.StreamQuery[*multimapprotocolv1.EntriesResponse](primitive)
			stream, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (client.QueryStream[*multimapprotocolv1.EntriesResponse], error) {
				return multimapprotocolv1.NewMultiMapClient(conn).Entries(server.Context(), &multimapprotocolv1.EntriesRequest{
					Headers: headers,
					EntriesInput: &multimapprotocolv1.EntriesInput{
						Watch: request.Watch,
					},
				})
			})
			if err != nil {
				log.Warnw("Entries",
					logging.Trunc128("EntriesRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*multimapv1.EntriesResponse]{
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
						ch <- streams.Result[*multimapv1.EntriesResponse]{
							Error: err,
						}
					}
					return
				}
				if err != nil {
					log.Debugw("Entries",
						logging.Trunc128("EntriesRequest", request),
						logging.Error("Error", err))
					ch <- streams.Result[*multimapv1.EntriesResponse]{
						Error: err,
					}
				} else {
					response := &multimapv1.EntriesResponse{
						Entry: multimapv1.Entry{
							Key:    output.Entry.Key,
							Values: output.Entry.Values,
						},
					}
					log.Debugw("Entries",
						logging.Trunc128("EntriesRequest", request),
						logging.Trunc128("EntriesResponse", response))
					ch <- streams.Result[*multimapv1.EntriesResponse]{
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

var _ runtimemultimapv1.MultiMapProxy = (*MultiMapSession)(nil)

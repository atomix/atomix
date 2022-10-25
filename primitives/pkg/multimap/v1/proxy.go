// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	multimapv1 "github.com/atomix/runtime/api/atomix/runtime/multimap/v1"
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

func NewMultiMapProxy(protocol *client.Protocol, spec runtime.PrimitiveSpec) (multimapv1.MultiMapServer, error) {
	return &multiMapProxy{
		Protocol:      protocol,
		PrimitiveSpec: spec,
	}, nil
}

type multiMapProxy struct {
	*client.Protocol
	runtime.PrimitiveSpec
}

func (s *multiMapProxy) Create(ctx context.Context, request *multimapv1.CreateRequest) (*multimapv1.CreateResponse, error) {
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
	response := &multimapv1.CreateResponse{}
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("CreateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapProxy) Close(ctx context.Context, request *multimapv1.CloseRequest) (*multimapv1.CloseResponse, error) {
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
	response := &multimapv1.CloseResponse{}
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("CloseResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapProxy) Size(ctx context.Context, request *multimapv1.SizeRequest) (*multimapv1.SizeResponse, error) {
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
			return NewMultiMapClient(conn).Size(ctx, &SizeRequest{
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
	response := &multimapv1.SizeResponse{
		Size_: uint32(size),
	}
	log.Debugw("Size",
		logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("SizeResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapProxy) Put(ctx context.Context, request *multimapv1.PutRequest) (*multimapv1.PutResponse, error) {
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
	_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*PutResponse, error) {
		input := &PutRequest{
			Headers: headers,
			PutInput: &PutInput{
				Key:   request.Key,
				Value: request.Value,
			},
		}
		return NewMultiMapClient(conn).Put(ctx, input)
	})
	if !ok {
		log.Warnw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &multimapv1.PutResponse{}
	log.Debugw("Put",
		logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("PutResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapProxy) PutAll(ctx context.Context, request *multimapv1.PutAllRequest) (*multimapv1.PutAllResponse, error) {
	log.Debugw("PutAll",
		logging.Stringer("PutAllRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("PutAll",
			logging.Stringer("PutAllRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("PutAll",
			logging.Stringer("PutAllRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*PutAllResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*PutAllResponse, error) {
		input := &PutAllRequest{
			Headers: headers,
			PutAllInput: &PutAllInput{
				Key:    request.Key,
				Values: request.Values,
			},
		}
		return NewMultiMapClient(conn).PutAll(ctx, input)
	})
	if !ok {
		log.Warnw("PutAll",
			logging.Stringer("PutAllRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("PutAll",
			logging.Stringer("PutAllRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &multimapv1.PutAllResponse{
		Updated: output.Updated,
	}
	log.Debugw("PutAll",
		logging.Stringer("PutAllRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("PutAllResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapProxy) PutEntries(ctx context.Context, request *multimapv1.PutEntriesRequest) (*multimapv1.PutEntriesResponse, error) {
	log.Debugw("PutEntries",
		logging.Stringer("PutEntriesRequest", stringer.Truncate(request, truncLen)))
	entries := make(map[int][]Entry)
	for _, entry := range request.Entries {
		index := s.PartitionIndex([]byte(entry.Key))
		entries[index] = append(entries[index], Entry{
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
				logging.Stringer("PutEntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return false, err
		}
		primitive, err := session.GetPrimitive(request.ID.Name)
		if err != nil {
			log.Warnw("PutEntries",
				logging.Stringer("PutEntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return false, err
		}
		command := client.Proposal[*PutEntriesResponse](primitive)
		input := &PutEntriesInput{
			Entries: entries[index],
		}
		output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*PutEntriesResponse, error) {
			return NewMultiMapClient(conn).PutEntries(ctx, &PutEntriesRequest{
				Headers:         headers,
				PutEntriesInput: input,
			})
		})
		if !ok {
			log.Warnw("PutEntries",
				logging.Stringer("PutEntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return false, err
		} else if err != nil {
			log.Debugw("PutEntries",
				logging.Stringer("PutEntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return false, err
		}
		return output.Updated, nil
	})
	if err != nil {
		return nil, errors.ToProto(err)
	}
	response := &multimapv1.PutEntriesResponse{}
	for _, updated := range results {
		if updated {
			response.Updated = true
		}
	}
	log.Debugw("PutEntries",
		logging.Stringer("PutEntriesRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("PutEntriesResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapProxy) Replace(ctx context.Context, request *multimapv1.ReplaceRequest) (*multimapv1.ReplaceResponse, error) {
	log.Debugw("Replace",
		logging.Stringer("ReplaceRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Replace",
			logging.Stringer("ReplaceRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Replace",
			logging.Stringer("ReplaceRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*ReplaceResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*ReplaceResponse, error) {
		input := &ReplaceRequest{
			Headers: headers,
			ReplaceInput: &ReplaceInput{
				Key:    request.Key,
				Values: request.Values,
			},
		}
		return NewMultiMapClient(conn).Replace(ctx, input)
	})
	if !ok {
		log.Warnw("Replace",
			logging.Stringer("ReplaceRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Replace",
			logging.Stringer("ReplaceRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &multimapv1.ReplaceResponse{
		PrevValues: output.PrevValues,
	}
	log.Debugw("Replace",
		logging.Stringer("ReplaceRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("ReplaceResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapProxy) Contains(ctx context.Context, request *multimapv1.ContainsRequest) (*multimapv1.ContainsResponse, error) {
	log.Debugw("Contains",
		logging.Stringer("ContainsRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.Key))
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
	query := client.Query[*ContainsResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*ContainsResponse, error) {
		return NewMultiMapClient(conn).Contains(ctx, &ContainsRequest{
			Headers: headers,
			ContainsInput: &ContainsInput{
				Key: request.Key,
			},
		})
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
	response := &multimapv1.ContainsResponse{
		Result: output.Result,
	}
	log.Debugw("Contains",
		logging.Stringer("ContainsRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("ContainsResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapProxy) Get(ctx context.Context, request *multimapv1.GetRequest) (*multimapv1.GetResponse, error) {
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
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*GetResponse, error) {
		return NewMultiMapClient(conn).Get(ctx, &GetRequest{
			Headers: headers,
			GetInput: &GetInput{
				Key: request.Key,
			},
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
	response := &multimapv1.GetResponse{
		Values: output.Values,
	}
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("GetResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapProxy) Remove(ctx context.Context, request *multimapv1.RemoveRequest) (*multimapv1.RemoveResponse, error) {
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
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*RemoveResponse, error) {
		input := &RemoveRequest{
			Headers: headers,
			RemoveInput: &RemoveInput{
				Key:   request.Key,
				Value: request.Value,
			},
		}
		return NewMultiMapClient(conn).Remove(ctx, input)
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
	response := &multimapv1.RemoveResponse{
		Values: output.Values,
	}
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("RemoveResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapProxy) RemoveAll(ctx context.Context, request *multimapv1.RemoveAllRequest) (*multimapv1.RemoveAllResponse, error) {
	log.Debugw("RemoveAll",
		logging.Stringer("RemoveAllRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.Key))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("RemoveAll",
			logging.Stringer("RemoveAllRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("RemoveAll",
			logging.Stringer("RemoveAllRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*RemoveAllResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*RemoveAllResponse, error) {
		input := &RemoveAllRequest{
			Headers: headers,
			RemoveAllInput: &RemoveAllInput{
				Key:    request.Key,
				Values: request.Values,
			},
		}
		return NewMultiMapClient(conn).RemoveAll(ctx, input)
	})
	if !ok {
		log.Warnw("RemoveAll",
			logging.Stringer("RemoveAllRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("RemoveAll",
			logging.Stringer("RemoveAllRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &multimapv1.RemoveAllResponse{
		Updated: output.Updated,
	}
	log.Debugw("RemoveAll",
		logging.Stringer("RemoveAllRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("RemoveAllResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapProxy) RemoveEntries(ctx context.Context, request *multimapv1.RemoveEntriesRequest) (*multimapv1.RemoveEntriesResponse, error) {
	log.Debugw("RemoveEntries",
		logging.Stringer("RemoveEntriesRequest", stringer.Truncate(request, truncLen)))
	entries := make(map[int][]Entry)
	for _, entry := range request.Entries {
		index := s.PartitionIndex([]byte(entry.Key))
		entries[index] = append(entries[index], Entry{
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
				logging.Stringer("RemoveEntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return false, err
		}
		primitive, err := session.GetPrimitive(request.ID.Name)
		if err != nil {
			log.Warnw("RemoveEntries",
				logging.Stringer("RemoveEntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return false, err
		}
		command := client.Proposal[*RemoveEntriesResponse](primitive)
		input := &RemoveEntriesInput{
			Entries: entries[index],
		}
		output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*RemoveEntriesResponse, error) {
			return NewMultiMapClient(conn).RemoveEntries(ctx, &RemoveEntriesRequest{
				Headers:            headers,
				RemoveEntriesInput: input,
			})
		})
		if !ok {
			log.Warnw("RemoveEntries",
				logging.Stringer("RemoveEntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return false, err
		} else if err != nil {
			log.Debugw("RemoveEntries",
				logging.Stringer("RemoveEntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return false, err
		}
		return output.Updated, nil
	})
	if err != nil {
		return nil, errors.ToProto(err)
	}
	response := &multimapv1.RemoveEntriesResponse{}
	for _, updated := range results {
		if updated {
			response.Updated = true
		}
	}
	log.Debugw("RemoveEntries",
		logging.Stringer("RemoveEntriesRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("RemoveEntriesResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapProxy) Clear(ctx context.Context, request *multimapv1.ClearRequest) (*multimapv1.ClearResponse, error) {
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
			return NewMultiMapClient(conn).Clear(ctx, &ClearRequest{
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
	response := &multimapv1.ClearResponse{}
	log.Debugw("Clear",
		logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("ClearResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapProxy) Events(request *multimapv1.EventsRequest, server multimapv1.MultiMap_EventsServer) error {
	log.Debugw("Events received",
		logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)))
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
					logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
					logging.Error("Error", err))
				ch <- streams.Result[*multimapv1.EventsResponse]{
					Error: err,
				}
				return
			}
			primitive, err := session.GetPrimitive(request.ID.Name)
			if err != nil {
				log.Warnw("Events",
					logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
					logging.Error("Error", err))
				ch <- streams.Result[*multimapv1.EventsResponse]{
					Error: err,
				}
				return
			}
			proposal := client.StreamProposal[*EventsResponse](primitive)
			stream, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (client.ProposalStream[*EventsResponse], error) {
				return NewMultiMapClient(conn).Events(server.Context(), &EventsRequest{
					Headers: headers,
					EventsInput: &EventsInput{
						Key: request.Key,
					},
				})
			})
			if err != nil {
				log.Warnw("Events",
					logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
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
							logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
							logging.Error("Error", err))
						ch <- streams.Result[*multimapv1.EventsResponse]{
							Error: err,
						}
					}
					return
				}
				if err != nil {
					log.Debugw("Events",
						logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
						logging.Error("Error", err))
					ch <- streams.Result[*multimapv1.EventsResponse]{
						Error: errors.ToProto(err),
					}
				} else {
					response := &multimapv1.EventsResponse{
						Event: multimapv1.Event{
							Key: output.Event.Key,
						},
					}
					switch e := output.Event.Event.(type) {
					case *Event_Added_:
						response.Event.Event = &multimapv1.Event_Added_{
							Added: &multimapv1.Event_Added{
								Value: e.Added.Value,
							},
						}
					case *Event_Removed_:
						response.Event.Event = &multimapv1.Event_Removed_{
							Removed: &multimapv1.Event_Removed{
								Value: e.Removed.Value,
							},
						}
					}
					log.Debugw("Events",
						logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
						logging.Stringer("EventsResponse", stringer.Truncate(response, truncLen)))
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
		logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)))
	return nil
}

func (s *multiMapProxy) Entries(request *multimapv1.EntriesRequest, server multimapv1.MultiMap_EntriesServer) error {
	log.Debugw("Entries received",
		logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)))
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
					logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
					logging.Error("Error", err))
				ch <- streams.Result[*multimapv1.EntriesResponse]{
					Error: err,
				}
				return
			}
			primitive, err := session.GetPrimitive(request.ID.Name)
			if err != nil {
				log.Warnw("Entries",
					logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
					logging.Error("Error", err))
				ch <- streams.Result[*multimapv1.EntriesResponse]{
					Error: err,
				}
				return
			}
			query := client.StreamQuery[*EntriesResponse](primitive)
			stream, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (client.QueryStream[*EntriesResponse], error) {
				return NewMultiMapClient(conn).Entries(server.Context(), &EntriesRequest{
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
							logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
							logging.Error("Error", err))
						ch <- streams.Result[*multimapv1.EntriesResponse]{
							Error: err,
						}
					}
					return
				}
				if err != nil {
					log.Debugw("Entries",
						logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
						logging.Error("Error", err))
					ch <- streams.Result[*multimapv1.EntriesResponse]{
						Error: errors.ToProto(err),
					}
				} else {
					response := &multimapv1.EntriesResponse{
						Entry: multimapv1.Entry{
							Key:    output.Entry.Key,
							Values: output.Entry.Values,
						},
					}
					log.Debugw("Entries",
						logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
						logging.Stringer("EntriesResponse", stringer.Truncate(response, truncLen)))
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
		logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)))
	return nil
}

var _ multimapv1.MultiMapServer = (*multiMapProxy)(nil)

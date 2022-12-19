// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	multimapv1 "github.com/atomix/atomix/api/pkg/runtime/multimap/v1"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	multimapprotocolv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/multimap/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/client"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	multimapruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/multimap/v1"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
	"github.com/atomix/atomix/runtime/pkg/utils/async"
	"github.com/atomix/atomix/runtime/pkg/utils/stringer"
	"google.golang.org/grpc"
	"io"
	"sync"
)

var log = logging.GetLogger()

const truncLen = 200

func NewMultiMapProxy(protocol *client.Protocol, spec runtimev1.PrimitiveSpec) (multimapruntimev1.MultiMap, error) {
	return &multiMapProxy{
		Protocol:      protocol,
		PrimitiveSpec: spec,
	}, nil
}

type multiMapProxy struct {
	*client.Protocol
	runtimev1.PrimitiveSpec
}

func (s *multiMapProxy) Create(ctx context.Context, request *multimapv1.CreateRequest) (*multimapv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)))
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		session, err := partition.GetSession(ctx)
		if err != nil {
			return err
		}
		return session.CreatePrimitive(ctx, s.PrimitiveMeta)
	})
	if err != nil {
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
		query := client.Query[*multimapprotocolv1.SizeResponse](primitive)
		output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*multimapprotocolv1.SizeResponse, error) {
			return multimapprotocolv1.NewMultiMapClient(conn).Size(ctx, &multimapprotocolv1.SizeRequest{
				Headers:   headers,
				SizeInput: &multimapprotocolv1.SizeInput{},
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
		command := client.Proposal[*multimapprotocolv1.ClearResponse](primitive)
		_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*multimapprotocolv1.ClearResponse, error) {
			return multimapprotocolv1.NewMultiMapClient(conn).Clear(ctx, &multimapprotocolv1.ClearRequest{
				Headers:    headers,
				ClearInput: &multimapprotocolv1.ClearInput{},
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

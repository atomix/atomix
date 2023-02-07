// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	mapprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/map/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/client"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtimemapv1 "github.com/atomix/atomix/runtime/pkg/runtime/map/v1"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
	"github.com/atomix/atomix/runtime/pkg/utils/async"
	"google.golang.org/grpc"
	"io"
	"sync"
)

var log = logging.GetLogger()

func NewMap(protocol *client.Protocol, id runtimev1.PrimitiveID) *MapSession {
	return &MapSession{
		Protocol: protocol,
		id:       id,
	}
}

type MapSession struct {
	*client.Protocol
	id runtimev1.PrimitiveID
}

func (s *MapSession) Open(ctx context.Context) error {
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
			Type:        mapv1.PrimitiveType,
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

func (s *MapSession) Close(ctx context.Context) error {
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

func (s *MapSession) Size(ctx context.Context, request *mapv1.SizeRequest) (*mapv1.SizeResponse, error) {
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
		query := client.Query[*mapprotocolv1.SizeResponse](primitive)
		output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*mapprotocolv1.SizeResponse, error) {
			return mapprotocolv1.NewMapClient(conn).Size(ctx, &mapprotocolv1.SizeRequest{
				Headers:   headers,
				SizeInput: &mapprotocolv1.SizeInput{},
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
	response := &mapv1.SizeResponse{
		Size_: uint32(size),
	}
	log.Debugw("Size",
		logging.Trunc128("SizeRequest", request),
		logging.Trunc128("SizeResponse", response))
	return response, nil
}

func (s *MapSession) Put(ctx context.Context, request *mapv1.PutRequest) (*mapv1.PutResponse, error) {
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
	command := client.Proposal[*mapprotocolv1.PutResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*mapprotocolv1.PutResponse, error) {
		input := &mapprotocolv1.PutRequest{
			Headers: headers,
			PutInput: &mapprotocolv1.PutInput{
				Key:       request.Key,
				Value:     request.Value,
				TTL:       request.TTL,
				PrevIndex: protocol.Index(request.PrevVersion),
			},
		}
		return mapprotocolv1.NewMapClient(conn).Put(ctx, input)
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
	response := &mapv1.PutResponse{
		Version: uint64(output.Index),
	}
	if output.PrevValue != nil {
		response.PrevValue = &mapv1.VersionedValue{
			Value:   output.PrevValue.Value,
			Version: uint64(output.PrevValue.Index),
		}
	}
	log.Debugw("Put",
		logging.Trunc128("PutRequest", request),
		logging.Trunc128("PutResponse", response))
	return response, nil
}

func (s *MapSession) Insert(ctx context.Context, request *mapv1.InsertRequest) (*mapv1.InsertResponse, error) {
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
	command := client.Proposal[*mapprotocolv1.InsertResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*mapprotocolv1.InsertResponse, error) {
		return mapprotocolv1.NewMapClient(conn).Insert(ctx, &mapprotocolv1.InsertRequest{
			Headers: headers,
			InsertInput: &mapprotocolv1.InsertInput{
				Key:   request.Key,
				Value: request.Value,
				TTL:   request.TTL,
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
	response := &mapv1.InsertResponse{
		Version: uint64(output.Index),
	}
	log.Debugw("Insert",
		logging.Trunc128("InsertRequest", request),
		logging.Trunc128("InsertResponse", response))
	return response, nil
}

func (s *MapSession) Update(ctx context.Context, request *mapv1.UpdateRequest) (*mapv1.UpdateResponse, error) {
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
	command := client.Proposal[*mapprotocolv1.UpdateResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*mapprotocolv1.UpdateResponse, error) {
		input := &mapprotocolv1.UpdateRequest{
			Headers: headers,
			UpdateInput: &mapprotocolv1.UpdateInput{
				Key:       request.Key,
				Value:     request.Value,
				TTL:       request.TTL,
				PrevIndex: protocol.Index(request.PrevVersion),
			},
		}
		return mapprotocolv1.NewMapClient(conn).Update(ctx, input)
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
	response := &mapv1.UpdateResponse{
		Version: uint64(output.Index),
		PrevValue: mapv1.VersionedValue{
			Value:   output.PrevValue.Value,
			Version: uint64(output.PrevValue.Index),
		},
	}
	log.Debugw("Update",
		logging.Trunc128("UpdateRequest", request),
		logging.Trunc128("UpdateResponse", response))
	return response, nil
}

func (s *MapSession) Get(ctx context.Context, request *mapv1.GetRequest) (*mapv1.GetResponse, error) {
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
	query := client.Query[*mapprotocolv1.GetResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*mapprotocolv1.GetResponse, error) {
		return mapprotocolv1.NewMapClient(conn).Get(ctx, &mapprotocolv1.GetRequest{
			Headers: headers,
			GetInput: &mapprotocolv1.GetInput{
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
	response := &mapv1.GetResponse{
		Value: mapv1.VersionedValue{
			Value:   output.Value.Value,
			Version: uint64(output.Value.Index),
		},
	}
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request),
		logging.Trunc128("GetResponse", response))
	return response, nil
}

func (s *MapSession) Remove(ctx context.Context, request *mapv1.RemoveRequest) (*mapv1.RemoveResponse, error) {
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
	command := client.Proposal[*mapprotocolv1.RemoveResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*mapprotocolv1.RemoveResponse, error) {
		input := &mapprotocolv1.RemoveRequest{
			Headers: headers,
			RemoveInput: &mapprotocolv1.RemoveInput{
				Key:       request.Key,
				PrevIndex: protocol.Index(request.PrevVersion),
			},
		}
		return mapprotocolv1.NewMapClient(conn).Remove(ctx, input)
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
	response := &mapv1.RemoveResponse{
		Value: mapv1.VersionedValue{
			Value:   output.Value.Value,
			Version: uint64(output.Value.Index),
		},
	}
	log.Debugw("Remove",
		logging.Trunc128("RemoveRequest", request),
		logging.Trunc128("RemoveResponse", response))
	return response, nil
}

func (s *MapSession) Clear(ctx context.Context, request *mapv1.ClearRequest) (*mapv1.ClearResponse, error) {
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
		command := client.Proposal[*mapprotocolv1.ClearResponse](primitive)
		_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*mapprotocolv1.ClearResponse, error) {
			return mapprotocolv1.NewMapClient(conn).Clear(ctx, &mapprotocolv1.ClearRequest{
				Headers:    headers,
				ClearInput: &mapprotocolv1.ClearInput{},
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
	response := &mapv1.ClearResponse{}
	log.Debugw("Clear",
		logging.Trunc128("ClearRequest", request),
		logging.Trunc128("ClearResponse", response))
	return response, nil
}

func (s *MapSession) Lock(ctx context.Context, request *mapv1.LockRequest) (*mapv1.LockResponse, error) {
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
		command := client.Proposal[*mapprotocolv1.LockResponse](primitive)
		_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*mapprotocolv1.LockResponse, error) {
			return mapprotocolv1.NewMapClient(conn).Lock(ctx, &mapprotocolv1.LockRequest{
				Headers: headers,
				LockInput: &mapprotocolv1.LockInput{
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
	response := &mapv1.LockResponse{}
	log.Debugw("Lock",
		logging.Trunc128("LockRequest", request),
		logging.Trunc128("LockResponse", response))
	return response, nil
}

func (s *MapSession) Unlock(ctx context.Context, request *mapv1.UnlockRequest) (*mapv1.UnlockResponse, error) {
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
		command := client.Proposal[*mapprotocolv1.UnlockResponse](primitive)
		_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*mapprotocolv1.UnlockResponse, error) {
			return mapprotocolv1.NewMapClient(conn).Unlock(ctx, &mapprotocolv1.UnlockRequest{
				Headers:     headers,
				UnlockInput: &mapprotocolv1.UnlockInput{},
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
	response := &mapv1.UnlockResponse{}
	log.Debugw("Unlock",
		logging.Trunc128("UnlockRequest", request),
		logging.Trunc128("UnlockResponse", response))
	return response, nil
}

func (s *MapSession) Commit(ctx context.Context, request *mapv1.CommitRequest) (*mapv1.CommitResponse, error) {
	log.Debugw("Commit",
		logging.Trunc128("CommitRequest", request))

	if len(request.Operations) == 0 {
		response := &mapv1.CommitResponse{}
		log.Debugw("Commit",
			logging.Trunc128("CommitRequest", request),
			logging.Trunc128("CommitResponse", response))
		return response, nil
	}

	indexes := make(map[protocol.PartitionID][]int)
	clients := make(map[protocol.PartitionID]*client.PartitionClient)
	inputs := make([]mapprotocolv1.MapInput, len(request.Operations))
	for i, operation := range request.Operations {
		var partition *client.PartitionClient
		switch o := operation.Operation.(type) {
		case *mapv1.CommitRequest_Operation_Put:
			partition = s.PartitionBy([]byte(o.Put.Key))
			inputs[i] = mapprotocolv1.MapInput{
				Input: &mapprotocolv1.MapInput_Put{
					Put: &mapprotocolv1.PutInput{
						Key:       o.Put.Key,
						Value:     o.Put.Value,
						TTL:       o.Put.TTL,
						PrevIndex: protocol.Index(o.Put.PrevVersion),
					},
				},
			}
		case *mapv1.CommitRequest_Operation_Insert:
			partition = s.PartitionBy([]byte(o.Insert.Key))
			inputs[i] = mapprotocolv1.MapInput{
				Input: &mapprotocolv1.MapInput_Insert{
					Insert: &mapprotocolv1.InsertInput{
						Key:   o.Insert.Key,
						Value: o.Insert.Value,
						TTL:   o.Insert.TTL,
					},
				},
			}
		case *mapv1.CommitRequest_Operation_Update:
			partition = s.PartitionBy([]byte(o.Update.Key))
			inputs[i] = mapprotocolv1.MapInput{
				Input: &mapprotocolv1.MapInput_Update{
					Update: &mapprotocolv1.UpdateInput{
						Key:       o.Update.Key,
						Value:     o.Update.Value,
						TTL:       o.Update.TTL,
						PrevIndex: protocol.Index(o.Update.PrevVersion),
					},
				},
			}
		case *mapv1.CommitRequest_Operation_Remove:
			partition = s.PartitionBy([]byte(o.Remove.Key))
			inputs[i] = mapprotocolv1.MapInput{
				Input: &mapprotocolv1.MapInput_Remove{
					Remove: &mapprotocolv1.RemoveInput{
						Key:       o.Remove.Key,
						PrevIndex: protocol.Index(o.Remove.PrevVersion),
					},
				},
			}
		}
		clients[partition.ID()] = partition
		indexes[partition.ID()] = append(indexes[partition.ID()], i)
	}

	var outputs []mapprotocolv1.MapOutput
	if len(indexes) == 1 {
		for _, partition := range clients {
			session, err := partition.GetSession(ctx)
			if err != nil {
				log.Warnw("Commit",
					logging.Trunc128("CommitRequest", request),
					logging.Error("Error", err))
				return nil, err
			}
			primitive, err := session.GetPrimitive(request.ID.Name)
			if err != nil {
				log.Warnw("Commit",
					logging.Trunc128("CommitRequest", request),
					logging.Error("Error", err))
				return nil, err
			}

			apply := client.Proposal[*mapprotocolv1.ApplyResponse](primitive)
			response, ok, err := apply.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*mapprotocolv1.ApplyResponse, error) {
				return mapprotocolv1.NewMapClient(conn).Apply(ctx, &mapprotocolv1.ApplyRequest{
					Headers: headers,
					ApplyInput: &mapprotocolv1.ApplyInput{
						Inputs: inputs,
					},
				})
			})
			if !ok {
				log.Warnw("Apply",
					logging.Trunc128("ApplyRequest", request),
					logging.Error("Error", err))
				return nil, err
			} else if err != nil {
				log.Debugw("Apply",
					logging.Trunc128("ApplyRequest", request),
					logging.Error("Error", err))
				return nil, err
			}
			outputs = response.Outputs
		}
	} else {
		partitions := make([]*client.PartitionClient, 0, len(indexes))
		primitives := make([]*client.PrimitiveClient, 0, len(indexes))
		proposals := make([]*client.ProposalContext[*mapprotocolv1.PrepareResponse], 0, len(indexes))
		for _, partition := range clients {
			partitions = append(partitions, partition)
			session, err := partition.GetSession(ctx)
			if err != nil {
				log.Warnw("Commit",
					logging.Trunc128("CommitRequest", request),
					logging.Error("Error", err))
				return nil, err
			}
			primitive, err := session.GetPrimitive(request.ID.Name)
			if err != nil {
				log.Warnw("Commit",
					logging.Trunc128("CommitRequest", request),
					logging.Error("Error", err))
				return nil, err
			}
			primitives = append(primitives, primitive)
			proposals = append(proposals, client.Proposal[*mapprotocolv1.PrepareResponse](primitive))
		}

		err := async.IterAsync(len(partitions), func(i int) error {
			partitionClient := partitions[i]
			partitionIndexes := indexes[partitionClient.ID()]
			partitionInputs := make([]mapprotocolv1.MapInput, len(partitionIndexes))
			for j, index := range partitionIndexes {
				partitionInputs[j] = inputs[index]
			}

			prepare := proposals[i]
			_, ok, err := prepare.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*mapprotocolv1.PrepareResponse, error) {
				return mapprotocolv1.NewMapClient(conn).Prepare(ctx, &mapprotocolv1.PrepareRequest{
					Headers: headers,
					PrepareInput: &mapprotocolv1.PrepareInput{
						SequenceNum: prepare.Headers.SequenceNum,
						Inputs:      partitionInputs,
					},
				})
			})
			if !ok {
				log.Warnw("Prepare",
					logging.Trunc128("PrepareRequest", request),
					logging.Error("Error", err))
				return err
			} else if err != nil {
				log.Debugw("Prepare",
					logging.Trunc128("PrepareRequest", request),
					logging.Error("Error", err))
				return err
			}
			return nil
		})
		if err != nil {
			_ = async.IterAsync(len(partitions), func(i int) error {
				primitive := primitives[i]
				prepare := proposals[i]
				proposal := client.Proposal[*mapprotocolv1.AbortResponse](primitive)
				_, ok, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*mapprotocolv1.AbortResponse, error) {
					return mapprotocolv1.NewMapClient(conn).Abort(ctx, &mapprotocolv1.AbortRequest{
						Headers: headers,
						AbortInput: &mapprotocolv1.AbortInput{
							SequenceNum: prepare.Headers.SequenceNum,
						},
					})
				})
				if !ok {
					log.Warnw("Abort",
						logging.Trunc128("AbortRequest", request),
						logging.Error("Error", err))
					return err
				} else if err != nil {
					log.Debugw("Abort",
						logging.Trunc128("AbortRequest", request),
						logging.Error("Error", err))
					return err
				}
				return nil
			})
			return nil, err
		}

		outputMaps, err := async.ExecuteAsync[map[int]mapprotocolv1.MapOutput](len(partitions), func(i int) (map[int]mapprotocolv1.MapOutput, error) {
			partition := partitions[i]
			primitive := primitives[i]
			prepare := proposals[i]
			commit := client.Proposal[*mapprotocolv1.CommitResponse](primitive)
			response, ok, err := commit.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*mapprotocolv1.CommitResponse, error) {
				return mapprotocolv1.NewMapClient(conn).Commit(ctx, &mapprotocolv1.CommitRequest{
					Headers: headers,
					CommitInput: &mapprotocolv1.CommitInput{
						SequenceNum: prepare.Headers.SequenceNum,
					},
				})
			})
			if !ok {
				log.Warnw("Commit",
					logging.Trunc128("CommitRequest", request),
					logging.Error("Error", err))
				return nil, err
			} else if err != nil {
				log.Debugw("Commit",
					logging.Trunc128("CommitRequest", request),
					logging.Error("Error", err))
				return nil, err
			}

			partitionIndexes := indexes[partition.ID()]
			partitionOutputs := make(map[int]mapprotocolv1.MapOutput)
			for j, output := range response.Outputs {
				partitionOutputs[partitionIndexes[j]] = output
			}
			return partitionOutputs, nil
		})
		if err != nil {
			return nil, err
		}

		outputs = make([]mapprotocolv1.MapOutput, len(request.Operations))
		for _, outputMap := range outputMaps {
			for index, output := range outputMap {
				outputs[index] = output
			}
		}
	}

	results := make([]mapv1.CommitResponse_Result, len(request.Operations))
	for i, output := range outputs {
		switch o := output.Output.(type) {
		case *mapprotocolv1.MapOutput_Put:
			result := &mapv1.CommitResponse_Put{
				Version: uint64(o.Put.Index),
			}
			if o.Put.PrevValue != nil {
				result.PrevValue = &mapv1.VersionedValue{
					Value:   o.Put.PrevValue.Value,
					Version: uint64(o.Put.PrevValue.Index),
				}
			}
			results[i] = mapv1.CommitResponse_Result{
				Result: &mapv1.CommitResponse_Result_Put{
					Put: result,
				},
			}
		case *mapprotocolv1.MapOutput_Insert:
			result := &mapv1.CommitResponse_Insert{
				Version: uint64(o.Insert.Index),
			}
			results[i] = mapv1.CommitResponse_Result{
				Result: &mapv1.CommitResponse_Result_Insert{
					Insert: result,
				},
			}
		case *mapprotocolv1.MapOutput_Update:
			result := &mapv1.CommitResponse_Update{
				Version: uint64(o.Update.Index),
				PrevValue: mapv1.VersionedValue{
					Value:   o.Update.PrevValue.Value,
					Version: uint64(o.Update.PrevValue.Index),
				},
			}
			results[i] = mapv1.CommitResponse_Result{
				Result: &mapv1.CommitResponse_Result_Update{
					Update: result,
				},
			}
		case *mapprotocolv1.MapOutput_Remove:
			result := &mapv1.CommitResponse_Remove{
				Value: mapv1.VersionedValue{
					Value:   o.Remove.Value.Value,
					Version: uint64(o.Remove.Value.Index),
				},
			}
			results[i] = mapv1.CommitResponse_Result{
				Result: &mapv1.CommitResponse_Result_Remove{
					Remove: result,
				},
			}
		}
	}

	response := &mapv1.CommitResponse{
		Results: results,
	}
	log.Debugw("Commit",
		logging.Trunc128("CommitRequest", request),
		logging.Trunc128("CommitResponse", response))
	return response, nil
}

func (s *MapSession) Events(request *mapv1.EventsRequest, server mapv1.Map_EventsServer) error {
	log.Debugw("Events received",
		logging.Trunc128("EventsRequest", request))
	partitions := s.Partitions()
	ch := make(chan streams.Result[*mapv1.EventsResponse])
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
				ch <- streams.Result[*mapv1.EventsResponse]{
					Error: err,
				}
				return
			}
			primitive, err := session.GetPrimitive(request.ID.Name)
			if err != nil {
				log.Warnw("Events",
					logging.Trunc128("EventsRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*mapv1.EventsResponse]{
					Error: err,
				}
				return
			}
			proposal := client.StreamProposal[*mapprotocolv1.EventsResponse](primitive)
			stream, err := proposal.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (client.ProposalStream[*mapprotocolv1.EventsResponse], error) {
				return mapprotocolv1.NewMapClient(conn).Events(server.Context(), &mapprotocolv1.EventsRequest{
					Headers: headers,
					EventsInput: &mapprotocolv1.EventsInput{
						Key: request.Key,
					},
				})
			})
			if err != nil {
				log.Warnw("Events",
					logging.Trunc128("EventsRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*mapv1.EventsResponse]{
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
						ch <- streams.Result[*mapv1.EventsResponse]{
							Error: err,
						}
					}
					return
				}
				if err != nil {
					log.Debugw("Events",
						logging.Trunc128("EventsRequest", request),
						logging.Error("Error", err))
					ch <- streams.Result[*mapv1.EventsResponse]{
						Error: err,
					}
				} else {
					response := &mapv1.EventsResponse{
						Event: mapv1.Event{
							Key: output.Event.Key,
						},
					}
					switch e := output.Event.Event.(type) {
					case *mapprotocolv1.Event_Inserted_:
						response.Event.Event = &mapv1.Event_Inserted_{
							Inserted: &mapv1.Event_Inserted{
								Value: mapv1.VersionedValue{
									Value:   e.Inserted.Value.Value,
									Version: uint64(e.Inserted.Value.Index),
								},
							},
						}
					case *mapprotocolv1.Event_Updated_:
						response.Event.Event = &mapv1.Event_Updated_{
							Updated: &mapv1.Event_Updated{
								Value: mapv1.VersionedValue{
									Value:   e.Updated.Value.Value,
									Version: uint64(e.Updated.Value.Index),
								},
								PrevValue: mapv1.VersionedValue{
									Value:   e.Updated.PrevValue.Value,
									Version: uint64(e.Updated.PrevValue.Index),
								},
							},
						}
					case *mapprotocolv1.Event_Removed_:
						response.Event.Event = &mapv1.Event_Removed_{
							Removed: &mapv1.Event_Removed{
								Value: mapv1.VersionedValue{
									Value:   e.Removed.Value.Value,
									Version: uint64(e.Removed.Value.Index),
								},
								Expired: e.Removed.Expired,
							},
						}
					}
					log.Debugw("Events",
						logging.Trunc128("EventsRequest", request),
						logging.Trunc128("EventsResponse", response))
					ch <- streams.Result[*mapv1.EventsResponse]{
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

func (s *MapSession) Entries(request *mapv1.EntriesRequest, server mapv1.Map_EntriesServer) error {
	log.Debugw("Entries received",
		logging.Trunc128("EntriesRequest", request))
	partitions := s.Partitions()
	ch := make(chan streams.Result[*mapv1.EntriesResponse])
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
				ch <- streams.Result[*mapv1.EntriesResponse]{
					Error: err,
				}
				return
			}
			primitive, err := session.GetPrimitive(request.ID.Name)
			if err != nil {
				log.Warnw("Entries",
					logging.Trunc128("EntriesRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*mapv1.EntriesResponse]{
					Error: err,
				}
				return
			}
			query := client.StreamQuery[*mapprotocolv1.EntriesResponse](primitive)
			stream, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (client.QueryStream[*mapprotocolv1.EntriesResponse], error) {
				return mapprotocolv1.NewMapClient(conn).Entries(server.Context(), &mapprotocolv1.EntriesRequest{
					Headers: headers,
					EntriesInput: &mapprotocolv1.EntriesInput{
						Watch: request.Watch,
					},
				})
			})
			if err != nil {
				log.Warnw("Entries",
					logging.Trunc128("EntriesRequest", request),
					logging.Error("Error", err))
				ch <- streams.Result[*mapv1.EntriesResponse]{
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
						ch <- streams.Result[*mapv1.EntriesResponse]{
							Error: err,
						}
					}
					return
				}
				if err != nil {
					log.Debugw("Entries",
						logging.Trunc128("EntriesRequest", request),
						logging.Error("Error", err))
					ch <- streams.Result[*mapv1.EntriesResponse]{
						Error: err,
					}
				} else {
					response := &mapv1.EntriesResponse{
						Entry: mapv1.Entry{
							Key: output.Entry.Key,
						},
					}
					if output.Entry.Value != nil {
						response.Entry.Value = &mapv1.VersionedValue{
							Value:   output.Entry.Value.Value,
							Version: uint64(output.Entry.Value.Index),
						}
					}
					log.Debugw("Entries",
						logging.Trunc128("EntriesRequest", request),
						logging.Trunc128("EntriesResponse", response))
					ch <- streams.Result[*mapv1.EntriesResponse]{
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

var _ runtimemapv1.MapProxy = (*MapSession)(nil)

// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	multimapv1 "github.com/atomix/atomix/api/runtime/multimap/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/runtime"
)

var log = logging.GetLogger()

const truncLen = 250

func newMultiMapServer(manager *runtime.PrimitiveManager[MultiMap]) multimapv1.MultiMapServer {
	return &multiMapServer{
		manager: manager,
	}
}

type multiMapServer struct {
	manager *runtime.PrimitiveManager[MultiMap]
}

func (s *multiMapServer) Create(ctx context.Context, request *multimapv1.CreateRequest) (*multimapv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	client, err := s.manager.Create(ctx, request.ID, request.Tags)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Create(ctx, request)
	if err != nil {
		log.Debugw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Create",
		logging.Trunc64("CreateResponse", response))
	return response, nil
}

func (s *multiMapServer) Close(ctx context.Context, request *multimapv1.CloseRequest) (*multimapv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Close(ctx, request)
	if err != nil {
		log.Debugw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Close",
		logging.Trunc64("CloseResponse", response))
	return response, nil
}

func (s *multiMapServer) Size(ctx context.Context, request *multimapv1.SizeRequest) (*multimapv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Trunc64("SizeRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Size",
			logging.Trunc64("SizeRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Size(ctx, request)
	if err != nil {
		log.Debugw("Size",
			logging.Trunc64("SizeRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Size",
		logging.Trunc64("SizeResponse", response))
	return response, nil
}

func (s *multiMapServer) Put(ctx context.Context, request *multimapv1.PutRequest) (*multimapv1.PutResponse, error) {
	log.Debugw("Put",
		logging.Trunc64("PutRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Put",
			logging.Trunc64("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Put(ctx, request)
	if err != nil {
		log.Debugw("Put",
			logging.Trunc64("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Put",
		logging.Trunc64("PutResponse", response))
	return response, nil
}

func (s *multiMapServer) PutAll(ctx context.Context, request *multimapv1.PutAllRequest) (*multimapv1.PutAllResponse, error) {
	log.Debugw("PutAll",
		logging.Trunc64("PutAllRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("PutAll",
			logging.Trunc64("PutAllRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.PutAll(ctx, request)
	if err != nil {
		log.Debugw("PutAll",
			logging.Trunc64("PutAllRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("PutAll",
		logging.Trunc64("PutAllResponse", response))
	return response, nil
}

func (s *multiMapServer) PutEntries(ctx context.Context, request *multimapv1.PutEntriesRequest) (*multimapv1.PutEntriesResponse, error) {
	log.Debugw("PutEntries",
		logging.Trunc64("PutEntriesRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("PutEntries",
			logging.Trunc64("PutEntriesRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.PutEntries(ctx, request)
	if err != nil {
		log.Debugw("PutEntries",
			logging.Trunc64("PutEntriesRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("PutEntries",
		logging.Trunc64("PutEntriesResponse", response))
	return response, nil
}

func (s *multiMapServer) Replace(ctx context.Context, request *multimapv1.ReplaceRequest) (*multimapv1.ReplaceResponse, error) {
	log.Debugw("Replace",
		logging.Trunc64("ReplaceRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Replace",
			logging.Trunc64("ReplaceRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Replace(ctx, request)
	if err != nil {
		log.Debugw("Replace",
			logging.Trunc64("ReplaceRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Replace",
		logging.Trunc64("ReplaceResponse", response))
	return response, nil
}

func (s *multiMapServer) Contains(ctx context.Context, request *multimapv1.ContainsRequest) (*multimapv1.ContainsResponse, error) {
	log.Debugw("Contains",
		logging.Trunc64("ContainsRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Contains",
			logging.Trunc64("ContainsRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Contains(ctx, request)
	if err != nil {
		log.Debugw("Contains",
			logging.Trunc64("ContainsRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Contains",
		logging.Trunc64("ContainsResponse", response))
	return response, nil
}

func (s *multiMapServer) Get(ctx context.Context, request *multimapv1.GetRequest) (*multimapv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Trunc64("GetRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Get",
			logging.Trunc64("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Get(ctx, request)
	if err != nil {
		log.Debugw("Get",
			logging.Trunc64("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Get",
		logging.Trunc64("GetResponse", response))
	return response, nil
}

func (s *multiMapServer) Remove(ctx context.Context, request *multimapv1.RemoveRequest) (*multimapv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Trunc64("RemoveRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Remove",
			logging.Trunc64("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Remove(ctx, request)
	if err != nil {
		log.Debugw("Remove",
			logging.Trunc64("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Remove",
		logging.Trunc64("RemoveResponse", response))
	return response, nil
}

func (s *multiMapServer) RemoveAll(ctx context.Context, request *multimapv1.RemoveAllRequest) (*multimapv1.RemoveAllResponse, error) {
	log.Debugw("RemoveAll",
		logging.Trunc64("RemoveAllRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("RemoveAll",
			logging.Trunc64("RemoveAllRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.RemoveAll(ctx, request)
	if err != nil {
		log.Debugw("RemoveAll",
			logging.Trunc64("RemoveAllRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("RemoveAll",
		logging.Trunc64("RemoveAllResponse", response))
	return response, nil
}

func (s *multiMapServer) RemoveEntries(ctx context.Context, request *multimapv1.RemoveEntriesRequest) (*multimapv1.RemoveEntriesResponse, error) {
	log.Debugw("RemoveEntries",
		logging.Trunc64("RemoveEntriesRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("RemoveEntries",
			logging.Trunc64("RemoveEntriesRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.RemoveEntries(ctx, request)
	if err != nil {
		log.Debugw("RemoveEntries",
			logging.Trunc64("RemoveEntriesRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("RemoveEntries",
		logging.Trunc64("RemoveEntriesResponse", response))
	return response, nil
}

func (s *multiMapServer) Clear(ctx context.Context, request *multimapv1.ClearRequest) (*multimapv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Trunc64("ClearRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Clear",
			logging.Trunc64("ClearRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Clear(ctx, request)
	if err != nil {
		log.Debugw("Clear",
			logging.Trunc64("ClearRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Clear",
		logging.Trunc64("ClearResponse", response))
	return response, nil
}

func (s *multiMapServer) Events(request *multimapv1.EventsRequest, server multimapv1.MultiMap_EventsServer) error {
	log.Debugw("Events",
		logging.Trunc64("EventsRequest", request),
		logging.String("State", "started"))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Events",
			logging.Trunc64("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	err = client.Events(request, server)
	if err != nil {
		log.Debugw("Events",
			logging.Trunc64("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	return nil
}

func (s *multiMapServer) Entries(request *multimapv1.EntriesRequest, server multimapv1.MultiMap_EntriesServer) error {
	log.Debugw("Entries",
		logging.Trunc64("EntriesRequest", request),
		logging.String("State", "started"))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Entries",
			logging.Trunc64("EntriesRequest", request),
			logging.Error("Error", err))
		return err
	}
	err = client.Entries(request, server)
	if err != nil {
		log.Debugw("Entries",
			logging.Trunc64("EntriesRequest", request),
			logging.Error("Error", err))
		return err
	}
	return nil
}

var _ multimapv1.MultiMapServer = (*multiMapServer)(nil)

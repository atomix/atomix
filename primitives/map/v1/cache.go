// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	mapv1 "github.com/atomix/runtime/api/atomix/primitive/map/v1"
	"github.com/atomix/runtime/pkg/time"
	"sync"
)

func newCachingMap(delegate mapv1.MapServer) mapv1.MapServer {
	return &cachingMap{
		MapServer: newDelegatingMap(delegate),
		entries:   make(map[string]*mapv1.Entry),
	}
}

type cachingMap struct {
	mapv1.MapServer
	entries map[string]*mapv1.Entry
	mu      sync.RWMutex
}

func (s *cachingMap) Put(ctx context.Context, request *mapv1.PutRequest) (*mapv1.PutResponse, error) {
	response, err := s.MapServer.Put(ctx, request)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[response.Entry.Key]
	if !ok {
		s.entries[response.Entry.Key] = &response.Entry
		return response, nil
	}

	localTS := entry.Timestamp
	if localTS == nil {
		s.entries[response.Entry.Key] = &response.Entry
		return response, nil
	}

	responseTS := response.Entry.Timestamp
	if responseTS == nil {
		s.entries[response.Entry.Key] = &response.Entry
		return response, nil
	}

	if time.NewTimestamp(*responseTS).After(time.NewTimestamp(*localTS)) {
		s.entries[response.Entry.Key] = &response.Entry
		return response, nil
	}

	return &mapv1.PutResponse{
		Headers: response.Headers,
		PutOutput: mapv1.PutOutput{
			Entry: *entry,
		},
	}, nil
}

func (s *cachingMap) Get(ctx context.Context, request *mapv1.GetRequest) (*mapv1.GetResponse, error) {
	s.mu.RLock()
	entry, ok := s.entries[request.Key]
	s.mu.RUnlock()
	if ok {
		response := &mapv1.GetResponse{
			GetOutput: mapv1.GetOutput{
				Entry: *entry,
			},
		}
		return response, nil
	}

	response, err := s.MapServer.Get(ctx, request)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok = s.entries[response.Entry.Key]
	if !ok {
		s.entries[response.Entry.Key] = &response.Entry
		return response, nil
	}

	localTS := entry.Timestamp
	if localTS == nil {
		s.entries[response.Entry.Key] = &response.Entry
		return response, nil
	}

	responseTS := response.Entry.Timestamp
	if responseTS == nil {
		s.entries[response.Entry.Key] = &response.Entry
		return response, nil
	}

	if time.NewTimestamp(*responseTS).After(time.NewTimestamp(*localTS)) {
		s.entries[response.Entry.Key] = &response.Entry
		return response, nil
	}

	return &mapv1.GetResponse{
		Headers: response.Headers,
		GetOutput: mapv1.GetOutput{
			Entry: *entry,
		},
	}, nil
}

func (s *cachingMap) Remove(ctx context.Context, request *mapv1.RemoveRequest) (*mapv1.RemoveResponse, error) {
	response, err := s.MapServer.Remove(ctx, request)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[response.Entry.Key]
	if !ok {
		delete(s.entries, response.Entry.Key)
		return response, nil
	}

	localTS := entry.Timestamp
	if localTS == nil {
		delete(s.entries, response.Entry.Key)
		return response, nil
	}

	responseTS := response.Entry.Timestamp
	if responseTS == nil {
		delete(s.entries, response.Entry.Key)
		return response, nil
	}

	if time.NewTimestamp(*responseTS).After(time.NewTimestamp(*localTS)) {
		delete(s.entries, response.Entry.Key)
		return response, nil
	}

	return &mapv1.RemoveResponse{
		Headers: response.Headers,
		RemoveOutput: mapv1.RemoveOutput{
			Entry: *entry,
		},
	}, nil
}

func (s *cachingMap) Clear(ctx context.Context, request *mapv1.ClearRequest) (*mapv1.ClearResponse, error) {
	response, err := s.MapServer.Clear(ctx, request)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	responseTS := response.Timestamp
	if responseTS == nil {
		s.entries = make(map[string]*mapv1.Entry)
		return response, nil
	}

	for key, entry := range s.entries {
		localTS := entry.Timestamp
		if localTS == nil || time.NewTimestamp(*responseTS).After(time.NewTimestamp(*localTS)) {
			delete(s.entries, key)
		}
	}
	return response, nil
}

var _ mapv1.MapServer = (*cachingMap)(nil)

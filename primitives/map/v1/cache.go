// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	mapv1 "github.com/atomix/runtime/api/atomix/map/v1"
	"github.com/atomix/runtime/pkg/time"
	lru "github.com/hashicorp/golang-lru"
	"math"
	"sync"
)

func newCachingMap(delegate Map, config mapv1.MapCacheConfig) (Map, error) {
	size := int(config.Size_)
	if size == 0 {
		size = math.MaxInt32
	}
	cache, err := lru.NewWithEvict(int(config.Size_), func(key interface{}, value interface{}) {

	})
	if err != nil {
		return nil, err
	}
	return &cachingMap{
		Map:   newDelegatingMap(delegate),
		cache: cache,
		size:  int(config.Size_),
	}, nil
}

type cachingMap struct {
	Map
	cache *lru.Cache
	size  int
	mu    sync.RWMutex
}

func (s *cachingMap) Put(ctx context.Context, request *mapv1.PutRequest) (*mapv1.PutResponse, error) {
	response, err := s.Map.Put(ctx, request)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.cache.Get(response.Entry.Key)
	if !ok {
		s.cache.Add(response.Entry.Key, &response.Entry)
		return response, nil
	}

	entry := object.(*mapv1.Entry)

	localTS := entry.Timestamp
	if localTS == nil {
		s.cache.Add(response.Entry.Key, &response.Entry)
		return response, nil
	}

	responseTS := response.Entry.Timestamp
	if responseTS == nil {
		s.cache.Add(response.Entry.Key, &response.Entry)
		return response, nil
	}

	if time.NewTimestamp(*responseTS).After(time.NewTimestamp(*localTS)) {
		s.cache.Add(response.Entry.Key, &response.Entry)
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
	object, ok := s.cache.Get(request.Key)
	s.mu.RUnlock()
	if ok {
		response := &mapv1.GetResponse{
			GetOutput: mapv1.GetOutput{
				Entry: *object.(*mapv1.Entry),
			},
		}
		return response, nil
	}

	response, err := s.Map.Get(ctx, request)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok = s.cache.Get(response.Entry.Key)
	if !ok {
		s.cache.Add(response.Entry.Key, &response.Entry)
		return response, nil
	}

	entry := object.(*mapv1.Entry)

	localTS := entry.Timestamp
	if localTS == nil {
		s.cache.Add(response.Entry.Key, &response.Entry)
		return response, nil
	}

	responseTS := response.Entry.Timestamp
	if responseTS == nil {
		s.cache.Add(response.Entry.Key, &response.Entry)
		return response, nil
	}

	if time.NewTimestamp(*responseTS).After(time.NewTimestamp(*localTS)) {
		s.cache.Add(response.Entry.Key, &response.Entry)
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
	response, err := s.Map.Remove(ctx, request)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.cache.Get(response.Entry.Key)
	if !ok {
		s.cache.Remove(response.Entry.Key)
		return response, nil
	}

	entry := object.(*mapv1.Entry)

	localTS := entry.Timestamp
	if localTS == nil {
		s.cache.Remove(response.Entry.Key)
		return response, nil
	}

	responseTS := response.Entry.Timestamp
	if responseTS == nil {
		s.cache.Remove(response.Entry.Key)
		return response, nil
	}

	if time.NewTimestamp(*responseTS).After(time.NewTimestamp(*localTS)) {
		s.cache.Remove(response.Entry.Key)
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
	response, err := s.Map.Clear(ctx, request)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	responseTS := response.Timestamp
	if responseTS == nil {
		s.cache.Purge()
		return response, nil
	}

	for _, key := range s.cache.Keys() {
		object, _ := s.cache.Get(key)
		localTS := object.(*mapv1.Entry).Timestamp
		if localTS == nil || time.NewTimestamp(*responseTS).After(time.NewTimestamp(*localTS)) {
			s.cache.Remove(key)
		}
	}
	return response, nil
}

var _ Map = (*cachingMap)(nil)

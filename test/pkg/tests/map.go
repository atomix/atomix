// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	"github.com/onosproject/helmit/pkg/test"
)

type MapTestSuite struct {
	PrimitiveTestSuite
	mapv1.MapServer
}

func (s *MapTestSuite) SetupSuite(ctx context.Context) {
	s.PrimitiveTestSuite.SetupSuite(ctx)
	_, err := s.Create(ctx, &mapv1.CreateRequest{
		ID: s.id,
	})
	s.NoError(err)
}

func (s *MapTestSuite) TearDownSuite(ctx context.Context) {
	_, err := s.Close(ctx, &mapv1.CloseRequest{
		ID: s.id,
	})
	s.NoError(err)
}

func (s *MapTestSuite) TestPut(ctx context.Context) {
	putResponse, err := s.Put(ctx, &mapv1.PutRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)
	s.Nil(putResponse.PrevValue)

	getResponse, err := s.Get(ctx, &mapv1.GetRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("bar", string(getResponse.Value.Value))
}

func (s *MapTestSuite) TestPutTTL(ctx context.Context) {
	// TODO
}

func (s *MapTestSuite) TestPutVersion(ctx context.Context) {
	putResponse, err := s.Put(ctx, &mapv1.PutRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)
	s.Nil(putResponse.PrevValue)
	if putResponse.Version == 0 {
		s.T().SkipNow()
	}

	prevVersion := putResponse.Version
	putResponse, err = s.Put(ctx, &mapv1.PutRequest{
		ID:          s.id,
		Key:         "foo",
		Value:       []byte("baz"),
		PrevVersion: prevVersion,
	})
	s.NoError(err)
	s.NotNil(putResponse.PrevValue)
	s.Equal("bar", string(putResponse.PrevValue.Value))
	s.NotEqual(prevVersion, putResponse.Version)

	_, err = s.Put(ctx, &mapv1.PutRequest{
		ID:          s.id,
		Key:         "foo",
		Value:       []byte("baz"),
		PrevVersion: putResponse.Version - 1,
	})
	s.ErrorConflict(err)
}

func (s *MapTestSuite) TestInsert(ctx context.Context) {
	_, err := s.Insert(ctx, &mapv1.InsertRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	_, err = s.Insert(ctx, &mapv1.InsertRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("baz"),
	})
	s.ErrorAlreadyExists(err)

	getResponse, err := s.Get(ctx, &mapv1.GetRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("bar", string(getResponse.Value.Value))
}

func (s *MapTestSuite) TestInsertTTL(ctx context.Context) {
	// TODO
}

func (s *MapTestSuite) TestPutUpdate(ctx context.Context) {
	_, err := s.Update(ctx, &mapv1.UpdateRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.ErrorNotFound(err)

	_, err = s.Put(ctx, &mapv1.PutRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	_, err = s.Update(ctx, &mapv1.UpdateRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("baz"),
	})
	s.NoError(err)

	getResponse, err := s.Get(ctx, &mapv1.GetRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("baz", string(getResponse.Value.Value))
}

func (s *MapTestSuite) TestInsertUpdate(ctx context.Context) {
	_, err := s.Update(ctx, &mapv1.UpdateRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.ErrorNotFound(err)

	_, err = s.Insert(ctx, &mapv1.InsertRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	_, err = s.Update(ctx, &mapv1.UpdateRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("baz"),
	})
	s.NoError(err)

	getResponse, err := s.Get(ctx, &mapv1.GetRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("baz", string(getResponse.Value.Value))
}

func (s *MapTestSuite) TestUpdateTTL(ctx context.Context) {
	// TODO
}

func (s *MapTestSuite) TestPutUpdateVersion(ctx context.Context) {
	_, err := s.Update(ctx, &mapv1.UpdateRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.ErrorNotFound(err)

	putResponse, err := s.Put(ctx, &mapv1.PutRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)
	if putResponse.Version == 0 {
		s.T().SkipNow()
	}

	updateResponse, err := s.Update(ctx, &mapv1.UpdateRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("baz"),
	})
	s.NoError(err)
	s.Equal("bar", string(updateResponse.PrevValue.Value))
	s.Equal(putResponse.Version, updateResponse.PrevValue.Version)

	getResponse, err := s.Get(ctx, &mapv1.GetRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("baz", string(getResponse.Value.Value))
	s.Equal(updateResponse.Version, getResponse.Value.Version)
}

func (s *MapTestSuite) TestInsertUpdateVersion(ctx context.Context) {
	_, err := s.Update(ctx, &mapv1.UpdateRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.ErrorNotFound(err)

	insertResponse, err := s.Insert(ctx, &mapv1.InsertRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)
	if insertResponse.Version == 0 {
		s.T().SkipNow()
	}

	updateResponse, err := s.Update(ctx, &mapv1.UpdateRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("baz"),
	})
	s.NoError(err)
	s.Equal("bar", string(updateResponse.PrevValue.Value))
	s.Equal(insertResponse.Version, updateResponse.PrevValue.Version)

	getResponse, err := s.Get(ctx, &mapv1.GetRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("baz", string(getResponse.Value.Value))
	s.Equal(updateResponse.Version, getResponse.Value.Version)
}

func (s *MapTestSuite) TestPutRemove(ctx context.Context) {
	_, err := s.Remove(ctx, &mapv1.RemoveRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.ErrorNotFound(err)

	putResponse, err := s.Put(ctx, &mapv1.PutRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	removeResponse, err := s.Remove(ctx, &mapv1.RemoveRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("bar", string(removeResponse.Value.Value))
	s.Equal(putResponse.Version, removeResponse.Value.Version)

	_, err = s.Get(ctx, &mapv1.GetRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.ErrorNotFound(err)
}

func (s *MapTestSuite) TestInsertRemove(ctx context.Context) {
	_, err := s.Remove(ctx, &mapv1.RemoveRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.ErrorNotFound(err)

	insertResponse, err := s.Insert(ctx, &mapv1.InsertRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	removeResponse, err := s.Remove(ctx, &mapv1.RemoveRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("bar", string(removeResponse.Value.Value))
	s.Equal(insertResponse.Version, removeResponse.Value.Version)

	_, err = s.Get(ctx, &mapv1.GetRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.ErrorNotFound(err)
}

func (s *MapTestSuite) TestRemoveTTL(ctx context.Context) {
	// TODO
}

func (s *MapTestSuite) TestPutRemoveVersion(ctx context.Context) {
	_, err := s.Remove(ctx, &mapv1.RemoveRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.ErrorNotFound(err)

	putResponse, err := s.Put(ctx, &mapv1.PutRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)
	if putResponse.Version == 0 {
		s.T().SkipNow()
	}

	removeResponse, err := s.Remove(ctx, &mapv1.RemoveRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("bar", string(removeResponse.Value.Value))
	s.Equal(putResponse.Version, removeResponse.Value.Version)

	_, err = s.Get(ctx, &mapv1.GetRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.ErrorNotFound(err)
}

func (s *MapTestSuite) TestInsertRemoveVersion(ctx context.Context) {
	_, err := s.Remove(ctx, &mapv1.RemoveRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.ErrorNotFound(err)

	insertResponse, err := s.Insert(ctx, &mapv1.InsertRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)
	if insertResponse.Version == 0 {
		s.T().SkipNow()
	}

	removeResponse, err := s.Remove(ctx, &mapv1.RemoveRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("bar", string(removeResponse.Value.Value))
	s.Equal(insertResponse.Version, removeResponse.Value.Version)

	_, err = s.Get(ctx, &mapv1.GetRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.ErrorNotFound(err)
}

func (s *MapTestSuite) TestPutSize(ctx context.Context) {
	sizeResponse, err := s.Size(ctx, &mapv1.SizeRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(uint32(0), sizeResponse.Size_)

	_, err = s.Put(ctx, &mapv1.PutRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	sizeResponse, err = s.Size(ctx, &mapv1.SizeRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(uint32(1), sizeResponse.Size_)

	_, err = s.Put(ctx, &mapv1.PutRequest{
		ID:    s.id,
		Key:   "bar",
		Value: []byte("baz"),
	})
	s.NoError(err)

	sizeResponse, err = s.Size(ctx, &mapv1.SizeRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(uint32(2), sizeResponse.Size_)
}

func (s *MapTestSuite) TestInsertSize(ctx context.Context) {
	sizeResponse, err := s.Size(ctx, &mapv1.SizeRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(uint32(0), sizeResponse.Size_)

	_, err = s.Insert(ctx, &mapv1.InsertRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	sizeResponse, err = s.Size(ctx, &mapv1.SizeRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(uint32(1), sizeResponse.Size_)

	_, err = s.Insert(ctx, &mapv1.InsertRequest{
		ID:    s.id,
		Key:   "bar",
		Value: []byte("baz"),
	})
	s.NoError(err)

	sizeResponse, err = s.Size(ctx, &mapv1.SizeRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(uint32(2), sizeResponse.Size_)
}

func (s *MapTestSuite) TestPutClear(ctx context.Context) {
	_, err := s.Put(ctx, &mapv1.PutRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	_, err = s.Put(ctx, &mapv1.PutRequest{
		ID:    s.id,
		Key:   "bar",
		Value: []byte("baz"),
	})
	s.NoError(err)

	_, err = s.Clear(ctx, &mapv1.ClearRequest{
		ID: s.id,
	})
	s.NoError(err)

	_, err = s.Get(ctx, &mapv1.GetRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.ErrorNotFound(err)

	_, err = s.Get(ctx, &mapv1.GetRequest{
		ID:  s.id,
		Key: "bar",
	})
	s.ErrorNotFound(err)
}

func (s *MapTestSuite) TestInsertClear(ctx context.Context) {
	_, err := s.Insert(ctx, &mapv1.InsertRequest{
		ID:    s.id,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	_, err = s.Insert(ctx, &mapv1.InsertRequest{
		ID:    s.id,
		Key:   "bar",
		Value: []byte("baz"),
	})
	s.NoError(err)

	_, err = s.Clear(ctx, &mapv1.ClearRequest{
		ID: s.id,
	})
	s.NoError(err)

	_, err = s.Get(ctx, &mapv1.GetRequest{
		ID:  s.id,
		Key: "foo",
	})
	s.ErrorNotFound(err)

	_, err = s.Get(ctx, &mapv1.GetRequest{
		ID:  s.id,
		Key: "bar",
	})
	s.ErrorNotFound(err)
}

var _ test.SetupSuite = (*MapTestSuite)(nil)
var _ test.TearDownSuite = (*MapTestSuite)(nil)

// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	"github.com/onosproject/helmit/pkg/test"
)

type MapTestSuite struct {
	PrimitiveTestSuite
	mapv1.MapClient
}

func (s *MapTestSuite) SetupSuite() {
	s.PrimitiveTestSuite.SetupSuite()
	s.MapClient = mapv1.NewMapClient(s.conn)
}

func (s *MapTestSuite) SetupTest() {
	s.PrimitiveTestSuite.SetupTest()
	_, err := s.Create(s.Context(), &mapv1.CreateRequest{
		ID: s.ID,
	})
	s.NoError(err)
}

func (s *MapTestSuite) TearDownTest() {
	_, err := s.Close(s.Context(), &mapv1.CloseRequest{
		ID: s.ID,
	})
	s.NoError(err)
}

func (s *MapTestSuite) TestPut() {
	putResponse, err := s.Put(s.Context(), &mapv1.PutRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)
	s.Nil(putResponse.PrevValue)

	getResponse, err := s.Get(s.Context(), &mapv1.GetRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("bar", string(getResponse.Value.Value))
}

func (s *MapTestSuite) TestPutTTL() {
	// TODO
}

func (s *MapTestSuite) TestPutVersion() {
	putResponse, err := s.Put(s.Context(), &mapv1.PutRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)
	s.Nil(putResponse.PrevValue)
	if putResponse.Version == 0 {
		s.T().SkipNow()
	}

	prevVersion := putResponse.Version
	putResponse, err = s.Put(s.Context(), &mapv1.PutRequest{
		ID:          s.ID,
		Key:         "foo",
		Value:       []byte("baz"),
		PrevVersion: prevVersion,
	})
	s.NoError(err)
	s.NotNil(putResponse.PrevValue)
	s.Equal("bar", string(putResponse.PrevValue.Value))
	s.NotEqual(prevVersion, putResponse.Version)

	_, err = s.Put(s.Context(), &mapv1.PutRequest{
		ID:          s.ID,
		Key:         "foo",
		Value:       []byte("baz"),
		PrevVersion: putResponse.Version - 1,
	})
	s.ErrorConflict(err)
}

func (s *MapTestSuite) TestInsert() {
	_, err := s.Insert(s.Context(), &mapv1.InsertRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	_, err = s.Insert(s.Context(), &mapv1.InsertRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("baz"),
	})
	s.ErrorAlreadyExists(err)

	getResponse, err := s.Get(s.Context(), &mapv1.GetRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("bar", string(getResponse.Value.Value))
}

func (s *MapTestSuite) TestInsertTTL() {
	// TODO
}

func (s *MapTestSuite) TestPutUpdate() {
	_, err := s.Update(s.Context(), &mapv1.UpdateRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.ErrorNotFound(err)

	_, err = s.Put(s.Context(), &mapv1.PutRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	_, err = s.Update(s.Context(), &mapv1.UpdateRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("baz"),
	})
	s.NoError(err)

	getResponse, err := s.Get(s.Context(), &mapv1.GetRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("baz", string(getResponse.Value.Value))
}

func (s *MapTestSuite) TestInsertUpdate() {
	_, err := s.Update(s.Context(), &mapv1.UpdateRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.ErrorNotFound(err)

	_, err = s.Insert(s.Context(), &mapv1.InsertRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	_, err = s.Update(s.Context(), &mapv1.UpdateRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("baz"),
	})
	s.NoError(err)

	getResponse, err := s.Get(s.Context(), &mapv1.GetRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("baz", string(getResponse.Value.Value))
}

func (s *MapTestSuite) TestUpdateTTL() {
	// TODO
}

func (s *MapTestSuite) TestPutUpdateVersion() {
	_, err := s.Update(s.Context(), &mapv1.UpdateRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.ErrorNotFound(err)

	putResponse, err := s.Put(s.Context(), &mapv1.PutRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)
	if putResponse.Version == 0 {
		s.T().SkipNow()
	}

	updateResponse, err := s.Update(s.Context(), &mapv1.UpdateRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("baz"),
	})
	s.NoError(err)
	s.Equal("bar", string(updateResponse.PrevValue.Value))
	s.Equal(putResponse.Version, updateResponse.PrevValue.Version)

	getResponse, err := s.Get(s.Context(), &mapv1.GetRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("baz", string(getResponse.Value.Value))
	s.Equal(updateResponse.Version, getResponse.Value.Version)
}

func (s *MapTestSuite) TestInsertUpdateVersion() {
	_, err := s.Update(s.Context(), &mapv1.UpdateRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.ErrorNotFound(err)

	insertResponse, err := s.Insert(s.Context(), &mapv1.InsertRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)
	if insertResponse.Version == 0 {
		s.T().SkipNow()
	}

	updateResponse, err := s.Update(s.Context(), &mapv1.UpdateRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("baz"),
	})
	s.NoError(err)
	s.Equal("bar", string(updateResponse.PrevValue.Value))
	s.Equal(insertResponse.Version, updateResponse.PrevValue.Version)

	getResponse, err := s.Get(s.Context(), &mapv1.GetRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("baz", string(getResponse.Value.Value))
	s.Equal(updateResponse.Version, getResponse.Value.Version)
}

func (s *MapTestSuite) TestPutRemove() {
	_, err := s.Remove(s.Context(), &mapv1.RemoveRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.ErrorNotFound(err)

	putResponse, err := s.Put(s.Context(), &mapv1.PutRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	removeResponse, err := s.Remove(s.Context(), &mapv1.RemoveRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("bar", string(removeResponse.Value.Value))
	s.Equal(putResponse.Version, removeResponse.Value.Version)

	_, err = s.Get(s.Context(), &mapv1.GetRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.ErrorNotFound(err)
}

func (s *MapTestSuite) TestInsertRemove() {
	_, err := s.Remove(s.Context(), &mapv1.RemoveRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.ErrorNotFound(err)

	insertResponse, err := s.Insert(s.Context(), &mapv1.InsertRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	removeResponse, err := s.Remove(s.Context(), &mapv1.RemoveRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("bar", string(removeResponse.Value.Value))
	s.Equal(insertResponse.Version, removeResponse.Value.Version)

	_, err = s.Get(s.Context(), &mapv1.GetRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.ErrorNotFound(err)
}

func (s *MapTestSuite) TestRemoveTTL() {
	// TODO
}

func (s *MapTestSuite) TestPutRemoveVersion() {
	_, err := s.Remove(s.Context(), &mapv1.RemoveRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.ErrorNotFound(err)

	putResponse, err := s.Put(s.Context(), &mapv1.PutRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)
	if putResponse.Version == 0 {
		s.T().SkipNow()
	}

	removeResponse, err := s.Remove(s.Context(), &mapv1.RemoveRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("bar", string(removeResponse.Value.Value))
	s.Equal(putResponse.Version, removeResponse.Value.Version)

	_, err = s.Get(s.Context(), &mapv1.GetRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.ErrorNotFound(err)
}

func (s *MapTestSuite) TestInsertRemoveVersion() {
	_, err := s.Remove(s.Context(), &mapv1.RemoveRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.ErrorNotFound(err)

	insertResponse, err := s.Insert(s.Context(), &mapv1.InsertRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)
	if insertResponse.Version == 0 {
		s.T().SkipNow()
	}

	removeResponse, err := s.Remove(s.Context(), &mapv1.RemoveRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.NoError(err)
	s.Equal("bar", string(removeResponse.Value.Value))
	s.Equal(insertResponse.Version, removeResponse.Value.Version)

	_, err = s.Get(s.Context(), &mapv1.GetRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.ErrorNotFound(err)
}

func (s *MapTestSuite) TestPutSize() {
	sizeResponse, err := s.Size(s.Context(), &mapv1.SizeRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(uint32(0), sizeResponse.Size_)

	_, err = s.Put(s.Context(), &mapv1.PutRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	sizeResponse, err = s.Size(s.Context(), &mapv1.SizeRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(uint32(1), sizeResponse.Size_)

	_, err = s.Put(s.Context(), &mapv1.PutRequest{
		ID:    s.ID,
		Key:   "bar",
		Value: []byte("baz"),
	})
	s.NoError(err)

	sizeResponse, err = s.Size(s.Context(), &mapv1.SizeRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(uint32(2), sizeResponse.Size_)
}

func (s *MapTestSuite) TestInsertSize() {
	sizeResponse, err := s.Size(s.Context(), &mapv1.SizeRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(uint32(0), sizeResponse.Size_)

	_, err = s.Insert(s.Context(), &mapv1.InsertRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	sizeResponse, err = s.Size(s.Context(), &mapv1.SizeRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(uint32(1), sizeResponse.Size_)

	_, err = s.Insert(s.Context(), &mapv1.InsertRequest{
		ID:    s.ID,
		Key:   "bar",
		Value: []byte("baz"),
	})
	s.NoError(err)

	sizeResponse, err = s.Size(s.Context(), &mapv1.SizeRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(uint32(2), sizeResponse.Size_)
}

func (s *MapTestSuite) TestPutClear() {
	_, err := s.Put(s.Context(), &mapv1.PutRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	_, err = s.Put(s.Context(), &mapv1.PutRequest{
		ID:    s.ID,
		Key:   "bar",
		Value: []byte("baz"),
	})
	s.NoError(err)

	_, err = s.Clear(s.Context(), &mapv1.ClearRequest{
		ID: s.ID,
	})
	s.NoError(err)

	_, err = s.Get(s.Context(), &mapv1.GetRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.ErrorNotFound(err)

	_, err = s.Get(s.Context(), &mapv1.GetRequest{
		ID:  s.ID,
		Key: "bar",
	})
	s.ErrorNotFound(err)
}

func (s *MapTestSuite) TestInsertClear() {
	_, err := s.Insert(s.Context(), &mapv1.InsertRequest{
		ID:    s.ID,
		Key:   "foo",
		Value: []byte("bar"),
	})
	s.NoError(err)

	_, err = s.Insert(s.Context(), &mapv1.InsertRequest{
		ID:    s.ID,
		Key:   "bar",
		Value: []byte("baz"),
	})
	s.NoError(err)

	_, err = s.Clear(s.Context(), &mapv1.ClearRequest{
		ID: s.ID,
	})
	s.NoError(err)

	_, err = s.Get(s.Context(), &mapv1.GetRequest{
		ID:  s.ID,
		Key: "foo",
	})
	s.ErrorNotFound(err)

	_, err = s.Get(s.Context(), &mapv1.GetRequest{
		ID:  s.ID,
		Key: "bar",
	})
	s.ErrorNotFound(err)
}

var _ test.SetupSuite = (*MapTestSuite)(nil)
var _ test.TearDownSuite = (*MapTestSuite)(nil)

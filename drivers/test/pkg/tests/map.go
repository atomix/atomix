// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	runtimemapv1 "github.com/atomix/atomix/runtime/pkg/runtime/map/v1"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

func NewMapTests(runtime *runtime.Runtime) PrimitiveSuite {
	return &MapTests{
		PrimitiveTests: &PrimitiveTests{},
		MapServer:      runtimemapv1.NewMapServer(runtime),
	}
}

type MapTests struct {
	*PrimitiveTests
	mapv1.MapServer
}

func (t *MapTests) CreatePrimitive() error {
	_, err := t.Create(t.Context(), &mapv1.CreateRequest{
		ID: t.ID(),
	})
	return err
}

func (t *MapTests) ClosePrimitive() error {
	_, err := t.Close(t.Context(), &mapv1.CloseRequest{
		ID: t.ID(),
	})
	return err
}

func (t *MapTests) TestPut() {
	putResponse, err := t.Put(t.Context(), &mapv1.PutRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.NoError(err)
	t.Nil(putResponse.PrevValue)

	getResponse, err := t.Get(t.Context(), &mapv1.GetRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.NoError(err)
	t.Equal("bar", string(getResponse.Value.Value))
}

func (t *MapTests) TestPutTTL() {
	// TODO
}

func (t *MapTests) TestPutVersion() {
	putResponse, err := t.Put(t.Context(), &mapv1.PutRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.NoError(err)
	t.Nil(putResponse.PrevValue)
	if putResponse.Version == 0 {
		t.SkipNow()
	}

	prevVersion := putResponse.Version
	putResponse, err = t.Put(t.Context(), &mapv1.PutRequest{
		ID:          t.ID(),
		Key:         "foo",
		Value:       []byte("baz"),
		PrevVersion: prevVersion,
	})
	t.NoError(err)
	t.NotNil(putResponse.PrevValue)
	t.Equal("bar", string(putResponse.PrevValue.Value))
	t.NotEqual(prevVersion, putResponse.Version)

	_, err = t.Put(t.Context(), &mapv1.PutRequest{
		ID:          t.ID(),
		Key:         "foo",
		Value:       []byte("baz"),
		PrevVersion: putResponse.Version - 1,
	})
	t.ErrorConflict(err)
}

func (t *MapTests) TestInsert() {
	_, err := t.Insert(t.Context(), &mapv1.InsertRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.NoError(err)

	_, err = t.Insert(t.Context(), &mapv1.InsertRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("baz"),
	})
	t.ErrorAlreadyExists(err)

	getResponse, err := t.Get(t.Context(), &mapv1.GetRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.NoError(err)
	t.Equal("bar", string(getResponse.Value.Value))
}

func (t *MapTests) TestInsertTTL() {
	// TODO
}

func (t *MapTests) TestPutUpdate() {
	_, err := t.Update(t.Context(), &mapv1.UpdateRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.ErrorNotFound(err)

	_, err = t.Put(t.Context(), &mapv1.PutRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.NoError(err)

	_, err = t.Update(t.Context(), &mapv1.UpdateRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("baz"),
	})
	t.NoError(err)

	getResponse, err := t.Get(t.Context(), &mapv1.GetRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.NoError(err)
	t.Equal("baz", string(getResponse.Value.Value))
}

func (t *MapTests) TestInsertUpdate() {
	_, err := t.Update(t.Context(), &mapv1.UpdateRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.ErrorNotFound(err)

	_, err = t.Insert(t.Context(), &mapv1.InsertRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.NoError(err)

	_, err = t.Update(t.Context(), &mapv1.UpdateRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("baz"),
	})
	t.NoError(err)

	getResponse, err := t.Get(t.Context(), &mapv1.GetRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.NoError(err)
	t.Equal("baz", string(getResponse.Value.Value))
}

func (t *MapTests) TestUpdateTTL() {
	// TODO
}

func (t *MapTests) TestPutUpdateVersion() {
	_, err := t.Update(t.Context(), &mapv1.UpdateRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.ErrorNotFound(err)

	putResponse, err := t.Put(t.Context(), &mapv1.PutRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.NoError(err)
	if putResponse.Version == 0 {
		t.SkipNow()
	}

	updateResponse, err := t.Update(t.Context(), &mapv1.UpdateRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("baz"),
	})
	t.NoError(err)
	t.Equal("bar", string(updateResponse.PrevValue.Value))
	t.Equal(putResponse.Version, updateResponse.PrevValue.Version)

	getResponse, err := t.Get(t.Context(), &mapv1.GetRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.NoError(err)
	t.Equal("baz", string(getResponse.Value.Value))
	t.Equal(updateResponse.Version, getResponse.Value.Version)
}

func (t *MapTests) TestInsertUpdateVersion() {
	_, err := t.Update(t.Context(), &mapv1.UpdateRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.ErrorNotFound(err)

	insertResponse, err := t.Insert(t.Context(), &mapv1.InsertRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.NoError(err)
	if insertResponse.Version == 0 {
		t.SkipNow()
	}

	updateResponse, err := t.Update(t.Context(), &mapv1.UpdateRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("baz"),
	})
	t.NoError(err)
	t.Equal("bar", string(updateResponse.PrevValue.Value))
	t.Equal(insertResponse.Version, updateResponse.PrevValue.Version)

	getResponse, err := t.Get(t.Context(), &mapv1.GetRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.NoError(err)
	t.Equal("baz", string(getResponse.Value.Value))
	t.Equal(updateResponse.Version, getResponse.Value.Version)
}

func (t *MapTests) TestPutRemove() {
	_, err := t.Remove(t.Context(), &mapv1.RemoveRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.ErrorNotFound(err)

	putResponse, err := t.Put(t.Context(), &mapv1.PutRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.NoError(err)

	removeResponse, err := t.Remove(t.Context(), &mapv1.RemoveRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.NoError(err)
	t.Equal("bar", string(removeResponse.Value.Value))
	t.Equal(putResponse.Version, removeResponse.Value.Version)

	_, err = t.Get(t.Context(), &mapv1.GetRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.ErrorNotFound(err)
}

func (t *MapTests) TestInsertRemove() {
	_, err := t.Remove(t.Context(), &mapv1.RemoveRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.ErrorNotFound(err)

	insertResponse, err := t.Insert(t.Context(), &mapv1.InsertRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.NoError(err)

	removeResponse, err := t.Remove(t.Context(), &mapv1.RemoveRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.NoError(err)
	t.Equal("bar", string(removeResponse.Value.Value))
	t.Equal(insertResponse.Version, removeResponse.Value.Version)

	_, err = t.Get(t.Context(), &mapv1.GetRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.ErrorNotFound(err)
}

func (t *MapTests) TestRemoveTTL() {
	// TODO
}

func (t *MapTests) TestPutRemoveVersion() {
	_, err := t.Remove(t.Context(), &mapv1.RemoveRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.ErrorNotFound(err)

	putResponse, err := t.Put(t.Context(), &mapv1.PutRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.NoError(err)
	if putResponse.Version == 0 {
		t.SkipNow()
	}

	removeResponse, err := t.Remove(t.Context(), &mapv1.RemoveRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.NoError(err)
	t.Equal("bar", string(removeResponse.Value.Value))
	t.Equal(putResponse.Version, removeResponse.Value.Version)

	_, err = t.Get(t.Context(), &mapv1.GetRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.ErrorNotFound(err)
}

func (t *MapTests) TestInsertRemoveVersion() {
	_, err := t.Remove(t.Context(), &mapv1.RemoveRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.ErrorNotFound(err)

	insertResponse, err := t.Insert(t.Context(), &mapv1.InsertRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.NoError(err)
	if insertResponse.Version == 0 {
		t.SkipNow()
	}

	removeResponse, err := t.Remove(t.Context(), &mapv1.RemoveRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.NoError(err)
	t.Equal("bar", string(removeResponse.Value.Value))
	t.Equal(insertResponse.Version, removeResponse.Value.Version)

	_, err = t.Get(t.Context(), &mapv1.GetRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.ErrorNotFound(err)
}

func (t *MapTests) TestPutSize() {
	sizeResponse, err := t.Size(t.Context(), &mapv1.SizeRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(uint32(0), sizeResponse.Size_)

	_, err = t.Put(t.Context(), &mapv1.PutRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.NoError(err)

	sizeResponse, err = t.Size(t.Context(), &mapv1.SizeRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(uint32(1), sizeResponse.Size_)

	_, err = t.Put(t.Context(), &mapv1.PutRequest{
		ID:    t.ID(),
		Key:   "bar",
		Value: []byte("baz"),
	})
	t.NoError(err)

	sizeResponse, err = t.Size(t.Context(), &mapv1.SizeRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(uint32(2), sizeResponse.Size_)
}

func (t *MapTests) TestInsertSize() {
	sizeResponse, err := t.Size(t.Context(), &mapv1.SizeRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(uint32(0), sizeResponse.Size_)

	_, err = t.Insert(t.Context(), &mapv1.InsertRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.NoError(err)

	sizeResponse, err = t.Size(t.Context(), &mapv1.SizeRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(uint32(1), sizeResponse.Size_)

	_, err = t.Insert(t.Context(), &mapv1.InsertRequest{
		ID:    t.ID(),
		Key:   "bar",
		Value: []byte("baz"),
	})
	t.NoError(err)

	sizeResponse, err = t.Size(t.Context(), &mapv1.SizeRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(uint32(2), sizeResponse.Size_)
}

func (t *MapTests) TestPutClear() {
	_, err := t.Put(t.Context(), &mapv1.PutRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.NoError(err)

	_, err = t.Put(t.Context(), &mapv1.PutRequest{
		ID:    t.ID(),
		Key:   "bar",
		Value: []byte("baz"),
	})
	t.NoError(err)

	_, err = t.Clear(t.Context(), &mapv1.ClearRequest{
		ID: t.ID(),
	})
	t.NoError(err)

	_, err = t.Get(t.Context(), &mapv1.GetRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.ErrorNotFound(err)

	_, err = t.Get(t.Context(), &mapv1.GetRequest{
		ID:  t.ID(),
		Key: "bar",
	})
	t.ErrorNotFound(err)
}

func (t *MapTests) TestInsertClear() {
	_, err := t.Insert(t.Context(), &mapv1.InsertRequest{
		ID:    t.ID(),
		Key:   "foo",
		Value: []byte("bar"),
	})
	t.NoError(err)

	_, err = t.Insert(t.Context(), &mapv1.InsertRequest{
		ID:    t.ID(),
		Key:   "bar",
		Value: []byte("baz"),
	})
	t.NoError(err)

	_, err = t.Clear(t.Context(), &mapv1.ClearRequest{
		ID: t.ID(),
	})
	t.NoError(err)

	_, err = t.Get(t.Context(), &mapv1.GetRequest{
		ID:  t.ID(),
		Key: "foo",
	})
	t.ErrorNotFound(err)

	_, err = t.Get(t.Context(), &mapv1.GetRequest{
		ID:  t.ID(),
		Key: "bar",
	})
	t.ErrorNotFound(err)
}

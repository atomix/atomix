// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	atomixv3beta4 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta4"
	"github.com/atomix/atomix/stores/shared-memory/pkg/apis/sharedmemory/v1beta2"
	sharedmemoryv1beta2 "github.com/atomix/atomix/stores/shared-memory/pkg/client/clientset/versioned/typed/sharedmemory/v1beta2"
	"github.com/onosproject/helmit/pkg/test"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
)

type SharedMemoryStoreTestSuite struct {
	test.Suite
	*atomixv3beta4.AtomixV3beta4Client
	*sharedmemoryv1beta2.SharedmemoryV1beta2Client
}

func (t *SharedMemoryStoreTestSuite) SetupSuite() {
	atomixV3beta4Client, err := atomixv3beta4.NewForConfig(t.Config())
	t.NoError(err)
	t.AtomixV3beta4Client = atomixV3beta4Client

	sharedmemoryV1beta3Client, err := sharedmemoryv1beta2.NewForConfig(t.Config())
	t.NoError(err)
	t.SharedmemoryV1beta2Client = sharedmemoryV1beta3Client
}

func (t *SharedMemoryStoreTestSuite) TestReadiness() {
	t.T().Log("Create SharedMemoryStore store")
	_, err := t.SharedMemoryStores(t.Namespace()).Create(t.Context(), &v1beta2.SharedMemoryStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "store",
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Log("Await SharedMemoryStore store ready")
	t.AwaitStoreReady("store")

	_, err = t.DataStores(t.Namespace()).Get(t.Context(), "store", metav1.GetOptions{})
	t.NoError(err)
}

func (t *SharedMemoryStoreTestSuite) Await(predicate func() bool) {
	for {
		if predicate() {
			return
		}
		time.Sleep(time.Second)
	}
}

func (t *SharedMemoryStoreTestSuite) AwaitStoreReady(name string) {
	t.Await(func() bool {
		cluster, err := t.SharedMemoryStores(t.Namespace()).Get(t.Context(), name, metav1.GetOptions{})
		t.NoError(err)
		return cluster.Status.State == v1beta2.SharedMemoryStoreReady
	})
}

func (t *SharedMemoryStoreTestSuite) NoError(err error, args ...interface{}) bool {
	if err == context.DeadlineExceeded {
		t.Fail(err.Error())
	}
	return t.Suite.NoError(err, args...)
}

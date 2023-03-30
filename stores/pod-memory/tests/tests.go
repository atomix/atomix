// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	atomixv3beta4 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta4"
	"github.com/atomix/atomix/stores/pod-memory/pkg/apis/podmemory/v1beta1"
	podmemoryv1beta1 "github.com/atomix/atomix/stores/pod-memory/pkg/client/clientset/versioned/typed/podmemory/v1beta1"
	"github.com/onosproject/helmit/pkg/test"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
)

type PodMemoryStoreTestSuite struct {
	test.Suite
	*atomixv3beta4.AtomixV3beta4Client
	*podmemoryv1beta1.PodmemoryV1beta1Client
}

func (t *PodMemoryStoreTestSuite) SetupSuite() {
	atomixV3beta4Client, err := atomixv3beta4.NewForConfig(t.Config())
	t.NoError(err)
	t.AtomixV3beta4Client = atomixV3beta4Client

	podmemoryV1beta3Client, err := podmemoryv1beta1.NewForConfig(t.Config())
	t.NoError(err)
	t.PodmemoryV1beta1Client = podmemoryV1beta3Client
}

func (t *PodMemoryStoreTestSuite) TestReadiness() {
	t.T().Log("Create PodMemoryStore store")
	_, err := t.PodMemoryStores(t.Namespace()).Create(t.Context(), &v1beta1.PodMemoryStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "store",
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Log("Await PodMemoryStore store ready")
	t.AwaitStoreReady("store")

	_, err = t.DataStores(t.Namespace()).Get(t.Context(), "store", metav1.GetOptions{})
	t.NoError(err)
}

func (t *PodMemoryStoreTestSuite) Await(predicate func() bool) {
	for {
		if predicate() {
			return
		}
		time.Sleep(time.Second)
	}
}

func (t *PodMemoryStoreTestSuite) AwaitStoreReady(name string) {
	t.Await(func() bool {
		cluster, err := t.PodMemoryStores(t.Namespace()).Get(t.Context(), name, metav1.GetOptions{})
		t.NoError(err)
		return cluster.Status.State == v1beta1.PodMemoryStoreReady
	})
}

func (t *PodMemoryStoreTestSuite) NoError(err error, args ...interface{}) bool {
	if err == context.DeadlineExceeded {
		t.Fail(err.Error())
	}
	return t.Suite.NoError(err, args...)
}

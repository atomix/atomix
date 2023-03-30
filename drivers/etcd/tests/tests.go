// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"encoding/json"
	"github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta4"
	atomixv3beta4 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta4"
	"github.com/atomix/atomix/tests/pkg/tests"
	"github.com/onosproject/helmit/pkg/helm"
	"github.com/onosproject/helmit/pkg/test"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type EtcdTestSuite struct {
	test.Suite
	*atomixv3beta4.AtomixV3beta4Client
	release *helm.Release
}

func (s *EtcdTestSuite) TestMap() {
	s.RunSuite(new(tests.MapTestSuite))
}

func (s *EtcdTestSuite) SetupSuite(ctx context.Context) {
	atomixV3beta4Client, err := atomixv3beta4.NewForConfig(s.Config())
	s.NoError(err)
	s.AtomixV3beta4Client = atomixV3beta4Client

	release, err := s.Helm().
		Install("etcd", "etcd").
		RepoURL("https://charts.bitnami.com/bitnami").
		Version("8.7.6").
		Set("replicaCount", 1).
		Set("rbac.enabled", false).
		Set("auth.rbac.enabled", false).
		Set("auth.rbac.create", false).
		Wait().
		Get(ctx)
	s.NoError(err)
	s.release = release

	config := clientv3.Config{
		Endpoints: []string{"etcd-etcd:2379"},
	}
	bytes, err := json.Marshal(config)
	s.NoError(err)

	dataStore := &v3beta4.DataStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "etcd",
		},
		Spec: v3beta4.DataStoreSpec{
			Driver: v3beta4.Driver{
				Name:       "atomix.io/etcd",
				APIVersion: "v3",
			},
			Config: runtime.RawExtension{
				Raw: bytes,
			},
		},
	}
	_, err = s.DataStores(s.Namespace()).Create(ctx, dataStore, metav1.CreateOptions{})
	s.NoError(err)

	storageProfile := &v3beta4.StorageProfile{
		ObjectMeta: metav1.ObjectMeta{
			Name: "etcd",
		},
		Spec: v3beta4.StorageProfileSpec{
			Routes: []v3beta4.Route{
				{
					Store: corev1.ObjectReference{
						Name: "etcd",
					},
				},
			},
		},
	}
	_, err = s.StorageProfiles(s.Namespace()).Create(ctx, storageProfile, metav1.CreateOptions{})
	s.NoError(err)
}

func (s *EtcdTestSuite) TearDownSuite(ctx context.Context) {
	s.NoError(s.StorageProfiles(s.Namespace()).Delete(ctx, "etcd", metav1.DeleteOptions{}))
	s.NoError(s.DataStores(s.Namespace()).Delete(ctx, "etcd", metav1.DeleteOptions{}))
	s.NoError(s.Helm().Uninstall(s.release.Name).Do(ctx))
}

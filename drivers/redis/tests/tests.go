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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type RedisTestSuite struct {
	test.Suite
	*atomixv3beta4.AtomixV3beta4Client
	release *helm.Release
}

func (s *RedisTestSuite) TestMap() {
	s.RunSuite(new(tests.MapTestSuite))
}

func (s *RedisTestSuite) SetupSuite(ctx context.Context) {
	atomixV3beta4Client, err := atomixv3beta4.NewForConfig(s.Config())
	s.NoError(err)
	s.AtomixV3beta4Client = atomixV3beta4Client

	release, err := s.Helm().
		Install("redis", "redis").
		RepoURL("https://charts.bitnami.com/bitnami").
		Version("17.8.2").
		Set("architecture", "standalone").
		Set("auth.enabled", false).
		Wait().
		Get(ctx)
	s.NoError(err)
	s.release = release

	config := map[string]string{
		"Addr": "redis-redis:6379",
	}
	bytes, err := json.Marshal(config)
	s.NoError(err)

	dataStore := &v3beta4.DataStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "redis",
		},
		Spec: v3beta4.DataStoreSpec{
			Driver: v3beta4.Driver{
				Name:       "atomix.io/redis",
				APIVersion: "v9",
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
			Name: "redis",
		},
		Spec: v3beta4.StorageProfileSpec{
			Routes: []v3beta4.Route{
				{
					Store: corev1.ObjectReference{
						Name: "redis",
					},
				},
			},
		},
	}
	_, err = s.StorageProfiles(s.Namespace()).Create(ctx, storageProfile, metav1.CreateOptions{})
	s.NoError(err)
}

func (s *RedisTestSuite) TearDownSuite(ctx context.Context) {
	s.NoError(s.StorageProfiles(s.Namespace()).Delete(ctx, "redis", metav1.DeleteOptions{}))
	s.NoError(s.DataStores(s.Namespace()).Delete(ctx, "redis", metav1.DeleteOptions{}))
	s.NoError(s.Helm().Uninstall(s.release.Name).Do(ctx))
}

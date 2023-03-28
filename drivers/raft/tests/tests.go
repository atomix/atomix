// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta4"
	atomixv3beta4 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta4"
	"github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta3"
	raftv1beta3 "github.com/atomix/atomix/stores/raft/pkg/client/clientset/versioned/typed/raft/v1beta3"
	"github.com/atomix/atomix/tests/pkg/tests"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

type raftTestSuite struct {
	*atomixv3beta4.AtomixV3beta4Client
	*raftv1beta3.RaftV1beta3Client
}

func (s *raftTestSuite) setup(ctx context.Context, namespace, name string, config *rest.Config) error {
	atomixV3beta4Client, err := atomixv3beta4.NewForConfig(config)
	if err != nil {
		return err
	}
	s.AtomixV3beta4Client = atomixV3beta4Client

	raftV1beta3Client, err := raftv1beta3.NewForConfig(config)
	if err != nil {
		return err
	}
	s.RaftV1beta3Client = raftV1beta3Client

	raftCluster := &v1beta3.RaftCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1beta3.RaftClusterSpec{
			Image:           "atomix/raft-node:latest",
			ImagePullPolicy: corev1.PullNever,
		},
	}
	_, err = s.RaftClusters(namespace).Create(ctx, raftCluster, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	raftStore := &v1beta3.RaftStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1beta3.RaftStoreSpec{
			Cluster: corev1.ObjectReference{
				Name: name,
			},
		},
	}
	_, err = s.RaftStores(namespace).Create(ctx, raftStore, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	storageProfile := &v3beta4.StorageProfile{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v3beta4.StorageProfileSpec{
			Routes: []v3beta4.Route{
				{
					Store: corev1.ObjectReference{
						Name: name,
					},
				},
			},
		},
	}
	_, err = s.StorageProfiles(namespace).Create(ctx, storageProfile, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (s *raftTestSuite) tearDown(ctx context.Context, namespace, name string) error {
	err := s.StorageProfiles(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	err = s.RaftStores(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	err = s.RaftClusters(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil
}

type CounterTestSuite struct {
	tests.CounterTestSuite
	raftTestSuite
}

func (s *CounterTestSuite) SetupSuite(ctx context.Context) {
	s.NoError(s.raftTestSuite.setup(ctx, s.Namespace(), "raft-counter-test", s.Config()))
	s.CounterTestSuite.SetupSuite(ctx)
}

func (s *CounterTestSuite) TearDownSuite(ctx context.Context) {
	s.CounterTestSuite.TearDownSuite(ctx)
	s.NoError(s.raftTestSuite.tearDown(ctx, s.Namespace(), "raft-counter-test"))
}

type MapTestSuite struct {
	tests.MapTestSuite
	raftTestSuite
}

func (s *MapTestSuite) SetupSuite(ctx context.Context) {
	s.NoError(s.raftTestSuite.setup(ctx, s.Namespace(), "raft-map-test", s.Config()))
	s.MapTestSuite.SetupSuite(ctx)
}

func (s *MapTestSuite) TearDownSuite(ctx context.Context) {
	s.MapTestSuite.TearDownSuite(ctx)
	s.NoError(s.raftTestSuite.tearDown(ctx, s.Namespace(), "raft-map-test"))
}

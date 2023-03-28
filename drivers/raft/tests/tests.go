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
	"github.com/onosproject/helmit/pkg/test"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type raftTestSuite struct {
	test.Suite
	*atomixv3beta4.AtomixV3beta4Client
	*raftv1beta3.RaftV1beta3Client
}

func (s *raftTestSuite) SetupSuite(ctx context.Context) {
	atomixV3beta4Client, err := atomixv3beta4.NewForConfig(s.Config())
	s.NoError(err)
	s.AtomixV3beta4Client = atomixV3beta4Client

	raftV1beta3Client, err := raftv1beta3.NewForConfig(s.Config())
	s.NoError(err)
	s.RaftV1beta3Client = raftV1beta3Client

	raftCluster := &v1beta3.RaftCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "raft",
		},
		Spec: v1beta3.RaftClusterSpec{
			Image:           "atomix/raft-node:latest",
			ImagePullPolicy: corev1.PullNever,
		},
	}
	_, err = s.RaftClusters(s.Namespace()).Create(ctx, raftCluster, metav1.CreateOptions{})
	s.NoError(err)

	raftStore := &v1beta3.RaftStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "raft",
		},
		Spec: v1beta3.RaftStoreSpec{
			Cluster: corev1.ObjectReference{
				Name: "raft",
			},
		},
	}
	_, err = s.RaftStores(s.Namespace()).Create(ctx, raftStore, metav1.CreateOptions{})
	s.NoError(err)

	storageProfile := &v3beta4.StorageProfile{
		ObjectMeta: metav1.ObjectMeta{
			Name: "raft",
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

func (s *raftTestSuite) TearDownSuite(ctx context.Context) {
	s.NoError(s.StorageProfiles(s.Namespace()).Delete(ctx, "raft", metav1.DeleteOptions{}))
	s.NoError(s.RaftStores(s.Namespace()).Delete(ctx, "raft", metav1.DeleteOptions{}))
	s.NoError(s.RaftClusters(s.Namespace()).Delete(ctx, "raft", metav1.DeleteOptions{}))
}

type CounterTestSuite struct {
	tests.CounterTestSuite
	raftTestSuite
}

func (s *CounterTestSuite) SetupSuite(ctx context.Context) {
	s.raftTestSuite.SetupSuite(ctx)
	s.CounterTestSuite.SetupSuite(ctx)
}

func (s *CounterTestSuite) TearDownSuite(ctx context.Context) {
	s.CounterTestSuite.TearDownSuite(ctx)
	s.raftTestSuite.TearDownSuite(ctx)
}

type MapTestSuite struct {
	tests.MapTestSuite
	raftTestSuite
}

func (s *MapTestSuite) SetupSuite(ctx context.Context) {
	s.raftTestSuite.SetupSuite(ctx)
	s.MapTestSuite.SetupSuite(ctx)
}

func (s *MapTestSuite) TearDownSuite(ctx context.Context) {
	s.MapTestSuite.TearDownSuite(ctx)
	s.raftTestSuite.TearDownSuite(ctx)
}

// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta4"
	atomixv3beta4 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta4"
	"github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta3"
	raftv1beta3 "github.com/atomix/atomix/stores/raft/pkg/client/clientset/versioned/typed/raft/v1beta3"
	"github.com/atomix/atomix/testing/pkg/tests"
	"github.com/onosproject/helmit/pkg/test"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type RaftTestSuite struct {
	test.Suite
	*atomixv3beta4.AtomixV3beta4Client
	*raftv1beta3.RaftV1beta3Client
}

func (s *RaftTestSuite) TestCounter() {
	s.RunSuite(new(tests.CounterTestSuite))
}

func (s *RaftTestSuite) TestMap() {
	s.RunSuite(new(tests.MapTestSuite))
}

func (s *RaftTestSuite) SetupSuite() {
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
			ImagePullPolicy: corev1.PullNever,
		},
	}
	_, err = s.RaftClusters(s.Namespace()).Create(s.Context(), raftCluster, metav1.CreateOptions{})
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
	_, err = s.RaftStores(s.Namespace()).Create(s.Context(), raftStore, metav1.CreateOptions{})
	s.NoError(err)

	storageProfile := &v3beta4.StorageProfile{
		ObjectMeta: metav1.ObjectMeta{
			Name: "raft",
		},
		Spec: v3beta4.StorageProfileSpec{
			Routes: []v3beta4.Route{
				{
					Store: corev1.ObjectReference{
						Name: "raft",
					},
				},
			},
		},
	}
	_, err = s.StorageProfiles(s.Namespace()).Create(s.Context(), storageProfile, metav1.CreateOptions{})
	s.NoError(err)
}

func (s *RaftTestSuite) TearDownSuite() {
	err := s.StorageProfiles(s.Namespace()).Delete(s.Context(), "raft", metav1.DeleteOptions{})
	s.NoError(err)
	err = s.RaftStores(s.Namespace()).Delete(s.Context(), "raft", metav1.DeleteOptions{})
	s.NoError(err)
	err = s.RaftClusters(s.Namespace()).Delete(s.Context(), "raft", metav1.DeleteOptions{})
	s.NoError(err)
}

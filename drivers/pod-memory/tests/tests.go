// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta4"
	atomixv3beta4 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta4"
	"github.com/atomix/atomix/stores/pod-memory/pkg/apis/podmemory/v1beta1"
	podmemoryv1beta1 "github.com/atomix/atomix/stores/pod-memory/pkg/client/clientset/versioned/typed/podmemory/v1beta1"
	"github.com/atomix/atomix/testing/pkg/tests"
	"github.com/onosproject/helmit/pkg/test"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type PodMemoryTestSuite struct {
	test.Suite
	*atomixv3beta4.AtomixV3beta4Client
	*podmemoryv1beta1.PodmemoryV1beta1Client
}

func (s *PodMemoryTestSuite) TestCounter() {
	s.RunSuite(new(tests.CounterTestSuite))
}

func (s *PodMemoryTestSuite) TestMap() {
	s.RunSuite(new(tests.MapTestSuite))
}

func (s *PodMemoryTestSuite) SetupSuite() {
	atomixV3beta4Client, err := atomixv3beta4.NewForConfig(s.Config())
	s.NoError(err)
	s.AtomixV3beta4Client = atomixV3beta4Client

	podMemoryV1beta1Client, err := podmemoryv1beta1.NewForConfig(s.Config())
	s.NoError(err)
	s.PodmemoryV1beta1Client = podMemoryV1beta1Client

	podMemoryStore := &v1beta1.PodMemoryStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-memory",
		},
	}
	_, err = s.PodMemoryStores(s.Namespace()).Create(s.Context(), podMemoryStore, metav1.CreateOptions{})
	s.NoError(err)

	storageProfile := &v3beta4.StorageProfile{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-memory",
		},
		Spec: v3beta4.StorageProfileSpec{
			Routes: []v3beta4.Route{
				{
					Store: corev1.ObjectReference{
						Name: "pod-memory",
					},
				},
			},
		},
	}
	_, err = s.StorageProfiles(s.Namespace()).Create(s.Context(), storageProfile, metav1.CreateOptions{})
	s.NoError(err)
}

func (s *PodMemoryTestSuite) TearDownSuite() {
	err := s.StorageProfiles(s.Namespace()).Delete(s.Context(), "pod-memory", metav1.DeleteOptions{})
	s.NoError(err)
	err = s.PodMemoryStores(s.Namespace()).Delete(s.Context(), "pod-memory", metav1.DeleteOptions{})
	s.NoError(err)
}

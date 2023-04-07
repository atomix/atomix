// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta3"
	"github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta4"
	atomixv3beta3 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta3"
	atomixv3beta4 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta4"
	"github.com/onosproject/helmit/pkg/test"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ControllerTestSuie is the onos-config HA test suite
type ControllerTestSuie struct {
	test.Suite
	*atomixv3beta3.AtomixV3beta3Client
	*atomixv3beta4.AtomixV3beta4Client
}

func (s *ControllerTestSuie) SetupSuite() {
	atomixV3beta3Client, err := atomixv3beta3.NewForConfig(s.Config())
	s.NoError(err)
	s.AtomixV3beta3Client = atomixV3beta3Client

	atomixV3beta4Client, err := atomixv3beta4.NewForConfig(s.Config())
	s.NoError(err)
	s.AtomixV3beta4Client = atomixV3beta4Client
}

func (s *ControllerTestSuie) TestAPIV3beta3() {
	s.T().Log("Create DataStore")
	_, err := s.AtomixV3beta3Client.DataStores(s.Namespace()).Create(s.Context(), &v3beta3.DataStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data-store",
		},
		Spec: v3beta3.DataStoreSpec{
			Driver: v3beta3.Driver{
				Name:    "test",
				Version: "v1",
			},
		},
	}, metav1.CreateOptions{})
	s.NoError(err)

	s.T().Log("Create StorageProfile")
	_, err = s.AtomixV3beta3Client.StorageProfiles(s.Namespace()).Create(s.Context(), &v3beta3.StorageProfile{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data-store",
		},
		Spec: v3beta3.StorageProfileSpec{
			Bindings: []v3beta3.Binding{
				{
					Store: corev1.ObjectReference{
						Name: "data-store",
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	s.NoError(err)
}

func (s *ControllerTestSuie) TestAPIV3beta4() {
	s.T().Log("Create DataStore")
	_, err := s.AtomixV3beta4Client.DataStores(s.Namespace()).Create(s.Context(), &v3beta4.DataStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data-store",
		},
		Spec: v3beta4.DataStoreSpec{
			Driver: v3beta4.Driver{
				Name:       "test",
				APIVersion: "v1",
			},
		},
	}, metav1.CreateOptions{})
	s.NoError(err)

	s.T().Log("Create StorageProfile")
	_, err = s.AtomixV3beta4Client.StorageProfiles(s.Namespace()).Create(s.Context(), &v3beta4.StorageProfile{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data-store",
		},
		Spec: v3beta4.StorageProfileSpec{
			Routes: []v3beta4.Route{
				{
					Store: corev1.ObjectReference{
						Name: "data-store",
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	s.NoError(err)
}

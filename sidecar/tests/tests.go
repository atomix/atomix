// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta4"
	atomixv3beta3 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta3"
	atomixv3beta4 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta4"
	"github.com/onosproject/helmit/pkg/test"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
)

// SidecarTestSuite is the onos-config HA test suite
type SidecarTestSuite struct {
	test.Suite
	*atomixv3beta3.AtomixV3beta3Client
	*atomixv3beta4.AtomixV3beta4Client
}

func (s *SidecarTestSuite) SetupSuite() {
	atomixV3beta3Client, err := atomixv3beta3.NewForConfig(s.Config())
	s.NoError(err)
	s.AtomixV3beta3Client = atomixV3beta3Client

	atomixV3beta4Client, err := atomixv3beta4.NewForConfig(s.Config())
	s.NoError(err)
	s.AtomixV3beta4Client = atomixV3beta4Client
}

func (s *SidecarTestSuite) TestSidecarInjection() {
	s.T().Log("Create StorageProfile")
	_, err := s.AtomixV3beta4Client.StorageProfiles(s.Namespace()).Create(s.Context(), &v3beta4.StorageProfile{
		ObjectMeta: metav1.ObjectMeta{
			Name: "sidecar",
		},
		Spec: v3beta4.StorageProfileSpec{
			Routes: []v3beta4.Route{
				{
					Store: corev1.ObjectReference{
						Name: "sidecar",
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	s.NoError(err)

	s.T().Log("Create Pod with sidecar")
	_, err = s.CoreV1().Pods(s.Namespace()).Create(s.Context(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "sidecar",
			Labels: map[string]string{
				"sidecar.atomix.io/inject":  "true",
				"runtime.atomix.io/profile": "sidecar",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    "sleep",
					Image:   "alpine:latest",
					Command: []string{"/bin/sh", "-c", "--"},
					Args:    []string{"while true; do sleep 30; done;"},
				},
			},
		},
	}, metav1.CreateOptions{})
	s.NoError(err)

	s.T().Log("Await Pod sidecar ready")
	s.Await(func() bool {
		pod, err := s.CoreV1().Pods(s.Namespace()).Get(s.Context(), "sidecar", metav1.GetOptions{})
		s.NoError(err)
		if len(pod.Status.ContainerStatuses) < 2 {
			return false
		}
		s.Len(pod.Status.ContainerStatuses, 2)
		return pod.Status.ContainerStatuses[0].Ready && pod.Status.ContainerStatuses[1].Ready
	})
}

func (s *SidecarTestSuite) Await(predicate func() bool) {
	for {
		if predicate() {
			return
		}
		time.Sleep(time.Second)
	}
}

func (s *SidecarTestSuite) NoError(err error, args ...interface{}) bool {
	if err == context.DeadlineExceeded {
		s.Fail(err.Error())
	}
	return s.Suite.NoError(err, args...)
}

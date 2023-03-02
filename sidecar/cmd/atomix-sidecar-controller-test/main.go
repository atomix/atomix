// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta4"
	atomixv3beta3 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta3"
	atomixv3beta4 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta4"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"os"
	"testing"
	"time"
)

func main() {
	cmd := getCommand()
	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}

func getCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "atomix-sidecar-controller-test",
		RunE: func(cmd *cobra.Command, args []string) error {
			timeout, err := cmd.Flags().GetDuration("timeout")
			if err != nil {
				return err
			}

			config, err := rest.InClusterConfig()
			if err != nil {
				return err
			}

			k8sClient, err := kubernetes.NewForConfig(config)
			if err != nil {
				return err
			}

			atomixV3beta3Client, err := atomixv3beta3.NewForConfig(config)
			if err != nil {
				return err
			}

			atomixV3beta4Client, err := atomixv3beta4.NewForConfig(config)
			if err != nil {
				return err
			}

			tests := []testing.InternalTest{
				{
					Name: "TestController",
					F: func(t *testing.T) {
						suite.Run(t, &TestSuite{
							name:                os.Getenv("TEST_NAME"),
							timeout:             timeout,
							Clientset:           k8sClient,
							AtomixV3beta3Client: atomixV3beta3Client,
							AtomixV3beta4Client: atomixV3beta4Client,
						})
					},
				},
			}

			// Hack to enable verbose testing.
			os.Args = []string{
				os.Args[0],
				"-test.v",
			}

			testing.Main(func(_, _ string) (bool, error) { return true, nil }, tests, nil, nil)
			return nil
		},
	}

	cmd.Flags().DurationP("timeout", "t", 10*time.Minute, "the test timeout")
	return cmd
}

type TestSuite struct {
	suite.Suite
	*kubernetes.Clientset
	*atomixv3beta3.AtomixV3beta3Client
	*atomixv3beta4.AtomixV3beta4Client
	name    string
	timeout time.Duration
}

func (t *TestSuite) AtomixV3beta3() *atomixv3beta3.AtomixV3beta3Client {
	return t.AtomixV3beta3Client
}

func (t *TestSuite) AtomixV3beta4() *atomixv3beta4.AtomixV3beta4Client {
	return t.AtomixV3beta4Client
}

func (t *TestSuite) TestSidecarInjection() {
	namespace := t.setup("sidecar")
	defer t.teardown("sidecar")

	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	t.T().Log("Create StorageProfile sidecar")
	_, err := t.AtomixV3beta4().StorageProfiles(namespace).Create(ctx, &v3beta4.StorageProfile{
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
	t.NoError(err)

	t.T().Log("Create Pod sidecar")
	_, err = t.CoreV1().Pods(namespace).Create(ctx, &corev1.Pod{
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
	t.NoError(err)

	t.T().Log("Await Pod sidecar ready")
	t.Await(func() bool {
		pod, err := t.CoreV1().Pods(namespace).Get(ctx, "sidecar", metav1.GetOptions{})
		t.NoError(err)
		if len(pod.Status.ContainerStatuses) < 2 {
			return false
		}
		t.Len(pod.Status.ContainerStatuses, 2)
		return pod.Status.ContainerStatuses[0].Ready && pod.Status.ContainerStatuses[1].Ready
	})
}

func (t *TestSuite) Await(predicate func() bool) {
	for {
		if predicate() {
			return
		}
		time.Sleep(time.Second)
	}
}

func (t *TestSuite) NoError(err error, args ...interface{}) bool {
	if err == context.DeadlineExceeded {
		t.Fail(err.Error())
	}
	return t.Suite.NoError(err, args...)
}

func (t *TestSuite) setup(name string) string {
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	namespace := fmt.Sprintf("%s-%s", t.name, name)
	_, err := t.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}, metav1.CreateOptions{})
	t.NoError(err)
	return namespace
}

func (t *TestSuite) teardown(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	namespace := fmt.Sprintf("%s-%s", t.name, name)
	err := t.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{})
	t.NoError(err)
}

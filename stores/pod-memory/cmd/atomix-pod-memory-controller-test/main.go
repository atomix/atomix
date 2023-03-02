// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	atomixv3beta3 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta3"
	atomixv3beta4 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta4"
	"github.com/atomix/atomix/stores/pod-memory/pkg/apis/podmemory/v1beta1"
	clientv1beta1 "github.com/atomix/atomix/stores/pod-memory/pkg/client/clientset/versioned/typed/podmemory/v1beta1"
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
		Use: "atomix-pod-memory-controller-test",
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

			podMemoryV1beta2Client, err := clientv1beta1.NewForConfig(config)
			if err != nil {
				return err
			}

			tests := []testing.InternalTest{
				{
					Name: "TestController",
					F: func(t *testing.T) {
						suite.Run(t, &TestSuite{
							name:                   os.Getenv("TEST_NAME"),
							timeout:                timeout,
							Clientset:              k8sClient,
							PodmemoryV1beta1Client: podMemoryV1beta2Client,
							AtomixV3beta3Client:    atomixV3beta3Client,
							AtomixV3beta4Client:    atomixV3beta4Client,
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
	*clientv1beta1.PodmemoryV1beta1Client
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

func (t *TestSuite) PodmemoryV1beta1() *clientv1beta1.PodmemoryV1beta1Client {
	return t.PodmemoryV1beta1Client
}

func (t *TestSuite) TestStore() {
	namespace := t.setup("store")
	defer t.teardown("store")

	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	t.T().Log("Create PodMemoryStore store")
	_, err := t.PodmemoryV1beta1().PodMemoryStores(namespace).Create(ctx, &v1beta1.PodMemoryStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "store",
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Log("Await PodMemoryStore store ready")
	t.AwaitStoreReady(ctx, namespace, "store")

	_, err = t.AtomixV3beta4().DataStores(namespace).Get(ctx, "store", metav1.GetOptions{})
	t.NoError(err)
}

func (t *TestSuite) Await(predicate func() bool) {
	for {
		if predicate() {
			return
		}
		time.Sleep(time.Second)
	}
}

func (t *TestSuite) AwaitStoreReady(ctx context.Context, namespace, name string) {
	t.Await(func() bool {
		cluster, err := t.PodmemoryV1beta1().PodMemoryStores(namespace).Get(ctx, name, metav1.GetOptions{})
		t.NoError(err)
		return cluster.Status.State == v1beta1.PodMemoryStoreReady
	})
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

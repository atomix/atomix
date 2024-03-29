// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0
// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	v1beta1 "github.com/atomix/atomix/stores/pod-memory/pkg/client/clientset/versioned/typed/podmemory/v1beta1"
	rest "k8s.io/client-go/rest"
	testing "k8s.io/client-go/testing"
)

type FakePodmemoryV1beta1 struct {
	*testing.Fake
}

func (c *FakePodmemoryV1beta1) PodMemoryStores(namespace string) v1beta1.PodMemoryStoreInterface {
	return &FakePodMemoryStores{c, namespace}
}

// RESTClient returns a RESTClient that is used to communicate
// with API server by this client implementation.
func (c *FakePodmemoryV1beta1) RESTClient() rest.Interface {
	var ret *rest.RESTClient
	return ret
}

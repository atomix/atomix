// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0
// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	v1beta3 "github.com/atomix/atomix/stores/raft/pkg/client/clientset/versioned/typed/raft/v1beta3"
	rest "k8s.io/client-go/rest"
	testing "k8s.io/client-go/testing"
)

type FakeRaftV1beta3 struct {
	*testing.Fake
}

func (c *FakeRaftV1beta3) RaftClusters(namespace string) v1beta3.RaftClusterInterface {
	return &FakeRaftClusters{c, namespace}
}

func (c *FakeRaftV1beta3) RaftPartitions(namespace string) v1beta3.RaftPartitionInterface {
	return &FakeRaftPartitions{c, namespace}
}

func (c *FakeRaftV1beta3) RaftReplicas(namespace string) v1beta3.RaftReplicaInterface {
	return &FakeRaftReplicas{c, namespace}
}

func (c *FakeRaftV1beta3) RaftStores(namespace string) v1beta3.RaftStoreInterface {
	return &FakeRaftStores{c, namespace}
}

// RESTClient returns a RESTClient that is used to communicate
// with API server by this client implementation.
func (c *FakeRaftV1beta3) RESTClient() rest.Interface {
	var ret *rest.RESTClient
	return ret
}

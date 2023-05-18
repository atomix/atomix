// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0
// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	clientset "github.com/atomix/atomix/stores/shared-memory/pkg/client/clientset/versioned"
	sharedmemoryv1beta1 "github.com/atomix/atomix/stores/shared-memory/pkg/client/clientset/versioned/typed/sharedmemory/v1beta1"
	fakesharedmemoryv1beta1 "github.com/atomix/atomix/stores/shared-memory/pkg/client/clientset/versioned/typed/sharedmemory/v1beta1/fake"
	sharedmemoryv1beta2 "github.com/atomix/atomix/stores/shared-memory/pkg/client/clientset/versioned/typed/sharedmemory/v1beta2"
	fakesharedmemoryv1beta2 "github.com/atomix/atomix/stores/shared-memory/pkg/client/clientset/versioned/typed/sharedmemory/v1beta2/fake"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/testing"
)

// NewSimpleClientset returns a clientset that will respond with the provided objects.
// It's backed by a very simple object tracker that processes creates, updates and deletions as-is,
// without applying any validations and/or defaults. It shouldn't be considered a replacement
// for a real clientset and is mostly useful in simple unit tests.
func NewSimpleClientset(objects ...runtime.Object) *Clientset {
	o := testing.NewObjectTracker(scheme, codecs.UniversalDecoder())
	for _, obj := range objects {
		if err := o.Add(obj); err != nil {
			panic(err)
		}
	}

	cs := &Clientset{tracker: o}
	cs.discovery = &fakediscovery.FakeDiscovery{Fake: &cs.Fake}
	cs.AddReactor("*", "*", testing.ObjectReaction(o))
	cs.AddWatchReactor("*", func(action testing.Action) (handled bool, ret watch.Interface, err error) {
		gvr := action.GetResource()
		ns := action.GetNamespace()
		watch, err := o.Watch(gvr, ns)
		if err != nil {
			return false, nil, err
		}
		return true, watch, nil
	})

	return cs
}

// Clientset implements clientset.Interface. Meant to be embedded into a
// struct to get a default implementation. This makes faking out just the method
// you want to test easier.
type Clientset struct {
	testing.Fake
	discovery *fakediscovery.FakeDiscovery
	tracker   testing.ObjectTracker
}

func (c *Clientset) Discovery() discovery.DiscoveryInterface {
	return c.discovery
}

func (c *Clientset) Tracker() testing.ObjectTracker {
	return c.tracker
}

var (
	_ clientset.Interface = &Clientset{}
	_ testing.FakeClient  = &Clientset{}
)

// SharedmemoryV1beta1 retrieves the SharedmemoryV1beta1Client
func (c *Clientset) SharedmemoryV1beta1() sharedmemoryv1beta1.SharedmemoryV1beta1Interface {
	return &fakesharedmemoryv1beta1.FakeSharedmemoryV1beta1{Fake: &c.Fake}
}

// SharedmemoryV1beta2 retrieves the SharedmemoryV1beta2Client
func (c *Clientset) SharedmemoryV1beta2() sharedmemoryv1beta2.SharedmemoryV1beta2Interface {
	return &fakesharedmemoryv1beta2.FakeSharedmemoryV1beta2{Fake: &c.Fake}
}
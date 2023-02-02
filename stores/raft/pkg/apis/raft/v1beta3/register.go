// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

// NOTE: Boilerplate only.  Ignore this file.

// Package v1beta3 contains API Schema definitions for the raft v1beta3 API group
// +k8s:deepcopy-gen=package,register
// +groupName=raft.atomix.io
package v1beta3

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// SchemeGroupVersion is group version used to register these objects
var SchemeGroupVersion = schema.GroupVersion{Group: "raft.atomix.io", Version: "v1beta3"}

// Kind takes an unqualified kind and returns a Group qualified GroupKind
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource takes an unqualified resource and returns a Group qualified GroupResource
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder initializes a scheme builder
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme is a global function that registers this API group & version to a scheme
	AddToScheme = SchemeBuilder.AddToScheme
)

// Adds the list of known types to Scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion, &RaftCluster{}, &RaftClusterList{})
	scheme.AddKnownTypes(SchemeGroupVersion, &RaftStore{}, &RaftStoreList{})
	scheme.AddKnownTypes(SchemeGroupVersion, &RaftPartition{}, &RaftPartitionList{})
	scheme.AddKnownTypes(SchemeGroupVersion, &RaftReplica{}, &RaftReplicaList{})
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

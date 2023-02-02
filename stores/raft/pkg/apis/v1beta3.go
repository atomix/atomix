// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package apis

import (
	raftv1beta2 "github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta2"
	raftv1beta3 "github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
)

func init() {
	// register the types with the Scheme so the components can map objects to GroupVersionKinds and back
	AddToSchemes = append(AddToSchemes, raftv1beta3.SchemeBuilder.AddToScheme)
}

func registerConversions_v1beta3(scheme *runtime.Scheme) error {
	if err := scheme.AddConversionFunc((*raftv1beta2.RaftCluster)(nil), (*raftv1beta3.RaftCluster)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return convertRaftCluster_v1beta2_to_v1beta3(a.(*raftv1beta2.RaftCluster), b.(*raftv1beta3.RaftCluster), scope)
	}); err != nil {
		return err
	}
	return nil
}

func convertRaftCluster_v1beta2_to_v1beta3(in *raftv1beta2.RaftCluster, out *raftv1beta3.RaftCluster, scope conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	out.SetGroupVersionKind(raftv1beta3.SchemeGroupVersion.WithKind("RaftCluster"))
	if in.Spec.VolumeClaimTemplate != nil {
		out.Spec.Persistence.Enabled = true
		out.Spec.Persistence.StorageClass = in.Spec.VolumeClaimTemplate.Spec.StorageClassName
		out.Spec.Persistence.AccessModes = in.Spec.VolumeClaimTemplate.Spec.AccessModes
		out.Spec.Persistence.Selector = in.Spec.VolumeClaimTemplate.Spec.Selector
		if size, ok := in.Spec.VolumeClaimTemplate.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
			out.Spec.Persistence.Size = &size
		}
	} else {
		out.Spec.Persistence.Enabled = false
	}
	return nil
}

// SPDX-FileCopyrightText: 2022-present Intel Corporation
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
	AddToSchemes = append(AddToSchemes, raftv1beta2.SchemeBuilder.AddToScheme)
}

func registerConversions_v1beta2(scheme *runtime.Scheme) error {
	if err := scheme.AddConversionFunc((*raftv1beta3.RaftCluster)(nil), (*raftv1beta2.RaftCluster)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return convertRaftCluster_v1beta3_to_v1beta2(a.(*raftv1beta3.RaftCluster), b.(*raftv1beta2.RaftCluster), scope)
	}); err != nil {
		return err
	}
	return nil
}

func convertRaftCluster_v1beta3_to_v1beta2(in *raftv1beta3.RaftCluster, out *raftv1beta2.RaftCluster, scope conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	out.SetGroupVersionKind(raftv1beta2.SchemeGroupVersion.WithKind("RaftCluster"))
	if in.Spec.Persistence.Enabled {
		out.Spec.VolumeClaimTemplate = &corev1.PersistentVolumeClaim{
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      in.Spec.Persistence.AccessModes,
				Selector:         in.Spec.Persistence.Selector,
				StorageClassName: in.Spec.Persistence.StorageClass,
			},
		}
		if in.Spec.Persistence.Size != nil {
			out.Spec.VolumeClaimTemplate.Spec.Resources = corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: *in.Spec.Persistence.Size,
				},
			}
		}
	}
	return nil
}

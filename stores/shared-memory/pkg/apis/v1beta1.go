// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package apis

import (
	sharedmemoryv1beta1 "github.com/atomix/atomix/stores/shared-memory/pkg/apis/sharedmemory/v1beta1"
	sharedmemoryv1beta2 "github.com/atomix/atomix/stores/shared-memory/pkg/apis/sharedmemory/v1beta2"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
)

func init() {
	// register the types with the Scheme so the components can map objects to GroupVersionKinds and back
	AddToSchemes = append(AddToSchemes, sharedmemoryv1beta1.SchemeBuilder.AddToScheme)
}

func registerConversions_v1beta1(scheme *runtime.Scheme) error {
	if err := scheme.AddConversionFunc((*sharedmemoryv1beta2.SharedMemoryStore)(nil), (*sharedmemoryv1beta1.SharedMemoryStore)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return convertSharedMemoryStore_v1beta2_to_v1beta1(a.(*sharedmemoryv1beta2.SharedMemoryStore), b.(*sharedmemoryv1beta1.SharedMemoryStore), scope)
	}); err != nil {
		return err
	}
	return nil
}

func convertSharedMemoryStore_v1beta2_to_v1beta1(in *sharedmemoryv1beta2.SharedMemoryStore, out *sharedmemoryv1beta1.SharedMemoryStore, scope conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	out.SetGroupVersionKind(sharedmemoryv1beta1.SchemeGroupVersion.WithKind("SharedMemoryStore"))
	out.Spec.Image = in.Spec.Image
	out.Spec.ImagePullPolicy = in.Spec.ImagePullPolicy
	out.Spec.ImagePullSecrets = in.Spec.ImagePullSecrets
	out.Spec.Config.Logging.Loggers = map[string]sharedmemoryv1beta1.LoggerConfig{
		"root": {
			Level: &in.Spec.Logging.RootLevel,
		},
	}
	for _, logger := range in.Spec.Logging.Loggers {
		out.Spec.Config.Logging.Loggers[logger.Name] = sharedmemoryv1beta1.LoggerConfig{
			Level: logger.Level,
		}
	}
	return nil
}

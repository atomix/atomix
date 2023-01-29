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
	AddToSchemes = append(AddToSchemes, sharedmemoryv1beta2.SchemeBuilder.AddToScheme)
}

func registerConversions_v1beta2(scheme *runtime.Scheme) error {
	if err := scheme.AddConversionFunc((*sharedmemoryv1beta1.SharedMemoryStore)(nil), (*sharedmemoryv1beta2.SharedMemoryStore)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return convertSharedMemoryStore_v1beta1_to_v1beta2(a.(*sharedmemoryv1beta1.SharedMemoryStore), b.(*sharedmemoryv1beta2.SharedMemoryStore), scope)
	}); err != nil {
		return err
	}
	return nil
}

func convertSharedMemoryStore_v1beta1_to_v1beta2(in *sharedmemoryv1beta1.SharedMemoryStore, out *sharedmemoryv1beta2.SharedMemoryStore, scope conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	out.SetGroupVersionKind(sharedmemoryv1beta2.SchemeGroupVersion.WithKind("SharedMemoryStore"))
	out.Spec.Image = in.Spec.Image
	out.Spec.ImagePullPolicy = in.Spec.ImagePullPolicy
	out.Spec.ImagePullSecrets = in.Spec.ImagePullSecrets
	if rootLogger, ok := in.Spec.Config.Logging.Loggers["root"]; ok && rootLogger.Level != nil {
		out.Spec.Logging.RootLevel = *rootLogger.Level
	}
	for name, logger := range in.Spec.Config.Logging.Loggers {
		if name != "root" {
			out.Spec.Logging.Loggers = append(out.Spec.Logging.Loggers, sharedmemoryv1beta2.LoggerConfig{
				Name:  name,
				Level: logger.Level,
			})
		}
	}
	return nil
}

// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package apis

import (
	atomixv3beta3 "github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta3"
	atomixv3beta4 "github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta4"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func init() {
	// register the types with the Scheme so the components can map objects to GroupVersionKinds and back
	AddToSchemes = append(AddToSchemes, atomixv3beta4.SchemeBuilder.AddToScheme)
}

func registerConversions_v3beta4(scheme *runtime.Scheme) error {
	if err := scheme.AddConversionFunc((*atomixv3beta3.DataStore)(nil), (*atomixv3beta4.DataStore)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return convertDataStore_v3beta3_to_v3beta4(a.(*atomixv3beta3.DataStore), b.(*atomixv3beta4.DataStore), scope)
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*atomixv3beta3.StorageProfile)(nil), (*atomixv3beta4.StorageProfile)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return convertStorageProfile_v3beta3_to_v3beta4(a.(*atomixv3beta3.StorageProfile), b.(*atomixv3beta4.StorageProfile), scope)
	}); err != nil {
		return err
	}
	return nil
}

func convertDataStore_v3beta3_to_v3beta4(in *atomixv3beta3.DataStore, out *atomixv3beta4.DataStore, scope conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	out.SetGroupVersionKind(atomixv3beta4.SchemeGroupVersion.WithKind("DataStore"))
	out.Spec.Driver = atomixv3beta4.Driver{
		Name:       in.Spec.Driver.Name,
		APIVersion: in.Spec.Driver.Version,
	}
	out.Spec.Config = in.Spec.Config
	return nil
}

func convertStorageProfile_v3beta3_to_v3beta4(in *atomixv3beta3.StorageProfile, out *atomixv3beta4.StorageProfile, scope conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	out.SetGroupVersionKind(atomixv3beta4.SchemeGroupVersion.WithKind("StorageProfile"))

	stores := make(map[types.NamespacedName]bool)
	for _, binding := range in.Spec.Bindings {
		storeName := types.NamespacedName{
			Namespace: binding.Store.Namespace,
			Name:      binding.Store.Name,
		}
		stores[storeName] = true
	}

	for storeName := range stores {
		route := atomixv3beta4.Route{
			Store: corev1.ObjectReference{
				Namespace: storeName.Namespace,
				Name:      storeName.Name,
			},
		}
		for _, binding := range in.Spec.Bindings {
			if binding.Store.Namespace == storeName.Namespace && binding.Store.Name == storeName.Name {
				route.Rules = append(route.Rules, atomixv3beta4.RoutingRule{
					Tags: binding.Tags,
				})
			}
		}
		out.Spec.Routes = append(out.Spec.Routes, route)
	}

	for _, podStatusIn := range in.Status.PodStatuses {
		var podStatusOut atomixv3beta4.PodStatus
		podStatusOut.ObjectReference = podStatusIn.ObjectReference
		for _, routeStatus := range podStatusIn.Proxy.Routes {
			podStatusOut.Runtime.Routes = append(podStatusOut.Runtime.Routes, atomixv3beta4.RouteStatus{
				Store:   routeStatus.Store,
				State:   atomixv3beta4.RouteState(routeStatus.State),
				Version: routeStatus.Version,
			})
		}
		out.Status.PodStatuses = append(out.Status.PodStatuses, podStatusOut)
	}
	return nil
}

// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package apis

import (
	atomixv3beta3 "github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta3"
	atomixv3beta4 "github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta4"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
)

func init() {
	// register the types with the Scheme so the components can map objects to GroupVersionKinds and back
	AddToSchemes = append(AddToSchemes, atomixv3beta3.SchemeBuilder.AddToScheme)
}

func registerConversions_v3beta3(scheme *runtime.Scheme) error {
	if err := scheme.AddConversionFunc((*atomixv3beta4.DataStore)(nil), (*atomixv3beta3.DataStore)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return convertDataStore_v3beta4_to_v3beta3(a.(*atomixv3beta4.DataStore), b.(*atomixv3beta3.DataStore), scope)
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*atomixv3beta4.StorageProfile)(nil), (*atomixv3beta3.StorageProfile)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return convertStorageProfile_v3beta4_to_v3beta3(a.(*atomixv3beta4.StorageProfile), b.(*atomixv3beta3.StorageProfile), scope)
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*atomixv3beta4.Route)(nil), (*atomixv3beta3.Binding)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return convertBinding_v3beta4_to_v3beta3(a.(*atomixv3beta4.Route), b.(*atomixv3beta3.Binding), scope)
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*atomixv3beta4.PodStatus)(nil), (*atomixv3beta3.PodStatus)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return convertPodStatus_v3beta4_to_v3beta3(a.(*atomixv3beta4.PodStatus), b.(*atomixv3beta3.PodStatus), scope)
	}); err != nil {
		return err
	}
	return nil
}

func convertDataStore_v3beta4_to_v3beta3(in *atomixv3beta4.DataStore, out *atomixv3beta3.DataStore, scope conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	out.SetGroupVersionKind(atomixv3beta3.SchemeGroupVersion.WithKind("StorageProfile"))
	out.Spec.Driver = atomixv3beta3.Driver{
		Name:    in.Spec.Driver.Name,
		Version: in.Spec.Driver.APIVersion,
	}
	out.Spec.Config = in.Spec.Config
	return nil
}

func convertStorageProfile_v3beta4_to_v3beta3(in *atomixv3beta4.StorageProfile, out *atomixv3beta3.StorageProfile, scope conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	out.SetGroupVersionKind(atomixv3beta3.SchemeGroupVersion.WithKind("StorageProfile"))

	for _, routeIn := range in.Spec.Routes {
		var bindingOut atomixv3beta3.Binding
		if err := scope.Convert(&routeIn, &bindingOut); err != nil {
			return err
		}
		out.Spec.Bindings = append(out.Spec.Bindings, bindingOut)
	}

	for _, podStatusIn := range in.Status.PodStatuses {
		var podStatusOut atomixv3beta3.PodStatus
		if err := scope.Convert(&podStatusIn, &podStatusOut); err != nil {
			return err
		}
		out.Status.PodStatuses = append(out.Status.PodStatuses, podStatusOut)
	}
	return nil
}

func convertBinding_v3beta4_to_v3beta3(in *atomixv3beta4.Route, out *atomixv3beta3.Binding, scope conversion.Scope) error {
	out.Store = in.Store
	for _, rule := range in.Rules {
		if rule.Kind == "" && rule.APIVersion == "" && len(rule.Names) == 0 {
			out.Tags = rule.Tags
		} else {
			primitive := atomixv3beta3.PrimitiveSpec{
				Kind:       rule.Kind,
				APIVersion: rule.APIVersion,
				Tags:       rule.Tags,
				Config:     rule.Config,
			}
			if len(rule.Names) > 0 {
				for _, name := range rule.Names {
					primitive.Name = name
					out.Primitives = append(out.Primitives, primitive)
				}
			} else {
				out.Primitives = append(out.Primitives, primitive)
			}
		}
	}
	return nil
}

func convertPodStatus_v3beta4_to_v3beta3(in *atomixv3beta4.PodStatus, out *atomixv3beta3.PodStatus, scope conversion.Scope) error {
	out.ObjectReference = in.ObjectReference
	for _, routeStatus := range in.Runtime.Routes {
		out.Proxy.Routes = append(out.Proxy.Routes, atomixv3beta3.RouteStatus{
			Store:   routeStatus.Store,
			State:   atomixv3beta3.RouteState(routeStatus.State),
			Version: routeStatus.Version,
		})
	}
	return nil
}

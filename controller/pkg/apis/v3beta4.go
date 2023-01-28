// SPDX-FileCopyrightText: 2023-present Intel Corporation
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
	AddToSchemes = append(AddToSchemes, atomixv3beta4.SchemeBuilder.AddToScheme)
}

func registerConversions_v3beta4(scheme *runtime.Scheme) error {
	if err := scheme.AddConversionFunc((*atomixv3beta3.StorageProfile)(nil), (*atomixv3beta4.StorageProfile)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return convertStorageProfile_v3beta3_to_v3beta4(a.(*atomixv3beta3.StorageProfile), b.(*atomixv3beta4.StorageProfile), scope)
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*atomixv3beta3.Binding)(nil), (*atomixv3beta4.Route)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return convertBinding_v3beta3_to_v3beta4(a.(*atomixv3beta3.Binding), b.(*atomixv3beta4.Route), scope)
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*atomixv3beta3.PodStatus)(nil), (*atomixv3beta4.PodStatus)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return convertPodStatus_v3beta3_to_v3beta4(a.(*atomixv3beta3.PodStatus), b.(*atomixv3beta4.PodStatus), scope)
	}); err != nil {
		return err
	}
	return nil
}

func convertStorageProfile_v3beta3_to_v3beta4(in *atomixv3beta3.StorageProfile, out *atomixv3beta4.StorageProfile, scope conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	out.SetGroupVersionKind(atomixv3beta4.SchemeGroupVersion.WithKind("StorageProfile"))

	for _, bindingIn := range in.Spec.Bindings {
		var routeOut atomixv3beta4.Route
		if err := scope.Convert(&bindingIn, &routeOut); err != nil {
			return err
		}
		out.Spec.Routes = append(out.Spec.Routes, routeOut)
	}

	for _, podStatusIn := range in.Status.PodStatuses {
		var podStatusOut atomixv3beta4.PodStatus
		if err := scope.Convert(&podStatusIn, &podStatusOut); err != nil {
			return err
		}
		out.Status.PodStatuses = append(out.Status.PodStatuses, podStatusOut)
	}
	return nil
}

func convertBinding_v3beta3_to_v3beta4(in *atomixv3beta3.Binding, out *atomixv3beta4.Route, scope conversion.Scope) error {
	out.Store = in.Store
	out.Rules = append(out.Rules, atomixv3beta4.RoutingRule{
		Tags: in.Tags,
	})
	for _, primitive := range in.Primitives {
		rule := atomixv3beta4.RoutingRule{
			Kind:       primitive.Kind,
			APIVersion: primitive.APIVersion,
			Tags:       primitive.Tags,
			Config:     primitive.Config,
		}
		if primitive.Name != "" {
			rule.Names = []string{primitive.Name}
		}
		out.Rules = append(out.Rules, rule)
	}
	return nil
}

func convertPodStatus_v3beta3_to_v3beta4(in *atomixv3beta3.PodStatus, out *atomixv3beta4.PodStatus, scope conversion.Scope) error {
	out.ObjectReference = in.ObjectReference
	for _, routeStatus := range in.Proxy.Routes {
		out.Runtime.Routes = append(out.Runtime.Routes, atomixv3beta4.RouteStatus{
			Store:   routeStatus.Store,
			State:   atomixv3beta4.RouteState(routeStatus.State),
			Version: routeStatus.Version,
		})
	}
	return nil
}

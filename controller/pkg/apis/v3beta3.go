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

const wildcard = "*"

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
	return nil
}

func convertDataStore_v3beta4_to_v3beta3(in *atomixv3beta4.DataStore, out *atomixv3beta3.DataStore, scope conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	out.SetGroupVersionKind(atomixv3beta3.SchemeGroupVersion.WithKind("DataStore"))
	if in.Spec.Driver.Name == "atomix.io/pod-memory" && in.Spec.Driver.APIVersion == "v1" {
		out.Spec.Driver = atomixv3beta3.Driver{
			Name:    "PodMemory",
			Version: "v1beta1",
		}
	} else if in.Spec.Driver.Name == "atomix.io/shared-memory" && in.Spec.Driver.APIVersion == "v1" {
		out.Spec.Driver = atomixv3beta3.Driver{
			Name:    "SharedMemory",
			Version: "v1beta1",
		}
	} else {
		out.Spec.Driver = atomixv3beta3.Driver{
			Name:    in.Spec.Driver.Name,
			Version: in.Spec.Driver.APIVersion,
		}
	}
	out.Spec.Config = in.Spec.Config
	return nil
}

func convertStorageProfile_v3beta4_to_v3beta3(in *atomixv3beta4.StorageProfile, out *atomixv3beta3.StorageProfile, scope conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	out.SetGroupVersionKind(atomixv3beta3.SchemeGroupVersion.WithKind("StorageProfile"))

	var priority uint32 = 0
	for _, routeIn := range in.Spec.Routes {
		if len(routeIn.Rules) > 0 {
			for _, ruleIn := range routeIn.Rules {
				var tags []string
				for _, tag := range ruleIn.Tags {
					if tag != wildcard {
						tags = append(tags, tag)
					}
				}
				out.Spec.Bindings = append(out.Spec.Bindings, atomixv3beta3.Binding{
					Store:    routeIn.Store,
					Priority: &priority,
					Tags:     tags,
				})
			}
		} else {
			out.Spec.Bindings = append(out.Spec.Bindings, atomixv3beta3.Binding{
				Store:    routeIn.Store,
				Priority: &priority,
			})
		}
	}

	for _, podStatusIn := range in.Status.PodStatuses {
		var podStatusOut atomixv3beta3.PodStatus
		podStatusOut.ObjectReference = podStatusIn.ObjectReference
		for _, routeStatus := range podStatusIn.Runtime.Routes {
			podStatusOut.Proxy.Routes = append(podStatusOut.Proxy.Routes, atomixv3beta3.RouteStatus{
				Store:   routeStatus.Store,
				State:   atomixv3beta3.RouteState(routeStatus.State),
				Version: routeStatus.Version,
			})
		}
		out.Status.PodStatuses = append(out.Status.PodStatuses, podStatusOut)
	}
	return nil
}

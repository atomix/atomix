// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v3beta3

import (
	"github.com/atomix/runtime/sdk/pkg/logging"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var log = logging.GetLogger()

// AddControllers adds sidecar controllers to the given manager
func AddControllers(mgr manager.Manager) error {
	if err := addProxyController(mgr); err != nil {
		return err
	}
	if err := addProfileController(mgr); err != nil {
		return err
	}
	if err := addPodController(mgr); err != nil {
		return err
	}
	return nil
}

func hasFinalizer(object client.Object, name string) bool {
	for _, finalizer := range object.GetFinalizers() {
		if finalizer == name {
			return true
		}
	}
	return false
}

func addFinalizer(object client.Object, name string) {
	object.SetFinalizers(append(object.GetFinalizers(), name))
}

func removeFinalizer(object client.Object, name string) {
	finalizers := make([]string, 0, len(object.GetFinalizers()))
	for _, finalizer := range object.GetFinalizers() {
		if finalizer != name {
			finalizers = append(finalizers, finalizer)
		}
	}
	object.SetFinalizers(finalizers)
}

func getNamespacedName(object client.Object) types.NamespacedName {
	return types.NamespacedName{
		Namespace: object.GetNamespace(),
		Name:      object.GetName(),
	}
}

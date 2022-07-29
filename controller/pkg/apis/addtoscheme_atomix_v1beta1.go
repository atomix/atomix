// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package apis

import (
	atomixv1beta1 "github.com/atomix/runtime/controller/pkg/apis/atomix/v3beta1"
)

func init() {
	// register the types with the Scheme so the components can map objects to GroupVersionKinds and back
	AddToSchemes = append(AddToSchemes, atomixv1beta1.SchemeBuilder.AddToScheme)
}

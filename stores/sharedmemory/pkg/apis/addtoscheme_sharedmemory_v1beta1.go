// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package apis

import (
	sharedmemoryv1beta1 "github.com/atomix/atomix/stores/sharedmemory/pkg/apis/sharedmemory/v1beta1"
)

func init() {
	// register the types with the Scheme so the components can map objects to GroupVersionKinds and back
	AddToSchemes = append(AddToSchemes, sharedmemoryv1beta1.SchemeBuilder.AddToScheme)
}

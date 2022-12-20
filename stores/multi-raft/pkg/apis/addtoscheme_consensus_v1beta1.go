// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package apis

import (
	multiraftv1beta1 "github.com/atomix/atomix/stores/multi-raft/pkg/apis/multiraft/v1beta1"
)

func init() {
	// register the types with the Scheme so the components can map objects to GroupVersionKinds and back
	AddToSchemes = append(AddToSchemes, multiraftv1beta1.SchemeBuilder.AddToScheme)
}

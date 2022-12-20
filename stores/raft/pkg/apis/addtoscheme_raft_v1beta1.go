// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package apis

import (
	raftv1beta1 "github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta1"
)

func init() {
	// register the types with the Scheme so the components can map objects to GroupVersionKinds and back
	AddToSchemes = append(AddToSchemes, raftv1beta1.SchemeBuilder.AddToScheme)
}

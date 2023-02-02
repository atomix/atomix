// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package apis

import (
	"k8s.io/apimachinery/pkg/runtime"
)

// AddToSchemes may be used to add all resources defined in the project to a Scheme
var AddToSchemes runtime.SchemeBuilder

// AddToScheme adds all Resources to the Scheme
func AddToScheme(s *runtime.Scheme) error {
	if err := AddToSchemes.AddToScheme(s); err != nil {
		return err
	}
	if err := RegisterConversions(s); err != nil {
		return err
	}
	return nil
}

func RegisterConversions(s *runtime.Scheme) error {
	if err := registerConversions_v1beta2(s); err != nil {
		return err
	}
	if err := registerConversions_v1beta3(s); err != nil {
		return err
	}
	return nil
}

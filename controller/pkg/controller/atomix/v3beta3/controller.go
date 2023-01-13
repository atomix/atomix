// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v3beta3

import (
	"github.com/atomix/atomix/runtime/pkg/logging"
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

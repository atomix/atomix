// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	"github.com/atomix/atomix/runtime/pkg/logging"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var log = logging.GetLogger()

// AddControllers adds memory controllers to the manager
func AddControllers(mgr manager.Manager) error {
	if err := addSharedMemoryStoreController(mgr); err != nil {
		return err
	}
	if err := addSharedMemoryStoreConverter(mgr); err != nil {
		return err
	}
	return nil
}

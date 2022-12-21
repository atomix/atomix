// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	"github.com/atomix/atomix/runtime/pkg/logging"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var log = logging.GetLogger()

// AddControllers adds raft controllers to the manager
func AddControllers(mgr manager.Manager) error {
	if err := addRaftStoreController(mgr); err != nil {
		return err
	}
	if err := addRaftStoreController(mgr); err != nil {
		return err
	}
	if err := addRaftClusterController(mgr); err != nil {
		return err
	}
	if err := addRaftStoreController(mgr); err != nil {
		return err
	}
	if err := addRaftPartitionController(mgr); err != nil {
		return err
	}
	if err := addRaftMemberController(mgr); err != nil {
		return err
	}
	if err := addPodController(mgr); err != nil {
		return err
	}
	return nil
}

func logWarn(log logging.Logger, err error) error {
	if err == nil {
		return nil
	}
	if k8serrors.IsNotFound(err) || k8serrors.IsAlreadyExists(err) || k8serrors.IsConflict(err) {
		log.Debug(err)
	} else {
		log.Error(err)
	}
	return err
}

func logError(log logging.Logger, err error) error {
	if err == nil {
		return nil
	}
	if k8serrors.IsNotFound(err) || k8serrors.IsAlreadyExists(err) || k8serrors.IsConflict(err) {
		log.Debug(err)
	} else {
		log.Error(err)
	}
	return err
}

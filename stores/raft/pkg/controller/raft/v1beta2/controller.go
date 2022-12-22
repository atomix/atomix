// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	"context"
	"github.com/atomix/atomix/runtime/pkg/logging"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	if err := addRaftReplicaController(mgr); err != nil {
		return err
	}
	if err := addPodController(mgr); err != nil {
		return err
	}
	return nil
}

func get(client client.Client, ctx context.Context, objectKey client.ObjectKey, object client.Object, log logging.Logger) (bool, error) {
	if err := client.Get(ctx, objectKey, object); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.WithSkipCalls(1).Error(err)
			return false, err
		}
		log.WithSkipCalls(1).Debug(err)
		return false, nil
	}
	return true, nil
}

func create(client client.Client, ctx context.Context, object client.Object, log logging.Logger) error {
	if err := client.Create(ctx, object); err != nil {
		if !k8serrors.IsAlreadyExists(err) {
			log.WithSkipCalls(1).Error(err)
		} else {
			log.WithSkipCalls(1).Debug(err)
		}
		return err
	}
	return nil
}

func update(client client.Client, ctx context.Context, object client.Object, log logging.Logger) error {
	if err := client.Update(ctx, object); err != nil {
		if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
			log.WithSkipCalls(1).Error(err)
		} else {
			log.WithSkipCalls(1).Debug(err)
		}
		return err
	}
	return nil
}

func updateStatus(client client.Client, ctx context.Context, object client.Object, log logging.Logger) error {
	if err := client.Status().Update(ctx, object); err != nil {
		if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
			log.WithSkipCalls(1).Error(err)
		} else {
			log.WithSkipCalls(1).Debug(err)
		}
		return err
	}
	return nil
}

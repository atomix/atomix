// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"os"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"time"
)

var log = logf.Log.WithName("leader")

const (
	podNameEnv      = "POD_NAME"
	podNamespaceEnv = "POD_NAMESPACE"

	maxBackoffInterval = time.Second * 16
)

// BecomeLeader ensures that the current pod is the leader within its namespace. If
// run outside a cluster, it will skip leader election and return nil. It
// continuously tries to create a ConfigMap with the provided name and the
// current pod set as the owner reference. Only one can exist at a time with
// the same name, so the pod that successfully creates the ConfigMap is the
// leader. Upon termination of that pod, the garbage collector will delete the
// ConfigMap, enabling a different pod to become the leader.
func BecomeLeader(ctx context.Context) error {
	log.Info("Trying to become the leader.")

	ns := os.Getenv(podNamespaceEnv)
	if ns == "" {
		log.Info("Skipping leader election; not running in a cluster.")
		return nil
	}

	name := os.Getenv(podNameEnv)
	if name == "" {
		log.Info("Skipping leader election; not running in a cluster.")
		return nil
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	client, err := crclient.New(config, crclient.Options{})
	if err != nil {
		return err
	}

	owner, err := myOwnerRef(ctx, client, ns, name)
	if err != nil {
		return err
	}

	// check for existing lock from this pod, in case we got restarted
	existing := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
	}
	key := crclient.ObjectKey{Namespace: ns, Name: name}
	err = client.Get(ctx, key, existing)

	switch {
	case err == nil:
		for _, existingOwner := range existing.GetOwnerReferences() {
			if existingOwner.Name == owner.Name {
				log.Info("Found existing lock with my name. I was likely restarted.")
				log.Info("Continuing as the leader.")
				return nil
			}
			log.Info("Found existing lock", "LockOwner", existingOwner.Name)
		}
	case errors.IsNotFound(err):
		log.Info("No pre-existing lock was found.")
	default:
		log.Error(err, "unknown error trying to get ConfigMap")
		return err
	}

	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       ns,
			OwnerReferences: []metav1.OwnerReference{*owner},
		},
	}

	// try to create a lock
	backoff := time.Second
	for {
		err := client.Create(ctx, cm)
		switch {
		case err == nil:
			log.Info("Became the leader.")
			return nil
		case errors.IsAlreadyExists(err):
			log.Info("Not the leader. Waiting.")
			select {
			case <-time.After(wait.Jitter(backoff, .2)):
				if backoff < maxBackoffInterval {
					backoff *= 2
				}
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		default:
			log.Error(err, "unknown error creating configmap")
			return err
		}
	}
}

// myOwnerRef returns an OwnerReference that corresponds to the controller.
// It expects the environment variable CONTROLLER_NAME to be set by the downwards API
func myOwnerRef(ctx context.Context, client crclient.Client, ns string, name string) (*metav1.OwnerReference, error) {
	myPod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
	}

	key := crclient.ObjectKey{Namespace: ns, Name: name}
	err := client.Get(ctx, key, myPod)
	if err != nil {
		log.Error(err, "failed to get pod", "Pod.Namespace", ns, "Pod.Name", name)
		return nil, err
	}

	owner := &metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "Pod",
		Name:       myPod.ObjectMeta.Name,
		UID:        myPod.ObjectMeta.UID,
	}
	return owner, nil
}

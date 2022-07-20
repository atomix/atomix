// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta1

import (
	"context"
	"fmt"
	atomixv1beta1 "github.com/atomix/runtime/controller/pkg/apis/atomix/v1beta1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"time"
)

const atomixReadyCondition = "AtomixReady"

func addPodController(mgr manager.Manager) error {
	// Create a new controller
	c, err := controller.New("pod-controller", mgr, controller.Options{
		Reconciler: &PodReconciler{
			client: mgr.GetClient(),
			scheme: mgr.GetScheme(),
			config: mgr.GetConfig(),
		},
		RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond*10, time.Second*5),
	})
	if err != nil {
		return err
	}

	// Watch for changes to Pods
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to Proxies
	err = c.Watch(&source.Kind{Type: &atomixv1beta1.Proxy{}}, &handler.EnqueueRequestForOwner{
		OwnerType: &corev1.Pod{},
	})
	if err != nil {
		return err
	}

	// Watch for changes to Profiles
	err = c.Watch(&source.Kind{Type: &atomixv1beta1.Profile{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		podList := &corev1.PodList{}
		if err := mgr.GetClient().List(context.Background(), podList, &client.ListOptions{Namespace: object.GetNamespace()}); err != nil {
			log.Error(err)
			return nil
		}

		var requests []reconcile.Request
		for _, pod := range podList.Items {
			if pod.Annotations[proxyProfileAnnotation] == object.GetName() {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: pod.Namespace,
						Name:      pod.Name,
					},
				})
			}
		}
		return requests
	}))
	if err != nil {
		return err
	}
	return nil
}

// PodReconciler is a Reconciler for Profiles
type PodReconciler struct {
	client client.Client
	scheme *runtime.Scheme
	config *rest.Config
}

// Reconcile reconciles Profile resources
func (r *PodReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log.Infof("Reconciling Pod '%s'", request.NamespacedName)
	pod := &corev1.Pod{}
	err := r.client.Get(context.TODO(), request.NamespacedName, pod)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		log.Error(err)
		return reconcile.Result{}, err
	}

	profileName, ok := pod.Annotations[proxyProfileAnnotation]
	if !ok {
		return reconcile.Result{}, nil
	}

	profileNamespacedName := types.NamespacedName{
		Namespace: pod.Namespace,
		Name:      profileName,
	}
	profile := &atomixv1beta1.Profile{}
	if err := r.client.Get(ctx, profileNamespacedName, profile); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	proxyNamespacedName := types.NamespacedName{
		Namespace: pod.Namespace,
		Name:      pod.Name,
	}
	proxy := &atomixv1beta1.Proxy{}
	if err := r.client.Get(ctx, proxyNamespacedName, proxy); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return reconcile.Result{}, err
		}

		proxy = &atomixv1beta1.Proxy{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: proxyNamespacedName.Namespace,
				Name:      proxyNamespacedName.Name,
			},
			Pod: corev1.LocalObjectReference{
				Name: pod.Name,
			},
			Profile: corev1.LocalObjectReference{
				Name: profileName,
			},
		}

		if err := controllerutil.SetOwnerReference(pod, proxy, r.scheme); err != nil {
			log.Error(err)
			return reconcile.Result{}, err
		}

		if err := r.client.Create(ctx, proxy); err != nil {
			log.Error(err)
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	if len(proxy.Status.Bindings) == 0 {
		if ok, err := r.setAtomixCondition(pod, corev1.ConditionFalse, "Configuring", "Configuring bindings"); err != nil {
			log.Error(err)
			return reconcile.Result{}, err
		} else if ok {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, nil
	}

	for _, binding := range proxy.Status.Bindings {
		if binding.State != atomixv1beta1.BindingBound {
			if ok, err := r.setAtomixCondition(pod, corev1.ConditionFalse, "Configuring", fmt.Sprintf("Configuring binding '%s'", binding.Name)); err != nil {
				log.Error(err)
				return reconcile.Result{}, err
			} else if ok {
				return reconcile.Result{}, nil
			}
		}
	}

	if proxy.Status.Ready {
		if ok, err := r.setAtomixCondition(pod, corev1.ConditionTrue, "", ""); err != nil {
			log.Error(err)
			return reconcile.Result{}, err
		} else if ok {
			return reconcile.Result{}, nil
		}
	}
	return reconcile.Result{}, nil
}

func (r *PodReconciler) setAtomixCondition(pod *corev1.Pod, status corev1.ConditionStatus, reason string, message string) (bool, error) {
	for i, condition := range pod.Status.Conditions {
		if condition.Type == atomixReadyCondition {
			if condition.Status == status && condition.Reason == reason && condition.Message == message {
				return false, nil
			}
			log.Infof("Updating Pod %s condition: status=%s, reason=%s, message=%s",
				types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, status, reason, message)
			if condition.Status != status {
				condition.LastTransitionTime = metav1.Now()
			}
			condition.Status = status
			condition.Reason = reason
			condition.Message = message
			pod.Status.Conditions[i] = condition
			if err := r.client.Status().Update(context.TODO(), pod); err != nil {
				log.Error(err)
				return false, err
			}
			return true, nil
		}
	}

	log.Infof("Initializing Pod %s condition: status=%s, reason=%s, message=%s",
		types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, status, reason, message)
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               atomixReadyCondition,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	})
	if err := r.client.Status().Update(context.TODO(), pod); err != nil {
		return false, err
	}
	return true, nil
}

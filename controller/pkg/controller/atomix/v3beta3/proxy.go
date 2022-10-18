// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v3beta3

import (
	"context"
	"fmt"
	atomixv3beta2 "github.com/atomix/runtime/controller/pkg/apis/atomix/v3beta3"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"net/http"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"strconv"
)

const (
	podIDEnv           = "POD_ID"
	podNamespaceEnv    = "POD_NAMESPACE"
	podNameEnv         = "POD_NAME"
	nodeIDEnv          = "NODE_ID"
	atomixNamespaceEnv = "ATOMIX_NAMESPACE"
)

const (
	proxyInjectPath             = "/inject-proxy"
	proxyInjectAnnotation       = "proxy.atomix.io/inject"
	proxyInjectStatusAnnotation = "proxy.atomix.io/status"
	proxyProfileAnnotation      = "proxy.atomix.io/profile"
	injectedStatus              = "injected"
	proxyContainerName          = "atomix-proxy"
	configVolumeName            = "atomix-config"
)

const (
	proxyImageEnv     = "PROXY_IMAGE"
	defaultProxyImage = "atomix/runtime-proxy:latest"
)

const (
	defaultProxyPort = 5679
)

func getProxyImage(image string) string {
	if image != "" {
		return image
	}
	image = os.Getenv(proxyImageEnv)
	if image != "" {
		return image
	}
	return defaultProxyImage
}

func addProxyController(mgr manager.Manager) error {
	mgr.GetWebhookServer().Register(proxyInjectPath, &webhook.Admission{
		Handler: &ProxyInjector{
			client: mgr.GetClient(),
			scheme: mgr.GetScheme(),
		},
	})
	return nil
}

// ProxyInjector is a mutating webhook that injects the proxy container into pods
type ProxyInjector struct {
	client  client.Client
	scheme  *runtime.Scheme
	decoder *admission.Decoder
}

// InjectDecoder :
func (i *ProxyInjector) InjectDecoder(decoder *admission.Decoder) error {
	i.decoder = decoder
	return nil
}

// Handle :
func (i *ProxyInjector) Handle(ctx context.Context, request admission.Request) admission.Response {
	log.Infof("Received admission request for Pod '%s'", request.UID)

	// Decode the pod
	pod := &corev1.Pod{}
	if err := i.decoder.Decode(request, pod); err != nil {
		log.Errorf("Could not decode Pod '%s'", request.UID, err)
		return admission.Errored(http.StatusBadRequest, err)
	}

	injectRuntime, ok := pod.Annotations[proxyInjectAnnotation]
	if !ok {
		log.Infof("Skipping proxy injection for Pod '%s'", request.UID)
		return admission.Allowed(fmt.Sprintf("'%s' annotation not found", proxyInjectAnnotation))
	}
	if inject, err := strconv.ParseBool(injectRuntime); err != nil {
		log.Errorf("Runtime injection failed for Pod '%s'", request.UID, err)
		return admission.Allowed(fmt.Sprintf("'%s' annotation could not be parsed", proxyInjectAnnotation))
	} else if !inject {
		log.Infof("Skipping proxy injection for Pod '%s'", request.UID)
		return admission.Allowed(fmt.Sprintf("'%s' annotation is false", proxyInjectAnnotation))
	}

	injectedRuntime, ok := pod.Annotations[proxyInjectStatusAnnotation]
	if ok && injectedRuntime == injectedStatus {
		log.Infof("Skipping proxy injection for Pod '%s'", request.UID)
		return admission.Allowed(fmt.Sprintf("'%s' annotation is '%s'", proxyInjectStatusAnnotation, injectedRuntime))
	}

	profileName, ok := pod.Annotations[proxyProfileAnnotation]
	if !ok {
		log.Warnf("No profile specified for Pod '%s'", request.UID)
		return admission.Denied(fmt.Sprintf("'%s' annotation not found", proxyProfileAnnotation))
	}

	profile := &atomixv3beta2.StorageProfile{}
	profileNamespacedName := types.NamespacedName{
		Namespace: request.Namespace,
		Name:      profileName,
	}
	if err := i.client.Get(ctx, profileNamespacedName, profile); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return admission.Errored(http.StatusInternalServerError, err)
		}
		return admission.Denied(fmt.Sprintf("profile %s not found", profileName))
	}

	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
		Name:            proxyContainerName,
		Image:           getProxyImage(profile.Spec.Proxy.Image),
		ImagePullPolicy: profile.Spec.Proxy.ImagePullPolicy,
		SecurityContext: profile.Spec.Proxy.SecurityContext,
		Args: []string{
			"--config",
			fmt.Sprintf("/etc/atomix/%s", configFile),
		},
		Env: []corev1.EnvVar{
			{
				Name: podIDEnv,
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.uid",
					},
				},
			},
			{
				Name: podNamespaceEnv,
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
			{
				Name: podNameEnv,
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				},
			},
			{
				Name: nodeIDEnv,
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "spec.nodeName",
					},
				},
			},
			{
				Name: atomixNamespaceEnv,
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          "runtime",
				ContainerPort: 5678,
			},
			{
				Name:          "control",
				ContainerPort: 5679,
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      configVolumeName,
				ReadOnly:  true,
				MountPath: "/etc/atomix",
			},
		},
	})
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: configVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: profileName,
				},
			},
		},
	})
	pod.Annotations[proxyInjectStatusAnnotation] = injectedStatus

	// Marshal the pod and return a patch response
	marshaledPod, err := json.Marshal(pod)
	if err != nil {
		log.Errorf("Runtime injection failed for Pod '%s'", request.UID, err)
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(request.Object.Raw, marshaledPod)
}

var _ admission.Handler = &ProxyInjector{}

// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v3beta3

import (
	"context"
	"fmt"
	atomixv3beta3 "github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta3"
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
	atomixProfileEnv   = "ATOMIX_PROFILE"
)

const (
	proxyInjectPath    = "/inject-proxy"
	proxyContainerName = "atomix-proxy"
	configVolumeName   = "atomix-config"
)

const (
	proxyProfileAnnotation      = "proxy.atomix.io/profile"
	proxyInjectAnnotation       = "proxy.atomix.io/inject"
	proxyInjectStatusAnnotation = "proxy.atomix.io/status"
	injectedStatus              = "injected"
)

const (
	proxyInjectLabel  = "proxy.atomix.io/inject"
	proxyProfileLabel = "proxy.atomix.io/profile"
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

	// If the proxy.atomix.io/inject label is not present, skip the mutation.
	injectRuntime, ok := pod.Labels[proxyInjectLabel]
	if !ok {
		log.Warnf("Denied proxy injection for Pod '%s': '%s' label was expected but not found", request.UID, proxyInjectLabel)
		return admission.Allowed(fmt.Sprintf("Denied proxy injection: '%s' label was expected but not found", proxyInjectLabel))
	}

	// If the proxy.atomix.io/inject label is false, skip the mutation.
	// TODO: Support removing the sidecar container on updates when proxy.atomix.io/inject label is changed to "false".
	if inject, err := strconv.ParseBool(injectRuntime); err != nil {
		log.Warnf("Denied proxy injection for Pod '%s': %s", request.UID, err.Error())
		return admission.Allowed(fmt.Sprintf("Denied proxy injection: '%s' label could not be parsed", proxyInjectLabel))
	} else if !inject {
		log.Debugf("Skipped proxy injection for Pod '%s': '%s' label is false", request.UID, proxyInjectLabel)
		return admission.Allowed(fmt.Sprintf("Skipped proxy injection: '%s' label is false", proxyInjectLabel))
	}

	// If the proxy sidecar was already injected, skip mutations.
	injectedRuntime, ok := pod.Annotations[proxyInjectStatusAnnotation]
	if ok && injectedRuntime == injectedStatus {
		log.Debugf("Skipped proxy injection for Pod '%s': '%s' annotation is already '%s'", request.UID, proxyInjectStatusAnnotation, injectedStatus)
		return admission.Allowed(fmt.Sprintf("Skipped proxy injection: '%s' annotation is already '%s'", proxyInjectStatusAnnotation, injectedStatus))
	}

	// If the proxy.atomix.io/profile label is missing, skip mutations.
	profileName, ok := pod.Labels[proxyProfileLabel]
	if !ok {
		log.Warnf("Denied proxy injection for Pod '%s': '%s' label was expected but not found", request.UID, proxyProfileLabel)
		return admission.Denied(fmt.Sprintf("Denied proxy injection: '%s' label was expected but not found", proxyProfileLabel))
	}

	// Lookup the StorageProfile associated with this Pod.
	profile := &atomixv3beta3.StorageProfile{}
	profileNamespacedName := types.NamespacedName{
		Namespace: request.Namespace,
		Name:      profileName,
	}
	if err := i.client.Get(ctx, profileNamespacedName, profile); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Errorf("Proxy injection failed for Pod '%s'", request.UID, err)
			return admission.Errored(http.StatusInternalServerError, err)
		}
		log.Debugf("Denied proxy injection for Pod '%s': StorageProfile '%s' not found", request.UID, profileName)
		return admission.Denied(fmt.Sprintf("Denied proxy injection: StorageProfile '%s' not found", profileName))
	}

	// Add the StorageProfile's ConfigMap to the Pod as a volume.
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

	// Add the sidecar proxy container to the Pod's containers list.
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
			{
				Name:  atomixProfileEnv,
				Value: profileName,
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

	// Add proxy metadata annotations to the Pod.
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[proxyInjectAnnotation] = injectRuntime
	pod.Annotations[proxyProfileAnnotation] = profileName
	pod.Annotations[proxyInjectStatusAnnotation] = injectedStatus

	// Marshal the pod and return a patch response
	marshaledPod, err := json.Marshal(pod)
	if err != nil {
		log.Errorf("Proxy injection failed for Pod '%s'", request.UID, err)
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(request.Object.Raw, marshaledPod)
}

var _ admission.Handler = &ProxyInjector{}

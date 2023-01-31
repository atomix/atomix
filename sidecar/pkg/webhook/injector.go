// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/runtime/pkg/logging"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/json"
	"net/http"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

var log = logging.GetLogger()

const (
	sidecarInjectPath    = "/inject-sidecar"
	sidecarContainerName = "atomix-sidecar"
)

const (
	sidecarInjectStatusAnnotation    = "sidecar.atomix.io/status"
	sidecarImageAnnotation           = "sidecar.atomix.io/image"
	sidecarImagePullPolicyAnnotation = "sidecar.atomix.io/imagePullPolicy"
	sidecarLogLevelAnnotation        = "sidecar.atomix.io/logLevel"
	injectedStatus                   = "injected"
)

const (
	atomixRuntimeEnv     = "ATOMIX_RUNTIME"
	atomixRuntimeSidecar = "sidecar"
	sidecarImageEnv      = "SIDECAR_IMAGE"
	defaultSidecarImage  = "atomix/sidecar"
)

func AddWebhook(mgr manager.Manager) error {
	mgr.GetWebhookServer().Register(sidecarInjectPath, &webhook.Admission{
		Handler: &SidecarInjector{
			client: mgr.GetClient(),
			scheme: mgr.GetScheme(),
		},
	})
	return nil
}

// SidecarInjector is a mutating webhook that injects the proxy container into pods
type SidecarInjector struct {
	client  client.Client
	scheme  *runtime.Scheme
	decoder *admission.Decoder
}

// InjectDecoder :
func (i *SidecarInjector) InjectDecoder(decoder *admission.Decoder) error {
	i.decoder = decoder
	return nil
}

// Handle :
func (i *SidecarInjector) Handle(ctx context.Context, request admission.Request) admission.Response {
	log.Infof("Received admission request for Pod '%s'", request.UID)

	// Decode the pod
	pod := &corev1.Pod{}
	if err := i.decoder.Decode(request, pod); err != nil {
		log.Errorf("Could not decode Pod '%s'", request.UID, err)
		return admission.Errored(http.StatusBadRequest, err)
	}

	// If the proxy sidecar was already injected, skip mutations.
	sidecarInjectStatus, ok := pod.Annotations[sidecarInjectStatusAnnotation]
	if ok && sidecarInjectStatus == injectedStatus {
		log.Debugf("Skipped sidecar injection for Pod '%s': '%s' annotation is already '%s'", request.UID, sidecarInjectStatusAnnotation, injectedStatus)
		return admission.Allowed(fmt.Sprintf("Skipped sidecar injection: '%s' annotation is already '%s'", sidecarInjectStatusAnnotation, injectedStatus))
	}

	// Get the sidecar image
	image, ok := pod.Annotations[sidecarImageAnnotation]
	if !ok {
		image = os.Getenv(sidecarImageEnv)
		if image == "" {
			image = defaultSidecarImage
		}
	}

	// Get the pull policy for the sidecar image
	imagePullPolicy, ok := pod.Annotations[sidecarImagePullPolicyAnnotation]
	if !ok {
		imagePullPolicy = string(corev1.PullAlways)
	}

	var args []string
	logLevel, ok := pod.Annotations[sidecarLogLevelAnnotation]
	if ok {
		args = append(args, "--log-level", logLevel)
	}

	// Add the sidecar proxy container to the Pod's containers list.
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
		Name:            sidecarContainerName,
		Image:           image,
		ImagePullPolicy: corev1.PullPolicy(imagePullPolicy),
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
		Args: args,
	})

	// Set the ATOMIX_RUNTIME environment variable on all containers
	for i, container := range pod.Spec.Containers {
		container.Env = append(container.Env, corev1.EnvVar{
			Name:  atomixRuntimeEnv,
			Value: atomixRuntimeSidecar,
		})
		pod.Spec.Containers[i] = container
	}

	// Add proxy metadata annotations to the Pod.
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[sidecarInjectStatusAnnotation] = injectedStatus

	// Marshal the pod and return a patch response
	marshaledPod, err := json.Marshal(pod)
	if err != nil {
		log.Errorf("Proxy injection failed for Pod '%s'", request.UID, err)
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(request.Object.Raw, marshaledPod)
}

var _ admission.Handler = &SidecarInjector{}

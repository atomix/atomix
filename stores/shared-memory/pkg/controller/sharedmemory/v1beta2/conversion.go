// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	"context"
	"github.com/atomix/atomix/controller/pkg/controller/util/k8s/conversion"
	sharedmemoryv1beta1 "github.com/atomix/atomix/stores/shared-memory/pkg/apis/sharedmemory/v1beta1"
	sharedmemoryv1beta2 "github.com/atomix/atomix/stores/shared-memory/pkg/apis/sharedmemory/v1beta2"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/json"
	"net/http"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	sharedMemoryStoreConvertPath = "/convert-store"
)

func addSharedMemoryStoreConverter(mgr manager.Manager) error {
	mgr.GetWebhookServer().Register(sharedMemoryStoreConvertPath, &conversion.Webhook{
		Handler: &SharedMemoryStoreConverter{
			client: mgr.GetClient(),
			scheme: mgr.GetScheme(),
		},
	})
	return nil
}

// SharedMemoryStoreConverter is a mutating webhook that converts old versions of SharedMemoryStore resources
type SharedMemoryStoreConverter struct {
	client  client.Client
	scheme  *runtime.Scheme
	decoder *conversion.Decoder
}

// InjectDecoder :
func (i *SharedMemoryStoreConverter) InjectDecoder(decoder *conversion.Decoder) error {
	i.decoder = decoder
	return nil
}

// Handle :
func (i *SharedMemoryStoreConverter) Handle(ctx context.Context, request conversion.Request) conversion.Response {
	log.Infof("Received conversion request for SharedMemoryStore '%s'", request.UID)

	desiredGV, err := schema.ParseGroupVersion(request.DesiredAPIVersion)
	if err != nil {
		return conversion.Errored(http.StatusBadRequest, "failed parsing desired API version: %s", err.Error())
	}

	objectsOut := make([]runtime.RawExtension, 0, len(request.Objects))
	for _, rawObjIn := range request.Objects {
		objIn := &unstructured.Unstructured{}
		if err := objIn.UnmarshalJSON(rawObjIn.Raw); err != nil {
			return conversion.Errored(http.StatusBadRequest, "converter failed unmarshaling object: %s", err.Error())
		}

		var in runtime.Object
		var out runtime.Object
		switch objIn.GetAPIVersion() {
		case sharedmemoryv1beta1.SchemeGroupVersion.Identifier():
			switch desiredGV.Version {
			case sharedmemoryv1beta2.SchemeGroupVersion.Version:
				in = &sharedmemoryv1beta1.SharedMemoryStore{}
				out = &sharedmemoryv1beta2.SharedMemoryStore{}
			}
		case sharedmemoryv1beta2.SchemeGroupVersion.Identifier():
			switch desiredGV.Version {
			case sharedmemoryv1beta1.SchemeGroupVersion.Version:
				in = &sharedmemoryv1beta2.SharedMemoryStore{}
				out = &sharedmemoryv1beta1.SharedMemoryStore{}
			}
		}

		if err := i.decoder.Decode(rawObjIn, in); err != nil {
			return conversion.Errored(http.StatusBadRequest, "converter failed decoding SharedMemoryStore: %s", err.Error())
		}
		if err := i.scheme.Convert(in, out, nil); err != nil {
			return conversion.Errored(http.StatusBadRequest, "converter failed converting SharedMemoryStore: %s", err.Error())
		}
		bytesOut, err := json.Marshal(out)
		if err != nil {
			return conversion.Errored(http.StatusBadRequest, "converter failed encoding SharedMemoryStore: %s", err.Error())
		}
		rawObjOut := runtime.RawExtension{
			Raw:    bytesOut,
			Object: out,
		}
		objectsOut = append(objectsOut, rawObjOut)
	}
	return conversion.Converted(objectsOut...)
}

var _ conversion.Handler = &SharedMemoryStoreConverter{}

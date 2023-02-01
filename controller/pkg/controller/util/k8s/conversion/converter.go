// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package conversion

import (
	"context"
	"encoding/json"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"net/http"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var log = logging.GetLogger()

func NewConverter(mgr manager.Manager, kind string, gvs ...schema.GroupVersion) *Converter {
	return &Converter{
		client: mgr.GetClient(),
		scheme: mgr.GetScheme(),
		kind:   kind,
		gvs:    gvs,
	}
}

// Converter is a mutating webhook that converts versions of resources
type Converter struct {
	client  client.Client
	scheme  *runtime.Scheme
	kind    string
	gvs     []schema.GroupVersion
	decoder *Decoder
}

// InjectDecoder :
func (i *Converter) InjectDecoder(decoder *Decoder) error {
	i.decoder = decoder
	return nil
}

// Handle :
func (i *Converter) Handle(ctx context.Context, request Request) Response {
	log.Infof("Received conversion request for %s '%s'", i.kind, request.UID)

	desiredGV, err := schema.ParseGroupVersion(request.DesiredAPIVersion)
	if err != nil {
		return Errored(http.StatusBadRequest, "failed parsing desired API version: %s", err.Error())
	}

	desiredGVK := schema.GroupVersionKind{
		Group:   desiredGV.Group,
		Version: desiredGV.Version,
		Kind:    i.kind,
	}

	objectsOut := make([]runtime.RawExtension, 0, len(request.Objects))
	for _, rawObjIn := range request.Objects {
		objIn := &unstructured.Unstructured{}
		if err := objIn.UnmarshalJSON(rawObjIn.Raw); err != nil {
			return Errored(http.StatusBadRequest, "converter failed unmarshaling %s: %s", i.kind, err.Error())
		}

		var gvk *schema.GroupVersionKind
		for _, gv := range i.gvs {
			if objIn.GetAPIVersion() == gv.Identifier() {
				gvk = &schema.GroupVersionKind{
					Group:   gv.Group,
					Version: gv.Version,
					Kind:    i.kind,
				}
				break
			}
		}

		in, err := i.scheme.New(*gvk)
		if err != nil {
			return Errored(http.StatusBadRequest, "converter failed initializing %s: %s", i.kind, err.Error())
		}
		out, err := i.scheme.New(desiredGVK)
		if err != nil {
			return Errored(http.StatusBadRequest, "converter failed initializing %s: %s", i.kind, err.Error())
		}

		if err := i.decoder.Decode(rawObjIn, in); err != nil {
			return Errored(http.StatusBadRequest, "converter failed decoding %s: %s", i.kind, err.Error())
		}
		if err := i.scheme.Convert(in, out, nil); err != nil {
			return Errored(http.StatusBadRequest, "converter failed converting %s: %s", i.kind, err.Error())
		}
		bytesOut, err := json.Marshal(out)
		if err != nil {
			return Errored(http.StatusBadRequest, "converter failed encoding %s: %s", i.kind, err.Error())
		}
		rawObjOut := runtime.RawExtension{
			Raw:    bytesOut,
			Object: out,
		}
		objectsOut = append(objectsOut, rawObjOut)
	}
	return Converted(objectsOut...)
}

var _ Handler = &Converter{}

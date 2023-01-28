// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package conversion

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/json"
)

// Decoder knows how to decode the contents of an admission
// request into a concrete object.
type Decoder struct {
	codecs serializer.CodecFactory
}

// NewDecoder creates a Decoder given the runtime.Scheme.
func NewDecoder(scheme *runtime.Scheme) (*Decoder, error) {
	return &Decoder{codecs: serializer.NewCodecFactory(scheme)}, nil
}

func (d *Decoder) Decode(rawObj runtime.RawExtension, into runtime.Object) error {
	// we error out if rawObj is an empty object.
	if len(rawObj.Raw) == 0 {
		return fmt.Errorf("there is no content to decode")
	}
	if unstructuredInto, isUnstructured := into.(*unstructured.Unstructured); isUnstructured {
		// unmarshal into unstructured's underlying object to avoid calling the decoder
		return json.Unmarshal(rawObj.Raw, &unstructuredInto.Object)
	}

	deserializer := d.codecs.UniversalDeserializer()
	return runtime.DecodeInto(deserializer, rawObj.Raw, into)
}

// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package conversion

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-logr/logr"
	"io"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"net/http"
)

var conversionScheme = runtime.NewScheme()
var conversionCodecs = serializer.NewCodecFactory(conversionScheme)

func init() {
	if err := apiextensionsv1.AddToScheme(conversionScheme); err != nil {
		panic(err)
	}
}

type Webhook struct {
	Handler Handler
	log     logr.Logger
	decoder *Decoder
}

func (wh *Webhook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body []byte
	var err error
	ctx := r.Context()

	var reviewResponse Response
	if r.Body == nil {
		err = errors.New("request body is empty")
		wh.log.Error(err, "bad request")
		reviewResponse = Errored(http.StatusBadRequest, "request body is empty: %s", err.Error())
		wh.writeResponse(w, reviewResponse)
		return
	}

	defer r.Body.Close()
	if body, err = io.ReadAll(r.Body); err != nil {
		wh.log.Error(err, "unable to read the body from the incoming request")
		reviewResponse = Errored(http.StatusBadRequest, "unable to read the body from the incoming request: %s", err.Error())
		wh.writeResponse(w, reviewResponse)
		return
	}

	// verify the content type is accurate
	if contentType := r.Header.Get("Content-Type"); contentType != "application/json" {
		err = fmt.Errorf("contentType=%s, expected application/json", contentType)
		wh.log.Error(err, "unable to process a request with an unknown content type", "content type", contentType)
		reviewResponse = Errored(http.StatusBadRequest, "unable to process a request with an unknown content type: %s", err.Error())
		wh.writeResponse(w, reviewResponse)
		return
	}

	// Both v1 and v1beta1 ConversionReview types are exactly the same, so the v1beta1 type can
	// be decoded into the v1 type. However the runtime codec's decoder guesses which type to
	// decode into by type name if an Object's TypeMeta isn't set. By setting TypeMeta of an
	// unregistered type to the v1 GVK, the decoder will coerce a v1beta1 ConversionReview to v1.
	// The actual ConversionReview GVK will be used to write a typed response in case the
	// webhook config permits multiple versions, otherwise this response will fail.
	req := Request{}
	cr := apiextensionsv1.ConversionReview{}
	// avoid an extra copy
	cr.Request = &req.ConversionRequest
	cr.SetGroupVersionKind(apiextensionsv1.SchemeGroupVersion.WithKind("ConversionReview"))
	_, actualConvRevGVK, err := conversionCodecs.UniversalDeserializer().Decode(body, nil, &cr)
	if err != nil {
		wh.log.Error(err, "unable to decode the request")
		reviewResponse = Errored(http.StatusBadRequest, "unable to decode the request: %s", err.Error())
		wh.writeResponse(w, reviewResponse)
		return
	}
	wh.log.V(1).Info("received request", "UID", req.UID, "desiredAPIVersion", req.DesiredAPIVersion)

	reviewResponse = wh.Handle(ctx, req)
	wh.writeResponseTyped(w, reviewResponse, actualConvRevGVK)
}

// writeResponse writes response to w generically, i.e. without encoding GVK information.
func (wh *Webhook) writeResponse(w io.Writer, response Response) {
	wh.writeConversionResponse(w, apiextensionsv1.ConversionReview{Response: &response.ConversionResponse})
}

// writeResponseTyped writes response to w with GVK set to admRevGVK, which is necessary
// if multiple ConversionReview versions are permitted by the webhook.
func (wh *Webhook) writeResponseTyped(w io.Writer, response Response, convRevGVK *schema.GroupVersionKind) {
	cr := apiextensionsv1.ConversionReview{
		Response: &response.ConversionResponse,
	}
	// Default to a v1 ConversionReview, otherwise the API server may not recognize the request
	// if multiple ConversionReview versions are permitted by the webhook config.
	// TODO(estroz): this should be configurable since older API servers won't know about v1.
	if convRevGVK == nil || *convRevGVK == (schema.GroupVersionKind{}) {
		cr.SetGroupVersionKind(apiextensionsv1.SchemeGroupVersion.WithKind("ConversionReview"))
	} else {
		cr.SetGroupVersionKind(*convRevGVK)
	}
	wh.writeConversionResponse(w, cr)
}

// writeConversionResponse writes ar to w.
func (wh *Webhook) writeConversionResponse(w io.Writer, cr apiextensionsv1.ConversionReview) {
	if err := json.NewEncoder(w).Encode(cr); err != nil {
		wh.log.Error(err, "unable to encode the response")
		wh.writeResponse(w, Errored(http.StatusInternalServerError, "unable to encode the response: %s", err.Error()))
	} else {
		res := cr.Response
		if log := wh.log; log.V(1).Enabled() {
			log = log.WithValues("code", res.Result.Code, "reason", res.Result.Reason)
			log.V(1).Info("wrote response", "UID", res.UID)
		}
	}
}

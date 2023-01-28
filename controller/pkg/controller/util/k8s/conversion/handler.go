// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package conversion

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"net/http"
	"sigs.k8s.io/controller-runtime/pkg/runtime/inject"
)

func (wh *Webhook) Handle(ctx context.Context, req Request) Response {
	resp := wh.Handler.Handle(ctx, req)
	if err := resp.Complete(req); err != nil {
		return Errored(http.StatusInternalServerError, "failed encoding conversion response: %s", err.Error())
	}
	return resp
}

func (wh *Webhook) InjectLogger(l logr.Logger) error {
	wh.log = l
	return nil
}

func (wh *Webhook) InjectScheme(s *runtime.Scheme) error {
	var err error
	wh.decoder, err = NewDecoder(s)
	if err != nil {
		return err
	}
	if wh.Handler != nil {
		if injector, ok := wh.Handler.(DecoderInjector); ok {
			return injector.InjectDecoder(wh.decoder)
		}
	}
	return nil
}

func (wh *Webhook) InjectFunc(f inject.Func) error {
	var setFields inject.Func
	setFields = func(target interface{}) error {
		if err := f(target); err != nil {
			return err
		}
		if _, err := inject.InjectorInto(setFields, target); err != nil {
			return err
		}
		if injector, ok := target.(DecoderInjector); ok {
			return injector.InjectDecoder(wh.decoder)
		}
		return nil
	}
	return setFields(wh.Handler)
}

type DecoderInjector interface {
	InjectDecoder(*Decoder) error
}

type Handler interface {
	Handle(context.Context, Request) Response
}

type Request struct {
	apiextensionsv1.ConversionRequest
}

type Response struct {
	apiextensionsv1.ConversionResponse
}

// Complete populates any fields that are yet to be set in
// the underlying ConversionResponse, It mutates the response.
func (r *Response) Complete(req Request) error {
	r.UID = req.UID
	if r.Result.Code == 0 {
		r.Result.Code = http.StatusOK
	}
	return nil
}

// Converted creates a new converted Response
func Converted(objects ...runtime.RawExtension) Response {
	return Response{
		ConversionResponse: apiextensionsv1.ConversionResponse{
			ConvertedObjects: objects,
			Result: metav1.Status{
				Code:   http.StatusOK,
				Status: metav1.StatusSuccess,
			},
		},
	}
}

// Errored creates a new Response for error-handling a request.
func Errored(code int32, msg string, args ...any) Response {
	return Response{
		ConversionResponse: apiextensionsv1.ConversionResponse{
			Result: metav1.Status{
				Code:    code,
				Message: fmt.Sprintf(msg, args...),
			},
		},
	}
}

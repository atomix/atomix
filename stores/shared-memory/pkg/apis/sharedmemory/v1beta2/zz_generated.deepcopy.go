//go:build !ignore_autogenerated
// +build !ignore_autogenerated

// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

// Code generated by deepcopy-gen. DO NOT EDIT.

package v1beta2

import (
	v1 "k8s.io/api/core/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *LoggerConfig) DeepCopyInto(out *LoggerConfig) {
	*out = *in
	if in.Level != nil {
		in, out := &in.Level, &out.Level
		*out = new(string)
		**out = **in
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new LoggerConfig.
func (in *LoggerConfig) DeepCopy() *LoggerConfig {
	if in == nil {
		return nil
	}
	out := new(LoggerConfig)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *LoggingConfig) DeepCopyInto(out *LoggingConfig) {
	*out = *in
	if in.Loggers != nil {
		in, out := &in.Loggers, &out.Loggers
		*out = make([]LoggerConfig, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new LoggingConfig.
func (in *LoggingConfig) DeepCopy() *LoggingConfig {
	if in == nil {
		return nil
	}
	out := new(LoggingConfig)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *SharedMemoryStore) DeepCopyInto(out *SharedMemoryStore) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	out.Status = in.Status
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new SharedMemoryStore.
func (in *SharedMemoryStore) DeepCopy() *SharedMemoryStore {
	if in == nil {
		return nil
	}
	out := new(SharedMemoryStore)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *SharedMemoryStore) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *SharedMemoryStoreList) DeepCopyInto(out *SharedMemoryStoreList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]SharedMemoryStore, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new SharedMemoryStoreList.
func (in *SharedMemoryStoreList) DeepCopy() *SharedMemoryStoreList {
	if in == nil {
		return nil
	}
	out := new(SharedMemoryStoreList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *SharedMemoryStoreList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *SharedMemoryStoreSpec) DeepCopyInto(out *SharedMemoryStoreSpec) {
	*out = *in
	if in.ImagePullSecrets != nil {
		in, out := &in.ImagePullSecrets, &out.ImagePullSecrets
		*out = make([]v1.LocalObjectReference, len(*in))
		copy(*out, *in)
	}
	if in.SecurityContext != nil {
		in, out := &in.SecurityContext, &out.SecurityContext
		*out = new(v1.SecurityContext)
		(*in).DeepCopyInto(*out)
	}
	in.Logging.DeepCopyInto(&out.Logging)
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new SharedMemoryStoreSpec.
func (in *SharedMemoryStoreSpec) DeepCopy() *SharedMemoryStoreSpec {
	if in == nil {
		return nil
	}
	out := new(SharedMemoryStoreSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *SharedMemoryStoreStatus) DeepCopyInto(out *SharedMemoryStoreStatus) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new SharedMemoryStoreStatus.
func (in *SharedMemoryStoreStatus) DeepCopy() *SharedMemoryStoreStatus {
	if in == nil {
		return nil
	}
	out := new(SharedMemoryStoreStatus)
	in.DeepCopyInto(out)
	return out
}

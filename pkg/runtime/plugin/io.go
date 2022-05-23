// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package plugin

import (
	"encoding/base64"
	"github.com/atomix/runtime/pkg/errors"
	"hash"
	"io"
	"os"
)

type Writer[T any] struct {
	plugin *Plugin[T]
	writer io.WriteCloser
	hash   hash.Hash
}

func (w *Writer[T]) Write(bytes []byte) (n int, err error) {
	if i, err := w.writer.Write(bytes); err != nil {
		return i, err
	} else {
		_, err := w.hash.Write(bytes)
		if err != nil {
			return i, err
		}
		return i, nil
	}
}

func (w *Writer[T]) Close(checksum string) error {
	defer w.plugin.lock.Unlock()
	if err := w.writer.Close(); err != nil {
		return err
	}
	sum := base64.RawURLEncoding.EncodeToString(w.hash.Sum(nil))
	if sum != checksum {
		_ = os.Remove(w.plugin.Path)
		return errors.NewFault("checksum mismatch")
	}
	return nil
}

type Reader[T any] struct {
	plugin *Plugin[T]
	reader io.ReadCloser
}

func (r *Reader[T]) Read(bytes []byte) (n int, err error) {
	return r.reader.Read(bytes)
}

func (r *Reader[T]) Close() error {
	defer r.plugin.lock.Unlock()
	return r.reader.Close()
}

var _ io.ReadCloser = (*Reader[any])(nil)

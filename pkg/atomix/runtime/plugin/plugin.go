// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package plugin

import (
	"crypto/sha256"
	"fmt"
	"github.com/gofrs/flock"
	"os"
	"path/filepath"
	"plugin"
)

func newPlugin[T any](name, version, path, symbol string) *Plugin[T] {
	return &Plugin[T]{
		Name:    name,
		Version: version,
		Path:    path,
		symbol:  symbol,
		lock:    flock.New(fmt.Sprintf("%s.lock", path)),
	}
}

type Plugin[T any] struct {
	Name    string
	Version string
	Path    string
	symbol  string
	lock    *flock.Flock
}

func (p *Plugin[T]) Create() (*Writer[T], error) {
	if err := p.lock.Lock(); err != nil {
		return nil, err
	}
	err := os.MkdirAll(filepath.Dir(p.Path), 0755)
	if err != nil {
		return nil, err
	}
	file, err := os.Create(p.Path)
	if err != nil {
		return nil, err
	}
	return &Writer[T]{
		plugin: p,
		writer: file,
		hash:   sha256.New(),
	}, nil
}

func (p *Plugin[T]) Load() (T, error) {
	p.lock.RLock()
	defer p.lock.Unlock()
	var t T
	plugin, err := plugin.Open(p.Path)
	if err != nil {
		return t, err
	}
	symbol, err := plugin.Lookup(p.symbol)
	if err != nil {
		return t, err
	}
	return symbol.(T), nil
}

func (p *Plugin[T]) Open() (*Reader[T], error) {
	if err := p.lock.RLock(); err != nil {
		return nil, err
	}
	file, err := os.Open(p.Path)
	if err != nil {
		return nil, err
	}
	return &Reader[T]{
		plugin: p,
		reader: file,
	}, nil
}

func (p *Plugin[T]) String() string {
	return getVersionedName(p.Name, p.Version)
}

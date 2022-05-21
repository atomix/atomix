// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/gofrs/flock"
	"hash"
	"io"
	"os"
	"path/filepath"
	"plugin"
	"sync"
)

const driverSymbol = "Driver"

func NewDriverCache(path string) *DriverCache {
	return &DriverCache{
		Path: path,
	}
}

type DriverCache struct {
	Path    string
	plugins map[string]*DriverPlugin
	mu      sync.RWMutex
}

func (c *DriverCache) Get(name, version string) *DriverPlugin {
	key := getVersionedName(name, version)
	c.mu.RLock()
	plugin, ok := c.plugins[key]
	c.mu.RUnlock()
	if ok {
		return plugin
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	plugin, ok = c.plugins[key]
	if ok {
		return plugin
	}

	path := filepath.Join(c.Path, fmt.Sprintf("%s@%s.so", name, version))
	plugin = newDriverPlugin(name, version, path)
	c.plugins[key] = plugin
	return plugin
}

func newDriverPlugin(name, version, path string) *DriverPlugin {
	return &DriverPlugin{
		Name:    name,
		Version: version,
		Path:    path,
		lock:    flock.New(fmt.Sprintf("%s.lock", path)),
	}
}

type DriverPlugin struct {
	Name    string
	Version string
	Path    string
	lock    *flock.Flock
}

func (p *DriverPlugin) Create() (*Writer, error) {
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
	return &Writer{
		plugin: p,
		writer: file,
		hash:   sha256.New(),
	}, nil
}

func (p *DriverPlugin) Load() (driver.Driver, error) {
	p.lock.RLock()
	defer p.lock.Unlock()
	plugin, err := plugin.Open(p.Path)
	if err != nil {
		return nil, err
	}
	symbol, err := plugin.Lookup(driverSymbol)
	if err != nil {
		return nil, err
	}
	return symbol.(driver.Driver), nil
}

func (p *DriverPlugin) Open() (*Reader, error) {
	if err := p.lock.RLock(); err != nil {
		return nil, err
	}
	file, err := os.Open(p.Path)
	if err != nil {
		return nil, err
	}
	return &Reader{
		plugin: p,
		reader: file,
	}, nil
}

func (p *DriverPlugin) String() string {
	return getVersionedName(p.Name, p.Version)
}

type Writer struct {
	plugin *DriverPlugin
	writer io.WriteCloser
	hash   hash.Hash
}

func (w *Writer) Write(bytes []byte) (n int, err error) {
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

func (w *Writer) Close(checksum string) error {
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

type Reader struct {
	plugin *DriverPlugin
	reader io.ReadCloser
}

func (r *Reader) Read(bytes []byte) (n int, err error) {
	return r.reader.Read(bytes)
}

func (r *Reader) Close() error {
	defer r.plugin.lock.Unlock()
	return r.reader.Close()
}

var _ io.ReadCloser = (*Reader)(nil)

func getVersionedName(name, version string) string {
	return fmt.Sprintf("%s/%s", name, version)
}

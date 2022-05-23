// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package plugin

import (
	"fmt"
	"path/filepath"
	"sync"
)

func NewCache[T any](path string) *Cache[T] {
	return &Cache[T]{
		Path:    path,
		plugins: make(map[string]*Plugin[T]),
	}
}

type Cache[T any] struct {
	Path    string
	plugins map[string]*Plugin[T]
	mu      sync.RWMutex
}

func (c *Cache[T]) Get(name, version string) *Plugin[T] {
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
	plugin = newPlugin[T](name, version, path)
	c.plugins[key] = plugin
	return plugin
}

func getVersionedName(name, version string) string {
	return fmt.Sprintf("%s/%s", name, version)
}

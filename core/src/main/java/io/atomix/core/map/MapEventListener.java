// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map;

import io.atomix.utils.event.EventListener;

/**
 * Listener to be notified about updates to a ConsistentMap.
 */
public interface MapEventListener<K, V> extends EventListener<MapEvent<K, V>> {
}

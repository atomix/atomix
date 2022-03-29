// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.map;

import io.atomix.utils.event.EventListener;

/**
 * Map protocol event listener.
 */
public interface MapDelegateEventListener<K, V> extends EventListener<MapDelegateEvent<K, V>> {
}

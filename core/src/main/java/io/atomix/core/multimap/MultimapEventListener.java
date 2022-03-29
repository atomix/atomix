// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.multimap;

import io.atomix.utils.event.EventListener;

/**
 * Listener to be notified about updates to a ConsistentMultimap.
 */
public interface MultimapEventListener<K, V> extends EventListener<MultimapEvent<K, V>> {
}

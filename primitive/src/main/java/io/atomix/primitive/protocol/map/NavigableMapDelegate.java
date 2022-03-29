// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.map;

import com.google.common.annotations.Beta;

import java.util.NavigableMap;

/**
 * Navigable map protocol.
 */
@Beta
public interface NavigableMapDelegate<K, V> extends SortedMapDelegate<K, V>, NavigableMap<K, V> {
}

// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.map;

import com.google.common.annotations.Beta;

import java.util.SortedMap;

/**
 * Sorted map protocol.
 */
@Beta
public interface SortedMapDelegate<K, V> extends MapDelegate<K, V>, SortedMap<K, V> {
}

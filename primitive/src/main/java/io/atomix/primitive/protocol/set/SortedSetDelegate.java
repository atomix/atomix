// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.set;

import com.google.common.annotations.Beta;

import java.util.SortedSet;

/**
 * Sorted set protocol.
 */
@Beta
public interface SortedSetDelegate<E> extends SetDelegate<E>, SortedSet<E> {
}

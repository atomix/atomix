// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set;

import java.util.SortedSet;

/**
 * Distributed sorted set.
 */
public interface DistributedSortedSet<E extends Comparable<E>> extends DistributedSet<E>, SortedSet<E> {
  @Override
  AsyncDistributedSortedSet<E> async();
}

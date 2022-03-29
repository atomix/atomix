// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set;

import java.util.NavigableSet;

/**
 * Distributed navigable set.
 */
public interface DistributedNavigableSet<E extends Comparable<E>> extends DistributedSortedSet<E>, NavigableSet<E> {
  @Override
  AsyncDistributedNavigableSet<E> async();
}

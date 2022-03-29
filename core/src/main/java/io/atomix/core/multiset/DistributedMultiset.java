// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.multiset;

import com.google.common.collect.Multiset;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.set.DistributedSet;

import java.util.Spliterator;
import java.util.Spliterators;

/**
 * Distributed multiset.
 */
public interface DistributedMultiset<E> extends DistributedCollection<E>, Multiset<E> {

  @Override
  DistributedSet<E> elementSet();

  @Override
  DistributedSet<Entry<E>> entrySet();

  @Override
  default Spliterator<E> spliterator() {
    return Spliterators.spliterator(this, 0);
  }

  @Override
  AsyncDistributedMultiset<E> async();
}

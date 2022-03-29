// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.list;

import io.atomix.core.collection.DistributedCollection;

import java.util.List;

/**
 * Distributed list.
 */
public interface DistributedList<E> extends DistributedCollection<E>, List<E> {

  @Override
  AsyncDistributedList<E> async();

}

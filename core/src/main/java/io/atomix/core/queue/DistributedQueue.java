// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.queue;

import io.atomix.core.collection.DistributedCollection;

import java.util.Queue;

/**
 * Distributed queue.
 */
public interface DistributedQueue<E> extends DistributedCollection<E>, Queue<E> {
  @Override
  AsyncDistributedQueue<E> async();
}

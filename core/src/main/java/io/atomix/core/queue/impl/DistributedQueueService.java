// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.queue.impl;

import io.atomix.core.collection.impl.DistributedCollectionService;
import io.atomix.primitive.operation.Command;

import java.util.NoSuchElementException;

/**
 * Distributed queue service.
 */
public interface DistributedQueueService extends DistributedCollectionService<String> {

  /**
   * Inserts the specified element into this queue if it is possible to do
   * so immediately without violating capacity restrictions.
   * When using a capacity-restricted queue, this method is generally
   * preferable to {@link #add}, which can fail to insert an element only
   * by throwing an exception.
   *
   * @param e the element to add
   * @return {@code true} if the element was added to this queue, else
   *         {@code false}
   * @throws ClassCastException if the class of the specified element
   *         prevents it from being added to this queue
   * @throws NullPointerException if the specified element is null and
   *         this queue does not permit null elements
   * @throws IllegalArgumentException if some property of this element
   *         prevents it from being added to this queue
   */
  @Command
  boolean offer(String e);

  /**
   * Retrieves and removes the head of this queue.  This method differs
   * from {@link #poll poll} only in that it throws an exception if this
   * queue is empty.
   *
   * @return the head of this queue
   * @throws NoSuchElementException if this queue is empty
   */
  @Command
  String remove();

  /**
   * Retrieves and removes the head of this queue,
   * or returns {@code null} if this queue is empty.
   *
   * @return the head of this queue, or {@code null} if this queue is empty
   */
  @Command
  String poll();

  /**
   * Retrieves, but does not remove, the head of this queue.  This method
   * differs from {@link #peek peek} only in that it throws an exception
   * if this queue is empty.
   *
   * @return the head of this queue
   * @throws NoSuchElementException if this queue is empty
   */
  @Command
  String element();

  /**
   * Retrieves, but does not remove, the head of this queue,
   * or returns {@code null} if this queue is empty.
   *
   * @return the head of this queue, or {@code null} if this queue is empty
   */
  @Command
  String peek();

}

// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

import java.util.List;

/**
 * Abstraction of an accumulator capable of collecting items and at some
 * point in time triggers processing of all previously accumulated items.
 *
 * @param <T> item type
 */
public interface Accumulator<T> {

  /**
   * Adds an item to the current batch. This operation may, or may not
   * trigger processing of the current batch of items.
   *
   * @param item item to be added to the current batch
   */
  void add(T item);

  /**
   * Processes the specified list of accumulated items.
   *
   * @param items list of accumulated items
   */
  void processItems(List<T> items);

  /**
   * Indicates whether the accumulator is ready to process items.
   *
   * @return true if ready to process
   */
  boolean isReady();
}

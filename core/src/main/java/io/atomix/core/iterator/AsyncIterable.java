// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.iterator;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Asynchronously iterable object.
 */
public interface AsyncIterable<T> {

  /**
   * Returns an asynchronous iterator.
   *
   * @return an asynchronous iterator
   */
  AsyncIterator<T> iterator();

  /**
   * Returns a stream.
   *
   * @return a new stream
   */
  default Stream<T> stream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator().sync(), Spliterator.ORDERED), false);
  }
}

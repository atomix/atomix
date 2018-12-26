/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.iterator.impl;

import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;

/**
 * Base interface for iterable services.
 */
public interface IterableService<E> {

  /**
   * Returns an iterator.
   *
   * @return the iterator ID
   */
  @Command
  IteratorBatch<E> iterate();

  /**
   * Returns the next batch of elements for the given iterator.
   *
   * @param iteratorId the iterator identifier
   * @param position   the iterator position
   * @return the next batch of entries for the iterator or {@code null} if the iterator is complete
   */
  @Query
  IteratorBatch<E> next(long iteratorId, int position);

  /**
   * Closes an iterator.
   *
   * @param iteratorId the iterator identifier
   */
  @Command
  void close(long iteratorId);

}

/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.collections;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous collection proxy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncCollectionProxy<T> {

  /**
   * Adds a entry to the collection.
   *
   * @param value The entry to add.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> add(T value);

  /**
   * Adds a collection of entries to the collection.
   *
   * @param values The values to add.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> addAll(Collection<? extends T> values);

  /**
   * Retains a collection of values in the collection.
   *
   * @param values The values to retain.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> retainAll(Collection<?> values);

  /**
   * Removes a entry from the collection.
   *
   * @param value The entry to remove.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> remove(Object value);

  /**
   * Removes a collection of values from the collection.
   *
   * @param values The collection of values to remove.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> removeAll(Collection<?> values);

  /**
   * Checks whether the collection contains a entry.
   *
   * @param value The entry to check.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> contains(Object value);

  /**
   * Checks whether the collection contains a collection of entries.
   *
   * @param values The collection of values to check.
   * @return A completable future to be completed with a boolean value indicating whether the collection contains
   *         the given set of values.
   */
  CompletableFuture<Boolean> containsAll(Collection<?> values);

  /**
   * Gets the current collection size.
   *
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Integer> size();

  /**
   * Checks whether the collection is empty.
   *
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> isEmpty();

  /**
   * Clears all values from the collection.
   *
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Void> clear();

}

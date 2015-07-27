/*
 * Copyright 2015 the original author or authors.
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

import net.kuujo.copycat.PersistenceLevel;
import net.kuujo.copycat.ConsistencyLevel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous set.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncSet<T> {

  /**
   * Adds a value to the set.
   *
   * @param value The value to add.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> add(T value);

  /**
   * Adds a value to the set with a TTL.
   *
   * @param value The value to add.
   * @param persistence The persistence persistence.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  CompletableFuture<Boolean> add(T value, PersistenceLevel persistence);

  /**
   * Adds a value to the set with a TTL.
   *
   * @param value The value to add.
   * @param ttl The time to live in milliseconds.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  CompletableFuture<Boolean> add(T value, long ttl);

  /**
   * Adds a value to the set with a TTL.
   *
   * @param value The value to add.
   * @param ttl The time to live.
   * @param unit The time to live unit.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  CompletableFuture<Boolean> add(T value, long ttl, TimeUnit unit);

  /**
   * Adds a value to the set with a TTL.
   *
   * @param value The value to add.
   * @param ttl The time to live in milliseconds.
   * @param persistence The persistence persistence.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  CompletableFuture<Boolean> add(T value, long ttl, PersistenceLevel persistence);

  /**
   * Adds a value to the set with a TTL.
   *
   * @param value The value to add.
   * @param ttl The time to live.
   * @param unit The time to live unit.
   * @param persistence The persistence persistence.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  CompletableFuture<Boolean> add(T value, long ttl, TimeUnit unit, PersistenceLevel persistence);

  /**
   * Removes a value from the set.
   *
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> remove(T value);

  /**
   * Checks whether the set contains a value.
   *
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> contains(Object value);

  /**
   * Checks whether the set contains a value.
   *
   * @param value The value to check.
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> contains(Object value, ConsistencyLevel consistency);

  /**
   * Gets the set size.
   *
   * @return A completable future to be completed with the set size.
   */
  CompletableFuture<Integer> size();

  /**
   * Gets the set size.
   *
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the set size.
   */
  CompletableFuture<Integer> size(ConsistencyLevel consistency);

  /**
   * Checks whether the set is empty.
   *
   * @return A completable future to be completed with a boolean value indicating whether the set is empty.
   */
  CompletableFuture<Boolean> isEmpty();

  /**
   * Checks whether the set is empty.
   *
   * @param consistency The query consistency level.
   * @return A completable future to be completed with a boolean value indicating whether the set is empty.
   */
  CompletableFuture<Boolean> isEmpty(ConsistencyLevel consistency);

  /**
   * Removes all values from the set.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  CompletableFuture<Void> clear();

}

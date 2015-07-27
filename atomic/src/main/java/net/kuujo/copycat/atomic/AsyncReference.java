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
package net.kuujo.copycat.atomic;

import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.Mode;
import net.kuujo.copycat.raft.ConsistencyLevel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous atomic reference.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncReference<T> {

  /**
   * Sets the default read consistency level.
   *
   * @param consistency The default read consistency level.
   * @throws java.lang.NullPointerException If the consistency level is {@code null}
   */
  void setDefaultConsistencyLevel(ConsistencyLevel consistency);

  /**
   * Sets the default consistency level, returning the resource for method chaining.
   *
   * @param consistency The default read consistency level.
   * @return The reference.
   * @throws java.lang.NullPointerException If the consistency level is {@code null}
   */
  DistributedReference<T> withDefaultConsistencyLevel(ConsistencyLevel consistency);

  /**
   * Returns the default consistency level.
   *
   * @return The default consistency level.
   */
  ConsistencyLevel getDefaultConsistencyLevel();

  /**
   * Gets the current value.
   *
   * @return A completable future to be completed with the current value.
   */
  CompletableFuture<T> get();

  /**
   * Gets the current value.
   *
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the current value.
   */
  CompletableFuture<T> get(ConsistencyLevel consistency);

  /**
   * Sets the current value.
   *
   * @param value The current value.
   * @return A completable future to be completed once the value has been set.
   */
  CompletableFuture<Void> set(T value);

  /**
   * Sets the value with a TTL.
   *
   * @param value The value to set.
   * @param ttl The time after which to expire the value.
   * @return A completable future to be completed once the value has been set.
   */
  CompletableFuture<Void> set(T value, long ttl);

  /**
   * Sets the value with a TTL.
   *
   * @param value The value to set.
   * @param ttl The time after which to expire the value.
   * @param unit The expiration time unit.
   * @return A completable future to be completed once the value has been set.
   */
  CompletableFuture<Void> set(T value, long ttl, TimeUnit unit);

  /**
   * Sets the value with a write mode.
   *
   * @param value The value to set.
   * @param mode The write mode.
   * @return A completable future to be completed once the value has been set.
   */
  CompletableFuture<Void> set(T value, Mode mode);

  /**
   * Sets the value with a write mode.
   *
   * @param value The value to set.
   * @param ttl The time after which to expire the value.
   * @param mode The write mode.
   * @return A completable future to be completed once the value has been set.
   */
  CompletableFuture<Void> set(T value, long ttl, Mode mode);

  /**
   * Sets the value with a write mode.
   *
   * @param value The value to set.
   * @param ttl The time after which to expire the value.
   * @param unit The expiration time unit.
   * @param mode The write mode.
   * @return A completable future to be completed once the value has been set.
   */
  CompletableFuture<Void> set(T value, long ttl, TimeUnit unit, Mode mode);

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @return A completable future to be completed with the previous value.
   */
  CompletableFuture<T> getAndSet(T value);

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param ttl The time after which to expire the value.
   * @return A completable future to be completed with the previous value.
   */
  CompletableFuture<T> getAndSet(T value, long ttl);

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param ttl The time after which to expire the value.
   * @param unit The expiration time unit.
   * @return A completable future to be completed with the previous value.
   */
  CompletableFuture<T> getAndSet(T value, long ttl, TimeUnit unit);

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param mode The write mode.
   * @return A completable future to be completed with the previous value.
   */
  CompletableFuture<T> getAndSet(T value, Mode mode);

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param ttl The time after which to expire the value.
   * @param mode The write mode.
   * @return A completable future to be completed with the previous value.
   */
  CompletableFuture<T> getAndSet(T value, long ttl, Mode mode);

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param ttl The time after which to expire the value.
   * @param unit The expiration time unit.
   * @param mode The write mode.
   * @return A completable future to be completed with the previous value.
   */
  CompletableFuture<T> getAndSet(T value, long ttl, TimeUnit unit, Mode mode);

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  CompletableFuture<Boolean> compareAndSet(T expect, T update);

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @param ttl The time after which to expire the value.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  CompletableFuture<Boolean> compareAndSet(T expect, T update, long ttl);

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @param ttl The time after which to expire the value.
   * @param unit The expiration time unit.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  CompletableFuture<Boolean> compareAndSet(T expect, T update, long ttl, TimeUnit unit);

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @param mode The write mode.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  CompletableFuture<Boolean> compareAndSet(T expect, T update, Mode mode);

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @param ttl The time after which to expire the value.
   * @param mode The write mode.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  CompletableFuture<Boolean> compareAndSet(T expect, T update, long ttl, Mode mode);

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @param ttl The time after which to expire the value.
   * @param unit The expiration time unit.
   * @param mode The write mode.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  CompletableFuture<Boolean> compareAndSet(T expect, T update, long ttl, TimeUnit unit, Mode mode);

  /**
   * Registers a change listener.
   *
   * @param listener The change listener.
   * @return A completable future to be completed once the change listener has been registered.
   */
  CompletableFuture<ListenerContext<T>> onChange(Listener<T> listener);

}

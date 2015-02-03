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

import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous atomic long proxy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncAtomicLongProxy {

  /**
   * Gets the current value.
   *
   * @return A completable future to be completed with the current value.
   */
  CompletableFuture<Long> get();

  /**
   * Sets the current value.
   *
   * @param value The current value.
   * @return A completable future to be completed once the value has been set.
   */
  CompletableFuture<Void> set(long value);

  /**
   * Adds to the current value and returns the updated value.
   *
   * @param value The value to add.
   * @return A completable future to be completed with the updated value.
   */
  CompletableFuture<Long> addAndGet(long value);

  /**
   * Gets the current value and then adds to it.
   *
   * @param value The value to add.
   * @return A completable future to be completed with the previous value.
   */
  CompletableFuture<Long> getAndAdd(long value);

  /**
   * Gets the current value and then sets it.
   *
   * @param value The value to set.
   * @return A completable future to be completed with the previous value.
   */
  CompletableFuture<Long> getAndSet(long value);

  /**
   * Gets the current value and increments it.
   *
   * @return A completable future to be completed with the previous value.
   */
  CompletableFuture<Long> getAndIncrement();

  /**
   * Gets the current value and decrements it.
   *
   * @return A completable future to be completed with the previous value.
   */
  CompletableFuture<Long> getAndDecrement();

  /**
   * Increments and returns the current value.
   *
   * @return A completable future to be completed with the updated value.
   */
  CompletableFuture<Long> incrementAndGet();

  /**
   * Decrements and returns the current value.
   *
   * @return A completable future to be completed with the updated value.
   */
  CompletableFuture<Long> decrementAndGet();

  /**
   * Compares the current value and updates the value if expected value == actual value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @return A completable future to be completed with a boolean indicating whether the value was updated.
   */
  CompletableFuture<Boolean> compareAndSet(long expect, long update);

}

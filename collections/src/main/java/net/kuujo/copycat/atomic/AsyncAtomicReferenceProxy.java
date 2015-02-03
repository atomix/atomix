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
 * Asynchronous atomic reference proxy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncAtomicReferenceProxy<T> {

  /**
   * Gets the value.
   *
   * @return A completable future to be completed with the current reference value.
   */
  CompletableFuture<T> get();

  /**
   * Sets the value.
   *
   * @param value The value to set.
   * @return A completable future to be completed once the value has been set.
   */
  CompletableFuture<Void> set(T value);

  /**
   * Sets and gets the value.
   *
   * @param value The new value to set.
   * @return A completable future to be completed with the previous value.
   */
  CompletableFuture<T> getAndSet(T value);

  /**
   * Sets the value if the current value == the expected value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @return A completable future to be completed once the operation is complete with a boolean indicating whether
   *         the updated value was set.
   */
  CompletableFuture<Boolean> compareAndSet(T expect, T update);

}

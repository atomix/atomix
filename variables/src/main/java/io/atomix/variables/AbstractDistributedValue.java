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
 * limitations under the License
 */
package io.atomix.variables;

import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.Resource;
import io.atomix.variables.state.ValueCommands;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Abstract distributed value.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@SuppressWarnings("unchecked")
public abstract class AbstractDistributedValue<T extends AbstractDistributedValue<T, U>, U> extends Resource<T> {

  protected AbstractDistributedValue(CopycatClient client) {
    super(client);
  }

  /**
   * Gets the current value.
   *
   * @return A completable future to be completed with the current value.
   */
  public CompletableFuture<U> get() {
    return submit(new ValueCommands.Get<>());
  }

  /**
   * Sets the current value.
   *
   * @param value The current value.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(U value) {
    return submit(new ValueCommands.Set(value));
  }

  /**
   * Sets the value with a TTL.
   *
   * @param value The value to set.
   * @param ttl The time after which to expire the value.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(U value, Duration ttl) {
    return submit(new ValueCommands.Set(value, ttl.toMillis()));
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<U> getAndSet(U value) {
    return submit(new ValueCommands.GetAndSet<>(value));
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param ttl The time after which to expire the value.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<U> getAndSet(U value, Duration ttl) {
    return submit(new ValueCommands.GetAndSet<>(value, ttl.toMillis()));
  }

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(U expect, U update) {
    return submit(new ValueCommands.CompareAndSet(expect, update));
  }

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @param ttl The time after which to expire the value.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(U expect, U update, Duration ttl) {
    return submit(new ValueCommands.CompareAndSet(expect, update, ttl.toMillis()));
  }

}

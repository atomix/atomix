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
package io.atomix.variables;

import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceTypeInfo;
import io.atomix.variables.state.LongCommands;
import io.atomix.variables.state.LongState;
import io.atomix.variables.state.ValueCommands;

import java.util.concurrent.CompletableFuture;

/**
 * Distributed long.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@ResourceTypeInfo(id=-2, stateMachine=LongState.class)
public class DistributedLong extends AbstractDistributedValue<DistributedLong, Resource.Options, Long> {

  public DistributedLong(CopycatClient client, Resource.Options options) {
    super(client, options);
  }

  @Override
  public CompletableFuture<Long> get() {
    return submit(new ValueCommands.Get<>());
  }

  /**
   * Adds a delta to the long and returns the updated value.
   *
   * @param delta The delta to add.
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<Long> addAndGet(long delta) {
    return submit(new LongCommands.AddAndGet(delta));
  }

  /**
   * Adds a delta to the value and returns the previous value.
   *
   * @param delta The delta to add.
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<Long> getAndAdd(long delta) {
    return submit(new LongCommands.GetAndAdd(delta));
  }

  /**
   * Increments the value and returns the updated value.
   *
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<Long> incrementAndGet() {
    return submit(new LongCommands.IncrementAndGet());
  }

  /**
   * Decrements the value and returns the updated value.
   *
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<Long> decrementAndGet() {
    return submit(new LongCommands.DecrementAndGet());
  }

  /**
   * Increments the value and returns the previous value.
   *
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<Long> getAndIncrement() {
    return submit(new LongCommands.GetAndIncrement());
  }

  /**
   * Decrements the value and returns the previous value.
   *
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<Long> getAndDecrement() {
    return submit(new LongCommands.GetAndDecrement());
  }

}

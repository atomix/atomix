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

import net.kuujo.copycat.log.CommitLog;
import net.kuujo.copycat.log.SharedCommitLog;
import net.kuujo.copycat.resource.Command;
import net.kuujo.copycat.resource.Resource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Asynchronous atomic value.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncReference<T> extends Resource<AsyncReference<T>> {
  private final AtomicReference<T> value = new AtomicReference<>();

  public AsyncReference(String name, SharedCommitLog log) {
    super(name, log);
  }

  public AsyncReference(CommitLog log) {
    super(log);
  }

  /**
   * Gets the current value.
   *
   * @return A completable future to be completed with the current value.
   */
  public CompletableFuture<T> get() {
    return submit("get", null);
  }

  @Command(value="get", type=Command.Type.READ)
  protected T applyGet() {
    return value.get();
  }

  /**
   * Sets the current value.
   *
   * @param value The current value.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(T value) {
    return submit("set", null, value);
  }

  @Command(value="set", type=Command.Type.WRITE)
  protected void applySet(T value) {
    this.value.set(value);
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value) {
    return submit("getAndSet", null, value);
  }

  @Command(value="getAndSet", type=Command.Type.WRITE)
  protected T applyGetAndSet(T value) {
    return this.value.getAndSet(value);
  }

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(T expect, T update) {
    return submit("compareAndSet", null, expect, update);
  }

  @Command(value="compareAndSet", type=Command.Type.WRITE)
  protected boolean applyCompareAndSet(T expect, T update) {
    return value.compareAndSet(expect, update);
  }

}

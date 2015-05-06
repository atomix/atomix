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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Asynchronous atomic long.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncLong extends Resource<AsyncLong> {
  private final AtomicLong value = new AtomicLong();

  public AsyncLong(String name, SharedCommitLog log) {
    super(name, log);
  }

  public AsyncLong(CommitLog log) {
    super(log);
  }

  /**
   * Gets the current value.
   *
   * @return A completable future to be completed with the current value.
   */
  public CompletableFuture<Long> get() {
    return submit("get", null);
  }

  @Command(value="get", type=Command.Type.READ)
  protected long applyGet() {
    return value.get();
  }

  /**
   * Sets the current value.
   *
   * @param value The current value.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(long value) {
    return submit("set", null, value);
  }

  @Command(value="set", type=Command.Type.WRITE)
  protected void applySet(long value) {
    this.value.set(value);
  }

  /**
   * Adds to the current value and returns the updated value.
   *
   * @param value The value to add.
   * @return A completable future to be completed with the updated value.
   */
  public CompletableFuture<Long> addAndGet(long value) {
    return submit("addAndGet", null, value);
  }

  @Command(value="addAndGet", type=Command.Type.WRITE)
  protected void applyAddAndGet(long value) {
    this.value.addAndGet(value);
  }

  /**
   * Gets the current value and then adds to it.
   *
   * @param value The value to add.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<Long> getAndAdd(long value) {
    return submit("getAndAdd", null, value);
  }

  @Command(value="getAndAdd", type=Command.Type.WRITE)
  protected void applyGetAndAdd(long value) {
    this.value.getAndAdd(value);
  }

  /**
   * Gets the current value and then sets it.
   *
   * @param value The value to set.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<Long> getAndSet(long value) {
    return submit("getAndSet", null, value);
  }

  @Command(value="getAndSet", type=Command.Type.WRITE)
  protected long applyGetAndSet(long value) {
    return this.value.getAndSet(value);
  }

  /**
   * Gets the current value and increments it.
   *
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<Long> getAndIncrement() {
    return submit("getAndIncrement", null);
  }

  @Command(value="getAndIncrement", type=Command.Type.WRITE)
  protected void applyGetAndIncrement() {
    this.value.getAndIncrement();
  }

  /**
   * Gets the current value and decrements it.
   *
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<Long> getAndDecrement() {
    return submit("getAndDecrement", null);
  }

  @Command(value="getAndDecrement", type=Command.Type.WRITE)
  protected void applyGetAndDecrement() {
    this.value.getAndDecrement();
  }

  /**
   * Increments and returns the current value.
   *
   * @return A completable future to be completed with the updated value.
   */
  public CompletableFuture<Long> incrementAndGet() {
    return submit("incrementAndGet", null);
  }

  @Command(value="incrementAndGet", type=Command.Type.WRITE)
  protected void applyIncrementAndGet() {
    this.value.incrementAndGet();
  }

  /**
   * Decrements and returns the current value.
   *
   * @return A completable future to be completed with the updated value.
   */
  public CompletableFuture<Long> decrementAndGet() {
    return submit("decrementAndGet", null);
  }

  @Command(value="decrementAndGet", type=Command.Type.WRITE)
  protected void applydecrementAndGet() {
    this.value.decrementAndGet();
  }

  /**
   * Compares the current value and updates the value if expected value == actual value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @return A completable future to be completed with a boolean indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(long expect, long update) {
    return submit("compareAndSet", null, expect, update);
  }

  @Command(value="compareAndSet", type=Command.Type.WRITE)
  protected boolean applyCompareAndSet(long expect, long update) {
    return value.compareAndSet(expect, update);
  }

}

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

import net.kuujo.copycat.log.CommitLog;
import net.kuujo.copycat.log.SharedCommitLog;
import net.kuujo.copycat.resource.Command;
import net.kuujo.copycat.resource.Resource;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous set.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <T> The set data type.
 */
public class AsyncSet<T> extends Resource<AsyncSet<T>> {
  private final Set<T> set;

  public AsyncSet(String name, SharedCommitLog log) {
    super(name, log);
    this.set = new HashSet<>();
  }

  public AsyncSet(CommitLog log) {
    super(log);
    this.set = new HashSet<>();
  }

  /**
   * Adds a entry to the set.
   *
   * @param value The entry to add.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> add(T value) {
    return submit("add", value);
  }

  @Command("add")
  protected boolean applyAdd(T value) {
    return set.add(value);
  }

  /**
   * Removes a entry from the collection.
   *
   * @param value The entry to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> remove(Object value) {
    return submit("remove", value);
  }

  @Command(value="remove", type=Command.Type.DELETE)
  protected boolean applyRemove(Object value) {
    return set.remove(value);
  }

  /**
   * Checks whether the collection contains a entry.
   *
   * @param value The entry to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> contains(Object value) {
    return submit("contains", value);
  }

  @Command(value="contains", type=Command.Type.READ)
  protected boolean applyContains(Object value) {
    return set.contains(value);
  }

}

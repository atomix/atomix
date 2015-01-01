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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.Managed;
import net.kuujo.copycat.Task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Abstract managed resource implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractManagedResource<T> implements Managed<T> {
  private final List<Task<CompletableFuture<Void>>> startupTasks = Collections.synchronizedList(new ArrayList<>());
  private final List<Task<CompletableFuture<Void>>> shutdownTasks = Collections.synchronizedList(new ArrayList<>());

  /**
   * Adds a startup task to the event log.
   *
   * @param task The startup task to add.
   * @return The Copycat context.
   */
  @SuppressWarnings("unchecked")
  public synchronized T withStartupTask(Task<CompletableFuture<Void>> task) {
    startupTasks.add(task);
    return (T) this;
  }

  /**
   * Adds a shutdown task to the event log.
   *
   * @param task The shutdown task to remove.
   * @return The Copycat context.
   */
  @SuppressWarnings("unchecked")
  public synchronized T withShutdownTask(Task<CompletableFuture<Void>> task) {
    shutdownTasks.add(task);
    return (T) this;
  }

  @Override
  @SuppressWarnings("all")
  public synchronized CompletableFuture<T> open() {
    return CompletableFuture.allOf(startupTasks.stream().map(t -> t.execute()).toArray(size -> new CompletableFuture[size])).thenApply(v -> (T) this);
  }

  @Override
  @SuppressWarnings("all")
  public synchronized CompletableFuture<Void> close() {
    return CompletableFuture.allOf(shutdownTasks.stream().map(t -> t.execute()).toArray(size -> new CompletableFuture[size]));
  }

}

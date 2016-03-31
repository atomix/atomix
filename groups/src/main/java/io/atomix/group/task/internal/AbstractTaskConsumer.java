/*
 * Copyright 2016 the original author or authors.
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
package io.atomix.group.task.internal;

import io.atomix.catalyst.util.Listener;
import io.atomix.group.task.Task;
import io.atomix.group.task.TaskConsumer;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Abstract task consumer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractTaskConsumer<T> implements TaskConsumer<T> {
  protected final String name;
  protected volatile Options options;
  protected final AbstractTaskService service;
  private volatile Listener<Task<T>> listener;

  public AbstractTaskConsumer(String name, Options options, AbstractTaskService service) {
    this.name = name;
    this.options = options;
    this.service = service;
  }

  /**
   * Returns the consumer name.
   *
   * @return The consumer name.
   */
  String name() {
    return name;
  }

  /**
   * Sets the producer options.
   */
  void setOptions(Options options) {
    this.options = options;
  }

  @Override
  public Listener<Task<T>> onTask(Consumer<Task<T>> callback) {
    listener = new ConsumerListener<>(callback);
    return listener;
  }

  /**
   * Called when a task is received.
   *
   * @param task The received task.
   * @return A completable future to be completed once the task has been processed.
   */
  public CompletableFuture<Boolean> onTask(GroupTask<T> task) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    listener.accept(task.setFuture(future));
    return future;
  }

  @Override
  public void close() {
    service.close(this);
  }

  /**
   * Message consumer listener.
   */
  private class ConsumerListener<T> implements Listener<Task<T>> {
    private final Consumer<Task<T>> callback;

    private ConsumerListener(Consumer<Task<T>> callback) {
      this.callback = callback;
    }

    @Override
    public void accept(Task<T> task) {
      callback.accept(task);
    }

    @Override
    public void close() {
      if ((Listener) listener == this) {
        AbstractTaskConsumer.this.close();
      }
    }
  }

}

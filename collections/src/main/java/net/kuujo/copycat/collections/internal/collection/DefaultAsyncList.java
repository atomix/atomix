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
package net.kuujo.copycat.collections.internal.collection;

import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.collections.AsyncList;

import java.util.concurrent.CompletableFuture;

/**
 * Default asynchronous list.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncList<T> implements AsyncList<T> {
  private final StateMachine<AsyncListState<T>> stateMachine;
  private AsyncListProxy<T> proxy;

  public DefaultAsyncList(StateMachine<AsyncListState<T>> stateMachine) {
    this.stateMachine = stateMachine;
  }

  @Override
  public String name() {
    return stateMachine.name();
  }

  @Override
  public Cluster cluster() {
    return stateMachine.cluster();
  }

  @Override
  public CopycatState state() {
    return stateMachine.state();
  }

  @Override
  public CompletableFuture<T> get(int index) {
    if (proxy == null) {
      CompletableFuture<T> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("List closed"));
      return future;
    }
    return proxy.get(index);
  }

  @Override
  public CompletableFuture<Void> set(int index, T value) {
    if (proxy == null) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("List closed"));
      return future;
    }
    return proxy.set(index, value);
  }

  @Override
  public CompletableFuture<T> remove(int index) {
    if (proxy == null) {
      CompletableFuture<T> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("List closed"));
      return future;
    }
    return proxy.remove(index);
  }

  @Override
  public CompletableFuture<Boolean> add(T value) {
    if (proxy == null) {
      CompletableFuture<Boolean> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("List closed"));
      return future;
    }
    return proxy.add(value);
  }

  @Override
  public CompletableFuture<Boolean> remove(T value) {
    if (proxy == null) {
      CompletableFuture<Boolean> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("List closed"));
      return future;
    }
    return proxy.remove(value);
  }

  @Override
  public CompletableFuture<Boolean> contains(Object value) {
    if (proxy == null) {
      CompletableFuture<Boolean> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("List closed"));
      return future;
    }
    return proxy.contains(value);
  }

  @Override
  public CompletableFuture<Integer> size() {
    if (proxy == null) {
      CompletableFuture<Integer> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("List closed"));
      return future;
    }
    return proxy.size();
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    if (proxy == null) {
      CompletableFuture<Boolean> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("List closed"));
      return future;
    }
    return proxy.isEmpty();
  }

  @Override
  public CompletableFuture<Void> clear() {
    if (proxy == null) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("List closed"));
      return future;
    }
    return proxy.clear();
  }

  @Override
  public CompletableFuture<Void> delete() {
    return stateMachine.delete();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> open() {
    return stateMachine.open().thenRun(() -> {
      this.proxy = stateMachine.createProxy(AsyncListProxy.class);
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    proxy = null;
    return stateMachine.close();
  }

}

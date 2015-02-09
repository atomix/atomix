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

import net.kuujo.copycat.resource.internal.ResourceManager;
import net.kuujo.copycat.collections.AsyncList;
import net.kuujo.copycat.collections.AsyncListProxy;
import net.kuujo.copycat.state.internal.DefaultStateMachine;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Default asynchronous list.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncList<T> extends AbstractAsyncCollection<AsyncList<T>, ListState<T>, AsyncListProxy<T>, T> implements AsyncList<T> {

  @SuppressWarnings("unchecked")
  public DefaultAsyncList(ResourceManager context) {
    super(context, new DefaultStateMachine(context, ListState.class, DefaultListState.class), AsyncListProxy.class);
  }

  @Override
  public CompletableFuture<T> get(int index) {
    return checkOpen(() -> proxy.get(index));
  }

  @Override
  public CompletableFuture<T> set(int index, T value) {
    return checkOpen(() -> proxy.set(index, value));
  }

  @Override
  public CompletableFuture<Void> add(int index, T value) {
    return checkOpen(() -> proxy.add(index, value));
  }

  @Override
  public CompletableFuture<Boolean> addAll(int index, Collection<? extends T> values) {
    return checkOpen(() -> proxy.addAll(index, values));
  }

  @Override
  public CompletableFuture<T> remove(int index) {
    return checkOpen(() -> proxy.remove(index));
  }

}

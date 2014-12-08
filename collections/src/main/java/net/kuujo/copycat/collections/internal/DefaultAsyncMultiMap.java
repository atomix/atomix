/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.collections.internal;

import net.kuujo.copycat.Coordinator;
import net.kuujo.copycat.CopycatContext;
import net.kuujo.copycat.SubmitOptions;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.collections.AsyncMultiMap;
import net.kuujo.copycat.internal.AbstractResource;
import net.kuujo.copycat.internal.util.FluentList;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Default asynchronous multi map.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncMultiMap<K, V> extends AbstractResource implements AsyncMultiMap<K, V> {

  public DefaultAsyncMultiMap(String name, Coordinator coordinator, Cluster cluster, CopycatContext context) {
    super(name, coordinator, cluster, context);
  }

  @Override
  public CompletableFuture<Boolean> put(K key, V value) {
    return context.submit(new FluentList().add("put").add(key).add(value), new SubmitOptions().withConsistent(true).withConsistent(true));
  }

  @Override
  public CompletableFuture<Collection<V>> get(K key) {
    return context.submit(new FluentList().add("get").add(key), new SubmitOptions().withConsistent(true).withConsistent(false));
  }

  @Override
  public CompletableFuture<Collection<V>> remove(K key) {
    return context.submit(new FluentList().add("remove").add(key), new SubmitOptions().withConsistent(true).withConsistent(true));
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return context.submit(new FluentList().add("remove").add(key).add(value), new SubmitOptions().withConsistent(true).withConsistent(false));
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return context.submit(new FluentList().add("containsKey").add(key), new SubmitOptions().withConsistent(true).withConsistent(false));
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V value) {
    return context.submit(new FluentList().add("containsValue").add(value), new SubmitOptions().withConsistent(true).withConsistent(false));
  }

  @Override
  public CompletableFuture<Boolean> containsEntry(K key, V value) {
    return context.submit(new FluentList().add("contains").add(key).add(value), new SubmitOptions().withConsistent(true).withConsistent(false));
  }

  @Override
  public CompletableFuture<Set<K>> keySet() {
    return context.submit(new FluentList().add("keys"), new SubmitOptions().withConsistent(true).withConsistent(false));
  }

  @Override
  public CompletableFuture<Set<Map.Entry<K, Collection<V>>>> entrySet() {
    return context.submit(new FluentList().add("entries"), new SubmitOptions().withConsistent(true).withConsistent(false));
  }

  @Override
  public CompletableFuture<Collection<V>> values() {
    return context.submit(new FluentList().add("values"), new SubmitOptions().withConsistent(true).withConsistent(false));
  }

  @Override
  public CompletableFuture<Integer> size() {
    return context.submit(new FluentList().add("size"), new SubmitOptions().withConsistent(true).withConsistent(false));
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return context.submit(new FluentList().add("empty"), new SubmitOptions().withConsistent(true).withConsistent(false));
  }

  @Override
  public CompletableFuture<Void> clear() {
    return context.submit(new FluentList().add("clear"), new SubmitOptions().withConsistent(true).withConsistent(true));
  }

}

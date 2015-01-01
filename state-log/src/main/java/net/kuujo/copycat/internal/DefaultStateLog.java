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

import net.kuujo.copycat.ResourceContext;
import net.kuujo.copycat.ResourcePartitionContext;
import net.kuujo.copycat.StateLog;
import net.kuujo.copycat.StateLogPartition;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.protocol.Consistency;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Event log implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultStateLog<T, U> extends AbstractPartitionedResource<StateLog<T, U>, StateLogPartition<U>> implements StateLog<T, U> {

  public DefaultStateLog(ResourceContext context) {
    super(context);
  }

  @Override
  protected StateLogPartition<U> createPartition(ResourcePartitionContext context) {
    return new DefaultStateLogPartition<>(context, Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-resource-" + context.name() + "-partition-" + context.config().getPartition() + "-%d")));
  }

  @Override
  public synchronized <V extends U, W> StateLog<T, U> registerCommand(String name, Function<V, W> command) {
    partitions.forEach(p -> p.registerCommand(name, command));
    return this;
  }

  @Override
  public synchronized StateLog<T, U> unregisterCommand(String name) {
    partitions.forEach(p -> p.unregisterCommand(name));
    return this;
  }

  @Override
  public synchronized <V extends U, W> StateLog<T, U> registerQuery(String name, Function<V, W> query) {
    partitions.forEach(p -> p.registerQuery(name, query));
    return this;
  }

  @Override
  public synchronized <V extends U, W> StateLog<T, U> registerQuery(String name, Function<V, W> query, Consistency consistency) {
    partitions.forEach(p -> p.registerQuery(name, query, consistency));
    return this;
  }

  @Override
  public synchronized StateLog<T, U> unregisterQuery(String name) {
    partitions.forEach(p -> p.unregisterQuery(name));
    return this;
  }

  @Override
  public synchronized StateLog<T, U> unregister(String name) {
    partitions.forEach(p -> p.unregister(name));
    return this;
  }

  @Override
  public synchronized <V> StateLog<T, U> snapshotWith(Function<Integer, V> snapshotter) {
    partitions.forEach(p -> p.snapshotWith(() -> snapshotter.apply(p.partition())));
    return this;
  }

  @Override
  public synchronized <V> StateLog<T, U> installWith(BiConsumer<Integer, V> installer) {
    partitions.forEach(p -> p.<V>installWith(snapshot -> installer.accept(p.partition(), snapshot)));
    return this;
  }

  @Override
  public <V> CompletableFuture<V> submit(String command, U entry) {
    if (partitions.size() == 1) {
      return partitions.get(0).submit(command, entry);
    }
    return partitions.get(entry.hashCode() % partitions.size()).submit(command, entry);
  }

  @Override
  public <V> CompletableFuture<V> submit(String command, T partitionKey, U entry) {
    return partitions.get(partitionKey.hashCode() % partitions.size()).submit(command, entry);
  }

}

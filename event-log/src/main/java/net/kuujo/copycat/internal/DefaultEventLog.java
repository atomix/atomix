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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.*;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

/**
 * Event log implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultEventLog<T, U> extends AbstractPartitionedResource<EventLog<T, U>, EventLogPartition<U>> implements EventLog<T, U> {

  public DefaultEventLog(ResourceContext context) {
    super(context);
  }

  @Override
  protected EventLogPartition<U> createPartition(ResourcePartitionContext context) {
    return new DefaultEventLogPartition<>(context, Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-resource-" + context.name() + "-partition-" + context.config().getPartition() + "-%d")));
  }

  @Override
  public synchronized EventLog<T, U> consumer(EventListener<U> consumer) {
    partitions.forEach(p -> p.consumer(consumer));
    return this;
  }

  @Override
  public synchronized CompletableFuture<Void> commit(U entry) {
    if (partitions.size() == 1) {
      return partitions.get(0).commit(entry).thenApply(index -> null);
    }
    return partitions.get(entry.hashCode() % partitions.size()).commit(entry).thenApply(index -> null);
  }

  @Override
  public synchronized CompletableFuture<Void> commit(T partitionKey, U entry) {
    return partitions.get(partitionKey.hashCode() % partitions.size()).commit(entry).thenApply(index -> null);
  }

}

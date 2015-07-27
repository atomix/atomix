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

import net.kuujo.copycat.PersistenceLevel;
import net.kuujo.copycat.Resource;
import net.kuujo.copycat.Stateful;
import net.kuujo.copycat.collections.state.SetCommands;
import net.kuujo.copycat.collections.state.SetState;
import net.kuujo.copycat.ConsistencyLevel;
import net.kuujo.copycat.Raft;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous set.
 *
 * @param <T> The set value type.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Stateful(SetState.class)
public class DistributedSet<T> extends Resource implements AsyncSet<T> {

  public DistributedSet(Raft protocol) {
    super(protocol);
  }

  @Override
  public CompletableFuture<Boolean> add(T value) {
    return submit(SetCommands.Add.builder()
      .withValue(value.hashCode())
      .build());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, PersistenceLevel persistence) {
    return submit(SetCommands.Add.builder()
      .withValue(value.hashCode())
      .withPersistence(persistence)
      .build());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, long ttl) {
    return submit(SetCommands.Add.builder()
      .withValue(value.hashCode())
      .withTtl(ttl)
      .build());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, long ttl, TimeUnit unit) {
    return submit(SetCommands.Add.builder()
      .withValue(value.hashCode())
      .withTtl(ttl, unit)
      .build());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, long ttl, PersistenceLevel persistence) {
    return submit(SetCommands.Add.builder()
      .withValue(value.hashCode())
      .withTtl(ttl)
      .withPersistence(persistence)
      .build());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, long ttl, TimeUnit unit, PersistenceLevel persistence) {
    return submit(SetCommands.Add.builder()
      .withValue(value.hashCode())
      .withTtl(ttl, unit)
      .withPersistence(persistence)
      .build());
  }

  @Override
  public CompletableFuture<Boolean> remove(T value) {
    return submit(SetCommands.Remove.builder()
      .withValue(value.hashCode())
      .build());
  }

  @Override
  public CompletableFuture<Boolean> contains(Object value) {
    return submit(SetCommands.Contains.builder()
      .withValue(value.hashCode())
      .build());
  }

  @Override
  public CompletableFuture<Boolean> contains(Object value, ConsistencyLevel consistency) {
    return submit(SetCommands.Contains.builder()
      .withValue(value.hashCode())
      .withConsistency(consistency)
      .build());
  }

  @Override
  public CompletableFuture<Integer> size() {
    return submit(SetCommands.Size.builder().build());
  }

  @Override
  public CompletableFuture<Integer> size(ConsistencyLevel consistency) {
    return submit(SetCommands.Size.builder().withConsistency(consistency).build());
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return submit(SetCommands.IsEmpty.builder().build());
  }

  @Override
  public CompletableFuture<Boolean> isEmpty(ConsistencyLevel consistency) {
    return submit(SetCommands.IsEmpty.builder().withConsistency(consistency).build());
  }

  @Override
  public CompletableFuture<Void> clear() {
    return submit(SetCommands.Clear.builder().build());
  }

}

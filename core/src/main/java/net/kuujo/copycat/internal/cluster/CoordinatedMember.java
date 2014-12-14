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
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.Task;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.coordinator.MemberCoordinator;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.concurrent.CompletableFuture;

/**
 * Internal cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedMember implements Member {
  protected final int id;
  private final MemberCoordinator coordinator;
  protected final ExecutionContext context;

  protected CoordinatedMember(int id, MemberCoordinator coordinator, ExecutionContext context) {
    this.id = id;
    this.coordinator = coordinator;
    this.context = context;
  }

  @Override
  public String uri() {
    return coordinator.uri();
  }

  @Override
  public State state() {
    return null;
  }

  @Override
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    return coordinator.send(topic, id, message);
  }

  @Override
  public CompletableFuture<Void> execute(Task<Void> task) {
    return coordinator.execute(id, task);
  }

  @Override
  public <T> CompletableFuture<T> submit(Task<T> task) {
    return coordinator.submit(id, task);
  }

}

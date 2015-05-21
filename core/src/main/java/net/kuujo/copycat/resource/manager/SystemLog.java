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
package net.kuujo.copycat.resource.manager;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.resource.*;

import java.util.concurrent.CompletableFuture;

/**
 * Resource commit log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SystemLog implements CommitLog {
  private final StateMachineProxy stateMachine;
  private final Protocol protocol;

  public SystemLog(StateMachine stateMachine, Protocol protocol) {
    this.stateMachine = new StateMachineProxy(stateMachine);
    this.protocol = protocol;
  }

  @Override
  public Cluster cluster() {
    return protocol.cluster();
  }

  /**
   * Filters an entry.
   *
   * @param index The entry index.
   * @param timestamp The entry timestamp.
   * @param entry The entry.
   * @return Indicates whether to filter the entry.
   */
  @SuppressWarnings("unchecked")
  private boolean filter(long index, long timestamp, Object entry) {
    return stateMachine.filter(new Commit(index, timestamp, (Command) entry));
  }

  /**
   * Handles a commit.
   *
   * @param index The commit index.
   * @param timestamp The commit timestamp.
   * @param entry The commit entry.
   * @return The commit object.
   */
  @SuppressWarnings("unchecked")
  private Object handle(long index, long timestamp, Object entry) {
    return stateMachine.apply(new Commit(index, timestamp, (Command) entry));
  }

  @Override
  public <R> CompletableFuture<R> submit(Command<R> command) {
    return protocol.submit(command, command.persistence(), command.consistency());
  }

  @Override
  public CompletableFuture<CommitLog> open() {
    return protocol.open().thenApply(v -> {
      protocol.setFilter(this::filter);
      protocol.setHandler(this::handle);
      return this;
    });
  }

  @Override
  public boolean isOpen() {
    return protocol.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return protocol.close();
  }

  @Override
  public boolean isClosed() {
    return protocol.isClosed();
  }

}

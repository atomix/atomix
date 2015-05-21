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
package net.kuujo.copycat.resource;

import net.kuujo.copycat.cluster.Cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Resource commit log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceLog implements CommitLog {
  private final ThreadLocal<Map<Class, Command>> commands = new ThreadLocal<Map<Class, Command>>() {
    @Override
    protected Map<Class, Command> initialValue() {
      return new HashMap<>();
    }
  };
  private final long resource;
  private final CommitLog log;
  private boolean open;

  public ResourceLog(long resource, CommitLog log) {
    this.resource = resource;
    this.log = log;
  }

  @Override
  public Cluster cluster() {
    return log.cluster();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <R> CompletableFuture<R> submit(Command<R> command) {
    if (!open)
      throw new IllegalStateException("log not open");
    return log.submit(command.setResource(resource));
  }

  /**
   * Submits a command to the log.
   *
   * @param type The command type.
   * @param processor The command processor.
   * @return A completable future to be completed with the command result.
   */
  @SuppressWarnings("unchecked")
  public <T extends Command<R>, R> CompletableFuture<R> submit(CommandType<T> type, CommandProcessor<T> processor) {
    if (!open)
      throw new IllegalStateException("log not open");

    Map<Class, Command> commands = this.commands.get();
    Command command = commands.get(type.type());
    if (command == null) {
      command = type.createCommand();
      commands.put(type.type(), command);
    }

    processor.process((T) command);
    return submit(command);
  }

  /**
   * Submits a command to the log.
   *
   * @param type The command type.
   * @param processor The command processor.
   * @return A completable future to be completed with the command result.
   */
  @SuppressWarnings("unchecked")
  public <T extends Command<R>, R> CompletableFuture<R> submit(Class<T> type, CommandProcessor<T> processor) {
    if (!open)
      throw new IllegalStateException("log not open");

    Map<Class, Command> commands = this.commands.get();
    Command command = commands.get(type);
    if (command == null) {
      try {
        command = type.newInstance();
        commands.put(type, command);
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IllegalArgumentException("failed to instantiate command: " + type, e);
      }
    }

    processor.process((T) command);
    return submit(command);
  }

  @Override
  public CompletableFuture<CommitLog> open() {
    open = true;
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    open = false;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open;
  }
}

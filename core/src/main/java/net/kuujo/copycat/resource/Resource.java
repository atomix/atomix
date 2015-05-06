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

import net.kuujo.copycat.log.CommitLog;
import net.kuujo.copycat.log.ResourceCommitLog;
import net.kuujo.copycat.log.SharedCommitLog;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.protocol.Persistence;
import net.kuujo.copycat.util.Managed;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Resource.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Resource<T extends Resource<T>> implements Managed<T> {
  private final CommitLog log;
  private final Map<Method, String> nameCache = new ConcurrentHashMap<>();
  private final Map<String, CommandHolder> commands = new ConcurrentHashMap<>();
  private Consistency readConsistency = Consistency.LEASE;

  @SuppressWarnings("unchecked")
  protected Resource(String name, SharedCommitLog log) {
    this(new ResourceCommitLog(name, log));
  }

  @SuppressWarnings("unchecked")
  protected Resource(CommitLog log) {
    this.log = log;
    findCommands();
    log.handler(this::commit);
  }

  /**
   * Sets the resource read consistency.
   *
   * @param consistency The resource read consistency.
   * @return The resource.
   */
  @SuppressWarnings("unchecked")
  public T setConsistency(Consistency consistency) {
    if (consistency == null)
      throw new NullPointerException("consistency cannot be null");
    this.readConsistency = consistency;
    return (T) this;
  }

  /**
   * Returns the resource read consistency.
   *
   * @return The resource read consistency.
   */
  public Consistency getConsistency() {
    return readConsistency;
  }

  /**
   * Finds internal command methods.
   */
  private void findCommands() {
    for (Method method : getClass().getMethods()) {
      Command command = method.getAnnotation(Command.class);
      if (command != null) {
        String name = nameCache.computeIfAbsent(method, m -> m.getName() + "(" + String.join(",", Arrays.asList(m.getParameterTypes()).stream().map(Class::getCanonicalName).collect(Collectors
          .toList())) + ")");
        commands.put(name, new CommandHolder(command, method));
      }
    }
  }

  /**
   * Handles a commit log commit.
   */
  private Object commit(long index, Object key, Object entry) {
    Commit commit = (Commit) entry;
    CommandHolder command = commands.get(commit.method);
    if (command != null) {
      Object[] keyedArgs = new Object[(commit.args != null ? commit.args.length : 0) + 1];
      keyedArgs[0] = key;
      if (commit.args != null)
        System.arraycopy(commit.args, 0, keyedArgs, 0, commit.args.length);
      return commit(index, commit.method, command.method, keyedArgs);
    }
    throw new UnsupportedOperationException();
  }

  /**
   * Commits a set of arguments.
   */
  protected Object commit(long index, String command, Method method, Object[] args) {
    try {
      return method.invoke(this, args);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new UnsupportedOperationException("failed to apply command", e);
    }
  }

  /**
   * Submits a command to the resource's commit log.
   */
  @SuppressWarnings("unchecked")
  protected <RESULT> CompletableFuture<RESULT> submit(String commandName, Object key, Object... args) {
    CommandHolder command = commands.get(commandName);
    if (command == null)
      throw new IllegalArgumentException("invalid command: " + commandName);

    switch (command.command.type()) {
      case WRITE:
        return log.commit(key, new Commit(command.command.value(), args), Persistence.PERSISTENT, Consistency.STRICT);
      case DELETE:
        return log.commit(key, new Commit(command.command.value(), args), Persistence.DURABLE, Consistency.STRICT);
      case READ:
        return log.commit(key, new Commit(command.command.value(), args), Persistence.NONE, readConsistency);
    }

    throw new IllegalStateException("unknown command type");
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> open() {
    return log.open().thenApply(v -> (T) this);
  }

  @Override
  public boolean isOpen() {
    return log.isOpen();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> close() {
    return log.close();
  }

  @Override
  public boolean isClosed() {
    return log.isClosed();
  }

  /**
   * Resource commit.
   */
  protected static class Commit implements Serializable {
    private String method;
    private Object[] args;

    private Commit(String method, Object[] args) {
      this.method = method;
      this.args = args;
    }
  }

  /**
   * Command holder.
   */
  private static class CommandHolder {
    private final Command command;
    private final Method method;

    private CommandHolder(Command command, Method method) {
      this.command = command;
      this.method = method;
    }
  }

}

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

import net.kuujo.copycat.*;
import net.kuujo.copycat.cluster.Cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Event log implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultStateLog<T> extends AbstractResource implements StateLog<T> {
  private final Map<String, CommandInfo> commands = new HashMap<>();
  private Supplier<byte[]> snapshotter;
  private Consumer<byte[]> installer;
  private boolean open;

  public DefaultStateLog(String name, Coordinator coordinator, Cluster cluster, CopycatContext context) {
    super(name, coordinator, cluster, context);
  }

  @Override
  public <U> StateLog<T> register(String name, Command<T, U> command) {
    return register(name, command, new CommandOptions());
  }

  @Override
  public <U> StateLog<T> register(String name, Command<T, U> command, CommandOptions options) {
    if (open) {
      throw new IllegalStateException("Cannot register command on open state log");
    }
    commands.put(name, new CommandInfo(name, command, options));
    return this;
  }

  @Override
  public StateLog<T> unregister(String name) {
    if (open) {
      throw new IllegalStateException("Cannot unregister command on open state log");
    }
    commands.remove(name);
    return this;
  }

  @Override
  public StateLog<T> snapshotter(Supplier<byte[]> snapshotter) {
    this.snapshotter = snapshotter;
    return this;
  }

  @Override
  public StateLog<T> installer(Consumer<byte[]> installer) {
    this.installer = installer;
    return this;
  }

  @Override
  public <U> CompletableFuture<U> submit(String command, T entry) {
    return context.submit(command, entry);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> open() {
    for (CommandInfo command : commands.values()) {
      context.register(command.name, (index, entry) -> command.command.execute(entry), new ActionOptions()
        .withConsistent(true)
        .withPersistent(command.options.isReadOnly()));
    }
    open = true;
    return super.open().whenComplete((result, error) -> {
      if (error != null) {
        open = false;
      }
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    open = false;
    for (String command : commands.keySet()) {
      context.unregister(command);
    }
    return super.close();
  }

  /**
   * State command info.
   */
  @SuppressWarnings("rawtypes")
  private static class CommandInfo {
    private final String name;
    private final Command command;
    private final CommandOptions options;

    private CommandInfo(String name, Command command, CommandOptions options) {
      this.name = name;
      this.command = command;
      this.options = options;
    }
  }

}

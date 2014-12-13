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
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.log.LogConfig;

import java.nio.ByteBuffer;
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
public class DefaultStateLog extends AbstractCopycatResource implements StateLog {
  private final Map<String, CommandInfo> commands = new HashMap<>();
  private final StateLogConfig config;
  private Supplier<ByteBuffer> snapshotter;
  private Consumer<ByteBuffer> installer;
  private long commitIndex;
  private boolean open;

  public DefaultStateLog(String name, CopycatCoordinator coordinator, StateLogConfig config) {
    super(name, coordinator, resource -> new BufferedLog(resource, new LogConfig()
      .withDirectory(config.getDirectory())
      .withSegmentSize(config.getSegmentSize())
      .withSegmentInterval(config.getSegmentInterval())
      .withFlushOnWrite(config.isFlushOnWrite())
      .withFlushInterval(config.getFlushInterval())));
    this.config = config;
  }

  @Override
  public StateLog register(String name, Command command) {
    return register(name, command, new CommandOptions());
  }

  @Override
  public StateLog register(String name, Command command, CommandOptions options) {
    Assert.state(open, "Cannot register command on open state log");
    commands.put(name, new CommandInfo(name, command, options));
    return this;
  }

  @Override
  public StateLog unregister(String name) {
    Assert.state(open, "Cannot unregister command on open state log");
    commands.remove(name);
    return this;
  }

  @Override
  public StateLog snapshotter(Supplier<ByteBuffer> snapshotter) {
    Assert.state(open, "Cannot modify state log once opened");
    this.snapshotter = snapshotter;
    return this;
  }

  @Override
  public StateLog installer(Consumer<ByteBuffer> installer) {
    Assert.state(open, "Cannot modify state log once opened");
    this.installer = installer;
    return this;
  }

  @Override
  public CompletableFuture<ByteBuffer> submit(String command, ByteBuffer entry) {
    Assert.state(open, "State log not open");
    return context.submit(command, entry);
  }

  /**
   * Checks whether to take a snapshot.
   */
  private void checkSnapshot() {
    if (context.log().size() > config.getMaxSize() || context.log().segments().size() > config.getMaxSegments()) {
      takeSnapshot();
    }
  }

  /**
   * Takes a snapshot and compacts the log.
   */
  private void takeSnapshot() {
    if (snapshotter != null) {
      ByteBuffer snapshot = snapshotter.get();
      context.log().compact(commitIndex, snapshot);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> open() {
    for (CommandInfo command : commands.values()) {
      context.register(command.name, command::execute, new ActionOptions()
        .withConsistent(true)
        .withPersistent(command.options.isReadOnly()));
    }
    open = true;
    return super.open().whenComplete((result, error) -> {
      if (error != null) {
        open = false;
      } else {
        commitIndex = context.log().firstIndex() - 1;
        takeSnapshot();
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
  private class CommandInfo {
    private final String name;
    private final Command command;
    private final CommandOptions options;

    private CommandInfo(String name, Command command, CommandOptions options) {
      this.name = name;
      this.command = command;
      this.options = options;
    }

    @SuppressWarnings("unchecked")
    private ByteBuffer execute(Long index, ByteBuffer entry) {
      ByteBuffer result = command.execute(entry);
      commitIndex++;
      checkSnapshot();
      return result;
    }
  }

}

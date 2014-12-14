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
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.util.serializer.Serializer;

import java.io.Serializable;
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
public class DefaultStateLog<T> extends AbstractCopycatResource implements StateLog<T> {
  private final Serializer serializer;
  private final ExecutionContext executor;
  private final Map<Integer, CommandInfo> commands = new HashMap<>();
  private final StateLogConfig config;
  private Supplier snapshotter;
  private Consumer installer;
  private long commitIndex;
  private boolean open;

  public DefaultStateLog(String name, CopycatCoordinator coordinator, StateLogConfig config, ExecutionContext context) {
    super(name, coordinator, resource -> new BufferedLog(resource, new LogConfig()
      .withDirectory(config.getDirectory())
      .withSegmentSize(config.getSegmentSize())
      .withSegmentInterval(config.getSegmentInterval())
      .withFlushOnWrite(config.isFlushOnWrite())
      .withFlushInterval(config.getFlushInterval())));
    this.serializer = config.getSerializer();
    this.executor = context;
    this.config = config;
  }

  @Override
  public <U extends T, V> StateLog<T> register(String name, Command<U, V> command) {
    return register(name, command, new CommandOptions());
  }

  @Override
  public <U extends T, V> StateLog<T> register(String name, Command<U, V> command, CommandOptions options) {
    Assert.state(open, "Cannot register command on open state log");
    commands.put(name.hashCode(), new CommandInfo(name, command, options));
    return this;
  }

  @Override
  public StateLog<T> unregister(String name) {
    Assert.state(open, "Cannot unregister command on open state log");
    commands.remove(name.hashCode());
    return this;
  }

  @Override
  public <U> StateLog<T> snapshotter(Supplier<U> snapshotter) {
    Assert.state(open, "Cannot modify state log once opened");
    this.snapshotter = snapshotter;
    return this;
  }

  @Override
  public <U> StateLog<T> installer(Consumer<U> installer) {
    Assert.state(open, "Cannot modify state log once opened");
    this.installer = installer;
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <U> CompletableFuture<U> submit(String command, T entry) {
    Assert.state(open, "State log not open");
    CommandInfo<T, U> commandInfo = commands.get(command.hashCode());
    if (commandInfo == null) {
      CompletableFuture<U> future = new CompletableFuture<>();
      future.completeExceptionally(new CopycatException(String.format("Invalid state log command %s", command)));
      return future;
    }
    if (commandInfo.options.isReadOnly()) {
      if (commandInfo.options.isConsistent()) {
        CompletableFuture<U> future = new CompletableFuture<>();
        cluster.leader().<T, U>send("query", entry).whenCompleteAsync((result, error) -> {
          if (error == null) {
            future.complete(result);
          } else {
            future.completeExceptionally(error);
          }
        }, executor);
        return future;
      } else {
        return CompletableFuture.completedFuture((U) commandInfo.command.execute(entry));
      }
    } else {
      ByteBuffer buffer = serializer.writeObject(entry);
      ByteBuffer commandEntry = ByteBuffer.allocateDirect(8 + buffer.capacity());
      commandEntry.putInt(1); // Entry type
      commandEntry.putInt(command.hashCode());
      commandEntry.put(buffer);
      return context.commit(commandEntry).thenApplyAsync(serializer::readObject, executor);
    }
  }

  /**
   * Consumes a log entry.
   *
   * @param index The entry index.
   * @param entry The log entry.
   * @return The entry output.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private ByteBuffer consume(Long index, ByteBuffer entry) {
    int entryType = entry.getInt();
    switch (entryType) {
      case 0: // Snapshot entry
        installSnapshot(entry.slice());
        return ByteBuffer.allocate(0);
      case 1: // Command entry
        int commandCode = entry.getInt();
        CommandInfo commandInfo = commands.get(commandCode);
        if (commandInfo != null) {
          commandInfo.execute(index, serializer.readObject(entry.slice()));
        }
        return ByteBuffer.allocate(0);
      default:
        throw new IllegalArgumentException("Invalid entry type");
    }
  }

  /**
   * Queries the state log.
   *
   * @param query The query with which to query state.
   * @return The query result.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private CompletableFuture<Object> query(Query query) {
    CommandInfo commandInfo = commands.get(query.command.hashCode());
    if (commandInfo == null) {
      CompletableFuture<Object> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("Invalid command " + query.command));
      return future;
    }
    return context.sync().thenApply(v -> commandInfo.command.execute(query.entry));
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
      context.log().compact(commitIndex, serializer.writeObject(snapshotter.get()));
    }
  }

  /**
   * Installs a snapshot.
   */
  @SuppressWarnings("unchecked")
  private void installSnapshot(ByteBuffer snapshot) {
    if (installer != null) {
      installer.accept(serializer.readObject(snapshot));
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> open() {
    context.consumer(this::consume);
    open = true;
    return super.open().whenComplete((result, error) -> {
      if (error != null) {
        open = false;
      } else {
        cluster.localMember().handler("query", this::query);
        commitIndex = context.log().firstIndex() - 1;
        takeSnapshot();
      }
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    open = false;
    context.consumer(null);
    cluster.localMember().handler("query", null);
    return super.close();
  }

  /**
   * Query sent between followers and leaders.
   */
  private class Query implements Serializable {
    private final String command;
    private final Object entry;
    private Query() {
      command = null;
      entry = null;
    }
    private Query(String command, Object entry) {
      this.command = command;
      this.entry = entry;
    }
  }

  /**
   * State command info.
   */
  @SuppressWarnings("rawtypes")
  private class CommandInfo<T, U> {
    private final String name;
    private final Command<T, U> command;
    private final CommandOptions options;

    private CommandInfo(String name, Command<T, U> command, CommandOptions options) {
      this.name = name;
      this.command = command;
      this.options = options;
    }

    @SuppressWarnings("unchecked")
    private U execute(Long index, T entry) {
      U result = command.execute(entry);
      commitIndex++;
      checkSnapshot();
      return result;
    }
  }

}

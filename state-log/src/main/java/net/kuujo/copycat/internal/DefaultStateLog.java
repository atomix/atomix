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

import net.kuujo.copycat.CopycatContext;
import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.StateLog;
import net.kuujo.copycat.StateLogConfig;
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.util.serializer.Serializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Event log implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultStateLog<T> extends AbstractCopycatResource<StateLog<T>> implements StateLog<T> {
  private final Serializer serializer;
  private final ExecutionContext executor;
  private final Map<Integer, OperationInfo> operations = new HashMap<>();
  private final StateLogConfig config;
  private Supplier snapshotter;
  private Consumer installer;
  private long commitIndex;
  private boolean open;

  public DefaultStateLog(String name, CopycatContext context, ClusterCoordinator coordinator, StateLogConfig config, ExecutionContext executor) {
    super(name, context, coordinator, executor);
    context.log().config()
      .withSegmentSize(config.getSegmentSize())
      .withSegmentInterval(config.getSegmentInterval())
      .withFlushOnWrite(config.isFlushOnWrite())
      .withFlushInterval(config.getFlushInterval());
    this.serializer = config.getSerializer();
    this.executor = executor;
    this.config = config;
  }

  @Override
  public <U extends T, V> StateLog<T> registerCommand(String name, Function<U, V> command) {
    Assert.state(!open, "Cannot register command on open state log");
    operations.put(name.hashCode(), new OperationInfo(name, command, false));
    return this;
  }

  @Override
  public StateLog<T> unregisterCommand(String name) {
    Assert.state(!open, "Cannot unregister command on open state log");
    operations.remove(name);
    return this;
  }

  @Override
  public <U extends T, V> StateLog<T> registerQuery(String name, Function<U, V> query) {
    Assert.state(!open, "Cannot register command on open state log");
    operations.put(name.hashCode(), new OperationInfo(name, query, true));
    return this;
  }

  @Override
  public <U extends T, V> StateLog<T> registerQuery(String name, Function<U, V> query, Consistency consistency) {
    Assert.state(!open, "Cannot register command on open state log");
    operations.put(name.hashCode(), new OperationInfo(name, query, true, consistency));
    return this;
  }

  @Override
  public StateLog<T> unregisterQuery(String name) {
    return null;
  }

  @Override
  public StateLog<T> unregister(String name) {
    Assert.state(!open, "Cannot unregister command on open state log");
    operations.remove(name.hashCode());
    return this;
  }

  @Override
  public <U> StateLog<T> takeSnapshotWith(Supplier<U> snapshotter) {
    Assert.state(!open, "Cannot modify state log once opened");
    this.snapshotter = snapshotter;
    return this;
  }

  @Override
  public <U> StateLog<T> installSnapshotWith(Consumer<U> installer) {
    Assert.state(!open, "Cannot modify state log once opened");
    this.installer = installer;
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <U> CompletableFuture<U> submit(String command, T entry) {
    Assert.state(open, "State log not open");
    OperationInfo<T, U> operationInfo = operations.get(command.hashCode());
    if (operationInfo == null) {
      CompletableFuture<U> future = new CompletableFuture<>();
      executor.execute(() -> future.completeExceptionally(new CopycatException(String.format("Invalid state log command %s", command))));
      return future;
    }

    // If this is a read-only command, check if the command is consistent. For consistent operations,
    // queries are forwarded to the current cluster leader for evaluation. Otherwise, it's safe to
    // read stale data from the local node.
    if (operationInfo.readOnly) {
      ByteBuffer buffer = serializer.writeObject(entry);
      ByteBuffer syncEntry = ByteBuffer.allocate(8 + buffer.capacity());
      syncEntry.putInt(1); // Entry type
      syncEntry.putInt(command.hashCode());
      syncEntry.put(buffer);
      syncEntry.rewind();
      return context.query(syncEntry, operationInfo.consistency).thenApplyAsync(serializer::readObject, executor);
    } else {
      // Write operations are always submitted to the context which will forward it to the cluster leader.
      ByteBuffer buffer = serializer.writeObject(entry);
      ByteBuffer commandEntry = ByteBuffer.allocate(8 + buffer.capacity());
      commandEntry.putInt(1); // Entry type
      commandEntry.putInt(command.hashCode());
      commandEntry.put(buffer);
      commandEntry.rewind();
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
        OperationInfo operationInfo = operations.get(commandCode);
        if (operationInfo != null) {
          return serializer.writeObject(operationInfo.execute(index, serializer.readObject(entry.slice())));
        }
        return ByteBuffer.allocate(0);
      default:
        throw new IllegalArgumentException("Invalid entry type");
    }
  }

  /**
   * Checks whether to take a snapshot.
   */
  private void checkSnapshot() {
    if (context.log().size() > config.getMaxSize()) {
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
  public CompletableFuture<Void> open() {
    context.consumer(this::consume);
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
    context.consumer(null);
    return super.close();
  }

  /**
   * State command info.
   */
  private class OperationInfo<TT, U> {
    private final String name;
    private final Function<TT, U> function;
    private final boolean readOnly;
    private final Consistency consistency;

    private OperationInfo(String name, Function<TT, U> function, boolean readOnly) {
      this(name, function, readOnly, Consistency.DEFAULT);
    }

    private OperationInfo(String name, Function<TT, U> function, boolean readOnly, Consistency consistency) {
      this.name = name;
      this.function = function;
      this.readOnly = readOnly;
      this.consistency = consistency;
    }

    private U execute(Long index, TT entry) {
      U result = function.apply(entry);
      commitIndex = index;
      checkSnapshot();
      return result;
    }
  }

}

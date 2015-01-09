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

import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.ResourceContext;
import net.kuujo.copycat.StateLog;
import net.kuujo.copycat.StateLogConfig;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.internal.util.concurrent.Futures;
import net.kuujo.copycat.protocol.Consistency;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Default state log partition implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SuppressWarnings("rawtypes")
public class DefaultStateLog<T> extends AbstractResource<StateLog<T>> implements StateLog<T> {
  private final Map<Integer, OperationInfo> operations = new ConcurrentHashMap<>(128);
  private Supplier snapshotter;
  private Consumer installer;
  private long commitIndex;

  public DefaultStateLog(ResourceContext context) {
    super(context);
    context.consumer(this::consume);
  }

  @Override
  public <U extends T, V> StateLog<T> registerCommand(String name, Function<U, V> command) {
    Assert.state(isClosed(), "Cannot register command on open state log");
    operations.put(name.hashCode(), new OperationInfo<>(name, command, false));
    return this;
  }

  @Override
  public StateLog<T> unregisterCommand(String name) {
    Assert.state(isClosed(), "Cannot unregister command on open state log");
    operations.remove(name.hashCode());
    return this;
  }

  @Override
  public <U extends T, V> StateLog<T> registerQuery(String name, Function<U, V> query) {
    Assert.state(isClosed(), "Cannot register command on open state log");
    operations.put(name.hashCode(), new OperationInfo<>(name, query, true, context.config()
      .<StateLogConfig>getResourceConfig()
      .getDefaultConsistency()));
    return this;
  }

  @Override
  public <U extends T, V> StateLog<T> registerQuery(String name, Function<U, V> query, Consistency consistency) {
    Assert.state(isClosed(), "Cannot register command on open state log");
    operations.put(name.hashCode(), new OperationInfo<>(name, query, true, consistency));
    return this;
  }

  @Override
  public StateLog<T> unregisterQuery(String name) {
    Assert.state(isClosed(), "Cannot unregister command on open state log");
    operations.remove(name.hashCode());
    return this;
  }

  @Override
  public StateLog<T> unregister(String name) {
    Assert.state(isClosed(), "Cannot unregister command on open state log");
    operations.remove(name.hashCode());
    return this;
  }

  @Override
  public <V> StateLog<T> snapshotWith(Supplier<V> snapshotter) {
    Assert.state(isClosed(), "Cannot modify state log once opened");
    this.snapshotter = snapshotter;
    return this;
  }

  @Override
  public <V> StateLog<T> installWith(Consumer<V> installer) {
    Assert.state(isClosed(), "Cannot modify state log once opened");
    this.installer = installer;
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <U> CompletableFuture<U> submit(String command, T entry) {
    Assert.state(isOpen(), "State log not open");
    OperationInfo<T, U> operationInfo = operations.get(command.hashCode());
    if (operationInfo == null) {
      return Futures.exceptionalFutureAsync(new CopycatException(String.format("Invalid state log command %s", command)), executor);
    }

    // If this is a read-only command, check if the command is consistent. For consistent operations,
    // queries are forwarded to the current cluster leader for evaluation. Otherwise, it's safe to
    // read stale data from the local node.
    ByteBuffer buffer = serializer.writeObject(entry);
    ByteBuffer commandEntry = ByteBuffer.allocate(8 + buffer.capacity());
    commandEntry.putInt(1); // Entry type
    commandEntry.putInt(command.hashCode());
    commandEntry.put(buffer);
    commandEntry.rewind();
    if (operationInfo.readOnly) {
      return context.query(commandEntry, operationInfo.consistency).thenApplyAsync(serializer::readObject, executor);
    } else {
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
          T value = serializer.readObject(entry.slice());
          return serializer.writeObject(executeInUserThread(() -> operationInfo.execute(index, value)));
        }
        throw new IllegalStateException("Invalid state log operation");
      default:
        throw new IllegalArgumentException("Invalid entry type");
    }
  }

  /**
   * Executes a callable in the user's thread.
   */
  @SuppressWarnings("unchecked")
  private <U> U executeInUserThread(Callable<U> callable) {
    AtomicBoolean complete = new AtomicBoolean();
    AtomicReference<Object> reference = new AtomicReference<>();

    executor.execute(() -> {
      synchronized (reference) {
        try {
          reference.set(callable.call());
          complete.set(true);
          reference.notify();
        } catch (Exception e) {
          reference.set(e);
          complete.set(true);
          reference.notify();
        }
      }
    });

    synchronized (reference) {
      try {
        while (!complete.get()) {
          reference.wait(1000);
        }
      } catch (InterruptedException e) {
        throw new CopycatException(e);
      }
    }

    Object result = reference.get();
    if (result instanceof Throwable) {
      throw new CopycatException((Throwable) result);
    }
    return (U) result;
  }

  /**
   * Checks whether to take a snapshot.
   */
  private void checkSnapshot() {
    if (context.log().size() > context.config().getResourceConfig().getLog().getSegmentSize()) {
      takeSnapshot();
    }
  }

  /**
   * Takes a snapshot and compacts the log.
   */
  private void takeSnapshot() {
    Object snapshot = snapshotter != null ? executeInUserThread(snapshotter::get) : null;
    context.log().compact(commitIndex, serializer.writeObject(snapshot));
  }

  /**
   * Installs a snapshot.
   */
  @SuppressWarnings("unchecked")
  private void installSnapshot(ByteBuffer snapshot) {
    if (installer != null) {
      Object value = serializer.readObject(snapshot);
      executeInUserThread(() -> {
        installer.accept(value);
        return null;
      });
    }
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
      if (index != null) {
        commitIndex = index;
      }
      checkSnapshot();
      return result;
    }
  }

}

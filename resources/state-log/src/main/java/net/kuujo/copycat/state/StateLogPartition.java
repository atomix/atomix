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
package net.kuujo.copycat.state;

import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.HeapBuffer;
import net.kuujo.copycat.io.HeapBufferPool;
import net.kuujo.copycat.io.util.HashFunctions;
import net.kuujo.copycat.io.util.ReferencePool;
import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.resource.PartitionContext;
import net.kuujo.copycat.resource.internal.AbstractPartition;
import net.kuujo.copycat.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Copycat state log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateLogPartition<K, V> extends AbstractPartition<StateLogPartition<K, V>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(StateLogPartition.class);
  private final Buffer EMPTY_BUFFER = HeapBuffer.allocate(0);
  private final ReferencePool<Buffer> bufferPool = new HeapBufferPool();
  private final Map<Long, OperationInfo> operations = new ConcurrentHashMap<>(128);
  private final Map<String, Long> hashMap = new ConcurrentHashMap<>(128);
  private final Consistency defaultConsistency;

  public StateLogPartition(PartitionContext context) {
    super(context);
    defaultConsistency = context.<StateLogConfig>getResourceConfig().getDefaultConsistency();
    context.setCommitHandler(this::commit);
  }

  /**
   * Registers a state command.
   *
   * @param name The command name.
   * @param type The command type.
   * @param command The command function.
   * @return The state log.
   */
  @SuppressWarnings("unchecked")
  protected StateLogPartition<K, V> register(String name, Command.Type type, Command<? extends K, ? extends V, ?> command) {
    if (!isClosed())
      throw new IllegalStateException("cannot register command on open state log");
    if (type == Command.Type.READ) {
      operations.put(HashFunctions.CITYHASH.hash64(name.getBytes()), new OperationInfo((Command) command, type, defaultConsistency));
    } else {
      operations.put(HashFunctions.CITYHASH.hash64(name.getBytes()), new OperationInfo((Command) command, type));
    }
    LOGGER.debug("{} - Registered state log command {}", context.getName(), name);
    return this;
  }

  /**
   * Registers a state command.
   *
   * @param name The command name.
   * @param type The command type.
   * @param command The command function.
   * @param consistency The operation consistency.
   * @return The state log.
   */
  @SuppressWarnings("unchecked")
  protected StateLogPartition<K, V> register(String name, Command.Type type, Command<? extends K, ? extends V, ?> command, Consistency consistency) {
    if (!isClosed())
      throw new IllegalStateException("cannot register command on open state log");
    if (type == Command.Type.READ) {
      operations.put(HashFunctions.CITYHASH.hash64(name.getBytes()), new OperationInfo((Command) command, type, consistency));
    } else {
      if (consistency != null && consistency != Consistency.STRONG)
        throw new IllegalArgumentException("consistency level STRONG is required for write and delete commands");
      operations.put(HashFunctions.CITYHASH.hash64(name.getBytes()), new OperationInfo((Command) command, type));
    }
    LOGGER.debug("{} - Registered state log command {}", context.getName(), name);
    return this;
  }

  /**
   * Unregisters a state command.
   *
   * @param name The command name.
   * @return The state log.
   */
  protected StateLogPartition<K, V> unregister(String name) {
    if (!isClosed())
      throw new IllegalStateException("cannot unregister command on open state log");
    OperationInfo info = operations.remove(HashFunctions.CITYHASH.hash64(name.getBytes()));
    if (info != null) {
      LOGGER.debug("{} - Unregistered state log command {}", context.getName(), name);
    }
    return this;
  }

  /**
   * Returns the hash for the given command.
   *
   * @param command The command.
   * @return The command hash.
   */
  private long commandHash(String command) {
    return hashMap.computeIfAbsent(command, c -> HashFunctions.CITYHASH.hash64(c.getBytes()));
  }

  /**
   * Submits a state command or query to the log.
   *
   * @param command The command name.
   * @param entry The command entry.
   * @return A completable future to be completed once the command output is received.
   */
  public <U> CompletableFuture<U> submit(String command, V entry) {
    return submit(command, null, entry);
  }

  /**
   * Submits a state command or query to the log.
   *
   * @param command The command name.
   * @param key The entry key.
   * @param entry The command entry.
   * @return A completable future to be completed once the command output is received.
   */
  public <U> CompletableFuture<U> submit(String command, K key, V entry) {
    Buffer keyBuffer = key != null ? serializer.writeObject(key, bufferPool.acquire()).flip() : null;
    Buffer entryBuffer = bufferPool.acquire().writeLong(commandHash(command));
    serializer.writeObject(entry, entryBuffer);
    entryBuffer.flip();

    return submit(command, keyBuffer, entryBuffer).thenApplyAsync(result -> {
      if (keyBuffer != null)
        keyBuffer.close();
      entryBuffer.close();
      return serializer.readObject(result);
    }, context.getExecutor());
  }

  /**
   * Submits a state command or query to the log.
   *
   * @param command The command name.
   * @param entry The command entry.
   * @return A completable future to be completed once the command output is received.
   */
  protected CompletableFuture<Buffer> submit(String command, Buffer entry) {
    return submit(command, EMPTY_BUFFER, entry);
  }

  /**
   * Submits a state command or query to the log.
   *
   * @param command The command name.
   * @param key The entry key.
   * @param entry The command entry.
   * @return A completable future to be completed once the command output is received.
   */
  protected CompletableFuture<Buffer> submit(String command, Buffer key, Buffer entry) {
    if (!isOpen())
      throw new IllegalStateException("state log not open");
    OperationInfo operationInfo = operations.get(commandHash(command));
    if (operationInfo == null) {
      return Futures.exceptionalFutureAsync(new CopycatException(String.format("Invalid state log command %s", command)), context.getExecutor());
    }

    switch (operationInfo.type) {
      case READ:
        LOGGER.debug("{} - Submitting read command {} with entry {}", context.getName(), command, entry);
        return context.read(key, entry, operationInfo.consistency);
      case WRITE:
        LOGGER.debug("{} - Submitting write command {} with entry {}", context.getName(), command, entry);
        return context.write(key, entry);
      case DELETE:
        LOGGER.debug("{} - Submitting delete command {} with entry {}", context.getName(), command, entry);
        return context.delete(key);
    }
    return Futures.exceptionalFutureAsync(new CopycatException(String.format("Invalid state log command %s", command)), context.getExecutor());
  }

  /**
   * Consumes a log entry.
   *
   * @param key The entry key.
   * @param entry The log entry.
   * @param result The result.
   * @return The entry output.
   */
  @SuppressWarnings({"unchecked"})
  private Buffer commit(Buffer key, Buffer entry, Buffer result) {
    long commandCode = entry.readLong();
    OperationInfo operationInfo = operations.get(commandCode);
    if (operationInfo != null) {
      return serializer.writeObject(operationInfo.execute(serializer.readObject(key), serializer.readObject(entry.slice())), result).flip();
    }
    throw new IllegalStateException("Invalid state log operation");
  }

  @Override
  public String toString() {
    return String.format("%s[name=%s, partition=%d]", getClass().getSimpleName(), context.getName(), context.getPartitionId());
  }

  /**
   * State command info.
   */
  private class OperationInfo<K, V, U> {
    private final Command<K, V, U> command;
    private final Command.Type type;
    private final Consistency consistency;

    private OperationInfo(Command<K, V, U> command, Command.Type type) {
      this(command, type, Consistency.DEFAULT);
    }

    private OperationInfo(Command<K, V, U> command, Command.Type type, Consistency consistency) {
      this.command = command;
      this.type = type;
      this.consistency = consistency;
    }

    private U execute(K key, V entry) {
      return command.apply(key, entry);
    }
  }

}

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
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.HeapBufferPool;
import net.kuujo.copycat.io.util.HashFunctions;
import net.kuujo.copycat.io.util.ReferencePool;
import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.resource.internal.AbstractResource;
import net.kuujo.copycat.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Copycat state log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateLog<K, V> extends AbstractResource<StateLog<K, V>> {

  /**
   * Creates a new state log with the given cluster and state log configurations.
   *
   * @param config The state log configuration.
   * @param cluster The cluster configuration.
   * @return A new state log instance.
   */
  public static <K, V> StateLog<K, V> create(StateLogConfig config, ClusterConfig cluster) {
    return new StateLog<>(config, cluster);
  }

  /**
   * Creates a new state log with the given cluster and state log configurations.
   *
   * @param config The state log configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute state log callbacks.
   * @return A new state log instance.
   */
  public static <K, V> StateLog<K, V> create(StateLogConfig config, ClusterConfig cluster, Executor executor) {
    return new StateLog<>(config, cluster, executor);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(StateLog.class);
  private final ReferencePool<Buffer> operationPool = new HeapBufferPool();
  private final Map<Long, OperationInfo> operations = new ConcurrentHashMap<>(128);
  private final Map<String, Long> hashMap = new ConcurrentHashMap<>(128);
  private final Consistency defaultConsistency;

  public StateLog(StateLogConfig config, ClusterConfig cluster) {
    this(new ResourceContext(config, cluster));
  }

  public StateLog(StateLogConfig config, ClusterConfig cluster, Executor executor) {
    this(new ResourceContext(config, cluster, executor));
  }

  public StateLog(ResourceContext context) {
    super(context);
    defaultConsistency = context.<StateLogConfig>config().getDefaultConsistency();
    context.commitHandler(this::commit);
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
  public StateLog<K, V> register(String name, Command.Type type, Command<? extends K, ? extends V, ?> command) {
    if (!isClosed())
      throw new IllegalStateException("cannot register command on open state log");
    if (type == Command.Type.READ) {
      operations.put(HashFunctions.CITYHASH.hash64(name.getBytes()), new OperationInfo((Command) command, type, defaultConsistency));
    } else {
      operations.put(HashFunctions.CITYHASH.hash64(name.getBytes()), new OperationInfo((Command) command, type));
    }
    LOGGER.debug("{} - Registered state log command {}", context.name(), name);
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
  public StateLog<K, V> register(String name, Command.Type type, Command<? extends K, ? extends V, ?> command, Consistency consistency) {
    if (!isClosed())
      throw new IllegalStateException("cannot register command on open state log");
    if (type == Command.Type.READ) {
      operations.put(HashFunctions.CITYHASH.hash64(name.getBytes()), new OperationInfo((Command) command, type, consistency));
    } else {
      if (consistency != null && consistency != Consistency.STRONG)
        throw new IllegalArgumentException("consistency level STRONG is required for write and delete commands");
      operations.put(HashFunctions.CITYHASH.hash64(name.getBytes()), new OperationInfo((Command) command, type));
    }
    LOGGER.debug("{} - Registered state log command {}", context.name(), name);
    return this;
  }

  /**
   * Unregisters a state command.
   *
   * @param name The command name.
   * @return The state log.
   */
  public StateLog<K, V> unregister(String name) {
    if (!isClosed())
      throw new IllegalStateException("cannot unregister command on open state log");
    OperationInfo info = operations.remove(HashFunctions.CITYHASH.hash64(name.getBytes()));
    if (info != null) {
      LOGGER.debug("{} - Unregistered state log command {}", context.name(), name);
    }
    return this;
  }

  /**
   * Submits a state command or query to the log.
   *
   * @param command The command name.
   * @param entry The command entry.
   * @param <U> The command return type.
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
   * @param <U> The command return type.
   * @return A completable future to be completed once the command output is received.
   */
  @SuppressWarnings("unchecked")
  public <U> CompletableFuture<U> submit(String command, K key, V entry) {
    if (!isOpen())
      throw new IllegalStateException("state log not open");
    OperationInfo<K, V, U> operationInfo = operations.get(HashFunctions.CITYHASH.hash64(command.getBytes()));
    if (operationInfo == null) {
      return Futures.exceptionalFutureAsync(new CopycatException(String.format("Invalid state log command %s", command)), context.executor());
    }

    // If this is a read-only command, check if the command is consistent. For consistent operations,
    // queries are forwarded to the current cluster leader for evaluation. Otherwise, it's safe to
    // read stale data from the local node.
    Buffer keyBuffer = null;
    if (key != null) {
      keyBuffer = operationPool.acquire();
      serializer.writeObject(key, keyBuffer);
      keyBuffer.flip();
    }

    Buffer entryBuffer = operationPool.acquire().writeLong(hashMap.computeIfAbsent(command, c -> HashFunctions.CITYHASH.hash64(c.getBytes())));
    serializer.writeObject(entry, entryBuffer);
    entryBuffer.flip();

    if (operationInfo.type == Command.Type.READ) {
      LOGGER.debug("{} - Submitting read command {} with entry {}", context.name(), command, entry);
      return context.read(keyBuffer, entryBuffer, operationInfo.consistency)
        .thenApplyAsync(serializer::readObject, context.executor());
    } else {
      LOGGER.debug("{} - Submitting write command {} with entry {}", context.name(), command, entry);
      return context.write(keyBuffer, entryBuffer)
        .thenApplyAsync(serializer::readObject, context.executor());
    }
  }

  /**
   * Consumes a log entry.
   *
   * @param term The entry term.
   * @param index The entry index.
   * @param key The entry key.
   * @param entry The log entry.
   * @return The entry output.
   */
  @SuppressWarnings({"unchecked"})
  private Buffer commit(long term, long index, Buffer key, Buffer entry) {
    long commandCode = entry.readLong();
    OperationInfo operationInfo = operations.get(commandCode);
    if (operationInfo != null) {
      return serializer.writeObject(operationInfo.execute(term, index, serializer.readObject(key), serializer.readObject(entry.slice())));
    }
    throw new IllegalStateException("Invalid state log operation");
  }

  @Override
  public String toString() {
    return String.format("%s[name=%s]", getClass().getSimpleName(), context.name());
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

    private U execute(long term, long index, K key, V entry) {
      return command.apply(key, entry);
    }
  }

}

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
package net.kuujo.copycat.state;

import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.HeapBufferPool;
import net.kuujo.copycat.io.util.HashFunctions;
import net.kuujo.copycat.io.util.ReferencePool;
import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.resource.DiscreteResource;
import net.kuujo.copycat.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Discrete state log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DiscreteStateLog<K, V> extends DiscreteResource<DiscreteStateLog<K, V>, StateLog<K, V>> implements StateLog<K, V> {

  /**
   * Returns a new state log builder.
   *
   * @return A new state log builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DiscreteStateLog.class);
  private final ReferencePool<Buffer> bufferPool = new HeapBufferPool();
  private final Map<Long, CommandInfo> commands = new ConcurrentHashMap<>(128);
  private final Map<String, Long> hashMap = new ConcurrentHashMap<>(128);
  private final Consistency defaultConsistency;

  public DiscreteStateLog(StateLogConfig config) {
    super(config);
    this.defaultConsistency = config.getDefaultConsistency();
    config.getCommands().forEach(c -> commands.put(HashFunctions.CITYHASH.hash64(c.name().getBytes()), c));
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
  protected DiscreteStateLog<K, V> register(String name, Command.Type type, Command<? extends K, ? extends V, ?> command) {
    if (!isClosed())
      throw new IllegalStateException("cannot register command on open state log");
    if (type == Command.Type.READ) {
      commands.put(HashFunctions.CITYHASH.hash64(name.getBytes()), new CommandInfo(name, type, (Command) command, defaultConsistency));
    } else {
      commands.put(HashFunctions.CITYHASH.hash64(name.getBytes()), new CommandInfo(name, type, (Command) command, Consistency.STRONG));
    }
    LOGGER.debug("{} - Registered state log command {}", name(), name);
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
  protected DiscreteStateLog<K, V> register(String name, Command.Type type, Command<? extends K, ? extends V, ?> command, Consistency consistency) {
    if (!isClosed())
      throw new IllegalStateException("cannot register command on open state log");
    if (type == Command.Type.READ) {
      commands.put(HashFunctions.CITYHASH.hash64(name.getBytes()), new CommandInfo(name, type, (Command) command, consistency));
    } else {
      if (consistency != null && consistency != Consistency.STRONG)
        throw new IllegalArgumentException("consistency level STRONG is required for write and delete commands");
      commands.put(HashFunctions.CITYHASH.hash64(name.getBytes()), new CommandInfo(name, type, (Command) command, Consistency.STRONG));
    }
    LOGGER.debug("{} - Registered state log command {}", name(), name);
    return this;
  }

  /**
   * Unregisters a state command.
   *
   * @param name The command name.
   * @return The state log.
   */
  protected DiscreteStateLog<K, V> unregister(String name) {
    if (!isClosed())
      throw new IllegalStateException("cannot unregister command on open state log");
    CommandInfo info = commands.remove(HashFunctions.CITYHASH.hash64(name.getBytes()));
    if (info != null) {
      LOGGER.debug("{} - Unregistered state log command {}", name(), name);
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

  @Override
  public <U> CompletableFuture<U> submit(String command, K key, V entry) {
    Buffer keyBuffer = key != null ? serializer.writeObject(key, bufferPool.acquire()).flip() : null;
    Buffer entryBuffer = bufferPool.acquire().writeLong(commandHash(command));
    serializer.writeObject(entry, entryBuffer);
    entryBuffer.flip();

    return submit(command, keyBuffer, entryBuffer).thenApply(result -> {
      if (keyBuffer != null)
        keyBuffer.close();
      entryBuffer.close();
      return serializer.readObject(result);
    });
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
    CommandInfo commandInfo = commands.get(commandHash(command));
    if (commandInfo == null) {
      return Futures.exceptionalFuture(new CopycatException(String.format("Invalid state log command %s", command)));
    }

    switch (commandInfo.type()) {
      case READ:
        LOGGER.debug("{} - Submitting read command {} with entry {}", name(), command, entry);
        Consistency consistency = commandInfo.consistency();
        return protocol.read(key, entry, consistency != null ? consistency : defaultConsistency);
      case WRITE:
        LOGGER.debug("{} - Submitting write command {} with entry {}", name(), command, entry);
        return protocol.write(key, entry, Consistency.STRONG);
      case DELETE:
        LOGGER.debug("{} - Submitting delete command {} with entry {}", name(), command, entry);
        return protocol.delete(key, null, Consistency.STRONG);
    }
    return Futures.exceptionalFuture(new CopycatException(String.format("Invalid state log command %s", command)));
  }

  @Override
  @SuppressWarnings({"unchecked"})
  protected Buffer commit(Buffer key, Buffer entry, Buffer result) {
    long commandCode = entry.readLong();
    CommandInfo commandInfo = commands.get(commandCode);
    if (commandInfo != null) {
      return serializer.writeObject(commandInfo.command().apply(serializer.readObject(key), serializer.readObject(entry.slice())), result).flip();
    }
    throw new IllegalStateException("Invalid state log operation");
  }

  /**
   * State log builder.
   */
  public static class Builder<K, V> extends DiscreteResource.Builder<Builder<K, V>, DiscreteStateLog<K, V>> {
    private final StateLogConfig config = new StateLogConfig();

    private Builder() {
      this(new StateLogConfig());
    }

    private Builder(StateLogConfig config) {
      super(config);
    }

    /**
     * Sets the default state log consistency.
     *
     * @param consistency The default state log consistency.
     * @return The state log builder.
     */
    public Builder<K, V> withDefaultConsistency(Consistency consistency) {
      config.setDefaultConsistency(consistency);
      return this;
    }

    /**
     * Adds a command to the state log.
     *
     * @param name The command name.
     * @param type The command type.
     * @param command The command to add.
     * @return The state log builder.
     */
    public Builder<K, V> addCommand(String name, Command.Type type, Command<K, V, ?> command) {
      config.addCommand(name, type, command);
      return this;
    }

    /**
     * Adds a command to the state log.
     *
     * @param name The command name.
     * @param type The command type.
     * @param command The command to add.
     * @param consistency The command consistency.
     * @return The state log builder.
     */
    public Builder<K, V> addCommand(String name, Command.Type type, Command<K, V, ?> command, Consistency consistency) {
      config.addCommand(name, type, command, consistency);
      return this;
    }

    @Override
    public DiscreteStateLog<K, V> build() {
      return new DiscreteStateLog<>(config);
    }
  }

}

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

import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.resource.PartitionedResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Copycat state log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PartitionedStateLog<K, V> extends PartitionedResource<PartitionedStateLog<K, V>, StateLogPartition<K, V>, StateLog<K, V>> implements StateLog<K, V> {

  /**
   * Returns a new partitioned state log builder.
   *
   * @return A new partitioned state log builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedStateLog.class);

  public PartitionedStateLog(PartitionedStateLogConfig config, List<StateLogPartition<K, V>> partitions) {
    super(config, partitions);
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
  protected StateLog<K, V> register(String name, Command.Type type, Command<? extends K, ? extends V, ?> command) {
    if (!isClosed())
      throw new IllegalStateException("cannot register command on open state log");
    partitions.forEach(p -> p.register(name, type, command));
    LOGGER.debug("{} - Registered state log command {}", this.name, name);
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
  protected StateLog<K, V> register(String name, Command.Type type, Command<? extends K, ? extends V, ?> command, Consistency consistency) {
    if (!isClosed())
      throw new IllegalStateException("cannot register command on open state log");
    partitions.forEach(p -> p.register(name, type, command, consistency));
    LOGGER.debug("{} - Registered state log command {}", this.name, name);
    return this;
  }

  /**
   * Unregisters a state command.
   *
   * @param name The command name.
   * @return The state log.
   */
  protected StateLog<K, V> unregister(String name) {
    if (!isClosed())
      throw new IllegalStateException("cannot unregister command on open state log");
    partitions.forEach(p -> p.unregister(name));
    LOGGER.debug("{} - Unregistered state log command {}", this.name, name);
    return this;
  }

  @Override
  public <U> CompletableFuture<U> submit(String command, K key, V entry) {
    if (!isOpen())
      throw new IllegalStateException("state log not open");
    return partition(key).submit(command, key, entry);
  }

  /**
   * Partitioned state log builder.
   *
   * @param <K> The state log key type.
   * @param <V> The state log value type.
   */
  public static class Builder<K, V> extends PartitionedResource.Builder<Builder<K, V>, PartitionedStateLog<K, V>, StateLogPartition<K, V>, StateLog<K, V>> {
    private final PartitionedStateLogConfig config;

    private Builder() {
      this(new PartitionedStateLogConfig());
    }

    private Builder(PartitionedStateLogConfig config) {
      super(config);
      this.config = config;
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
    public PartitionedStateLog<K, V> build() {
      return null;
    }
  }

}

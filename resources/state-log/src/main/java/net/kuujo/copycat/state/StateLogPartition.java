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

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.resource.Partition;
import net.kuujo.copycat.resource.PartitionedResourceConfig;

import java.util.concurrent.CompletableFuture;

/**
 * State log partition.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateLogPartition<K, V> extends Partition<StateLog<K, V>> implements StateLog<K, V> {

  /**
   * Returns a new state log partition builder.
   *
   * @return A new state log partition builder.
   */
  public static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  private final StateLogConfig config;
  private final int partitionId;
  private DiscreteStateLog<K, V> stateLog;

  private StateLogPartition(StateLogConfig config, int partitionId) {
    this.config = config;
    this.partitionId = partitionId;
  }

  @Override
  protected void init(PartitionedResourceConfig config) {
    this.config.setName(String.format("%s-%d", config.getName(), partitionId));
    this.config.setCluster(config.getCluster());
    this.config.setPartitions(config.getPartitions().size());
    this.config.setDefaultConsistency(((PartitionedStateLogConfig) config).getDefaultConsistency());
    this.config.setCommands(((PartitionedStateLogConfig) config).getCommands());
    this.stateLog = new DiscreteStateLog<>(this.config);
  }

  @Override
  public String name() {
    return config.getName();
  }

  @Override
  public Cluster cluster() {
    return stateLog.cluster();
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
    stateLog.register(name, type, command);
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
    stateLog.register(name, type, command, consistency);
    return this;
  }

  /**
   * Unregisters a state command.
   *
   * @param name The command name.
   * @return The state log.
   */
  protected StateLog<K, V> unregister(String name) {
    stateLog.unregister(name);
    return this;
  }

  @Override
  public <U> CompletableFuture<U> submit(String command, K key, V entry) {
    return stateLog.submit(command, key, entry);
  }

  @Override
  public CompletableFuture<StateLog<K, V>> open() {
    return stateLog.open();
  }

  @Override
  public boolean isOpen() {
    return stateLog.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return stateLog.close();
  }

  @Override
  public boolean isClosed() {
    return stateLog.isClosed();
  }

  /**
   * State log partition builder.
   */
  public static class Builder<K, V> extends Partition.Builder<Builder<K, V>, StateLogPartition<K, V>> {
    private final StateLogConfig config;

    private Builder() {
      this(new StateLogConfig());
    }

    private Builder(StateLogConfig config) {
      super(config);
      this.config = config;
    }

    @Override
    public StateLogPartition<K, V> build() {
      return new StateLogPartition<>(config, partitionId);
    }
  }

}

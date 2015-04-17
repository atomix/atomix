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

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.resource.PartitionContext;
import net.kuujo.copycat.resource.internal.AbstractPartitionedResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Partitioned state log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateLog<K, V> extends AbstractPartitionedResource<StateLog<K, V>, StateLogPartition<K, V>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(StateLog.class);

  public StateLog(StateLogConfig config, ClusterConfig cluster) {
    super(config, cluster);
  }

  public StateLog(StateLogConfig config, ClusterConfig cluster, Executor executor) {
    super(config, cluster, executor);
  }

  @Override
  protected StateLogPartition<K, V> createPartition(PartitionContext context) {
    return new StateLogPartition<>(context);
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
  public StateLog<K, V> register(String name, Command.Type type, Command<? extends K, ? extends V, ?> command, Consistency consistency) {
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
  public StateLog<K, V> unregister(String name) {
    if (!isClosed())
      throw new IllegalStateException("cannot unregister command on open state log");
    partitions.forEach(p -> p.unregister(name));
    LOGGER.debug("{} - Unregistered state log command {}", this.name, name);
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
    return partition(key).submit(command, key, entry);
  }

  @Override
  public String toString() {
    return String.format("%s[name=%s]", getClass().getSimpleName(), name);
  }

}

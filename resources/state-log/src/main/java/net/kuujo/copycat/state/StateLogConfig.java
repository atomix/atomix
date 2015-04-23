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

import net.kuujo.copycat.cluster.ManagedCluster;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.resource.DiscreteResourceConfig;
import net.kuujo.copycat.resource.ReplicationStrategy;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * State log configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateLogConfig extends DiscreteResourceConfig {
  private Consistency defaultConsistency = Consistency.DEFAULT;
  private final Map<String, CommandInfo> commands = new HashMap<>();

  @Override
  protected void setName(String name) {
    super.setName(name);
  }

  @Override
  protected void setCluster(ManagedCluster cluster) {
    super.setCluster(cluster);
  }

  @Override
  protected void setPartitions(int partitions) {
    super.setPartitions(partitions);
  }

  @Override
  protected void setReplicationStrategy(ReplicationStrategy replicationStrategy) {
    super.setReplicationStrategy(replicationStrategy);
  }

  /**
   * Sets the default state log consistency.
   *
   * @param consistency The default state log consistency.
   */
  protected void setDefaultConsistency(Consistency consistency) {
    if (consistency == null)
      throw new NullPointerException("consistency cannot be null");
    this.defaultConsistency = consistency;
  }

  /**
   * Returns the default state log consistency.
   *
   * @return The default state log consistency.
   */
  public Consistency getDefaultConsistency() {
    return defaultConsistency;
  }

  /**
   * Adds a command to the state log.
   *
   * @param name The command name.
   * @param type The command type.
   * @param command The command.
   */
  protected void addCommand(String name, Command.Type type, Command command) {
    commands.put(name, new CommandInfo(name, type, command, null));
  }

  /**
   * Adds a command to the state log.
   *
   * @param name The command name.
   * @param type The command type.
   * @param command The command.
   * @param consistency The command consistency.
   */
  protected void addCommand(String name, Command.Type type, Command command, Consistency consistency) {
    commands.put(name, new CommandInfo(name, type, command, consistency));
  }

  /**
   * Sets all commands.
   *
   * @param commands The set of commands.
   */
  protected void setCommands(Map<String, CommandInfo> commands) {
    this.commands.putAll(commands);
  }

  /**
   * Returns a collection of commands.
   *
   * @return A collection of configured commands.
   */
  public Collection<CommandInfo> getCommands() {
    return commands.values();
  }

}

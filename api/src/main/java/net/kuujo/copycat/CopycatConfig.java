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
package net.kuujo.copycat;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.util.Copyable;

/**
 * Copycat configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatConfig implements Copyable<CopycatConfig> {
  private String name;
  private ClusterConfig cluster;

  public CopycatConfig() {
  }

  private CopycatConfig(CopycatConfig config) {

  }

  @Override
  public CopycatConfig copy() {
    return new CopycatConfig(this);
  }

  /**
   * Sets the Copycat instance name.
   *
   * @param name The Copycat instance name.
   * @throws java.lang.NullPointerException If the name is {@code null}
   */
  public void setName(String name) {
    if (name == null)
      throw new NullPointerException("name cannot be null");
    this.name = name;
  }

  /**
   * Returns the Copycat instance name.
   *
   * @return The Copycat instance name.
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the Copycat instance name, returning the configuration for method chaining.
   *
   * @param name The Copycat instance name.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If the name is {@code null}
   */
  public CopycatConfig withName(String name) {
    setName(name);
    return this;
  }

  /**
   * Sets the Copycat cluster configuration.
   *
   * @param cluster The Copycat cluster configuration.
   * @throws java.lang.NullPointerException If the cluster configuration is {@code null}
   */
  public void setClusterConfig(ClusterConfig cluster) {
    if (cluster == null)
      throw new NullPointerException("cluster cannot be null");
    this.cluster = cluster;
  }

  /**
   * Returns the Copycat cluster configuration.
   *
   * @return The Copycat cluster configuration.
   */
  public ClusterConfig getClusterConfig() {
    return cluster;
  }

  /**
   * Sets the Copycat cluster configuration, returning the Copycat configuration for method chaining.
   *
   * @param config The Copycat cluster configuration.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If the cluster configuration is {@code null}
   */
  public CopycatConfig withClusterConfig(ClusterConfig config) {
    setClusterConfig(config);
    return this;
  }

}

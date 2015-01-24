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
package net.kuujo.copycat.cluster.coordinator;

import net.kuujo.copycat.AbstractConfigurable;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Copycat configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatorConfig extends AbstractConfigurable {
  public static final String COORDINATOR_NAME = "name";
  public static final String COORDINATOR_CLUSTER = "cluster";
  public static final String COORDINATOR_EXECUTOR = "executor";

  private static final String DEFAULT_COORDINATOR_NAME = "copycat";
  private final Executor DEFAULT_COORDINATOR_EXECUTOR = Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-coordinator-%d"));

  public CoordinatorConfig() {
    super();
  }

  public CoordinatorConfig(CoordinatorConfig config) {
    super(config);
  }

  public CoordinatorConfig(Map<String, Object> config) {
    super(config);
  }

  @Override
  public CoordinatorConfig copy() {
    return new CoordinatorConfig(this);
  }

  /**
   * Sets the Copycat instance name.
   *
   * @param name The Copycat instance name.
   * @throws java.lang.NullPointerException If the name is {@code null}
   */
  public void setName(String name) {
    put(COORDINATOR_NAME, Assert.isNotNull(name, "name"));
  }

  /**
   * Returns the Copycat instance name.
   *
   * @return The Copycat instance name.
   */
  public String getName() {
    return get(COORDINATOR_NAME, DEFAULT_COORDINATOR_NAME);
  }

  /**
   * Sets the Copycat instance name, returning the configuration for method chaining.
   *
   * @param name The Copycat instance name.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If the name is {@code null}
   */
  public CoordinatorConfig withName(String name) {
    setName(name);
    return this;
  }

  /**
   * Sets the Copycat cluster configuration.
   *
   * @param config The Copycat cluster configuration.
   * @throws java.lang.NullPointerException If the cluster configuration is {@code null}
   */
  public void setClusterConfig(ClusterConfig config) {
    put(COORDINATOR_CLUSTER, Assert.isNotNull(config, "config").toMap());
  }

  /**
   * Returns the Copycat cluster configuration.
   *
   * @return The Copycat cluster configuration.
   */
  public ClusterConfig getClusterConfig() {
    return get(COORDINATOR_CLUSTER, key -> new ClusterConfig());
  }

  /**
   * Sets the Copycat cluster configuration, returning the Copycat configuration for method chaining.
   *
   * @param config The Copycat cluster configuration.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If the cluster configuration is {@code null}
   */
  public CoordinatorConfig withClusterConfig(ClusterConfig config) {
    setClusterConfig(config);
    return this;
  }

  /**
   * Sets the coordinator executor.
   *
   * @param executor The coordinator executor.
   */
  public void setExecutor(Executor executor) {
    put(COORDINATOR_EXECUTOR, executor);
  }

  /**
   * Returns the coordinator executor.
   *
   * @return The coordinator executor or {@code null} if no executor was specified.
   */
  public Executor getExecutor() {
    return get(COORDINATOR_EXECUTOR, DEFAULT_COORDINATOR_EXECUTOR);
  }

  /**
   * Sets the coordinator executor, returning the configuration for method chaining.
   *
   * @param executor The coordinator executor.
   * @return The coordinator configuration.
   */
  public CoordinatorConfig withExecutor(Executor executor) {
    setExecutor(executor);
    return this;
  }

}

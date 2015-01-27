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
package net.kuujo.copycat.collections;

import com.typesafe.config.ConfigValueFactory;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.internal.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.collections.internal.map.DefaultAsyncMap;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.resource.ResourceConfig;
import net.kuujo.copycat.state.StateLogConfig;
import net.kuujo.copycat.util.internal.Assert;

import java.util.Map;

/**
 * Asynchronous map configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncMapConfig extends ResourceConfig<AsyncMapConfig> {
  private static final String ASYNC_MAP_CONSISTENCY = "consistency";

  private static final String DEFAULT_CONFIGURATION = "map-defaults";
  private static final String CONFIGURATION = "map";

  public AsyncMapConfig() {
    super(CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public AsyncMapConfig(Map<String, Object> config) {
    super(config, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public AsyncMapConfig(String resource) {
    super(resource, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  protected AsyncMapConfig(AsyncMapConfig config) {
    super(config);
  }

  @Override
  public AsyncMapConfig copy() {
    return new AsyncMapConfig(this);
  }

  /**
   * Sets the map read consistency.
   *
   * @param consistency The map read consistency.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public void setConsistency(String consistency) {
    this.config = config.withValue(ASYNC_MAP_CONSISTENCY, ConfigValueFactory.fromAnyRef(Consistency.parse(Assert.isNotNull(consistency, "consistency")).toString()));
  }

  /**
   * Sets the map read consistency.
   *
   * @param consistency The map read consistency.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public void setConsistency(Consistency consistency) {
    this.config = config.withValue(ASYNC_MAP_CONSISTENCY, ConfigValueFactory.fromAnyRef(Assert.isNotNull(consistency, "consistency").toString()));
  }

  /**
   * Returns the map read consistency.
   *
   * @return The map read consistency.
   */
  public Consistency getConsistency() {
    return Consistency.parse(config.getString(ASYNC_MAP_CONSISTENCY));
  }

  /**
   * Sets the map read consistency, returning the configuration for method chaining.
   *
   * @param consistency The map read consistency.
   * @return The map configuration.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public AsyncMapConfig withConsistency(String consistency) {
    setConsistency(consistency);
    return this;
  }

  /**
   * Sets the map read consistency, returning the configuration for method chaining.
   *
   * @param consistency The map read consistency.
   * @return The map configuration.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public AsyncMapConfig withConsistency(Consistency consistency) {
    setConsistency(consistency);
    return this;
  }

  @Override
  public CoordinatedResourceConfig resolve(ClusterConfig cluster) {
    return new StateLogConfig(toMap())
      .resolve(cluster)
      .withResourceType(DefaultAsyncMap.class);
  }

}

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

import net.kuujo.copycat.ResourceConfig;
import net.kuujo.copycat.StateLogConfig;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.collections.internal.map.DefaultAsyncMultiMap;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.protocol.Consistency;

import java.util.Map;

/**
 * Asynchronous multi-map configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncMultiMapConfig extends ResourceConfig<AsyncMultiMapConfig> {
  public static final String ASYNC_MULTIMAP_CONSISTENCY = "consistency";

  private static final String DEFAULT_ASYNC_MULTIMAP_CONSISTENCY = "default";

  public AsyncMultiMapConfig() {
  }

  public AsyncMultiMapConfig(Map<String, Object> config) {
    super(config);
  }

  protected AsyncMultiMapConfig(AsyncMultiMapConfig config) {
    super(config);
  }

  @Override
  public AsyncMultiMapConfig copy() {
    return new AsyncMultiMapConfig(this);
  }

  /**
   * Sets the multimap read consistency.
   *
   * @param consistency The multimap read consistency.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public void setConsistency(String consistency) {
    put(ASYNC_MULTIMAP_CONSISTENCY, Consistency.parse(Assert.isNotNull(consistency, "consistency")).toString());
  }

  /**
   * Sets the multimap read consistency.
   *
   * @param consistency The multimap read consistency.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public void setConsistency(Consistency consistency) {
    put(ASYNC_MULTIMAP_CONSISTENCY, Assert.isNotNull(consistency, "consistency").toString());
  }

  /**
   * Returns the multimap read consistency.
   *
   * @return The multimap read consistency.
   */
  public Consistency getConsistency() {
    return Consistency.parse(get(ASYNC_MULTIMAP_CONSISTENCY, DEFAULT_ASYNC_MULTIMAP_CONSISTENCY));
  }

  /**
   * Sets the multimap read consistency, returning the configuration for method chaining.
   *
   * @param consistency The multimap read consistency.
   * @return The multimap configuration.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public AsyncMultiMapConfig withConsistency(String consistency) {
    setConsistency(consistency);
    return this;
  }

  /**
   * Sets the multimap read consistency, returning the configuration for method chaining.
   *
   * @param consistency The multimap read consistency.
   * @return The multimap configuration.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public AsyncMultiMapConfig withConsistency(Consistency consistency) {
    setConsistency(consistency);
    return this;
  }

  @Override
  public CoordinatedResourceConfig resolve(ClusterConfig cluster) {
    return new StateLogConfig(toMap())
      .resolve(cluster)
      .withResourceFactory(DefaultAsyncMultiMap::new);
  }

}

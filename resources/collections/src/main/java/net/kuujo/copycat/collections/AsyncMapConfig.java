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

import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.resource.PartitionedResourceConfig;

/**
 * Asynchronous map configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncMapConfig extends PartitionedResourceConfig<AsyncMapConfig> {
  private Consistency consistency = Consistency.DEFAULT;

  public AsyncMapConfig() {
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
  public void setConsistency(Consistency consistency) {
    if (consistency == null)
      consistency = Consistency.DEFAULT;
    this.consistency = consistency;
  }

  /**
   * Returns the map read consistency.
   *
   * @return The map read consistency.
   */
  public Consistency getConsistency() {
    return consistency;
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

}

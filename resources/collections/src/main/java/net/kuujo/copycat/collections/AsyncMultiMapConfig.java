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
import net.kuujo.copycat.resource.ResourceConfig;

/**
 * Asynchronous multi-map configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncMultiMapConfig extends ResourceConfig<AsyncMultiMapConfig> {
  private Consistency consistency = Consistency.DEFAULT;

  public AsyncMultiMapConfig() {
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
  public void setConsistency(Consistency consistency) {
    if (consistency == null)
      consistency = Consistency.DEFAULT;
    this.consistency = consistency;
  }

  /**
   * Returns the multimap read consistency.
   *
   * @return The multimap read consistency.
   */
  public Consistency getConsistency() {
    return consistency;
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

}

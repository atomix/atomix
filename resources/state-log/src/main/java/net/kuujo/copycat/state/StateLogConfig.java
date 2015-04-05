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
package net.kuujo.copycat.state;

import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.resource.ResourceConfig;

/**
 * State log configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateLogConfig extends ResourceConfig<StateLogConfig> {
  private Consistency defaultConsistency = Consistency.DEFAULT;

  public StateLogConfig() {
  }

  protected StateLogConfig(StateLogConfig config) {
    super(config);
  }

  @Override
  public StateLogConfig copy() {
    return new StateLogConfig(this);
  }

  /**
   * Sets the status log read consistency.
   *
   * @param consistency The status log read consistency.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public void setDefaultConsistency(String consistency) {
    if (consistency == null)
      throw new NullPointerException("consistency cannot be null");
    this.defaultConsistency = Consistency.forName(consistency);
  }

  /**
   * Sets the status log read consistency.
   *
   * @param consistency The status log read consistency.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public void setDefaultConsistency(Consistency consistency) {
    if (consistency == null)
      throw new NullPointerException("consistency cannot be null");
    this.defaultConsistency = consistency;
  }

  /**
   * Returns the status log read consistency.
   *
   * @return The status log read consistency.
   */
  public Consistency getDefaultConsistency() {
    return defaultConsistency;
  }

  /**
   * Sets the status log read consistency, returning the configuration for method chaining.
   *
   * @param consistency The status log read consistency.
   * @return The status log configuration.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public StateLogConfig withDefaultConsistency(String consistency) {
    setDefaultConsistency(consistency);
    return this;
  }

  /**
   * Sets the status log read consistency, returning the configuration for method chaining.
   *
   * @param consistency The status log read consistency.
   * @return The status log configuration.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public StateLogConfig withDefaultConsistency(Consistency consistency) {
    setDefaultConsistency(consistency);
    return this;
  }

}

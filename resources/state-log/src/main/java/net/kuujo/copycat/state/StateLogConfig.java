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

import com.typesafe.config.ConfigValueFactory;
import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.resource.ResourceConfig;
import net.kuujo.copycat.state.internal.SnapshottableLog;
import net.kuujo.copycat.util.internal.Assert;

import java.util.Map;

/**
 * State log configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateLogConfig extends ResourceConfig<StateLogConfig> {
  private static final String STATE_LOG_CONSISTENCY = "consistency";

  private static final String DEFAULT_CONFIGURATION = "event-log-defaults";
  private static final String CONFIGURATION = "event-log";

  public StateLogConfig() {
    super(CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public StateLogConfig(Map<String, Object> config) {
    super(config, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public StateLogConfig(String resource) {
    super(resource, CONFIGURATION, DEFAULT_CONFIGURATION);
    setDefaultName(resource);
  }

  protected StateLogConfig(String... resources) {
    super(addResources(resources, CONFIGURATION, DEFAULT_CONFIGURATION));
  }

  protected StateLogConfig(Map<String, Object> config, String... resources) {
    super(config, addResources(resources, CONFIGURATION, DEFAULT_CONFIGURATION));
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
    this.config = config.withValue(STATE_LOG_CONSISTENCY, ConfigValueFactory.fromAnyRef(Consistency.parse(Assert.isNotNull(consistency, "consistency")).toString()));
  }

  /**
   * Sets the status log read consistency.
   *
   * @param consistency The status log read consistency.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public void setDefaultConsistency(Consistency consistency) {
    this.config = config.withValue(STATE_LOG_CONSISTENCY, ConfigValueFactory.fromAnyRef(Assert.isNotNull(consistency, "consistency").toString()));
  }

  /**
   * Returns the status log read consistency.
   *
   * @return The status log read consistency.
   */
  public Consistency getDefaultConsistency() {
    return Consistency.parse(config.getString(STATE_LOG_CONSISTENCY));
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

  @Override
  public ResourceConfig<?> resolve() {
    super.resolve();
    return withLog(new SnapshottableLog(getLog()));
  }

}

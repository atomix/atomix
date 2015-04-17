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
import net.kuujo.copycat.resource.PartitionedResourceConfig;
import net.kuujo.copycat.resource.ResourceConfig;

/**
 * State machine configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateMachineConfig extends StateLogConfig {

  public StateMachineConfig() {
  }

  public StateMachineConfig(ResourceConfig<?> config) {
    super(config);
  }

  public StateMachineConfig(PartitionedResourceConfig<?> config) {
    super(config);
  }

  protected StateMachineConfig(StateMachineConfig config) {
    super(config);
  }

  @Override
  public StateMachineConfig copy() {
    return new StateMachineConfig(this);
  }

  @Override
  public StateMachineConfig withDefaultConsistency(Consistency consistency) {
    super.setDefaultConsistency(consistency);
    return this;
  }

}

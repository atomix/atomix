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
package net.kuujo.copycat.atomic;

import net.kuujo.copycat.atomic.internal.LongState;
import net.kuujo.copycat.resource.ResourceConfig;
import net.kuujo.copycat.state.StateMachineConfig;

import java.util.Map;

/**
 * Asynchronous atomic long configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncLongConfig extends ResourceConfig<AsyncLongConfig> {
  private static final String DEFAULT_CONFIGURATION = "atomic-defaults";
  private static final String CONFIGURATION = "atomic";

  public AsyncLongConfig() {
    super(CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public AsyncLongConfig(Map<String, Object> config) {
    super(config, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public AsyncLongConfig(String resource) {
    super(resource, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  protected AsyncLongConfig(AsyncLongConfig config) {
    super(config);
  }

  @Override
  public AsyncLongConfig copy() {
    return new AsyncLongConfig(this);
  }

  @Override
  public ResourceConfig<?> resolve() {
    return new StateMachineConfig(toMap())
      .withStateType(LongState.class)
      .withInitialState(LongState.class);
  }

}

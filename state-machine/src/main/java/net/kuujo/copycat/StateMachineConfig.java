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

import com.typesafe.config.Config;

import java.util.Map;

/**
 * State machine configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateMachineConfig extends StateLogConfig {

  public StateMachineConfig() {
    super();
  }

  public StateMachineConfig(String resource) {
    super(resource);
  }

  public StateMachineConfig(Map<String, Object> config) {
    super(config);
  }

  public StateMachineConfig(Config config) {
    super(config);
  }

}

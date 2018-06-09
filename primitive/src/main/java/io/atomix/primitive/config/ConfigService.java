/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.primitive.config;

/**
 * Configuration service.
 */
public interface ConfigService {

  /**
   * Returns the registered configuration for the given primitive.
   *
   * @param primitiveName the primitive name
   * @param <C>           the configuration type
   * @return the primitive configuration
   */
  <C extends PrimitiveConfig<C>> C getConfig(String primitiveName);

  /**
   * Adds a primitive configuration.
   * <p>
   * If a configuration is already registered it will not be overridden. The returned configuration represents the
   * configuration that will be used when constructing a new primitive with the given name.
   *
   * @param config the configuration to add
   * @return the registered primitive configuration
   */
  PrimitiveConfig addConfig(PrimitiveConfig config);

}
